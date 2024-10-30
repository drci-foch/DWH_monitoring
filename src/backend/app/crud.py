import asyncio
import logging
import time
import pandas as pd
from collections import defaultdict, Counter
from datetime import datetime
from functools import lru_cache, wraps
from sqlalchemy import text
from typing import Dict, List, Any, Set, Tuple
import multiprocessing as mp

import numpy as np
from dateutil.relativedelta import relativedelta
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

def process_large_array(data):
    """Process entire dataset using vectorized operations"""
    if not data:
        return {}
    
    # Convert to structured array in one go
    dtype = [('origin', 'U50'), ('doc_num', 'U50')]
    arr = np.array(data, dtype=dtype)
    
    # Pre-compute string operations once for entire dataset
    origins = arr['origin']
    docs = arr['doc_num']
    
    # Vectorized string replacements
    mask_easily = np.char.startswith(origins, "Easily")
    mask_doc_externe = np.char.startswith(origins, "DOC_EXTERNE")
    
    # Create modified origins array
    origins = origins.copy()
    origins[mask_easily] = "Easily"
    origins[mask_doc_externe] = "DOC_EXTERNE"
    
    # Stack arrays for unique combination counting
    stacked = np.column_stack((origins, docs))
    
    # Get unique combinations efficiently
    unique_combinations = np.unique(stacked, axis=0)
    
    # Count unique origins
    unique_origins, counts = np.unique(unique_combinations[:, 0], return_counts=True)
    
    return dict(zip(unique_origins, counts))

def process_chunk(chunk_data):
    """Process a single chunk of data in a separate process"""
    if not chunk_data:
        return None
    
    # Convert to structured array for efficient processing
    dtype = [('origin', 'U50'), ('doc_num', 'U50')]
    chunk_array = np.array(chunk_data, dtype=dtype)
    
    # Vectorized string operations
    mask_easily = np.char.startswith(chunk_array['origin'], "Easily")
    mask_doc_externe = np.char.startswith(chunk_array['origin'], "DOC_EXTERNE")
    
    # Modify origins
    modified_origins = chunk_array['origin'].copy()
    modified_origins[mask_easily] = "Easily"
    modified_origins[mask_doc_externe] = "DOC_EXTERNE"
    
    # Create unique pairs using structured array
    unique_pairs = np.unique(
        np.core.records.fromarrays(
            [modified_origins, chunk_array['doc_num']],
            names='origin,doc_num'
        )
    )
    
    # Count unique combinations
    origins, counts = np.unique(unique_pairs['origin'], return_counts=True)
    return dict(zip(origins, counts))

def cache_with_ttl(seconds: int):
    """Custom TTL cache decorator using timestamp checking"""
    def decorator(func):
        # Cache the function with standard LRU
        func = lru_cache(maxsize=1)(func)
        # Store the timestamp of last execution
        func.last_execution = 0
        
        def wrapper(*args, **kwargs):
            now = datetime.now().timestamp()
            # Check if cache has expired
            if now - func.last_execution > seconds:
                func.cache_clear()
                func.last_execution = now
            return func(*args, **kwargs)
        return wrapper
    return decorator


def ttl_cache(ttl_seconds):
    def decorator(func):
        cached_result = None
        last_update = None

        async def wrapper(*args, **kwargs):
            nonlocal cached_result, last_update
            now = datetime.now()
            if (cached_result is None or 
                last_update is None or 
                (now - last_update).total_seconds() > ttl_seconds):
                cached_result = await func(*args, **kwargs)
                last_update = now
            return cached_result
        return wrapper
    return decorator


class DatabaseQualityChecker:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.async_session = sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        self.logger = logging.getLogger(__name__)
        self.num_processes = mp.cpu_count() - 1  # Leave one CPU free
        self.process_pool = ProcessPoolExecutor(max_workers=self.num_processes)




    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Tuple]:
        async with self.async_session() as session:
            try:
                result = await session.execute(text(query), params or {})
                return result.fetchall()
            except SQLAlchemyError as e:
                self.logger.error(f"Error executing query: {e}")
                return []

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_document_origins(self) -> List[str]:
        """Cache document origins as they don't change often"""
        query = """
        SELECT /*+ PARALLEL(8) INDEX_FFS(d IDX_DOCUMENT_ORIGIN_CODE) */
            DISTINCT DOCUMENT_ORIGIN_CODE
        FROM DWH.DWH_DOCUMENT d
        """
        result = await self.execute_query(query)
        return [row[0] for row in result]

    @ttl_cache(ttl_seconds=3600)  # Cache for 1 hour
    async def get_patient_counts(self) -> Dict[str, int]:
        """Optimized patient count query with Python-side aggregation"""
        query = """
        SELECT DISTINCT
            d.PATIENT_NUM,
            p.LASTNAME
        FROM 
            DWH.DWH_DOCUMENT d, DWH.DWH_PATIENT p
        WHERE 
            d.PATIENT_NUM = p.PATIENT_NUM
        """

        try:
            results = await self.execute_query(query)

            # Initialize counters
            counts = defaultdict(int)
            total_count = 0

            # Single pass through results to count everything
            for _, lastname in results:
                total_count += 1
                if lastname in ("TEST", "INSECTE", "FLEUR"):
                    counts[lastname] += 1

            return {
                "patient_count": total_count,
                "test_patient_count": counts["TEST"],
                "celebrity_patient_count": counts["INSECTE"],
                "research_patient_count": counts["FLEUR"],
            }

        except Exception as e:
            logger.error(f"Error getting patient counts: {str(e)}", exc_info=True)
            return {
                "patient_count": 0,
                "test_patient_count": 0,
                "celebrity_patient_count": 0,
                "research_patient_count": 0,
            }

    @lru_cache(maxsize=1)
    def _get_cache_key(self):
        """Generate cache key based on current timestamp divided by TTL"""
        return int(datetime.now().timestamp() / 300)  # 300 seconds = 5 minutes
        
    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_document_counts(self) -> List[Dict[str, Any]]:
        """Efficient document count processing using pre-aggregated SQL data"""
        query = """
        SELECT /*+ PARALLEL(8) */
            DOCUMENT_ORIGIN_CODE,
            COUNT(DOCUMENT_NUM) as DOC_COUNT
        FROM DWH.DWH_DOCUMENT
        GROUP BY DOCUMENT_ORIGIN_CODE
        """
        try:
            results = await self.execute_query(query)
            
            # Simple dictionary-based grouping without numpy
            grouped_data = {}
            
            for row in results:
                origin, count = row[0], int(row[1])  # Explicit conversion to int
                
                # Group by prefix
                if origin and isinstance(origin, str):
                    if origin.startswith('Easily'):
                        key = 'Easily'
                    elif origin.startswith('DOC_EXTERNE'):
                        key = 'DOC_EXTERNE'
                    else:
                        key = origin
                    
                    grouped_data[key] = grouped_data.get(key, 0) + count
            
            # Sort and format results
            result = [
                {
                    "document_origin_code": origin,
                    "unique_document_count": count
                }
                for origin, count in sorted(
                    grouped_data.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
            ]
            
            logger.debug(f"Processed {len(results)} rows into {len(result)} grouped results")
            return result

        except Exception as e:
            logger.error(f"Error getting document counts: {str(e)}", exc_info=True)
            logger.debug("Raw results sample:", str(results[:5]) if results else "No results")
            return []

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_recent_document_counts(self) -> List[Dict[str, Any]]:
        """Vectorized recent document counts using numpy"""
        query = """
            SELECT /*+ PARALLEL(4) */
                d.DOCUMENT_ORIGIN_CODE,
                COUNT(DISTINCT d.DOCUMENT_NUM) as DOC_COUNT
            FROM DWH.DWH_DOCUMENT d
            WHERE TRUNC(d.DOCUMENT_DATE) >= TRUNC(SYSDATE - 7)
            AND TRUNC(d.DOCUMENT_DATE) <= TRUNC(SYSDATE)
            GROUP BY d.DOCUMENT_ORIGIN_CODE
        """

        try:
            results = await self.execute_query(query)

            if not results:
                return []

            # Convert to numpy arrays
            origins = np.array([r[0] for r in results])
            counts = np.array([r[1] for r in results])

            # Create masks for each condition
            easily_mask = np.char.startswith(origins.astype(str), "Easily")
            doc_externe_mask = np.char.startswith(origins.astype(str), "DOC_EXTERNE")

            # Create grouped origins array
            grouped_origins = origins.copy()
            grouped_origins[easily_mask] = "Easily"
            grouped_origins[doc_externe_mask] = "DOC_EXTERNE"

            # Get unique origins and their indices
            unique_origins, inverse_indices = np.unique(
                grouped_origins, return_inverse=True
            )

            # Sum counts for each unique origin
            summed_counts = np.zeros(len(unique_origins))
            np.add.at(summed_counts, inverse_indices, counts)

            # Sort by counts descending
            sort_indices = np.argsort(-summed_counts)
            sorted_origins = unique_origins[sort_indices]
            sorted_counts = summed_counts[sort_indices]

            # Convert to final format
            return [
                {"document_origin_code": origin, "unique_document_count": int(count)}
                for origin, count in zip(sorted_origins, sorted_counts)
            ]

        except Exception as e:
            logger.error(
                f"Error getting recent document counts: {str(e)}", exc_info=True
            )
            return []

    @staticmethod
    @lru_cache
    def get_codoc_users() -> Set[str]:
        return {
            "admin admin",
            "admin2 admin2",
            "Demo Nicolas",
            "ADMIN_ANONYM",
            "Fannie Lothaire",
            "Nicolas Garcelon",
            "codon admin",
            "codoc support",
            "Virgin Bitton",
            "Gabriel Silva",
            "Margaux Peschiera",
            "Antoine Motte",
            "Paul Montecot",
            "Julien Terver",
            "Thomas Pagoet",
            "Sofia Houriez--Gombaud-Saintonge",
            "Roxanne Schmidt",
            "Phillipe Fernandez",
            "Tanguy De Poix",
            "Charlotte Monthéan",
        }

    async def get_top_users(self, current_year: bool = False) -> List[Dict[str, Any]]:
        """Get top users with simplified query and Python-side processing"""
        # Simplified SQL query that just gets the basic user data
        query = """
        SELECT /*+ PARALLEL(4) */
            u.FIRSTNAME,
            u.LASTNAME,
            COUNT(*) as QUERY_COUNT
        FROM DWH.DWH_LOG_QUERY l
        JOIN DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
        {where_clause}
        GROUP BY u.FIRSTNAME, u.LASTNAME
        """

        where_clause = (
            "WHERE EXTRACT(YEAR FROM l.LOG_DATE) = EXTRACT(YEAR FROM SYSDATE)"
            if current_year
            else ""
        )
        results = await self.execute_query(
            query=query.format(where_clause=where_clause)
        )

        # Process results in Python
        user_stats = {}
        codoc_users = self.get_codoc_users()

        # Aggregate results
        for firstname, lastname, query_count in results:
            full_name = f"{firstname} {lastname}"

            if full_name in codoc_users:
                # Aggregate CODOC users
                key = ("CODOC", "CODOC")
                user_stats[key] = user_stats.get(key, 0) + query_count
            else:
                # Keep regular users as is
                key = (firstname, lastname)
                user_stats[key] = query_count

        # Sort and get top 10
        top_users = sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:10]

        # Format results
        return [
            {"firstname": firstname, "lastname": lastname, "query_count": query_count}
            for (firstname, lastname), query_count in top_users
        ]


    async def get_users_stats(self, current_year: bool = False) -> Dict[str, Any]:
        """Get extended user statistics including low activity users."""
        query = """
        WITH user_stats AS (
            SELECT /*+ PARALLEL(4) */
                u.FIRSTNAME,
                u.LASTNAME,
                COUNT(*) as QUERY_COUNT
            FROM DWH.DWH_LOG_QUERY l
            JOIN DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
            {where_clause}
            GROUP BY u.FIRSTNAME, u.LASTNAME
        )
        SELECT
            COUNT(CASE WHEN QUERY_COUNT <= 3 THEN 1 END) as LOW_ACTIVITY_USERS,
            COUNT(*) as TOTAL_USERS,
            AVG(QUERY_COUNT) as AVG_QUERIES,
            MAX(QUERY_COUNT) as MAX_QUERIES,
            MIN(QUERY_COUNT) as MIN_QUERIES,
            LISTAGG(CASE WHEN QUERY_COUNT <= 3 
                    THEN FIRSTNAME || ' ' || LASTNAME || ':' || QUERY_COUNT 
                    END, '|') 
            WITHIN GROUP (ORDER BY QUERY_COUNT) as LOW_ACTIVITY_DETAILS
        FROM user_stats
        """
        
        where_clause = (
            "WHERE EXTRACT(YEAR FROM l.LOG_DATE) = EXTRACT(YEAR FROM SYSDATE)"
            if current_year
            else ""
        )
        
        results = await self.execute_query(query.format(where_clause=where_clause))
        if not results:
            return {}
            
        low_activity, total, avg, max_q, min_q, details = results[0]
        
        # Process low activity users details
        low_activity_users = []
        if details:
            for user_detail in details.split('|'):
                if user_detail:
                    name, count = user_detail.rsplit(':', 1)
                    low_activity_users.append({
                        "name": name,
                        "count": int(count)
                    })
        
        return {
            "low_activity_users_count": int(low_activity),
            "total_users": int(total),
            "avg_queries": float(avg),
            "max_queries": int(max_q),
            "min_queries": int(min_q),
            "low_activity_percentage": (low_activity / total * 100) if total else 0,
            "low_activity_details": low_activity_users
        }

    @ttl_cache(ttl_seconds=3600)  # Cache for 1 hour
    async def get_document_metrics(self) -> Dict[str, float]:
        """Optimized document metrics query with Python-side calculations"""
        query = """
        SELECT /*+ PARALLEL(4) */
            ROUND(UPDATE_DATE - DOCUMENT_DATE, 2) AS DELAY_DAYS
        FROM DWH.DWH_DOCUMENT
        WHERE
            UPDATE_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)
            AND DOCUMENT_ORIGIN_CODE != 'RDV_DOCTOLIB'
            AND UPDATE_DATE IS NOT NULL
            AND DOCUMENT_DATE IS NOT NULL
        """
        try:
            results = await self.execute_query(query)
            # Convert Decimal to float during array creation
            delays = np.array([float(row[0]) for row in results if row[0] is not None])
            
            if len(delays) == 0:
                logger.warning("No valid document delay data found")
                return {}
                
            metrics = {
                "min_delay": float(np.min(delays)),
                "q1": float(np.percentile(delays, 25)),
                "median": float(np.percentile(delays, 50)),
                "q3": float(np.percentile(delays, 75)),
                "max_delay": float(np.max(delays)),
                "avg_delay": float(np.round(np.mean(delays), 2)),
            }
            
            logger.debug(f"Calculated metrics from {len(delays)} documents: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating document metrics: {str(e)}", exc_info=True)
            return {}@cache_with_ttl(seconds=300)
        



    @cache_with_ttl(seconds=300)
    async def get_archive_status(self) -> Dict[str, Any]:
        """High-performance archive status calculation with pre-aggregated data"""
        query = """
        WITH stats AS (
            SELECT /*+ PARALLEL(8) */
                MIN(DOCUMENT_DATE) as MIN_GLOBAL_DATE
            FROM DWH.DWH_DOCUMENT
            WHERE DOCUMENT_DATE IS NOT NULL
        ),
        date_stats AS (
            SELECT 
                TRUNC(d.DOCUMENT_DATE, 'MONTH') as MONTH_DATE,
                d.DOCUMENT_ORIGIN_CODE,
                COUNT(*) as DOC_COUNT,
                s.MIN_GLOBAL_DATE
            FROM DWH.DWH_DOCUMENT d
            CROSS JOIN stats s
            WHERE d.DOCUMENT_DATE IS NOT NULL
            GROUP BY 
                TRUNC(d.DOCUMENT_DATE, 'MONTH'),
                d.DOCUMENT_ORIGIN_CODE,
                s.MIN_GLOBAL_DATE
        )
        SELECT 
            CAST(MONTH_DATE AS TIMESTAMP) as MONTH_DATE,
            DOCUMENT_ORIGIN_CODE,
            DOC_COUNT,
            CAST(MIN_GLOBAL_DATE AS TIMESTAMP) as MIN_GLOBAL_DATE
        FROM date_stats
        """
        
        try:
            # Pre-calculate cutoff date
            current_date = datetime.now()
            cutoff_date = current_date - relativedelta(months=240)
            
            # Fetch pre-aggregated results
            results = await self.execute_query(query)
            
            # Early return if no results
            if not results:
                return {
                    "archive_period": 0,
                    "total_documents_to_suppress": 0,
                    "documents_to_suppress": []
                }
            
            # Convert results to numpy arrays
            dates = np.array([row[0] for row in results], dtype='datetime64[ns]')
            origins = np.array([str(row[1]) for row in results])
            counts = np.array([int(row[2]) for row in results])
            min_date = np.datetime64(results[0][3])  # Global min date
            
            # Calculate archive period in years
            days_diff = (np.datetime64(current_date) - min_date) / np.timedelta64(1, 'D')
            archive_period = days_diff / 365.25
            
            # Find documents to suppress
            cutoff_ts = np.datetime64(cutoff_date)
            suppress_mask = dates < cutoff_ts
            
            if np.any(suppress_mask):
                # Get unique origins and sum their counts
                unique_origins, indices = np.unique(origins[suppress_mask], return_inverse=True)
                suppress_counts = np.zeros(len(unique_origins), dtype=int)
                np.add.at(suppress_counts, indices, counts[suppress_mask])
                
                # Sort by count in descending order
                sort_idx = np.argsort(-suppress_counts)
                
                # Convert numpy types to Python native types
                documents_to_suppress = [
                    (str(origin), int(count)) 
                    for origin, count in zip(
                        unique_origins[sort_idx],
                        suppress_counts[sort_idx]
                    )
                ]
                total_to_suppress = int(np.sum(suppress_counts))
            else:
                documents_to_suppress = []
                total_to_suppress = 0
            
            return {
                "archive_period": round(float(archive_period), 2),
                "total_documents_to_suppress": int(total_to_suppress),
                "documents_to_suppress": documents_to_suppress
            }
                
        except Exception as e:
            logger.error(f"Error getting archive status: {str(e)}", exc_info=True)
            logger.debug("Exception details:", exc_info=True)
            return {
                "archive_period": 0,
                "total_documents_to_suppress": 0,
                "documents_to_suppress": []
            }


    @staticmethod
    def extract_year(dates: np.ndarray) -> np.ndarray:
        """Extract years from datetime array efficiently"""
        return dates.astype('datetime64[Y]').astype(int) + 1970

    @staticmethod
    def extract_month(dates: np.ndarray) -> np.ndarray:
        """Extract month start dates from datetime array"""
        return dates.astype('datetime64[M]')


    async def get_document_counts_batch(self, origin_codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """High-performance document counts using pre-aggregation and vectorized operations"""
        placeholders = ', '.join(f':code{i}' for i in range(len(origin_codes)))
        
        # Calculate date bounds
        end_date = datetime.now().replace(day=1) + relativedelta(months=1)
        start_date = end_date - relativedelta(months=12)
        
        yearly_query = f"""
        SELECT /*+ PARALLEL(8) */
            DOCUMENT_ORIGIN_CODE,
            EXTRACT(YEAR FROM UPDATE_DATE) as YEAR,
            COUNT(DISTINCT DOCUMENT_NUM) as DOC_COUNT
        FROM DWH.DWH_DOCUMENT
        WHERE DOCUMENT_ORIGIN_CODE IN ({placeholders})
            AND UPDATE_DATE IS NOT NULL
        GROUP BY 
            DOCUMENT_ORIGIN_CODE,
            EXTRACT(YEAR FROM UPDATE_DATE)
        ORDER BY 
            DOCUMENT_ORIGIN_CODE,
            YEAR
        """
        
        monthly_query = f"""
        SELECT /*+ PARALLEL(8) */
            DOCUMENT_ORIGIN_CODE,
            TO_CHAR(TRUNC(DOCUMENT_DATE, 'MM'), 'YYYY-MM-DD') as MONTH_DATE,
            COUNT(DISTINCT DOCUMENT_NUM) as DOC_COUNT
        FROM DWH.DWH_DOCUMENT
        WHERE DOCUMENT_ORIGIN_CODE IN ({placeholders})
            AND DOCUMENT_DATE >= :start_date
            AND DOCUMENT_DATE < :end_date
        GROUP BY 
            DOCUMENT_ORIGIN_CODE,
            TRUNC(DOCUMENT_DATE, 'MM')
        ORDER BY 
            DOCUMENT_ORIGIN_CODE,
            MONTH_DATE
        """
        
        try:
            # Prepare parameters
            params = {
                **{f'code{i}': code for i, code in enumerate(origin_codes)},
                'start_date': start_date,
                'end_date': end_date
            }
            
            # Execute both queries concurrently
            yearly_results, monthly_results = await asyncio.gather(
                self.execute_query(yearly_query, params),
                self.execute_query(monthly_query, params)
            )
            
            result = {"yearly": [], "monthly": []}
            
            # Process yearly data
            if yearly_results:
                # Create arrays separately to handle types correctly
                origins = np.array([row[0] for row in yearly_results])
                years = np.array([int(row[1]) for row in yearly_results])
                counts = np.array([int(row[2]) for row in yearly_results])
                
                result["yearly"] = [
                    {
                        "document_origin_code": str(origin),
                        "year": int(year),
                        "count": int(count)
                    }
                    for origin, year, count in zip(origins, years, counts)
                ]
            
            # Process monthly data with string dates from SQL
            if monthly_results:
                monthly_data = np.array([
                    (str(row[0]), str(row[1]), int(row[2]))
                    for row in monthly_results if row[2] > 0  # Only include months with documents
                ], dtype=[('origin', 'U50'), ('month', 'U10'), ('count', 'i4')])
                
                result["monthly"] = [
                    {
                        "document_origin_code": str(origin),
                        "month": month,  # Already in YYYY-MM-DD format from SQL
                        "count": int(count)
                    }
                    for origin, month, count in zip(
                        monthly_data['origin'],
                        monthly_data['month'],
                        monthly_data['count']
                    )
                ]
            
            return result
                
        except Exception as e:
            logger.error(f"Error getting batch document counts: {str(e)}", exc_info=True)
            logger.debug("Exception details:", exc_info=True)
            return {"yearly": [], "monthly": []}

    async def get_pmsi(self) -> Dict[str, Any]:
        """Get PMSI upload and document creation statistics."""
        
        queries = {
            "last_upload": """
                SELECT /*+ PARALLEL(8) INDEX_FFS(d IDX_DOCUMENT_UPDATE_DATE) */ 
                MAX(UPDATE_DATE) as LAST_UPLOAD
                FROM DWH.DWH_DOCUMENT d
                WHERE DOCUMENT_ORIGIN_CODE = 'PMSI_MCO'
            """,
            
            "time_period": """
                SELECT /*+ PARALLEL(8) INDEX_FFS(d IDX_DOCUMENT_DATE) */
                MIN(DOCUMENT_DATE) as START_DATE,
                MAX(DOCUMENT_DATE) as END_DATE,
                COUNT(DISTINCT TRUNC(DOCUMENT_DATE, 'MM')) as MONTHS_COUNT
                FROM DWH.DWH_DOCUMENT d
                WHERE DOCUMENT_ORIGIN_CODE = 'PMSI_MCO'
                AND DOCUMENT_DATE IS NOT NULL
            """,
            
            "monthly_data": """
                SELECT /*+ PARALLEL(8) INDEX_FFS(d IDX_DOCUMENT_DATE) */
                TRUNC(DOCUMENT_DATE, 'MM') as MONTH_DATE,
                COUNT(*) as DOC_COUNT
                FROM DWH.DWH_DOCUMENT d
                WHERE DOCUMENT_ORIGIN_CODE = 'PMSI_MCO'
                AND DOCUMENT_DATE >= ADD_MONTHS(SYSDATE, -24)
                GROUP BY TRUNC(DOCUMENT_DATE, 'MM')
                ORDER BY MONTH_DATE
            """
        }
        
        try:
            last_upload, time_period, monthly_data = await asyncio.gather(
                *(self.execute_query(query) for query in queries.values())
            )

            if not any([last_upload, time_period, monthly_data]):
                return self._get_empty_pmsi_result()

            # Convert to numpy arrays for fast processing
            if monthly_data:
                dates = np.array([row[0] for row in monthly_data])
                dates = dates.astype('datetime64[M]')
                counts = np.array([row[1] for row in monthly_data], dtype=np.int32)
                
                # Generate all months in range
                start_month = dates.min()
                end_month = dates.max()
                date_range = np.arange(start_month, end_month + 1, dtype='datetime64[M]')
                
                # Find gaps using month precision
                gaps = date_range[~np.isin(date_range, dates)]

                # Calculate statistics
                stats = {
                    "total_documents": int(np.sum(counts)),
                    "avg_monthly_documents": float(np.mean(counts)),
                    "max_monthly_documents": int(np.max(counts)),
                    "months_with_gaps": len(gaps)
                }

                monthly_counts = [
                    {"month": pd.Timestamp(d).strftime('%Y-%m-%d'), "count": int(c)}
                    for d, c in zip(dates, counts)
                ]
            else:
                gaps = []
                monthly_counts = []
                stats = self._get_empty_pmsi_result()["stats"]

            return {
                "last_upload": last_upload[0][0].strftime('%Y-%m-%d %H:%M:%S') if last_upload and last_upload[0][0] else None,
                "time_period": {
                    "start_date": time_period[0][0].strftime('%Y-%m-%d') if time_period and time_period[0][0] else None,
                    "end_date": time_period[0][1].strftime('%Y-%m-%d') if time_period and time_period[0][1] else None,
                    "months_count": int(time_period[0][2]) if time_period and time_period[0][2] else 0
                },
                "monthly_counts": monthly_counts,
                "gaps": [pd.Timestamp(g).strftime('%Y-%m-%d') for g in gaps],
                "stats": stats
            }
            
        except Exception as e:
            logger.error(f"Error getting PMSI statistics: {str(e)}", exc_info=True)
            return self._get_empty_pmsi_result()

    def _get_empty_pmsi_result(self) -> Dict[str, Any]:
        return {
            "last_upload": None,
            "time_period": {"start_date": None, "end_date": None, "months_count": 0},
            "monthly_counts": [],
            "gaps": [],
            "stats": {
                "total_documents": 0,
                "avg_monthly_documents": 0,
                "max_monthly_documents": 0,
                "months_with_gaps": 0
            }
        }

    async def get_all_statistics_with_timing(self) -> Dict[str, Any]:
        """Version with timing information for performance monitoring"""
        import time
        
        start_time = time.time()
        timings = {}
        
        try:
            # Get origins
            origin_start = time.time()
            origins = await self.get_document_origins()
            timings['origins'] = time.time() - origin_start
            
            # Execute main tasks
            tasks_start = time.time()
            tasks = [
                self.get_patient_counts(),
                self.get_document_counts(),
                self.get_recent_document_counts(),
                self.get_top_users(),
                self.get_top_users(current_year=True),
                self.get_document_metrics(),
                self.get_archive_status(),
                self.get_document_counts_batch(origins),
            ]
            
            results = await asyncio.gather(*tasks)
            timings['main_tasks'] = time.time() - tasks_start
            
            # Extract batch results
            document_counts_batch = results[7]
            
            stats = {
                **results[0],
                "document_counts": results[1],
                "recent_document_counts": results[2],
                "top_users": results[3],
                "top_users_current_year": results[4],
                "document_metrics": results[5],
                "archive_status": results[6],
                "document_origins": origins,
                "document_counts_by_year": document_counts_batch["yearly"],
                "recent_document_counts_by_month": document_counts_batch["monthly"],
            }
            
            total_time = time.time() - start_time
            timings['total'] = total_time
            
            return {
                **stats,
                "_timing": timings
            }
            
        except Exception as e:
            logger.error(f"Error gathering statistics with timing: {str(e)}", exc_info=True)
            return {
                "patient_count": 0,
                "test_patient_count": 0,
                "celebrity_patient_count": 0,
                "research_patient_count": 0,
                "document_counts": [],
                "recent_document_counts": [],
                "top_users": [],
                "top_users_current_year": [],
                "document_metrics": {},
                "archive_status": {},
                "document_origins": origins,
                "document_counts_by_year": [],
                "recent_document_counts_by_month": [],
                "_timing": {
                    "error": str(e),
                    "total": time.time() - start_time
                }
            }


# Usage example
if __name__ == "__main__":
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine
    from app.core.config import settings

    async def main():
        engine = create_async_engine(settings.SQLALCHEMY_DATABASE_URI)
        db_checker = DatabaseQualityChecker(engine)
        all_stats = await db_checker.get_all_statistics()
        print(all_stats)

    asyncio.run(main())
