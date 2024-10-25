import asyncio
import logging
import time
from collections import defaultdict, Counter
from datetime import datetime
from functools import lru_cache, wraps
from sqlalchemy import text
from typing import Dict, List, Any, Set, Tuple

import numpy as np
from dateutil.relativedelta import relativedelta
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


def ttl_cache(ttl_seconds=3600, maxsize=128):
    def decorator(func):
        cache = {}

        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            current_time = time.time()
            if key in cache:
                result, timestamp = cache[key]
                if current_time - timestamp < ttl_seconds:
                    return result
            result = await func(*args, **kwargs)
            cache[key] = (result, current_time)
            if len(cache) > maxsize:
                oldest_key = min(cache, key=lambda k: cache[k][1])
                del cache[oldest_key]
            return result

        return wrapper

    return decorator


class DatabaseQualityChecker:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.async_session = sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        self.logger = logging.getLogger(__name__)

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
        SELECT /*+ INDEX(d PK_DWH_DOCUMENT) */ 
        DISTINCT DOCUMENT_ORIGIN_CODE 
        FROM DWH.DWH_DOCUMENT d
        """
        result = await self.execute_query(query)
        return [row[0] for row in result]

    @ttl_cache(ttl_seconds=3600)  # Cache for 1 hour
    async def get_patient_counts(self) -> Dict[str, int]:
        """Optimized patient count query with Python-side aggregation"""
        query = """
        SELECT /*+ INDEX(p PK_DWH_PATIENT) */
            DISTINCT PATIENT_NUM, LASTNAME
        FROM DWH.DWH_PATIENT p
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

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_document_counts(self) -> List[Dict[str, Any]]:
        """Optimized document counts using numpy for vectorized operations"""
        query = """
        SELECT /*+ PARALLEL(4) */
            DOCUMENT_ORIGIN_CODE,
            DOCUMENT_NUM
        FROM DWH.DWH_DOCUMENT
        """

        try:
            results = await self.execute_query(query)

            # Convert to numpy arrays
            origins, doc_nums = np.array(results).T

            # Create masks for each condition
            easily_mask = np.char.startswith(origins.astype(str), "Easily")
            doc_externe_mask = np.char.startswith(origins.astype(str), "DOC_EXTERNE")

            # Apply masks to create grouped origins array
            grouped_origins = origins.copy()
            grouped_origins[easily_mask] = "Easily"
            grouped_origins[doc_externe_mask] = "DOC_EXTERNE"

            # Create unique combinations of grouped origins and doc numbers
            unique_combinations = np.unique(
                np.column_stack((grouped_origins, doc_nums)), axis=0
            )

            # Count unique documents per origin
            counts = Counter(unique_combinations[:, 0])

            # Convert to sorted list of dictionaries
            result = [
                {"document_origin_code": origin, "unique_document_count": count}
                for origin, count in counts.most_common()
            ]

            return result

        except Exception as e:
            logger.error(f"Error getting document counts: {str(e)}", exc_info=True)
            return []

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_recent_document_counts(self) -> List[Dict[str, Any]]:
        """Vectorized recent document counts using numpy"""
        query = """
        SELECT /*+ PARALLEL(4) */
            d.DOCUMENT_ORIGIN_CODE,
            COUNT(DISTINCT d.DOCUMENT_NUM) as DOC_COUNT
        FROM DWH.DWH_DOCUMENT PARTITION(
            FOR(TRUNC(SYSDATE)) 
            FOR(TRUNC(SYSDATE)-1)
            FOR(TRUNC(SYSDATE)-2)
            FOR(TRUNC(SYSDATE)-3)
            FOR(TRUNC(SYSDATE)-4)
            FOR(TRUNC(SYSDATE)-5)
            FOR(TRUNC(SYSDATE)-6)
            FOR(TRUNC(SYSDATE)-7)
        ) d
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

    @ttl_cache(ttl_seconds=3600)  # Cache for 1 hour
    async def get_document_metrics(self) -> Dict[str, float]:
        """Optimized document metrics query with Python-side calculations"""
        # Simplified query that just gets the delay days
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

            # Convert to numpy array for efficient calculations
            delays = np.array([row[0] for row in results if row[0] is not None])

            if len(delays) == 0:
                return {}

            return {
                "min_delay": float(np.min(delays)),
                "q1": float(np.percentile(delays, 25)),
                "median": float(np.percentile(delays, 50)),
                "q3": float(np.percentile(delays, 75)),
                "max_delay": float(np.max(delays)),
                "avg_delay": float(np.round(np.mean(delays), 2)),
            }

        except Exception as e:
            logger.error(f"Error calculating document metrics: {str(e)}", exc_info=True)
            return {}

    @ttl_cache(ttl_seconds=300)
    async def get_archive_status(self) -> Dict[str, Any]:
        """Chunked processing version for very large datasets"""
        base_query = """
        SELECT /*+ PARALLEL(4) INDEX(d PK_DWH_DOCUMENT) */
            d.DOCUMENT_DATE,
            d.DOCUMENT_ORIGIN_CODE,
            COUNT(*) OVER () as total_rows
        FROM DWH.DWH_DOCUMENT d
        WHERE d.DOCUMENT_DATE >= :start_date
        AND d.DOCUMENT_DATE < :end_date
        """

        try:
            # Initialize accumulators
            oldest_date = None
            suppress_counts = {}
            total_to_suppress = 0

            # Calculate date ranges for chunking
            current_date = datetime.now()
            cutoff_date = current_date - relativedelta(months=240)

            # Process in 1-year chunks
            # Start 10 years before cutoff
            chunk_start = cutoff_date - relativedelta(years=10)
            chunk_end = current_date

            while chunk_start < chunk_end:
                next_chunk = chunk_start + relativedelta(years=1)

                # Get chunk of data
                results = await self.execute_query(
                    base_query,
                    {"start_date": chunk_start, "end_date": min(next_chunk, chunk_end)},
                )

                if not results:
                    break

                # Process chunk with numpy
                dates = np.array([r[0] for r in results], dtype="datetime64[s]")
                origins = np.array([r[1] for r in results])

                # Update oldest date
                chunk_min = np.min(dates) if len(dates) > 0 else None
                oldest_date = min(oldest_date, chunk_min) if oldest_date else chunk_min

                # Count documents to suppress
                suppress_mask = dates < np.datetime64(cutoff_date)
                chunk_suppress = np.sum(suppress_mask)
                total_to_suppress += chunk_suppress

                if chunk_suppress > 0:
                    # Count by origin
                    suppress_origins = origins[suppress_mask]
                    unique, counts = np.unique(suppress_origins, return_counts=True)
                    for origin, count in zip(unique, counts):
                        suppress_counts[origin] = suppress_counts.get(origin, 0) + count

                chunk_start = next_chunk

            # Calculate archive period
            archive_period = (
                (current_date - oldest_date).days / 365.25 if oldest_date else 0
            )

            # Sort suppress counts
            documents_to_suppress = sorted(
                suppress_counts.items(), key=lambda x: x[1], reverse=True
            )

            return {
                "archive_period": float(archive_period),
                "total_documents_to_suppress": int(total_to_suppress),
                "documents_to_suppress": documents_to_suppress,
            }

        except Exception as e:
            logger.error(f"Error getting archive status: {str(e)}", exc_info=True)
            return {
                "archive_period": 0,
                "total_documents_to_suppress": 0,
                "documents_to_suppress": [],
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
        """Get both yearly and monthly counts in one query using numpy"""
        placeholders = ', '.join(f':code{i}' for i in range(len(origin_codes)))
        
        # Calculate date bounds
        end_date = datetime.now().replace(day=1) + relativedelta(months=1)
        start_date = end_date - relativedelta(months=12)
        
        query = f"""
        SELECT /*+ PARALLEL(4) INDEX(d PK_DWH_DOCUMENT) */
            d.DOCUMENT_ORIGIN_CODE,
            d.DOCUMENT_NUM,
            d.DOCUMENT_DATE,
            d.UPDATE_DATE
        FROM DWH.DWH_DOCUMENT d
        WHERE d.DOCUMENT_ORIGIN_CODE IN ({placeholders})
        """
        
        try:
            params = {f'code{i}': code for i, code in enumerate(origin_codes)}
            results = await self.execute_query(query, params)
            
            if not results:
                return {"yearly": [], "monthly": []}

            # Convert to numpy arrays
            data = np.array(results, dtype=object)
            origins = data[:, 0]
            doc_nums = data[:, 1]
            doc_dates = np.array(data[:, 2], dtype='datetime64[ns]')
            update_dates = np.array(data[:, 3], dtype='datetime64[ns]')

            # Process yearly counts
            years = self.extract_year(update_dates)
            year_combinations = np.unique(np.array(list(zip(origins, years))), axis=0)
            
            yearly_counts = []
            for origin, year in year_combinations:
                mask = (origins == origin) & (years == year)
                unique_docs = len(np.unique(doc_nums[mask]))
                yearly_counts.append({
                    "document_origin_code": origin,
                    "year": int(year),
                    "count": int(unique_docs)
                })

            # Process monthly counts (only for recent dates)
            recent_mask = (doc_dates >= np.datetime64(start_date)) & (doc_dates < np.datetime64(end_date))
            recent_origins = origins[recent_mask]
            recent_doc_nums = doc_nums[recent_mask]
            recent_dates = doc_dates[recent_mask]

            months = self.extract_month(recent_dates)
            month_combinations = np.unique(np.array(list(zip(recent_origins, months))), axis=0)
            
            monthly_counts = []
            for origin, month in month_combinations:
                mask = (recent_origins == origin) & (months == month)
                unique_docs = len(np.unique(recent_doc_nums[mask]))
                monthly_counts.append({
                    "document_origin_code": origin,
                    "month": np.datetime64(month).astype(datetime).strftime("%Y-%m-%d"),
                    "count": int(unique_docs)
                })

            return {
                "yearly": sorted(yearly_counts, key=lambda x: (x["document_origin_code"], x["year"])),
                "monthly": sorted(monthly_counts, key=lambda x: (x["document_origin_code"], x["month"]))
            }

        except Exception as e:
            logger.error(f"Error getting batch document counts: {str(e)}", exc_info=True)
            return {"yearly": [], "monthly": []}
        
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
