import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
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


def cache_with_ttl(seconds: int):
    """
    A decorator that caches function results with a time-to-live (TTL) using LRU cache.

    This decorator combines Python's built-in LRU cache with a TTL mechanism. When the TTL
    expires, the cache is cleared and the function is re-executed. Different argument
    combinations are cached separately.

    Args:
        seconds (int): The number of seconds to keep the cached result valid.

    Returns:
        callable: A decorated function that implements caching with TTL.

    Example:
        @cache_with_ttl(seconds=60)
        def fetch_data(user_id: int) -> dict:
            return expensive_database_query(user_id)

        # First call - executes function
        result1 = fetch_data(123)
        # Within TTL - returns cached result
        result2 = fetch_data(123)
        # Different args - executes function
        result3 = fetch_data(456)

    Note:
        - Uses lru_cache with maxsize=1 per unique argument combination
        - Thread-safe as it uses Python's built-in caching mechanism
        - Cache is cleared when TTL expires, regardless of access patterns
    """

    def decorator(func):
        func = lru_cache(maxsize=1)(func)
        # Store the timestamp of last execution
        func.last_execution = 0

        def wrapper(*args, **kwargs):
            now = datetime.now().timestamp()
            if now - func.last_execution > seconds:
                func.cache_clear()
                func.last_execution = now
            return func(*args, **kwargs)

        return wrapper

    return decorator


def ttl_cache(ttl_seconds: int):
    """
    A decorator that provides simple TTL caching for async functions.

    Implements a basic caching mechanism that stores only the most recent result
    regardless of input arguments. The cached result is returned until the TTL expires,
    after which the function is re-executed and the cache is updated.

    Args:
        ttl_seconds (int): The number of seconds to keep the cached result valid.

    Returns:
        callable: A decorated async function that implements simple TTL caching.

    Example:
        @ttl_cache(ttl_seconds=60)
        async def fetch_api_data(endpoint: str) -> dict:
            return await make_api_request(endpoint)

        # First call - executes function
        result1 = await fetch_api_data("/users")
        # Within TTL - returns cached result
        result2 = await fetch_api_data("/users")
        # Different args but within TTL - returns same cached result
        result3 = await fetch_api_data("/posts")

    Note:
        - Only caches the most recent result regardless of input arguments
        - Not thread-safe as it uses simple variable storage
        - Suitable for async functions where only the latest result needs to be cached
        - More memory efficient than LRU cache but less flexible
        - Cache is checked and updated based on the TTL duration
    """

    def decorator(func):
        cached_result = None
        last_update = None

        async def wrapper(*args, **kwargs):
            nonlocal cached_result, last_update
            now = datetime.now()
            if (
                cached_result is None
                or last_update is None
                or (now - last_update).total_seconds() > ttl_seconds
            ):
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

    async def execute_query(
        self, query: str, params: Dict[str, Any] = None
    ) -> List[Tuple]:
        async with self.async_session() as session:
            try:
                result = await session.execute(text(query), params or {})
                return result.fetchall()
            except SQLAlchemyError as e:
                self.logger.error(f"Error executing query: {e}")
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
        SELECT /*+ INDEX(p PK_DWH_PATIENT) */
            DISTINCT PATIENT_NUM, LASTNAME
        FROM DWH.DWH_PATIENT p
        """

        try:
            results = await self.execute_query(query)

            counts = defaultdict(int)
            total_count = 0

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

            grouped_data = {}

            for row in results:
                origin, count = row[0], int(row[1])  # Explicit conversion to int

                if origin and isinstance(origin, str):
                    if origin.startswith("Easily"):
                        key = "Easily"
                    elif origin.startswith("DOC_EXTERNE"):
                        key = "DOC_EXTERNE"
                    else:
                        key = origin

                    grouped_data[key] = grouped_data.get(key, 0) + count

            result = [
                {"document_origin_code": origin, "unique_document_count": count}
                for origin, count in sorted(
                    grouped_data.items(), key=lambda x: x[1], reverse=True
                )
            ]

            logger.debug(
                f"Processed {len(results)} rows into {len(result)} grouped results"
            )
            return result

        except Exception as e:
            logger.error(f"Error getting document counts: {str(e)}", exc_info=True)
            logger.debug(
                "Raw results sample:", str(results[:5]) if results else "No results"
            )
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

            origins = np.array([r[0] for r in results])
            counts = np.array([r[1] for r in results])

            easily_mask = np.char.startswith(origins.astype(str), "Easily")
            doc_externe_mask = np.char.startswith(origins.astype(str), "DOC_EXTERNE")

            grouped_origins = origins.copy()
            grouped_origins[easily_mask] = "Easily"
            grouped_origins[doc_externe_mask] = "DOC_EXTERNE"

            unique_origins, inverse_indices = np.unique(
                grouped_origins, return_inverse=True
            )

            summed_counts = np.zeros(len(unique_origins))
            np.add.at(summed_counts, inverse_indices, counts)

            sort_indices = np.argsort(-summed_counts)
            sorted_origins = unique_origins[sort_indices]
            sorted_counts = summed_counts[sort_indices]

            return [
                {"document_origin_code": origin, "unique_document_count": int(count)}
                for origin, count in zip(sorted_origins, sorted_counts)
            ]

        except Exception as e:
            logger.error(
                f"Error getting recent document counts: {str(e)}", exc_info=True
            )
            return []

    async def get_top_users(self, current_year: bool = False) -> List[Dict[str, Any]]:
        """Get top users with simplified query and Python-side processing"""
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

        user_stats = {}
        codoc_users = self.get_codoc_users()

        for firstname, lastname, query_count in results:
            full_name = f"{firstname} {lastname}"

            if full_name in codoc_users:
                key = ("CODOC", "CODOC")
                user_stats[key] = user_stats.get(key, 0) + query_count
            else:
                key = (firstname, lastname)
                user_stats[key] = query_count

        top_users = sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:10]

        return [
            {"firstname": firstname, "lastname": lastname, "query_count": query_count}
            for (firstname, lastname), query_count in top_users
        ]

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
            return {} @ cache_with_ttl(seconds=300)

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
                    "documents_to_suppress": [],
                }

            # Convert results to numpy arrays
            dates = np.array([row[0] for row in results], dtype="datetime64[ns]")
            origins = np.array([str(row[1]) for row in results])
            counts = np.array([int(row[2]) for row in results])
            min_date = np.datetime64(results[0][3])  # Global min date

            # Calculate archive period in years
            days_diff = (np.datetime64(current_date) - min_date) / np.timedelta64(
                1, "D"
            )
            archive_period = days_diff / 365.25

            # Find documents to suppress
            cutoff_ts = np.datetime64(cutoff_date)
            suppress_mask = dates < cutoff_ts

            if np.any(suppress_mask):
                # Get unique origins and sum their counts
                unique_origins, indices = np.unique(
                    origins[suppress_mask], return_inverse=True
                )
                suppress_counts = np.zeros(len(unique_origins), dtype=int)
                np.add.at(suppress_counts, indices, counts[suppress_mask])

                # Sort by count in descending order
                sort_idx = np.argsort(-suppress_counts)

                # Convert numpy types to Python native types
                documents_to_suppress = [
                    (str(origin), int(count))
                    for origin, count in zip(
                        unique_origins[sort_idx], suppress_counts[sort_idx]
                    )
                ]
                total_to_suppress = int(np.sum(suppress_counts))
            else:
                documents_to_suppress = []
                total_to_suppress = 0

            return {
                "archive_period": round(float(archive_period), 2),
                "total_documents_to_suppress": int(total_to_suppress),
                "documents_to_suppress": documents_to_suppress,
            }

        except Exception as e:
            logger.error(f"Error getting archive status: {str(e)}", exc_info=True)
            logger.debug("Exception details:", exc_info=True)
            return {
                "archive_period": 0,
                "total_documents_to_suppress": 0,
                "documents_to_suppress": [],
            }

    async def get_document_counts_batch(
        self, origin_codes: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """High-performance document counts using pre-aggregation and vectorized operations"""
        placeholders = ", ".join(f":code{i}" for i in range(len(origin_codes)))

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
            params = {
                **{f"code{i}": code for i, code in enumerate(origin_codes)},
                "start_date": start_date,
                "end_date": end_date,
            }

            # Execute both queries concurrently
            yearly_results, monthly_results = await asyncio.gather(
                self.execute_query(yearly_query, params),
                self.execute_query(monthly_query, params),
            )

            result = {"yearly": [], "monthly": []}

            if yearly_results:
                origins = np.array([row[0] for row in yearly_results])
                years = np.array([int(row[1]) for row in yearly_results])
                counts = np.array([int(row[2]) for row in yearly_results])

                result["yearly"] = [
                    {
                        "document_origin_code": str(origin),
                        "year": int(year),
                        "count": int(count),
                    }
                    for origin, year, count in zip(origins, years, counts)
                ]

            if monthly_results:
                monthly_data = np.array(
                    [
                        (str(row[0]), str(row[1]), int(row[2]))
                        for row in monthly_results
                        if row[2] > 0  # Only include months with documents
                    ],
                    dtype=[("origin", "U50"), ("month", "U10"), ("count", "i4")],
                )

                result["monthly"] = [
                    {
                        "document_origin_code": str(origin),
                        "month": month,  # Already in YYYY-MM-DD format from SQL
                        "count": int(count),
                    }
                    for origin, month, count in zip(
                        monthly_data["origin"],
                        monthly_data["month"],
                        monthly_data["count"],
                    )
                ]

            return result

        except Exception as e:
            logger.error(
                f"Error getting batch document counts: {str(e)}", exc_info=True
            )
            logger.debug("Exception details:", exc_info=True)
            return {"yearly": [], "monthly": []}

    async def get_pmsi(self) -> List[Dict[str, Any]]:
        # TODO : query for PMSI
        pass

    async def get_all_statistics_with_timing(self) -> Dict[str, Any]:
        """Version with timing information for performance monitoring"""

        start_time = time.time()
        timings = {}

        try:
            # Get origins
            origin_start = time.time()
            origins = await self.get_document_origins()
            timings["origins"] = time.time() - origin_start

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
            timings["main_tasks"] = time.time() - tasks_start

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
            timings["total"] = total_time

            return {**stats, "_timing": timings}

        except Exception as e:
            logger.error(
                f"Error gathering statistics with timing: {str(e)}", exc_info=True
            )
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
                "_timing": {"error": str(e), "total": time.time() - start_time},
            }


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
