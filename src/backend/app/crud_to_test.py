from typing import Dict, List, Any, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
import asyncio
from functools import lru_cache
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
import time
from functools import wraps


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
        """Combined patient count query to reduce database calls"""
        query = """
        SELECT
            SUM(CASE WHEN p.PATIENT_NUM IS NOT NULL THEN 1 ELSE 0 END) as total_count,
            SUM(CASE WHEN p.LASTNAME = 'TEST' THEN 1 ELSE 0 END) as test_count,
            SUM(CASE WHEN p.LASTNAME = 'INSECTE' THEN 1 ELSE 0 END) as celebrity_count,
            SUM(CASE WHEN p.LASTNAME = 'FLEUR' THEN 1 ELSE 0 END) as research_count
        FROM (
            SELECT /*+ INDEX(p PK_DWH_PATIENT) */
            DISTINCT PATIENT_NUM, LASTNAME
            FROM DWH.DWH_PATIENT p
        )
        """
        result = await self.execute_query(query)
        return {
            "patient_count": result[0][0] or 0,
            "test_patient_count": result[0][1] or 0,
            "celebrity_patient_count": result[0][2] or 0,
            "research_patient_count": result[0][3] or 0
        }

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_document_counts(self) -> List[Dict[str, Any]]:
        """Optimized document counts query"""
        query = """
        SELECT /*+ PARALLEL(4) */
            GROUPED_ORIGIN,
            COUNT(*) as TOTAL_UNIQUE_DOCUMENT_COUNT
        FROM (
            SELECT DISTINCT
                CASE
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'Easily%' THEN 'Easily'
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'DOC_EXTERNE%' THEN 'DOC_EXTERNE'
                    ELSE DOCUMENT_ORIGIN_CODE
                END AS GROUPED_ORIGIN,
                DOCUMENT_NUM
            FROM DWH.DWH_DOCUMENT
        )
        GROUP BY GROUPED_ORIGIN
        ORDER BY TOTAL_UNIQUE_DOCUMENT_COUNT DESC
        """
        results = await self.execute_query(query)
        return [{"document_origin_code": row[0], "unique_document_count": row[1]} for row in results]

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_recent_document_counts(self) -> List[Dict[str, Any]]:
        """Optimized recent document counts query"""
        query = """
        SELECT /*+ PARALLEL(4) */
            GROUPED_ORIGIN,
            COUNT(*) as TOTAL_UNIQUE_DOCUMENT_COUNT
        FROM (
            SELECT DISTINCT
                CASE
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'Easily%' THEN 'Easily'
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'DOC_EXTERNE%' THEN 'DOC_EXTERNE'
                    ELSE DOCUMENT_ORIGIN_CODE
                END AS GROUPED_ORIGIN,
                DOCUMENT_NUM
            FROM DWH.DWH_DOCUMENT
            WHERE UPDATE_DATE >= TRUNC(SYSDATE) - 7
        )
        GROUP BY GROUPED_ORIGIN
        ORDER BY TOTAL_UNIQUE_DOCUMENT_COUNT DESC
        """
        results = await self.execute_query(query)
        return [{"document_origin_code": row[0], "unique_document_count": row[1]} for row in results]

    @ttl_cache(ttl_seconds=300)  # Cache for 5 minutes
    async def get_top_users(self, current_year: bool = False) -> List[Dict[str, Any]]:
        """Optimized top users query with materialized CODOC users list"""
        codoc_users_query = """
        WITH CODOC_USERS AS (
            SELECT FIRSTNAME || ' ' || LASTNAME as FULL_NAME
            FROM (
                VALUES
                    ('admin admin'), ('admin2 admin2'), ('Demo Nicolas'),
                    ('ADMIN_ANONYM'), ('Fannie Lothaire'), ('Nicolas Garcelon'),
                    ('codon admin'), ('codoc support'), ('Virgin Bitton'),
                    ('Gabriel Silva'), ('Margaux Peschiera'), ('Antoine Motte'),
                    ('Paul Montecot'), ('Julien Terver'), ('Thomas Pagoet'),
                    ('Sofia Houriez--Gombaud-Saintonge'), ('Roxanne Schmidt'),
                    ('Phillipe Fernandez'), ('Tanguy De Poix'), ('Charlotte Monthéan')
            ) t(FULL_NAME)
        ),
        USER_STATS AS (
            SELECT /*+ PARALLEL(4) */
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN (SELECT FULL_NAME FROM CODOC_USERS)
                    THEN 'CODOC' ELSE u.FIRSTNAME
                END AS FIRSTNAME,
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN (SELECT FULL_NAME FROM CODOC_USERS)
                    THEN 'CODOC' ELSE u.LASTNAME
                END AS LASTNAME,
                COUNT(*) AS QUERY_COUNT
            FROM DWH.DWH_LOG_QUERY l
            JOIN DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
            {where_clause}
            GROUP BY
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN (SELECT FULL_NAME FROM CODOC_USERS)
                    THEN 'CODOC' ELSE u.FIRSTNAME
                END,
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN (SELECT FULL_NAME FROM CODOC_USERS)
                    THEN 'CODOC' ELSE u.LASTNAME
                END
        )
        SELECT FIRSTNAME, LASTNAME, QUERY_COUNT
        FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY QUERY_COUNT DESC) AS RN
            FROM USER_STATS
        )
        WHERE RN <= 10
        """
        where_clause = "WHERE EXTRACT(YEAR FROM l.LOG_DATE) = EXTRACT(YEAR FROM SYSDATE)" if current_year else ""
        results = await self.execute_query(query=codoc_users_query.format(where_clause=where_clause))
        return [{"firstname": row[0], "lastname": row[1], "query_count": row[2]} for row in results]

    @ttl_cache(ttl_seconds=3600)  # Cache for 1 hour
    async def get_document_metrics(self) -> Dict[str, float]:
        """Optimized document metrics query with parallel hint"""
        query = """
        WITH delay_data AS (
            SELECT /*+ PARALLEL(4) */
                ROUND(UPDATE_DATE - DOCUMENT_DATE, 2) AS DELAY_DAYS
            FROM DWH.DWH_DOCUMENT
            WHERE UPDATE_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)
            AND DOCUMENT_ORIGIN_CODE != 'RDV_DOCTOLIB'
            AND UPDATE_DATE IS NOT NULL
            AND DOCUMENT_DATE IS NOT NULL
        )
        SELECT
            MIN(DELAY_DAYS) AS MIN_DELAY,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY DELAY_DAYS) AS Q1,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DELAY_DAYS) AS MEDIAN,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY DELAY_DAYS) AS Q3,
            MAX(DELAY_DAYS) AS MAX_DELAY,
            ROUND(AVG(DELAY_DAYS), 2) AS AVG_DELAY
        FROM delay_data
        """
        result = await self.execute_query(query)
        if result:
            return {
                "min_delay": result[0][0],
                "q1": result[0][1],
                "median": result[0][2],
                "q3": result[0][3],
                "max_delay": result[0][4],
                "avg_delay": result[0][5]
            }
        return {}

    async def get_all_statistics(self) -> Dict[str, Any]:
        """Optimized statistics gathering with concurrent execution"""
        origins = await self.get_document_origins()
        patient_counts = await self.get_patient_counts()
        
        # Group related tasks
        tasks = [
            self.get_document_counts(),
            self.get_recent_document_counts(),
            self.get_top_users(),
            self.get_top_users(current_year=True),
            self.get_document_metrics(),
            self.get_archive_status(),
            self.get_document_counts_by_year(origins),
            self.get_recent_document_counts_by_month(origins)
        ]
        
        results = await asyncio.gather(*tasks)
        
        return {
            **patient_counts,  # Spread the combined patient counts
            "document_counts": results[0],
            "recent_document_counts": results[1],
            "top_users": results[2],
            "top_users_current_year": results[3],
            "document_metrics": results[4],
            "archive_status": results[5],
            "document_origins": origins,
            "document_counts_by_year": results[6],
            "recent_document_counts_by_month": results[7]
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