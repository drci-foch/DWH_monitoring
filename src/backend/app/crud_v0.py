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

    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Tuple]:
        async with self.async_session() as session:
            try:
                result = await session.execute(text(query), params or {})
                return result.fetchall()
            except SQLAlchemyError as e:
                self.logger.error(f"Error executing query: {e}")
                return []

    async def get_patient_count(self) -> int:
        query = "SELECT COUNT(DISTINCT PATIENT_NUM) FROM DWH.DWH_DOCUMENT"
        result = await self.execute_query(query)
        return result[0][0] if result else 0
    
    async def get_test_patient_count(self) -> int:
            query = "SELECT COUNT(DISTINCT PATIENT_NUM)  FROM DWH.DWH_PATIENT WHERE LASTNAME = 'TEST'"
            result = await self.execute_query(query)
            return result[0][0] if result else 0
    
    async def get_celebrity_patient_count(self) ->int:
            query = "SELECT COUNT(DISTINCT PATIENT_NUM)  FROM DWH.DWH_PATIENT WHERE LASTNAME = 'INSECTE'"
            result = await self.execute_query(query)
            return result[0][0] if result else 0

    async def get_research_patient_count(self) ->int:
            query = "SELECT COUNT(DISTINCT PATIENT_NUM)  FROM DWH.DWH_PATIENT WHERE LASTNAME = 'FLEUR'"
            result = await self.execute_query(query)
            return result[0][0] if result else 0


    async def get_document_counts(self) -> List[Dict[str, Any]]:
        query = """
        SELECT 
            GROUPED_ORIGIN,
            SUM(UNIQUE_DOCUMENT_COUNT) as TOTAL_UNIQUE_DOCUMENT_COUNT
        FROM (
            SELECT 
                CASE
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'Easily%' THEN 'Easily'
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'DOC_EXTERNE%' THEN 'DOC_EXTERNE'
                    ELSE DOCUMENT_ORIGIN_CODE
                END AS GROUPED_ORIGIN,
                COUNT(DISTINCT DOCUMENT_NUM) as UNIQUE_DOCUMENT_COUNT
            FROM 
                DWH.DWH_DOCUMENT
            GROUP BY 
                DOCUMENT_ORIGIN_CODE
        )
        GROUP BY 
            GROUPED_ORIGIN
        ORDER BY 
            TOTAL_UNIQUE_DOCUMENT_COUNT DESC
        """
        results = await self.execute_query(query)
        return [{"document_origin_code": row[0], "unique_document_count": row[1]} for row in results]

    async def get_recent_document_counts(self) -> List[Dict[str, Any]]:
        query = """
        SELECT 
            GROUPED_ORIGIN,
            SUM(UNIQUE_DOCUMENT_COUNT) as TOTAL_UNIQUE_DOCUMENT_COUNT
        FROM (
            SELECT 
                CASE
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'Easily%' THEN 'Easily'
                    WHEN DOCUMENT_ORIGIN_CODE LIKE 'DOC_EXTERNE%' THEN 'DOC_EXTERNE'
                    ELSE DOCUMENT_ORIGIN_CODE
                END AS GROUPED_ORIGIN,
                COUNT(DISTINCT DOCUMENT_NUM) as UNIQUE_DOCUMENT_COUNT
            FROM 
                DWH.DWH_DOCUMENT
            WHERE 
                UPDATE_DATE >= SYSDATE - 7
            GROUP BY 
                DOCUMENT_ORIGIN_CODE
        )
        GROUP BY 
            GROUPED_ORIGIN
        ORDER BY 
            TOTAL_UNIQUE_DOCUMENT_COUNT DESC
        """
        results = await self.execute_query(query)
        return [{"document_origin_code": row[0], "unique_document_count": row[1]} for row in results]

    
    async def get_top_users(self, current_year: bool = False) -> List[Dict[str, Any]]:
        codoc_users = (
            "'admin admin', 'admin2 admin2', 'Demo Nicolas', 'ADMIN_ANONYM', 'Fannie Lothaire', "
            "'Nicolas Garcelon', 'codon admin', 'codoc support', 'Virgin Bitton', 'Gabriel Silva', "
            "'Margaux Peschiera', 'Antoine Motte', 'Paul Montecot', 'Julien Terver', 'Thomas Pagoet', "
            "'Sofia Houriez--Gombaud-Saintonge', 'Roxanne Schmidt', 'Phillipe Fernandez', "
            "'Tanguy De Poix', 'Charlotte Monthéan'"
        )

        query = f"""
        WITH user_counts AS (
            SELECT
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN ({codoc_users}) THEN 'CODOC'
                    ELSE u.FIRSTNAME
                END AS FIRSTNAME,
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN ({codoc_users}) THEN 'CODOC'
                    ELSE u.LASTNAME
                END AS LASTNAME,
                COUNT(*) AS QUERY_COUNT
            FROM
                DWH.DWH_LOG_QUERY l
            JOIN
                DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
            {{where_clause}}
            GROUP BY
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN ({codoc_users}) THEN 'CODOC'
                    ELSE u.FIRSTNAME
                END,
                CASE
                    WHEN u.FIRSTNAME || ' ' || u.LASTNAME IN ({codoc_users}) THEN 'CODOC'
                    ELSE u.LASTNAME
                END
        )
        SELECT FIRSTNAME, LASTNAME, QUERY_COUNT
        FROM (
            SELECT FIRSTNAME, LASTNAME, QUERY_COUNT,
                ROW_NUMBER() OVER (ORDER BY QUERY_COUNT DESC) AS RN
            FROM user_counts
        )
        WHERE RN <= 10
        ORDER BY QUERY_COUNT DESC
        """

        where_clause = "WHERE EXTRACT(YEAR FROM l.LOG_DATE) = EXTRACT(YEAR FROM SYSDATE)" if current_year else ""
        results = await self.execute_query(query.format(where_clause=where_clause))

        return [
            {
                "firstname": row[0],
                "lastname": row[1],
                "query_count": row[2]
            }
            for row in results
        ]

    async def get_document_metrics(self) -> Dict[str, float]:
        query = """
        WITH delay_data AS (
            SELECT 
                ROUND(UPDATE_DATE - DOCUMENT_DATE, 2) AS DELAY_DAYS
            FROM DWH.DWH_DOCUMENT
            WHERE UPDATE_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)
            AND DOCUMENT_ORIGIN_CODE != 'RDV_DOCTOLIB'
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

    async def get_archive_status(self) -> Dict[str, Any]:
        archive_period_query = "SELECT MIN(UPDATE_DATE) AS OLDEST_DATE FROM DWH.DWH_DOCUMENT"
        oldest_date = (await self.execute_query(archive_period_query))[0][0]
        archive_period = (datetime.now() - oldest_date).days / 365.25 if oldest_date else 0

        total_to_suppress_query = """
        SELECT COUNT(*)
        FROM DWH.DWH_DOCUMENT
        WHERE UPDATE_DATE < ADD_MONTHS(SYSDATE, -240)
        """
        total_to_suppress = (await self.execute_query(total_to_suppress_query))[0][0]

        documents_to_suppress_query = """
        SELECT 
            DOCUMENT_ORIGIN_CODE,
            COUNT(*) AS DOCUMENTS_TO_SUPPRESS
        FROM 
            DWH.DWH_DOCUMENT
        WHERE 
            UPDATE_DATE < ADD_MONTHS(SYSDATE, -240)
        GROUP BY 
            DOCUMENT_ORIGIN_CODE
        ORDER BY 
            DOCUMENTS_TO_SUPPRESS DESC
        """
        documents_to_suppress = await self.execute_query(documents_to_suppress_query)

        return {
            "archive_period": archive_period,
            "total_documents_to_suppress": total_to_suppress,
            "documents_to_suppress": documents_to_suppress
        }


    async def get_document_counts_by_year(self, origin_codes: List[str]) -> List[Dict[str, Any]]:
        placeholders = ', '.join(f':code{i}' for i in range(len(origin_codes)))
        query = f"""
        SELECT
            DOCUMENT_ORIGIN_CODE,
            EXTRACT(YEAR FROM UPDATE_DATE) AS YEAR,
            COUNT(DISTINCT DOCUMENT_NUM) as DOCUMENT_COUNT
        FROM
            DWH.DWH_DOCUMENT
        WHERE
            DOCUMENT_ORIGIN_CODE IN ({placeholders})
        GROUP BY
            DOCUMENT_ORIGIN_CODE, EXTRACT(YEAR FROM UPDATE_DATE)
        ORDER BY
            DOCUMENT_ORIGIN_CODE, YEAR
        """
        params = {f'code{i}': code for i, code in enumerate(origin_codes)}
        results = await self.execute_query(query, params)
        return [{"document_origin_code": row[0], "year": int(row[1]), "count": row[2]} for row in results]

    async def get_recent_document_counts_by_month(self, origin_codes: List[str]) -> List[Dict[str, Any]]:
        placeholders = ', '.join(f':code{i}' for i in range(len(origin_codes)))
        query = f"""
        SELECT
            DOCUMENT_ORIGIN_CODE,
            TRUNC(DOCUMENT_DATE, 'MM') AS MONTH,
            COUNT(DISTINCT DOCUMENT_NUM) AS DOCUMENT_COUNT
        FROM
            DWH.DWH_DOCUMENT
        WHERE
            DOCUMENT_ORIGIN_CODE IN ({placeholders})
            AND DOCUMENT_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), -11)
            AND DOCUMENT_DATE < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)
        GROUP BY
            DOCUMENT_ORIGIN_CODE, TRUNC(DOCUMENT_DATE, 'MM')
        ORDER BY
            DOCUMENT_ORIGIN_CODE, MONTH
        """
        params = {f'code{i}': code for i, code in enumerate(origin_codes)}
        results = await self.execute_query(query, params)
        return [{"document_origin_code": row[0], "month": row[1].strftime("%Y-%m-%d") if row[1] else None, "count": row[2]} for row in results]
    
    async def get_document_origins(self) -> List[str]:
        query = "SELECT DISTINCT DOCUMENT_ORIGIN_CODE FROM DWH.DWH_DOCUMENT"
        result = await self.execute_query(query)
        return [row[0] for row in result]

    async def get_all_statistics(self) -> Dict[str, Any]:
        origins = await self.get_document_origins()
        
        tasks = [
            self.get_patient_count(),
            self.get_test_patient_count(),
            self.get_research_patient_count(),
            self.get_celebrity_patient_count(),
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
            "patient_count": results[0],
            "test_patient_count": results[1],
            "research_patient_count": results[2],
            "celebrity_patient_count": results[3],
            "document_counts": results[4],
            "recent_document_counts": results[5],
            "top_users": results[6],
            "top_users_current_year": results[7],
            "document_metrics": results[8],  # Changed from results[85] to results[8]
            "archive_status": results[9],
            "document_origins": origins,
            "document_counts_by_year": results[10],
            "recent_document_counts_by_month": results[11]
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