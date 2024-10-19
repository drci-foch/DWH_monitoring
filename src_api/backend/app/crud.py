from typing import Dict, List, Any, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging

class DatabaseQualityChecker:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.logger = logging.getLogger(__name__)

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Tuple]:
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            self.logger.error(f"Error executing query: {e}")
            return []

    def get_patient_count(self) -> int:
        query = "SELECT COUNT(DISTINCT PATIENT_NUM) FROM DWH.DWH_DOCUMENT"
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def get_document_counts(self) -> List[Tuple[str, int]]:
        query = """
        SELECT 
            DOCUMENT_ORIGIN_CODE,
            COUNT(DISTINCT DOCUMENT_NUM) as UNIQUE_DOCUMENT_COUNT
        FROM 
            DWH.DWH_DOCUMENT
        GROUP BY 
            DOCUMENT_ORIGIN_CODE
        ORDER BY 
            UNIQUE_DOCUMENT_COUNT DESC
        """
        return self.execute_query(query)

    def get_recent_document_counts(self) -> List[Tuple[str, int]]:
        query = """
        SELECT 
            DOCUMENT_ORIGIN_CODE,
            COUNT(DISTINCT DOCUMENT_NUM) as UNIQUE_DOCUMENT_COUNT
        FROM 
            DWH.DWH_DOCUMENT
        WHERE 
            UPDATE_DATE >= SYSDATE - 7
        GROUP BY 
            DOCUMENT_ORIGIN_CODE
        ORDER BY 
            UNIQUE_DOCUMENT_COUNT DESC
        """
        return self.execute_query(query)

    def get_top_users(self, current_year: bool = False) -> List[Tuple[str, str, int]]:
        query = """
        SELECT 
            u.FIRSTNAME, 
            u.LASTNAME,
            COUNT(*) AS QUERY_COUNT
        FROM 
            DWH.DWH_LOG_QUERY l
        JOIN 
            DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
        {where_clause}
        GROUP BY 
            u.FIRSTNAME, u.LASTNAME
        ORDER BY 
            QUERY_COUNT DESC
        FETCH FIRST 10 ROWS ONLY
        """
        where_clause = "WHERE EXTRACT(YEAR FROM l.LOG_DATE) = EXTRACT(YEAR FROM SYSDATE)" if current_year else ""
        return self.execute_query(query.format(where_clause=where_clause))

    def get_document_metrics(self) -> Dict[str, float]:
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
        result = self.execute_query(query)
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

    def get_archive_status(self) -> Dict[str, Any]:
        archive_period_query = "SELECT MIN(UPDATE_DATE) AS OLDEST_DATE FROM DWH.DWH_DOCUMENT"
        oldest_date = self.execute_query(archive_period_query)[0][0]
        archive_period = (datetime.now() - oldest_date).days / 365.25 if oldest_date else 0

        total_to_suppress_query = """
        SELECT COUNT(*)
        FROM DWH.DWH_DOCUMENT
        WHERE UPDATE_DATE < ADD_MONTHS(SYSDATE, -240)
        """
        total_to_suppress = self.execute_query(total_to_suppress_query)[0][0]

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
        documents_to_suppress = self.execute_query(documents_to_suppress_query)

        return {
            "archive_period": archive_period,
            "total_documents_to_suppress": total_to_suppress,
            "documents_to_suppress": documents_to_suppress
        }

    def get_document_counts_by_year(self, origin_code: str) -> List[Tuple[int, int]]:
        query = """
        SELECT 
            EXTRACT(YEAR FROM UPDATE_DATE) AS YEAR,
            COUNT(DISTINCT DOCUMENT_NUM) as DOCUMENT_COUNT
        FROM 
            DWH.DWH_DOCUMENT
        WHERE 
            DOCUMENT_ORIGIN_CODE = :origin_code
        GROUP BY 
            EXTRACT(YEAR FROM UPDATE_DATE)
        ORDER BY 
            YEAR
        """
        return self.execute_query(query, {"origin_code": origin_code})

    def get_recent_document_counts_by_month(self, origin_code: str) -> List[Tuple[datetime, int]]:
        query = """
        SELECT
            TRUNC(DOCUMENT_DATE, 'MM') AS MONTH,
            COUNT(DISTINCT DOCUMENT_NUM) AS DOCUMENT_COUNT
        FROM
            DWH.DWH_DOCUMENT
        WHERE
            DOCUMENT_ORIGIN_CODE = :origin_code
            AND DOCUMENT_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), -11)
            AND DOCUMENT_DATE < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)
        GROUP BY
            TRUNC(DOCUMENT_DATE, 'MM')
        ORDER BY
            MONTH
        """
        return self.execute_query(query, {"origin_code": origin_code})

    def get_all_statistics(self) -> Dict[str, Any]:
        return {
            "patient_count": self.get_patient_count(),
            "document_counts": self.get_document_counts(),
            "recent_document_counts": self.get_recent_document_counts(),
            "top_users": self.get_top_users(),
            "top_users_current_year": self.get_top_users(current_year=True),
            "document_metrics": self.get_document_metrics(),
            "archive_status": self.get_archive_status(),
            "document_origins": [row[0] for row in self.execute_query("SELECT DISTINCT DOCUMENT_ORIGIN_CODE FROM DWH.DWH_DOCUMENT")],
            "document_counts_by_year": {origin: self.get_document_counts_by_year(origin) for origin in self.get_document_origins()},
            "recent_document_counts_by_month": {origin: self.get_recent_document_counts_by_month(origin) for origin in self.get_document_origins()}
        }

    def get_document_origins(self) -> List[str]:
        query = "SELECT DISTINCT DOCUMENT_ORIGIN_CODE FROM DWH.DWH_DOCUMENT"
        return [row[0] for row in self.execute_query(query)]

# Usage example
if __name__ == "__main__":
    from sqlalchemy import create_engine
    from app.core.config import settings

    engine = create_engine(settings.SQLALCHEMY_DATABASE_URI)
    db_checker = DatabaseQualityChecker(engine)
    all_stats = db_checker.get_all_statistics()
    print(all_stats)