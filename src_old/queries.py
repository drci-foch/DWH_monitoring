import logging
from database_manager import DatabaseManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import statistics

class DatabaseQualityChecker:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = logging.getLogger(__name__)

    def execute_query(self, query, params=None):
        connection = self.db_manager.get_connection()
        try:
            cursor = connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return None

    def execute_queries_parallel(self, queries):
        with ThreadPoolExecutor() as executor:
            future_to_query = {executor.submit(self.execute_query, query, params): name 
                               for name, (query, params) in queries.items()}
            results = {}
            for future in as_completed(future_to_query):
                name = future_to_query[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    self.logger.error(f"Error executing query {name}: {e}")
                    results[name] = None
        return results

    def get_median_document_count(self, counts):
        if not counts:
            return 0
        return statistics.median(count for _, count in counts)
    
    def get_all_statistics(self):
        self.logger.info("Starting get_all_statistics method")
        queries = {
            "patient_count": ("SELECT COUNT(DISTINCT PATIENT_NUM) FROM DWH.DWH_DOCUMENT", None),
            "document_counts": ("""
                SELECT 
                    DOCUMENT_ORIGIN_CODE,
                    COUNT(DISTINCT DOCUMENT_NUM) as UNIQUE_DOCUMENT_COUNT
                FROM 
                    DWH.DWH_DOCUMENT
                GROUP BY 
                    DOCUMENT_ORIGIN_CODE
                ORDER BY 
                    UNIQUE_DOCUMENT_COUNT DESC
            """, None),
            "recent_document_counts": ("""
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
            """, None),
            "top_users": ("""
                SELECT 
                    u.FIRSTNAME, 
                    u.LASTNAME,
                    COUNT(*) AS QUERY_COUNT
                FROM 
                    DWH.DWH_LOG_QUERY l
                JOIN 
                    DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
                GROUP BY 
                    u.FIRSTNAME, u.LASTNAME
                ORDER BY 
                    QUERY_COUNT DESC
                FETCH FIRST 10 ROWS ONLY
            """, None),
            "top_users_current_year": ("""
                SELECT 
                    u.FIRSTNAME, 
                    u.LASTNAME,
                    COUNT(*) AS QUERY_COUNT
                FROM 
                    DWH.DWH_LOG_QUERY l
                JOIN 
                    DWH.DWH_USER u ON l.USER_NUM = u.USER_NUM
                WHERE 
                    EXTRACT(YEAR FROM l.LOG_DATE) = EXTRACT(YEAR FROM SYSDATE)
                GROUP BY 
                    u.FIRSTNAME, u.LASTNAME
                ORDER BY 
                    QUERY_COUNT DESC
                FETCH FIRST 10 ROWS ONLY
            """, None),
            "stats_and_delays": ("""
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
            """, None),
            "delay_results": ("""
                WITH delay_data AS (
                    SELECT 
                        TITLE,
                        DOCUMENT_DATE,
                        DOCUMENT_ORIGIN_CODE,
                        UPDATE_DATE,
                        ROUND(UPDATE_DATE - DOCUMENT_DATE, 2) AS DELAY_DAYS
                    FROM DWH.DWH_DOCUMENT
                    WHERE UPDATE_DATE >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)
                    AND DOCUMENT_ORIGIN_CODE != 'RDV_DOCTOLIB'
                ),
                min_max_delays AS (
                    SELECT
                        MIN(DELAY_DAYS) AS MIN_DELAY,
                        MAX(DELAY_DAYS) AS MAX_DELAY
                    FROM delay_data
                )
                SELECT 
                    d.TITLE,
                    d.DOCUMENT_DATE,
                    DOCUMENT_ORIGIN_CODE,
                    d.UPDATE_DATE,
                    d.DELAY_DAYS,
                    CASE 
                        WHEN d.DELAY_DAYS = mm.MIN_DELAY THEN 'Minimum Delay'
                        WHEN d.DELAY_DAYS = mm.MAX_DELAY THEN 'Maximum Delay'
                    END AS DELAY_TYPE
                FROM 
                    delay_data d
                JOIN 
                    min_max_delays mm ON d.DELAY_DAYS IN (mm.MIN_DELAY, mm.MAX_DELAY)
                ORDER BY 
                    d.DELAY_DAYS
            """, None),
            "test_patients": ("SELECT FIRSTNAME, LASTNAME FROM DWH.DWH_PATIENT WHERE LASTNAME = 'TEST'", None),
            "research_patients": ("SELECT FIRSTNAME, LASTNAME FROM DWH.DWH_PATIENT WHERE LASTNAME = 'INSECTE'", None),
            "celebrity_patients": ("SELECT FIRSTNAME, LASTNAME FROM DWH.DWH_PATIENT WHERE LASTNAME = 'FLEUR'", None),
            "document_origins": ("SELECT DISTINCT DOCUMENT_ORIGIN_CODE FROM DWH.DWH_DOCUMENT", None),
            "archive_period": ("SELECT MIN(UPDATE_DATE) AS OLDEST_DATE FROM DWH.DWH_DOCUMENT", None),
            "documents_to_suppress": ("""
                SELECT 
                    DOCUMENT_ORIGIN_CODE,
                    COUNT(*) AS DOCUMENTS_TO_SUPPRESS
                FROM 
                    DWH.DWH_DOCUMENT
                WHERE 
                    UPDATE_DATE < ADD_MONTHS(SYSDATE, -240)  -- 20 years * 12 months
                GROUP BY 
                    DOCUMENT_ORIGIN_CODE
                ORDER BY 
                    DOCUMENTS_TO_SUPPRESS DESC
            """, None),
            "total_documents_to_suppress": ("""
                SELECT COUNT(*)
                FROM DWH.DWH_DOCUMENT
                WHERE UPDATE_DATE < ADD_MONTHS(SYSDATE, -240)  -- 20 years * 12 months
            """, None),
        }

        self.logger.info("Executing document_origins query")
        document_origins = self.execute_query(queries["document_origins"][0])
        self.logger.debug(f"Document origins: {document_origins}")

        self.logger.info("get_all_statistics method completed")
        for origin in document_origins:
            origin_code = origin[0]
            queries[f"document_counts_by_year_{origin_code}"] = ("""
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
            """, {"origin_code": origin_code})
            
            queries[f"recent_document_counts_by_month_{origin_code}"] = ("""
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

            """, {"origin_code": origin_code})

        self.logger.info("Executing all queries in parallel")
        results = self.execute_queries_parallel(queries)
        self.logger.debug(f"Query results keys: {results.keys()}")

        self.logger.info("Post-processing results")
        if results['archive_period'] and results['archive_period'][0][0]:
            oldest_date = results['archive_period'][0][0]
            current_date = datetime.now()
            archive_period = current_date - oldest_date
            results['archive_period'] = archive_period.days / 365.25  # Convert days to years
        else:
            self.logger.warning("No archive_period data found")

        self.logger.info("Organizing document counts by year and month")
        results['document_counts_by_year'] = {}
        results['recent_document_counts_by_month'] = {}
        for origin in document_origins:
            origin_code = origin[0]
            year_key = f"document_counts_by_year_{origin_code}"
            month_key = f"recent_document_counts_by_month_{origin_code}"
            
            if year_key in results:
                results['document_counts_by_year'][origin_code] = results[year_key]
            else:
                self.logger.warning(f"No yearly data found for origin: {origin_code}")
            
            if month_key in results:
                results['recent_document_counts_by_month'][origin_code] = results[month_key]
            else:
                self.logger.warning(f"No monthly data found for origin: {origin_code}")

        self.logger.debug(f"document_counts_by_year keys: {results['document_counts_by_year'].keys()}")
        self.logger.debug(f"recent_document_counts_by_month keys: {results['recent_document_counts_by_month'].keys()}")

        self.logger.info("Disconnecting from database")
        self.db_manager.disconnect()

        self.logger.info("get_all_statistics method completed")
        return results

    # Keep individual methods for backward compatibility and specific use cases
    def get_patient_count(self):
        results = self.execute_query("SELECT COUNT(DISTINCT PATIENT_NUM) FROM DWH.DWH_DOCUMENT")
        if results:
            count = results[0][0]
            self.logger.info(f"Total number of patients: {count}")
            return count
        return None


# Example usage
if __name__ == "__main__":
    db_checker = DatabaseQualityChecker()
    
    all_stats = db_checker.get_all_statistics()
    
    print(f"Total number of patients: {all_stats['patient_count'][0][0]}")
    
    print("Document counts by origin:")
    for origin, count in all_stats['document_counts']:
        print(f"{origin}: {count}")

    print("Recent document counts by origin (last 7 days):")
    for origin, count in all_stats['recent_document_counts']:
        print(f"{origin}: {count}")

    print("Top Users:")
    for firstname, lastname, count in all_stats['top_users']:
        print(f"{firstname} {lastname}: {count} queries")

    print("Top Users for Current Year:")
    for firstname, lastname, count in all_stats['top_users_current_year']:
        print(f"{firstname} {lastname}: {count} queries")

    stats = all_stats['stats_and_delays'][0]
    print(f"Average delay between DOCUMENT_DATE and UPDATE_DATE for documents created in the last month: {stats[5]:.2f} days")

    print(f"Archive period: {all_stats['archive_period']:.2f} years")

    print("Documents to suppress by origin:")
    for origin, count in all_stats['documents_to_suppress']:
        print(f"{origin}: {count}")

    print(f"Total documents to suppress: {all_stats['total_documents_to_suppress'][0][0]}")