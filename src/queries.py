import logging
from database_manager import DatabaseManager

logger = logging.getLogger(__name__)

class DatabaseQualityChecker:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def execute_query(self, query):
        with self.db_manager:
            try:
                cursor = self.db_manager.connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                return results
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                return None

    # Update other methods to use execute_query
    def get_patient_count(self):
        query = "SELECT COUNT(DISTINCT PATIENT_NUM) FROM DWH.DWH_DOCUMENT"
        results = self.execute_query(query)
        if results:
            count = results[0][0]
            logger.info(f"Total number of patients: {count}")
            return count
        return None

    def get_document_counts_by_origin(self):
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
        results = self.execute_query(query)
        if results:
            logger.info(f"Retrieved document counts for {len(results)} origin codes")
        return results

    def get_recent_document_counts_by_origin(self):
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
        results = self.execute_query(query)
        if results:
            logger.info(f"Retrieved recent document counts for {len(results)} origin codes")
        return results

    def get_top_users(self, limit=10):
        query = f"""
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
        FETCH FIRST {limit} ROWS ONLY
        """
        results = self.execute_query(query)
        if results:
            logger.info(f"Retrieved top {len(results)} users by query count")
        return results

    def get_top_users_current_year(self, limit=10):
        query = f"""
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
        FETCH FIRST {limit} ROWS ONLY
        """
        results = self.execute_query(query)
        if results:
            logger.info(f"Retrieved top {len(results)} users by query count for the current year")
        return results
    
    def get_stats_and_delays_document_monthly(self):
        stats_query = """
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

        delay_query = """
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
        """
        
        stats_results = self.execute_query(stats_query)
        delay_results = self.execute_query(delay_query)
        
        if stats_results and delay_results:
            logger.info(f"Retrieved statistics and delay data for document uploading (excluding RDV_DOCTOLIB).")
        else:
            logger.error("Failed to retrieve document statistics or delay data.")
        
        return stats_results, delay_results

# Example usage
if __name__ == "__main__":
    db_checker = DatabaseQualityChecker()
    
    print(f"Total number of patients: {db_checker.get_patient_count()}")
    
    print("Document counts by origin:")
    for origin, count in db_checker.get_document_counts_by_origin():
        print(f"{origin}: {count}")

    print("Recent document counts by origin (last 7 days):")
    for origin, count in db_checker.get_recent_document_counts_by_origin():
        print(f"{origin}: {count}")

    print("Top Users:")
    for firstname, lastname, count in db_checker.get_top_users():
        print(f"{firstname} {lastname}: {count} queries")

    print("Top Users for Current Year:")
    for firstname, lastname, count in db_checker.get_top_users_current_year():
        print(f"{firstname} {lastname}: {count} queries")

    avg_delay = db_checker.get_avg_document_delay_monthly()
    print(f"Average delay between DOCUMENT_DATE and UPDATE_DATE for documents created in the last month: {avg_delay:.2f} days")