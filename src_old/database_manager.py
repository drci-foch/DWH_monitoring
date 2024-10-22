import logging
import oracledb
from lock_parameters import username_oracle, password_oracle, hostname_oracle, port_oracle, service_name_oracle

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.oracle_connection_params = {
            "user": username_oracle,
            "password": password_oracle,
            "dsn": f"{hostname_oracle}:{port_oracle}/{service_name_oracle}"
        }
        self.connection = None

    def connect(self):
        if self.connection is None or not self.connection.is_healthy():
            try:
                self.connection = oracledb.connect(**self.oracle_connection_params)
                logger.info("Connected to Oracle database")
            except oracledb.Error as e:
                logger.error(f"Oracle database connection error: {e}")
                raise

    def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
                logger.info("Disconnected from Oracle database")
            except oracledb.Error as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self.connection = None

    def get_connection(self):
        self.connect()
        return self.connection

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()