import pyodbc
import streamlit as st
import logging
import oracledb
from typing import List, Tuple, Any
from lock_parameters import  database_oracle, username_oracle, password_oracle, hostname_oracle, port_oracle, service_name_oracle
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):

        self.oracle_connection_params = {
            "user": username_oracle,
            "password": password_oracle,
            "dsn": f"{hostname_oracle}:{port_oracle}/{service_name_oracle}"
        }

    def connect_to_db(self):
        try:
            conn = pyodbc.connect(self.connection_string)
            return conn
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            st.error("Unable to connect to the database. Please try again later.")
            return None
        
    def connect(self):
        try:
            self.connection = oracledb.connect(**self.oracle_connection_params)
            logger.info("Connected to Oracle database")
        except oracledb.Error as e:
            logger.error(f"Oracle database connection error: {e}")
            st.error("Unable to connect to the Oracle database. Please try again later.")
            raise

    def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
                logger.info("Disconnected from Oracle database")
            except oracledb.Error as e:
                logger.error(f"Error closing database connection: {e}")
        

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
