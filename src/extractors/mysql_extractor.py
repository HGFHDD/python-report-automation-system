"""
MySQL Data Extractor
"""
from typing import Any, Dict, Optional
import pandas as pd
import pymysql
from src.extractors.base import BaseExtractor


class MySQLExtractor(BaseExtractor):
    """
    Extractor for MySQL databases
    """

    def __init__(self, host: str = "localhost", port: int = 3306,
                 database: str = "", user: str = "", password: str = "", **kwargs):
        """
        Initialize MySQL extractor

        Args:
            host: MySQL host
            port: MySQL port
            database: Database name
            user: Username
            password: Password
            **kwargs: Additional connection parameters
        """
        super().__init__(kwargs)
        self.connection_config.update({
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password,
        })

    def connect(self) -> None:
        """Establish connection to MySQL"""
        try:
            self._connection = pymysql.connect(
                host=self.connection_config['host'],
                port=self.connection_config['port'],
                database=self.connection_config['database'],
                user=self.connection_config['user'],
                password=self.connection_config['password'],
                cursorclass=pymysql.cursors.DictCursor
            )
            self.logger.info("Successfully connected to MySQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close MySQL connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            self.logger.info("MySQL connection closed")

    def extract(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Extract data using SQL query

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            DataFrame with query results
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")

        try:
            df = pd.read_sql_query(query, self._connection, params=params)
            self.logger.info(f"Query executed successfully, retrieved {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    def extract_table(self, table_name: str, columns: Optional[list] = None,
                      where: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Extract data from a specific table

        Args:
            table_name: Name of the table
            columns: List of columns to select
            where: Optional WHERE clause
            limit: Optional LIMIT clause

        Returns:
            DataFrame with table data
        """
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        if limit:
            query += f" LIMIT {limit}"

        return self.extract_query(query)

    def get_tables(self) -> list:
        """Get list of tables in the database"""
        query = "SHOW TABLES"
        df = self.extract_query(query)
        return df.iloc[:, 0].tolist()
