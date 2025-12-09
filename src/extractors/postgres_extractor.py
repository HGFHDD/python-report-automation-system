"""
PostgreSQL Data Extractor
"""
from typing import Any, Dict, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from src.extractors.base import BaseExtractor
from src.core.config import settings


class PostgresExtractor(BaseExtractor):
    """
    Extractor for PostgreSQL databases
    """

    def __init__(self, connection_string: Optional[str] = None, **kwargs):
        """
        Initialize PostgreSQL extractor

        Args:
            connection_string: PostgreSQL connection string
            **kwargs: Additional connection parameters
        """
        super().__init__(kwargs)
        self.connection_string = connection_string or settings.DATABASE_URL
        self._parse_connection_string()

    def _parse_connection_string(self) -> None:
        """Parse connection string into components"""
        if self.connection_string:
            # Connection string format: postgresql://user:pass@host:port/db
            from urllib.parse import urlparse
            parsed = urlparse(self.connection_string)
            self.connection_config.update({
                'host': parsed.hostname or settings.POSTGRES_HOST,
                'port': parsed.port or settings.POSTGRES_PORT,
                'database': parsed.path.lstrip('/') or settings.POSTGRES_DB,
                'user': parsed.username or settings.POSTGRES_USER,
                'password': parsed.password or settings.POSTGRES_PASSWORD,
            })

    def connect(self) -> None:
        """Establish connection to PostgreSQL"""
        try:
            self._connection = psycopg2.connect(
                host=self.connection_config.get('host', settings.POSTGRES_HOST),
                port=self.connection_config.get('port', settings.POSTGRES_PORT),
                database=self.connection_config.get('database', settings.POSTGRES_DB),
                user=self.connection_config.get('user', settings.POSTGRES_USER),
                password=self.connection_config.get('password', settings.POSTGRES_PASSWORD),
                cursor_factory=RealDictCursor
            )
            self.logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        if self._connection:
            self._connection.close()
            self._connection = None
            self.logger.info("PostgreSQL connection closed")

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
            columns: List of columns to select (default: all)
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
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        df = self.extract_query(query)
        return df['table_name'].tolist()
