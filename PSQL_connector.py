import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from typing import Optional

class PSQLConnector:
    def __init__(
        self,
        host: str,
        port: int,
        db_name: str,
        username: str,
        password: str,
        schema: Optional[str] = None
    ):
        """
        Initialize the PostgreSQL database connector.
        
        Parameters:
        - host: Database host address
        - port: Database port number
        - db_name: Database name
        - username: Database username
        - password: Database password
        - schema: Optional default schema name
        """
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.schema = schema
        self.connection = None
        
    def connect(self) -> None:
        """Establish a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.username,
                password=self.password
            )
            if self.schema:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"SET search_path TO {self.schema}")
            print("Successfully connected to PostgreSQL database")
        except OperationalError as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise
            
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("PostgreSQL connection closed")
            
    def _execute_query(self, query: str) -> pd.DataFrame:
        """Internal method to execute a query and return a DataFrame."""
        try:
            return pd.read_sql(query, self.connection)
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
            
    def get_all_dishes(self) -> pd.DataFrame:
        """Retrieve all dishes from restaurant.dish table."""
        query = "SELECT * FROM restaurant.dish"
        return self._execute_query(query)
    
    def get_distinct_dishes(self) -> pd.DataFrame:
        """Retrieve all dishes from restaurant.dish table."""
        query = "SELECT distinct dish_name, id, category_id FROM restaurant.dish"
        return self._execute_query(query)
        
    def get_all_dish_categories(self) -> pd.DataFrame:
        """Retrieve all dish categories from restaurant.dish_category table."""
        query = "SELECT * FROM restaurant.dish_category"
        return self._execute_query(query)

    def execute_query(self, query):
        return self._execute_query(query)
        
    def get_all_iiko_receipt_items(self, first_n_rows=1000) -> pd.DataFrame:
        """Retrieve all receipt items from iikoconnector.iiko_receiptitems table."""

        if first_n_rows > 50_000:
            first_n_rows = 50_000

        query = f'SELECT * FROM iikoconnector.iiko_receiptitems ORDER BY "ItemSaleEvent_Id" ASC LIMIT {first_n_rows}'

        return self._execute_query(query)

    def __enter__(self):
        """Support for context manager (with statement)."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for context manager (with statement)."""
        self.disconnect()