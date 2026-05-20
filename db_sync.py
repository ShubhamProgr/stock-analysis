"""
Database Sync Script: SQL Server <-> Supabase PostgreSQL
Syncs data between local SQL Server and cloud-based Supabase database
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()


class DatabaseSync:
    """Handles synchronization between SQL Server and Supabase PostgreSQL"""
    
    def __init__(self):
        """Initialize database connections"""
        self.mssql_conn = None
        self.pg_conn = None
        self.pg_cursor = None
        
        # Configuration from .env
        self.mssql_server = os.getenv("MSSQL_SERVER", ".\\SQLEXPRESS")
        self.mssql_database = os.getenv("MSSQL_DATABASE", "Market_data")
        self.mssql_driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
        
        # Supabase Configuration (add to .env)
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_password = os.getenv("SUPABASE_PASSWORD")
        self.supabase_user = os.getenv("SUPABASE_USER", "postgres")
        self.supabase_port = os.getenv("SUPABASE_PORT", "5432")
        
        self._validate_config()
        
    def _validate_config(self):
        """Validate that all required configuration is present"""
        if not self.supabase_url:
            raise ValueError("SUPABASE_URL not set in .env file")
        if not self.supabase_password:
            raise ValueError("SUPABASE_PASSWORD not set in .env file")
        logger.info("Configuration validated successfully")
    
    def connect_mssql(self):
        """Establish connection to SQL Server"""
        try:
            conn_str = (
                f"DRIVER={{{self.mssql_driver}}};"
                f"SERVER={self.mssql_server};"
                f"DATABASE={self.mssql_database};"
                f"Trusted_Connection=yes;"
            )
            self.mssql_conn = pyodbc.connect(conn_str)
            logger.info(f"Connected to SQL Server: {self.mssql_server}/{self.mssql_database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            return False
    
    def connect_supabase(self):
        """Establish connection to Supabase PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.supabase_url,
                user=self.supabase_user,
                password=self.supabase_password,
                port=self.supabase_port
            )
            self.pg_cursor = self.pg_conn.cursor(cursor_factory=RealDictCursor)
            logger.info(f"Connected to Supabase: {self.supabase_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {e}")
            return False
    
    def close_connections(self):
        """Close database connections"""
        if self.mssql_conn:
            self.mssql_conn.close()
            logger.info("Closed SQL Server connection")
        if self.pg_conn:
            self.pg_cursor.close()
            self.pg_conn.close()
            logger.info("Closed Supabase connection")
    
    def get_mssql_tables(self) -> List[str]:
        """Get list of tables from SQL Server"""
        try:
            cursor = self.mssql_conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables in SQL Server: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to get SQL Server tables: {e}")
            return []
    
    def get_supabase_tables(self) -> List[str]:
        """Get list of tables from Supabase"""
        try:
            self.pg_cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in self.pg_cursor.fetchall()]
            logger.info(f"Found {len(tables)} tables in Supabase: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to get Supabase tables: {e}")
            return []
    
    def get_mssql_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get column schema from SQL Server table"""
        try:
            cursor = self.mssql_conn.cursor()
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            schema = {row[0]: row[1] for row in cursor.fetchall()}
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return {}
    
    def read_mssql_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Read data from SQL Server table"""
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.mssql_conn)
            logger.info(f"Read {len(df)} rows from SQL Server table: {table_name}")
            return df
        except Exception as e:
            logger.error(f"Failed to read table {table_name}: {e}")
            return pd.DataFrame()
    
    def read_supabase_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Read data from Supabase table"""
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            self.pg_cursor.execute(query)
            rows = self.pg_cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            columns = [desc[0] for desc in self.pg_cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Read {len(df)} rows from Supabase table: {table_name}")
            return df
        except Exception as e:
            logger.error(f"Failed to read table {table_name} from Supabase: {e}")
            return pd.DataFrame()
    
    def sync_mssql_to_supabase(self, table_name: str, if_exists: str = 'replace', 
                               chunk_size: int = 5000) -> bool:
        """
        Sync table from SQL Server to Supabase
        
        Args:
            table_name: Name of table to sync
            if_exists: 'replace', 'append', or 'ignore'
            chunk_size: Number of rows to insert at once
            
        Returns:
            Boolean indicating success
        """
        try:
            logger.info(f"Starting sync: SQL Server -> Supabase for table {table_name}")
            
            # Read from SQL Server
            df = self.read_mssql_table(table_name)
            if df.empty:
                logger.warning(f"Table {table_name} is empty in SQL Server")
                return True
            
            # Prepare data for PostgreSQL
            df = self._sanitize_dataframe(df)
            
            # Handle existing table
            if if_exists == 'replace':
                try:
                    self.pg_cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                    self.pg_conn.commit()
                    logger.info(f"Dropped existing table {table_name}")
                except Exception as e:
                    logger.warning(f"Could not drop table {table_name}: {e}")
            
            # Insert data in chunks
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                self._insert_chunk_to_supabase(table_name, chunk)
                logger.info(f"Inserted {min(chunk_size, len(df)-i)} rows into {table_name}")
            
            logger.info(f"Successfully synced table {table_name} to Supabase")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync table {table_name}: {e}")
            return False
    
    def sync_supabase_to_mssql(self, table_name: str, if_exists: str = 'replace') -> bool:
        """
        Sync table from Supabase to SQL Server
        
        Args:
            table_name: Name of table to sync
            if_exists: 'replace' or 'append'
            
        Returns:
            Boolean indicating success
        """
        try:
            logger.info(f"Starting sync: Supabase -> SQL Server for table {table_name}")
            
            # Read from Supabase
            df = self.read_supabase_table(table_name)
            if df.empty:
                logger.warning(f"Table {table_name} is empty in Supabase")
                return True
            
            # Write to SQL Server using SQLAlchemy
            engine = self._get_mssql_engine()
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            
            logger.info(f"Successfully synced table {table_name} to SQL Server")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync table {table_name}: {e}")
            return False
    
    def sync_all_tables_mssql_to_supabase(self, if_exists: str = 'replace', 
                                         exclude_tables: Optional[List[str]] = None) -> Dict[str, bool]:
        """Sync all tables from SQL Server to Supabase"""
        exclude_tables = exclude_tables or []
        results = {}
        
        try:
            tables = self.get_mssql_tables()
            
            for table in tables:
                if table in exclude_tables:
                    logger.info(f"Skipping table {table} (excluded)")
                    continue
                
                results[table] = self.sync_mssql_to_supabase(table, if_exists)
            
            logger.info(f"Batch sync completed. Results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Batch sync failed: {e}")
            return {}
    
    def sync_all_tables_supabase_to_mssql(self, if_exists: str = 'replace',
                                          exclude_tables: Optional[List[str]] = None) -> Dict[str, bool]:
        """Sync all tables from Supabase to SQL Server"""
        exclude_tables = exclude_tables or []
        results = {}
        
        try:
            tables = self.get_supabase_tables()
            
            for table in tables:
                if table in exclude_tables:
                    logger.info(f"Skipping table {table} (excluded)")
                    continue
                
                results[table] = self.sync_supabase_to_mssql(table, if_exists)
            
            logger.info(f"Batch sync completed. Results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Batch sync failed: {e}")
            return {}
    
    def _sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize dataframe for PostgreSQL compatibility
        - Handle NULL values
        - Convert data types
        """
        df = df.where(pd.notna(df), None)  # Convert NaN to None
        
        # Convert datetime columns
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass
        
        return df
    
    def _insert_chunk_to_supabase(self, table_name: str, df: pd.DataFrame):
        """Insert a chunk of data into Supabase table"""
        columns = df.columns.tolist()
        values = df.values.tolist()
        
        placeholders = ','.join(['%s'] * len(columns))
        insert_statement = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        
        try:
            self.pg_cursor.executemany(insert_statement, values)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            raise e
    
    def _get_mssql_engine(self):
        """Create SQLAlchemy engine for SQL Server"""
        driver = self.mssql_driver.replace(" ", "+")
        connection_string = (
            f"mssql+pyodbc://{self.mssql_server}/{self.mssql_database}"
            f"?driver={driver}&trusted_connection=yes"
        )
        return create_engine(connection_string)
    
    def compare_tables(self, table_name: str) -> Dict[str, Any]:
        """Compare table data between SQL Server and Supabase"""
        try:
            logger.info(f"Comparing table {table_name} between databases")
            
            mssql_df = self.read_mssql_table(table_name)
            supabase_df = self.read_supabase_table(table_name)
            
            comparison = {
                'table': table_name,
                'mssql_rows': len(mssql_df),
                'supabase_rows': len(supabase_df),
                'mssql_columns': list(mssql_df.columns),
                'supabase_columns': list(supabase_df.columns),
                'rows_match': len(mssql_df) == len(supabase_df),
                'columns_match': set(mssql_df.columns) == set(supabase_df.columns)
            }
            
            logger.info(f"Comparison result: {comparison}")
            return comparison
            
        except Exception as e:
            logger.error(f"Failed to compare table {table_name}: {e}")
            return {}


def main():
    """Main execution function"""
    sync = DatabaseSync()
    
    try:
        # Connect to both databases
        if not sync.connect_mssql():
            logger.error("Failed to connect to SQL Server")
            return
        
        if not sync.connect_supabase():
            logger.error("Failed to connect to Supabase")
            return
        
        # Sync all tables from SQL Server to Supabase
        logger.info("=" * 60)
        logger.info("STARTING DATABASE SYNCHRONIZATION")
        logger.info("=" * 60)
        
        results = sync.sync_all_tables_mssql_to_supabase(if_exists='replace')
        
        # Print results
        logger.info("=" * 60)
        logger.info("SYNC RESULTS")
        logger.info("=" * 60)
        for table, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED"
            logger.info(f"{table}: {status}")
        
        logger.info("=" * 60)
        logger.info("SYNCHRONIZATION COMPLETE")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Sync process failed: {e}")
    finally:
        sync.close_connections()


if __name__ == "__main__":
    main()
