import json
import os
import logging
from pathlib import Path
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, List, Any
import shutil

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PostgresLoader:
    def __init__(self, db_params: Dict[str, str]):
        """Initialize the PostgreSQL loader with database parameters."""
        self.db_params = db_params
        self.conn = None
        self.cur = None
        
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cur = self.conn.cursor()
            logging.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL: {str(e)}")
            raise
            
    def disconnect(self):
        """Close the database connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

    def drop_tables(self, table_name: str) -> None:
        """Drop tables in PostgreSQL."""
        try:
            self.cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            logging.info(f"Table {table_name} dropped successfully")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error dropping table {table_name}: {str(e)}")
            raise
            
    def create_tables(self, table_name: str) -> None:
        """Create necessary tables in PostgreSQL."""
        try:
            # Create campsites table
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY,file_name VARCHAR(255), data JSONB)")
            self.conn.commit()
            logging.info(f"Table {table_name} created successfully")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error creating table {table_name}: {str(e)}")
            raise
            
    def load_json_to_table(self, json_file: str, table_name: str, truncate: bool = False) -> None:
        """Load JSON data to a table in PostgreSQL."""
        try:
            # Read and parse JSON file
            with open(json_file, 'r') as f:
                json_object = json.load(f)
            
            if truncate:
                self.cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")

            # Insert JSON data into table
            self.cur.execute(
                f"INSERT INTO {table_name} (file_name, data) VALUES (%s, %s)",
                (json_file, json.dumps(json_object),)
            )
            self.conn.commit()
            logging.info(f"Successfully loaded JSON data from {json_file} into {table_name}")
            
        except json.JSONDecodeError as e:
            self.conn.rollback()
            logging.error(f"Error parsing JSON file {json_file}: {str(e)}")
            raise
        except IOError as e:
            self.conn.rollback()
            logging.error(f"Error reading file {json_file}: {str(e)}")
            raise
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error loading JSON data into {table_name}: {str(e)}")
            raise
            
    def archive_json_file(self, json_file: str, archive_dir: str) -> None:
        """Archive JSON file by moving it to the specified archive folder.
        
        Args:
            json_file: Path to the JSON file to archive
            archive_dir: Path to the archive directory
        """
        try:
            # Create archive directory if it doesn't exist
            os.makedirs(archive_dir, exist_ok=True)
            
            # Get the filename and create archive path
            filename = os.path.basename(json_file)
            archive_path = os.path.join(archive_dir, filename)
            
            # Move the file to archive directory
            shutil.move(json_file, archive_path)
            logging.info(f"Successfully archived {json_file} to {archive_path}")
            
        except Exception as e:
            logging.error(f"Error archiving file {json_file} to {archive_dir}: {str(e)}")
            raise
            
    def process_directory(self, base_dir: str, table_name: str, truncate: bool = False) -> None:
        """Process all JSON files in the specified directory.
        
        Args:
            base_dir: Base directory containing JSON files
            table_name: Name of the table to load data into
            truncate: Whether to truncate the table before loading
        """
        try:
            # Create archive directory in the same parent directory as base_dir
            archive_dir = base_dir.replace('raw', 'archive')
            logging.info(f"Using archive directory: {archive_dir}")
            # Process all JSON files in the directory
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if file.endswith('.json'):
                        file_path = os.path.join(root, file)
                        try:
                            # Load JSON data to database
                            self.load_json_to_table(file_path, table_name=table_name, truncate=truncate)
                            
                            # Archive the file after successful loading
                            self.archive_json_file(file_path, archive_dir)
                            
                        except Exception as e:
                            logging.error(f"Error processing file {file_path}: {str(e)}")
                            continue
                            
        except Exception as e:
            logging.error(f"Error processing directory {base_dir}: {str(e)}")
            raise

def main():
    # Database connection parameters
    db_params = {
        'dbname': 'raw',
        'user': 'postgres',
        'password': 'whjqb1984',
        'host': '192.168.1.214',
        'port': '5432'
    }
    
    # Get the absolute path of the project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    # Base directory containing JSON files
    base_dir = os.path.join(project_root, 'api', 'data', 'raw', 'doc')
    logging.info(f"Using base directory: {base_dir}")
    
    # Initialize loader
    loader = PostgresLoader(db_params)
    
    try:
        # Connect to database
        loader.connect()
        
        # Drop tables
        # loader.drop_tables('doc.campsites')
        loader.drop_tables('doc.campsites_alerts')
        loader.drop_tables('doc.campsites_detail')
        
        # Create tables
        # loader.create_tables('doc.campsites')
        loader.create_tables('doc.campsites_alerts')
        loader.create_tables('doc.campsites_detail')
        
        # loader.process_directory(os.path.join(base_dir,'campsites'), 'doc.campsites')

        # Process all JSON files
        # loader.process_directory(base_dir)
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
    finally:
        # Disconnect from database
        loader.disconnect()

if __name__ == "__main__":
    main() 