import json
import os
import logging
from pathlib import Path
from psycopg2.extras import Json
from datetime import datetime
import shutil
from utils import DBConnection, AppLogger, WORKSPACE_ROOT

class PostgresLoader:
    def __init__(self, log_level: int = logging.INFO):
        """Initialize the PostgreSQL loader with database parameters."""
        self.log_level = log_level
        self.logger = self.__get_logger()
        self.db_connection = DBConnection(logger=self.logger)
    
    def __get_logger(self) -> AppLogger:
        """Get the logger instance."""
        log_dir = Path('doc') / datetime.now().strftime('%Y') / datetime.now().strftime('%m') / datetime.now().strftime('%d')
        return AppLogger(name="load_to_postgres", log_dir=log_dir, log_file="load_to_postgres.log", level=self.log_level).get_logger()
            
    def load_json_to_table(self, json_file: str, table_name: str, truncate: bool = False) -> None:
        """Load JSON data to a table in PostgreSQL."""
        try:
            file_path = Path(json_file)
            # Read and parse JSON file
            with open(json_file, 'r') as f:
                json_object = json.load(f)
            self.db_connection.connect()
            if truncate:
                self.db_connection.execute_query(query=f"DROP TABLE IF EXISTS {table_name}")
                self.db_connection.execute_query(
                    query=f"""
                        CREATE TABLE {table_name} (
                            id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                            file_path VARCHAR(255), 
                            file_name VARCHAR(255), 
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            raw_data JSONB)
                    """
                )

            # Insert JSON data into table
            self.db_connection.execute_query(
                query=f"INSERT INTO {table_name} (file_path, file_name, raw_data) VALUES (%s, %s, %s)",
                params=(str(file_path.parent), str(file_path.name), json.dumps(json_object),)
            )
            self.logger.info(f"Successfully loaded JSON data from {json_file} into {table_name}")
        except json.JSONDecodeError as e:
            self.db_connection.rollback()
            self.logger.error(f"Error parsing JSON file {json_file}: {str(e)}")
            raise
        except IOError as e:
            self.db_connection.rollback()
            logging.error(f"Error reading file {json_file}: {str(e)}")
            raise
        except Exception as e:
            self.db_connection.rollback()
            self.logger.error(f"Error loading JSON data into {table_name}: {str(e)}")
            raise
        finally:
            self.db_connection.disconnect()
            
    def archive_json_file(self, json_file: str, archive_dir: str) -> None:
        """Archive JSON file by moving it to the specified archive folder.
        
        Args:
            json_file: Path to the JSON file to archive
            archive_dir: Path to the archive directory
        """
        try:
            source = Path(json_file)
            target = Path(archive_dir)
            if target.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                shutil.move(json_file, target)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    target.unlink()
                source.rename(target)

            self.logger.info(f"Successfully archived {json_file} to {archive_dir}")
            
        except Exception as e:
            self.logger.error(f"Error archiving file {json_file} to {archive_dir}: {str(e)}")
            raise
            
    def process_directory(self, base_dir: str, table_name: str, truncate: bool = False) -> None:
        """Process all JSON files in the specified directory.
        
        Args:
            base_dir: Base directory containing JSON files
            table_name: Name of the table to load data into
            truncate: Whether to truncate the table before loading
        """
        try:
            self.logger.info(f"Loading table {table_name} from {base_dir}")
            if base_dir is None:
                self.logger.error("Base directory is None")
                
            if os.path.isfile(base_dir):
                base_dir = os.path.dirname(base_dir)
            
            # Process all JSON files in the directory
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if file.endswith('.json'):
                        file_path = os.path.join(root, file)
                        try:
                            # Load JSON data to database
                            if truncate:
                                # only truncate the table for the first file
                                self.load_json_to_table(file_path, table_name=table_name, truncate=truncate)
                                truncate = False
                            else:
                                self.load_json_to_table(file_path, table_name=table_name)
                            
                            # Archive the file after successful loading
                            archive_dir = str(file_path).replace('raw', 'archive')
                            self.archive_json_file(file_path, archive_dir)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing file {file_path}: {str(e)}")
                            continue
                            
        except Exception as e:
            self.logger.error(f"Error processing directory {base_dir}: {str(e)}")
            raise

