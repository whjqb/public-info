import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Get workspace root from environment variable or use default
WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

class AppLogger:
    def __init__(self, name: str, log_dir: str = None, log_file: str = None, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.propagate = False  # Avoid duplicate logs in some environments

        # Avoid adding multiple handlers if already set
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

            self.log_dir = Path(WORKSPACE_ROOT) / 'api' / 'log' 
            if log_dir:
                self.log_dir = self.log_dir / log_dir
            os.makedirs(self.log_dir, exist_ok=True)

            # Optional file handler
            if log_file:              
                # Create full path for log file
                log_path = self.log_dir / log_file
                
                file_handler = logging.FileHandler(log_path)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger

class APIClient:
    """A client for making API requests with configurable retry logic and error handling."""

    def __init__(
            self,
            max_retries: int = 3,
            timeout: int = 30,
            logger: logging.Logger = AppLogger(__name__).get_logger()
        ) -> None:
        """Initialize the API client with configuration."""
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logger
        self._setup_session()

    def _setup_session(self) -> None:
        """Set up the requests session with retry logic."""
        self.session = requests.Session()
            
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
            
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get_data_json(self, url: str, headers: dict = None, params: dict = None) -> Optional[Dict[str, Any]]:
        """
        Fetch data from the API with retry logic and error handling.   

        Args:
            url: The URL to fetch data from
            headers: The headers to send with the request

        Returns:
            Dict containing the JSON response data, or None if an error occurs
        """
        try:
            self.logger.debug(f"Fetching data from {url}")
            self.logger.debug(f"Params: {params}")
            response = self.session.get(url, headers=headers, timeout=self.timeout, params=params)
            self.logger.debug(f"Response code: {response.status_code}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching data from {url}: {str(e)}")
            return None

    def __enter__(self) -> 'APIClient':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.session.close()

class DBConnection:
    """Handles PostgreSQL database connections and operations."""
    
    def __init__(self, logger: logging.Logger = AppLogger(__name__).get_logger()):
        """Initialize the database connection with configuration."""
        self.logger = logger
        self.config = load_config(config_name="db_config.yml", logger=self.logger)
        self.connection = None
        self.cursor = None
        
    def connect(self) -> bool:
        """Establish a connection to the PostgreSQL database."""
        try:
            if self.connection is None or self.connection.closed:
                load_dotenv()
                self.connection = psycopg2.connect(
                    dbname=self.config['database'],
                    user=self.config['user'],
                    password=os.getenv(self.config['password']),
                    host=self.config['host'],
                    port=self.config['port']
                )
                self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
                self.logger.info("Successfully connected to the database")
                return True
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict[str, Any]]]:
        """Execute a SQL query and return the results."""
        self.logger.debug(f"Executing query: {query}")
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith(('SELECT', 'WITH')):
                results = self.cursor.fetchall()
                self.logger.debug(f"Query executed successfully: {len(results)} rows returned")
                return results
            else:
                self.connection.commit()
                self.logger.debug("Query executed successfully")
                return None
                
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return None
            
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        
    def get_table_schema(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """Get the schema information for a table."""
        query = """
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM 
                information_schema.columns 
            WHERE 
                table_name = %s
            ORDER BY 
                ordinal_position;
        """
        return self.execute_query(query, (table_name,))
        
    def get_table_data(self, table_name: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Get data from a table with optional limit."""
        query = f"SELECT * FROM {table_name} LIMIT %s;"
        return self.execute_query(query, (limit,))
        
    def insert_data(self, table_name: str, data: Dict[str, Any]) -> bool:
        """Insert data into a table."""
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
        
        try:
            self.execute_query(query, tuple(data.values()))
            self.logger.info(f"Data inserted successfully into {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error inserting data: {str(e)}")
            return False 

def save_response_to_file(
        response_data: Dict[str, Any], 
        file_path: str, 
        file_name: str,
        logger: logging.Logger = AppLogger(__name__).get_logger()
    ) -> str:
    """
    Save the JSON response to a file in a structured directory.
    
    Args:
        response_data: The JSON response data to save
        file_path: The base directory to save the file
        file_name: The name of the file to save
        logger: The logger to use for logging
            
    Returns:
        str: Path to the saved file, or None if failed
    """
    try:  
        data_path = Path(WORKSPACE_ROOT) / 'api' / 'data' / 'raw' / file_path
        data_path.mkdir(parents=True, exist_ok=True)
        
        json_file = data_path / file_name
        
        with open(json_file, 'w') as f:
            json.dump(response_data, f, indent=2)
            
        logger.debug(f"Response saved to: {json_file}")
        return json_file
    
    except Exception as e:
        logger.error(f"Error saving response to file: {str(e)}")
        return None
    
def load_config(
        config_path: str = "api", 
        config_name: str = "api_config.yml",
        logger: logging.Logger = AppLogger(__name__).get_logger()
    ) -> Dict[str, Any]:
    """
    Load and validate the configuration file.
    
    Args:
        config_path: Path to the config file
        logger: Logger instance to use
        
    Returns:
        Dict containing the configuration, or empty dict if error occurs
    """
    try:
        config_file = Path(WORKSPACE_ROOT) / config_path / config_name
        logger.info(f"Loading config from: {config_file}")
        
        if not os.path.exists(config_file):
            error_msg = f"Config file not found at: {config_file}"
            logger.error(error_msg)
            return {}
            
        with open(config_file, 'r') as config_file:
            config = yaml.safe_load(config_file)

        logger.info(f"Configuration loaded successfully.")
        return config
        
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        return {}
