import os
import requests
import logging
from datetime import datetime
import json
from typing import Dict, Optional, Any, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time

class APIClient:
    """A client for making API requests with configurable retry logic and error handling."""
    
    def __init__(
        self,
        api_key: str,
        url: str,
        data_directory: str,
        headers: Dict[str, str],
        log_level: int = logging.INFO,
        max_retries: int = 3,
        timeout: int = 30
    ) -> None:
        """
        Initialize the API client.
        
        Args:
            api_key: API key for authentication
            url: API endpoint URL
            headers: Request headers
            log_level: Logging level (default: INFO)
            max_retries: Maximum number of retry attempts (default: 3)
            timeout: Request timeout in seconds (default: 30)
        """
        self.api_key = api_key
        self.url = url
        self.headers = headers.copy()  # Create a copy to avoid modifying the original
        self.headers['x-api-key'] = api_key
        self.data_directory = data_directory
        self.log_level = log_level
        self.max_retries = max_retries
        self.timeout = timeout
        self.now = datetime.now()
        self.timestamp = self.now.strftime("%Y_%m_%d_%H_%M_%S")
        
        self._setup_session()
        self._setup_logging()

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

    def _setup_logging(self) -> None:
        """Setup logging configuration with multiple handlers."""
        log_directory = Path("api/log")
        log_path = log_directory / str(self.now.year) / str(self.now.month).zfill(2) / str(self.now.day).zfill(2)
        log_path.mkdir(parents=True, exist_ok=True)

        # Create handlers with specific log levels
        info_handler = logging.FileHandler(log_path / 'info.log')
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        debug_handler = logging.FileHandler(log_path / 'debug.log')
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        error_handler = logging.FileHandler(log_path / 'error.log')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # Set up the root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Set to lowest level to allow all handlers to filter
        
        # Clear any existing handlers
        root_logger.handlers = []
        
        # Add handlers based on log level
        root_logger.addHandler(console_handler)
        root_logger.addHandler(info_handler)
        
        if self.log_level <= logging.DEBUG:
            root_logger.addHandler(debug_handler)
        if self.log_level <= logging.ERROR:
            root_logger.addHandler(error_handler)

    def save_response_to_file(self, response_data: Dict[str, Any]) -> str:
        """
        Save the JSON response to a file in a structured directory.
        
        Args:
            response_data: The JSON response data to save
            
        Returns:
            str: Path to the saved file, or None if failed
        """
        try:
            data_directory = Path(self.data_directory)
            data_path = data_directory / str(self.now.year) / str(self.now.month).zfill(2) / str(self.now.day).zfill(2)
            data_path.mkdir(parents=True, exist_ok=True)
            
            json_filename = data_path / f'{self.timestamp}.json'
            
            with open(json_filename, 'w') as json_file:
                json.dump(response_data, json_file, indent=4)
            
            logging.info(f"Response data saved to {json_filename}")
            return str(json_filename)
            
        except (IOError, json.JSONDecodeError) as e:
            error_msg = f"Error saving response to file: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return None

    def get_data_json(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Fetch data from the API with retry logic and error handling.
        
        Args:
            **kwargs: URL parameters to replace in the URL template
            
        Returns:
            Dict containing the JSON response data, or None if an error occurs
        """
        try:
            # Replace URL parameters if any
            url = self.url
            if kwargs:
                url = url.format(**kwargs)
            
            logging.debug(f"Sending GET request to {url}")
            response = self.session.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            response_json = response.json()
            
            logging.info(f"Response status code: {response.status_code}")
            return response_json
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return None
        except ValueError as e:
            error_msg = f"Failed to decode JSON response: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return None
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return None

    def __enter__(self) -> 'APIClient':
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup resources."""
        self.session.close()