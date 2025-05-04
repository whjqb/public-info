from utils import save_response_to_file, APIClient, AppLogger, load_config, DBConnection
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime
import yaml

class DOCAPIClient:
    """Client for DOC API operations."""
    
    def __init__(self, log_level: int = logging.INFO):
        """Initialize the DOC API client with configuration."""
        self.log_level = log_level
        self.logger = self.__get_logger()
        self.config = load_config(config_name="api_config.yml", logger=self.logger)['doc']
        self.headers = self.__get_headers()
        self.api_client = self.__get_client()
        
    def __get_logger(self) -> logging.Logger:
        """Get the logger instance."""
        log_dir = Path('doc') / datetime.now().strftime('%Y') / datetime.now().strftime('%m') / datetime.now().strftime('%d')
        return AppLogger(name="doc_api", log_dir=log_dir, log_file="doc_api.log", level=self.log_level).get_logger()
    
    def __get_api_key(self) -> str:
        """Get the API key from environment variables."""
        load_dotenv()
        api_key = os.getenv(self.config['api_key_env_var'])
        if not api_key:
            error_msg = f"API key not found in environment variable: {self.config.get('api_key_env_var', 'DOC_API_KEY')}"
            self.logger.error(error_msg)
            self.logger.error(f"Environment variables: {dict(os.environ)}")
            return None
        self.logger.debug(f"API key: {api_key}")
        self.logger.info("API key loaded successfully.")
        return api_key
    
    def __get_headers(self) -> dict:
        """Get the headers from the config."""
        headers = self.config['headers']
        if not headers:
            self.logger.error("Headers not found in config")
            return None
        api_key = self.__get_api_key()
        if not api_key:
            return None
        headers['x-api-key'] = api_key
        self.logger.debug(f"Headers: {headers}")
        self.logger.info("Headers loaded successfully.")
        return headers

    def __get_client(self) -> APIClient:
        """Create an APIClient instance."""
        if not self.config:
            self.logger.error("Config not found in config")
            return None
            
        api_client = APIClient(
            max_retries=self.config.get('max_retries', 3),
            timeout=self.config.get('timeout', 30),
            logger=self.logger
        )
        
        if api_client is None:
            self.logger.error("Failed to create API client")
            return None
        else:
            self.logger.debug(f"API client: {api_client}")
            self.logger.info("API client loaded successfully.")
            return api_client

    def __get_doc(self, url_name: str, id: str = None) -> str:
        """Get data from DOC API."""           
        try:
            url = self.config['urls'][url_name]
            file_path = Path('doc') / url_name / datetime.now().strftime('%Y') / datetime.now().strftime('%m') / datetime.now().strftime('%d')
            file_name = f"{url_name}.json"
            if id:
                url = url.format(id=id)
                file_name = f"{url_name}_{id}.json"
            
            with self.api_client as client:
                # Fetch data from the API
                response_data = client.get_data_json(url=url, headers=self.headers)

                if response_data is not None:
                    self.logger.debug(f"{url_name} data fetched {len(response_data)} records")
                    
                    # Save the response to a file
                    json_file = save_response_to_file(
                        response_data=response_data,
                        file_path=str(file_path),
                        file_name=file_name,
                        logger=self.logger
                    )
                    return json_file
                else:
                    self.logger.warning("Failed to fetch data from API.")
                    return None
                    
        except Exception as e:
            # Log unexpected errors with full stack trace but continue execution
            self.logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            return None

    def get_doc_campsites_alerts(self) -> str:
        """Get DOC campsites alerts data."""
        return self.__get_doc(url_name='campsites_alerts')

    def get_doc_campsites(self) -> str:
        """Get DOC campsites data."""
        return self.__get_doc(url_name='campsites')

    def get_doc_campsite_detail(self) -> str:
        """Get detailed information for a specific DOC campsite."""
        with DBConnection(logger=self.logger) as db_connection:
            result = db_connection.execute_query(query="SELECT asset_id FROM mart.doc_campsites")
            if not result:
                self.logger.error("There is no data in mart.doc_campsites")
                loaded_ids = set()
            else:
                loaded_ids = set([row['asset_id'] for row in result])
            
        response_data = self.get_doc_campsites()

        if response_data:
            with open(response_data, 'r') as f:
                campsites_data = json.load(f)
                asset_ids = set([campsite['assetId'] for campsite in campsites_data])
                for asset_id in asset_ids - loaded_ids:
                    self.logger.info(f"Getting details for campsite {asset_id}")
                    file_dir = self.__get_doc(url_name='campsites_detail', id=asset_id)
        
        if file_dir:
            return os.path.dirname(file_dir)
        else:
            return None
