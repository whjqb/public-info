from api_client import APIClient
import logging
import json
from pathlib import Path
import yaml
from dotenv import load_dotenv
import os

class DOCAPIClient:
    """Client for DOC API operations."""
    
    def __init__(self, config_path: str = None):
        """Initialize the DOC API client with configuration."""
        # Get the absolute path of the project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        
        # Set default config path if not provided
        if config_path is None:
            config_path = os.path.join(project_root, 'api', 'config', 'config.yml')
            
        self.config_path = config_path
        self.api_name = 'doc'
        self.config = self._load_config()
        self.api_key = self._get_api_key()
        
    def _load_config(self) -> dict:
        """Load and validate the configuration file."""
        try:
            logging.info(f"Loading config from: {self.config_path}")
            
            if not os.path.exists(self.config_path):
                error_msg = f"Config file not found at: {self.config_path}"
                logging.error(error_msg)
                return {}
                
            with open(self.config_path, 'r') as config_file:
                config = yaml.safe_load(config_file)
            
            if self.api_name not in config:
                error_msg = "DOC configuration not found in config file"
                logging.error(error_msg)
                logging.error(f"Available configurations: {list(config.keys())}")
                return {}
            
            self.data_directory = config['data_directory']

            return config[self.api_name]
            
        except (yaml.YAMLError, KeyError) as e:
            error_msg = f"Error loading configuration: {str(e)}"
            logging.error(error_msg)
            logging.error(f"Config file path: {self.config_path}")
            if isinstance(e, KeyError):
                logging.error(f"Missing required key: {str(e)}")
            return {}
    
    def _get_api_key(self) -> str:
        """Get the API key from environment variables."""
        load_dotenv()
        api_key = os.getenv(self.config['api_key_env_var'])
        if not api_key:
            error_msg = f"API key not found in environment variable: {self.config.get('api_key_env_var', 'DOC_API_KEY')}"
            logging.error(error_msg)
            logging.error(f"Environment variables: {dict(os.environ)}")
        return api_key
    
    def _get_client(self, url_name: str) -> APIClient:
        """Create an APIClient instance for the specified URL."""
        if not self.config or not self.api_key:
            return None
            
        if 'urls' not in self.config or url_name not in self.config['urls']:
            error_msg = f"URL configuration '{url_name}' not found in DOC config"
            logging.error(error_msg)
            logging.error(f"Available URLs: {list(self.config.get('urls', {}).keys())}")
            return None
        
        url=self.config['urls'][url_name]
        data_directory = self.data_directory + '/' + self.api_name + '/' + url_name

        return APIClient(
            api_key=self.api_key,
            url=url,
            data_directory=data_directory,
            headers=self.config['headers'],
            log_level=self.config.get('log_level', logging.INFO),
            max_retries=self.config.get('max_retries', 3),
            timeout=self.config.get('timeout', 30)
        )

def get_doc(url_name: str, **kwargs) -> str:
    """Get data from DOC API."""
    try:
        doc_client = DOCAPIClient()
        api_client = doc_client._get_client(url_name)
        
        if not api_client:
            return None
            
        with api_client as client:
            # Fetch data from the API
            response_data = client.get_data_json(**kwargs)
            
            if response_data is not None:
                # Save the response to a file
                json_file = client.save_response_to_file(response_data)
                if json_file:
                    logging.info("Data fetched and saved successfully.")
                else:
                    logging.warning("Data was fetched but could not be saved to file.")
                return json_file
            else:
                logging.warning("Failed to fetch data from API.")
                return None
                
    except Exception as e:
        # Log unexpected errors with full stack trace but continue execution
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
        return None

def get_doc_campsites_alerts() -> str:
    """Get DOC campsites alerts data."""
    return get_doc(url_name='campsites_alerts')

def get_doc_campsites() -> str:
    """Get DOC campsites data."""
    return get_doc(url_name='campsites')

def get_doc_campsite_detail(campsite_id: str) -> str:
    """Get detailed information for a specific DOC campsite."""
    return get_doc(url_name='campsites_detail', id=campsite_id)

def process_campsites_details(json_file: str) -> None:
    """Read campsites JSON file and get details for each campsite."""
    try:
        # Read the campsites JSON file
        with open(json_file, 'r') as f:
            campsites_data = json.load(f)
        
        # Extract all asset IDs
        asset_ids = []
        if isinstance(campsites_data, list):
            for campsite in campsites_data:
                if 'assetId' in campsite:
                    asset_ids.append(campsite['assetId'])
        elif isinstance(campsites_data, dict) and 'data' in campsites_data:
            for campsite in campsites_data['data']:
                if 'assetId' in campsite:
                    asset_ids.append(campsite['assetId'])
        
        logging.info(f"Found {len(asset_ids)} campsites to process")
        
        # Get details for each campsite
        for asset_id in asset_ids:
            logging.info(f"Getting details for campsite {asset_id}")
            get_doc_campsite_detail(asset_id)
            
    except Exception as e:
        logging.error(f"Error processing campsites: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # Example usage
    # First get all campsites
    campsites_file = get_doc_campsites()
    # get_doc_campsites_alerts()
    
    campsites_file = "api/data/doc/campsites/2025/04/30/2025_04_30_15_20_06.json"

    # if campsites_file:
    #     # Then process details for each campsite
    #     process_campsites_details(campsites_file)