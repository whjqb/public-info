import os
import requests
import logging
import yaml
from dotenv import load_dotenv
from datetime import datetime
import json

class APIClient:
    def __init__(self, config_path: str, api_name: str, log_level: int = logging.INFO):
        self.api_name = api_name
        self.log_level = log_level
        # Load the configuration from the config file
        with open(config_path, 'r') as config_file:
            self.config = yaml.safe_load(config_file)

        # Extract URL and headers from the config file
        self.api_key_env_var = self.config[api_name]['api_key_env_var']
        self.url = self.config[api_name]['url']
        self.headers = self.config[api_name]['headers']

        # Load environment variables from .env file
        load_dotenv()

        # Fetch the API key from environment variable
        self.api_key = os.getenv(self.api_key_env_var)

        # Update headers with the API key from .env file
        self.headers['x-api-key'] = self.api_key

        # Set up logging configuration
        self.now = datetime.now()
        self.timestamp = self.now.strftime("%Y_%m_%d_%H_%M_%S")

        self.setup_logging()

    def setup_logging(self):
        """Setup logging files"""
        # Get the log directory from the config file
        log_directory = self.config['logging']['log_directory']
        # Create the log path using year, month, and day
        log_path = os.path.join(log_directory, self.api_name, str(self.now.year), str(self.now.month).zfill(2), str(self.now.day).zfill(2))
        os.makedirs(log_path, exist_ok=True)

        # Always create an info.log
        info_handler = logging.FileHandler(os.path.join(log_path, 'info.log'))
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        handlers = [info_handler, logging.StreamHandler()]

        # Conditionally create debug.log if level is DEBUG
        if self.log_level <= logging.DEBUG:
            debug_handler = logging.FileHandler(os.path.join(log_path, 'debug.log'))
            debug_handler.setLevel(logging.DEBUG)
            debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            handlers.append(debug_handler)

        # Conditionally create error.log if level is ERROR or lower
        if self.log_level <= logging.ERROR:
            error_handler = logging.FileHandler(os.path.join(log_path, 'error.log'))
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            handlers.append(error_handler)

        # Basic configuration
        logging.basicConfig(
            level=self.log_level,
            handlers=handlers
        )

    def save_response_to_file(self, response_data: dict) -> None:
        """Save the JSON response to a dynamically constructed folder based on the current timestamp."""
        # Get the log directory from the config file
        self.data_directory = self.config['data']['data_directory']

        # Create the data path using year, month, and day
        self.data_path = os.path.join(self.data_directory, self.api_name, str(self.now.year), str(self.now.month).zfill(2), str(self.now.day).zfill(2))
        os.makedirs(self.data_path, exist_ok=True)
        
        # Construct the file path using the timestamp
        json_filename = os.path.join(self.data_path, f'{self.timestamp}.json')

        # Write the JSON response to the file
        with open(json_filename, 'w') as json_file:
            json.dump(response_data, json_file, indent=4)

        logging.info(f"Response data saved to {json_filename}")

    def get_data_json(self) -> dict:
        """Fetch data from the provided API URL with the given headers and return the JSON response."""
        try:
            logging.debug(f"Sending GET request to {self.url}")
            response = requests.get(self.url, headers=self.headers)

            # Check if the response was successful
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

            # Get the JSON response
            response_json = response.json()

            # Log the status code and response data
            logging.info(f"Response status code: {response.status_code}")
            # logging.debug(f"Response data: {response_json}")

            return response_json  # Return the JSON response

        except requests.exceptions.Timeout:
            logging.error("The request timed out.")
        except requests.exceptions.TooManyRedirects:
            logging.error("Too many redirects encountered.")
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred: {e}")
        except ValueError as e:
            logging.error(f"Failed to decode JSON response: {e}")

        return {}  # Return an empty dictionary if there is an error


if __name__ == "__main__":
    # Create the API client object with configuration file and API key environment variable
    api_client = APIClient(
        config_path='api/config/config.yml',
        api_name='doc_campsites_alerts',
        log_level=logging.INFO
    )

    if api_client.api_key:
        response_data = api_client.get_data_json()
        if response_data:
            # Save the JSON response to a file after fetching it
            api_client.save_response_to_file(response_data)
            logging.info("Data fetched and saved successfully.")
        else:
            logging.error("Failed to fetch data.")
    else:
        logging.error("API_KEY not found in environment variables.")
