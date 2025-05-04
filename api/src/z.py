import os
import tempfile
import json
import yaml
from pathlib import Path
import logging
from utils import AppLogger, APIClient, save_response_to_file, load_config

def test_app_logger():
    """Test AppLogger functionality with api/log directory"""
    print("\nTesting AppLogger...")
    
    # Test basic logger without file handler
    logger = AppLogger("test_logger").get_logger()
    logger.info("Test info message")
    logger.error("Test error message")
    print("✓ AppLogger console logging test passed")
    
    # Test logger with file handler
    test_log_file = "test_app.log"
    logger = AppLogger("file_logger", log_file=test_log_file).get_logger()
    test_message = "Test file logging message"
    logger.info(test_message)
    
    # Verify log file was created in api/log directory
    log_path = Path("api/log") / test_log_file
    assert log_path.exists(), f"Log file not found at {log_path}"
    
    # Verify log content
    with open(log_path, 'r') as f:
        log_content = f.read()
        assert test_message in log_content, "Test message not found in log file"
        assert "INFO" in log_content, "Log level not found in log file"
        assert "file_logger" in log_content, "Logger name not found in log file"
    
    print("✓ AppLogger file logging test passed")
    
    # Test multiple loggers with different files
    logger1 = AppLogger("logger1", log_file="logger1.log", log_dir="test/01").get_logger()
    logger2 = AppLogger("logger2", log_file="logger2.log", log_dir="test/02").get_logger()
    
    logger1.info("Logger1 test message")
    logger2.info("Logger2 test message")
    
    # Verify both log files exist
    assert (Path("api/log") / "logger1.log").exists(), "Logger1 log file not found"
    assert (Path("api/log") / "logger2.log").exists(), "Logger2 log file not found"
    
    print("✓ AppLogger multiple loggers test passed")
    


def test_api_client():
    """Test APIClient functionality"""
    print("\nTesting APIClient...")
    
    # Test client initialization
    client = APIClient(max_retries=2, timeout=10)
    assert client.max_retries == 2
    assert client.timeout == 10
    print("✓ APIClient initialization test passed")
    
    # Test context manager
    with APIClient() as client:
        assert client.session is not None
    print("✓ APIClient context manager test passed")

def test_save_response_to_file():
    """Test save_response_to_file functionality"""
    print("\nTesting save_response_to_file...")
    
    # Create test data
    test_data = {"test": "data", "value": 123}
    test_dir = tempfile.mkdtemp()
    test_file = "test_response.json"
    
    # Test saving response
    save_response_to_file(
        response_data=test_data,
        file_path=test_dir,
        file_name=test_file,
        logger=AppLogger("test_save").get_logger()
    )
    
    # Verify file was created and contains correct data
    saved_file = Path(test_dir) / test_file
    assert saved_file.exists()
    
    with open(saved_file, 'r') as f:
        loaded_data = json.load(f)
        assert loaded_data == test_data
    
    print("✓ save_response_to_file test passed")

def test_load_config():
    """Test load_config functionality"""
    print("\nTesting load_config...")
    
    # Create test config file
    return load_config()

def main():
    """Run all tests"""
    print("Starting utils.py tests...")
    
    test_app_logger()
    test_api_client()
    test_save_response_to_file()
    test_load_config()
    
    print("\nAll tests completed successfully!")

if __name__ == "__main__":
    # main()
    test_api_client()