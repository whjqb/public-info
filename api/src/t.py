import logging
import os

def setup_logging(log_path: str, log_level: int):
    os.makedirs(log_path, exist_ok=True)

    # Always create an info.log
    info_handler = logging.FileHandler(os.path.join(log_path, 'info.log'))
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    handlers = [info_handler, logging.StreamHandler()]

    # Conditionally create debug.log if level is DEBUG
    if log_level <= logging.DEBUG:
        debug_handler = logging.FileHandler(os.path.join(log_path, 'debug.log'))
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        handlers.append(debug_handler)

    # Conditionally create error.log if level is ERROR or lower
    if log_level <= logging.ERROR:
        error_handler = logging.FileHandler(os.path.join(log_path, 'error.log'))
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        handlers.append(error_handler)

    # Basic configuration
    logging.basicConfig(
        level=log_level,
        handlers=handlers
    )

# Example usage:
if __name__ == "__main__":
    setup_logging(log_path="api/log/2025/04/27", log_level=logging.DEBUG)

    logging.debug("This is a debug message")
    logging.info("This is an info message")
    logging.error("This is an error message")
