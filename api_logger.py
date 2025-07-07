import logging

# Set up logger
access_logger = logging.getLogger("access_logger")
access_logger.setLevel(logging.INFO)

# Create a file handler
access_handler = logging.FileHandler("/tmp/app_requests.log")
access_handler.setFormatter(logging.Formatter("%(message)s"))

# Avoid adding multiple handlers
if not access_logger.hasHandlers():
    access_logger.addHandler(access_handler)

# Logging function — only logs response size
def log_request(response_size: int):
    response_kb = max(response_size / 1024, 0.01)
    access_logger.info(f"Response: {response_kb:.2f}KB")
