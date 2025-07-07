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

# Logging function
def log_request(ip: str, method: str, url: str, sent: int, status: int, response_size: int):
    # Convert to KB and round up small sizes to minimum visible value
    sent_kb = max(sent / 1024, 0.01)
    response_kb = max(response_size / 1024, 0.01)
    access_logger.info(
        f"IP: {ip} | Method: {method} | URL: {url} | Sent: {sent_kb:.2f}KB | Status: {status} | Response: {response_kb:.2f}KB"
    )
