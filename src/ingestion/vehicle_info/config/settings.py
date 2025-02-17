import os
from dotenv import load_dotenv

RATE_LIMIT_DELAY = 0.5
MAX_RETRIES = 5
API_TIMEOUT = 30  # seconds
ACTIVATION_TIMEOUT = 20  # seconds
INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 32

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s'
} 
