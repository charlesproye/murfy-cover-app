import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
BMW_AUTH_URL = os.getenv('BMW_AUTH_URL')
BMW_BASE_URL = os.getenv('BMW_BASE_URL')
BMW_CLIENT_ID = os.getenv('BMW_CLIENT_ID')
BMW_FLEET_ID = os.getenv('BMW_FLEET_ID')
BMW_CLIENT_USERNAME = os.getenv('BMW_CLIENT_USERNAME')
BMW_CLIENT_PASSWORD = os.getenv('BMW_CLIENT_PASSWORD')

HM_BASE_URL = os.getenv('HM_BASE_URL')
HM_CLIENT_ID = os.getenv('HM_CLIENT_ID')
HM_CLIENT_SECRET = os.getenv('HM_CLIENT_SECRET')

STELLANTIS_BASE_URL = os.getenv('MS_BASE_URL')
STELLANTIS_EMAIL = os.getenv('MS_EMAIL')
STELLANTIS_PASSWORD = os.getenv('MS_PASSWORD')
STELLANTIS_FLEET_ID = os.getenv('MS_FLEET_ID')
STELLANTIS_COMPANY_ID = os.getenv('MS_COMPANY')

SLACK_TOKEN = os.getenv('SLACK_TOKEN')

SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')

RATE_LIMIT_DELAY = 0.5
MAX_RETRIES = 3
API_TIMEOUT = 30  # seconds
ACTIVATION_TIMEOUT = 20  # seconds

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s'
} 
