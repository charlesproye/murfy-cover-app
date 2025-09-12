import os
from dotenv import load_dotenv
from core.env_utils import get_env_var

load_dotenv()

# API Configuration
BMW_AUTH_URL = get_env_var('BMW_AUTH_URL')
BMW_BASE_URL = get_env_var('BMW_BASE_URL')
BMW_CLIENT_ID = get_env_var('BMW_CLIENT_ID')
BMW_FLEET_ID = get_env_var('BMW_FLEET_ID')
BMW_CLIENT_USERNAME = get_env_var('BMW_CLIENT_USERNAME')
BMW_CLIENT_PASSWORD = get_env_var('BMW_CLIENT_PASSWORD')

HM_BASE_URL = get_env_var('HM_BASE_URL')
HM_CLIENT_ID = get_env_var('HM_CLIENT_ID')
HM_CLIENT_SECRET = get_env_var('HM_CLIENT_SECRET')

STELLANTIS_BASE_URL = get_env_var('MS_BASE_URL')
STELLANTIS_EMAIL = get_env_var('MS_EMAIL')
STELLANTIS_PASSWORD = get_env_var('MS_PASSWORD')
STELLANTIS_FLEET_ID = get_env_var('MS_FLEET_ID')
STELLANTIS_COMPANY_ID = get_env_var('MS_COMPANY')

# SLACK_TOKEN = get_env_var('SLACK_TOKEN')
# SLACK_CHANNEL_ID = get_env_var('SLACK_CHANNEL_ID')
SPREADSHEET_ID = get_env_var('SPREADSHEET_ID')

RENAULT_KID = get_env_var('RENAULT_KID')
RENAULT_AUD = get_env_var('RENAULT_AUD')
RENAULT_CLIENT = get_env_var('RENAULT_CLIENT')
RENAULT_SCOPE = get_env_var('RENAULT_SCOPE')
RENAULT_PWD = get_env_var('RENAULT_PWD')

VW_AUTH_URL = get_env_var('VW_AUTH_URL')
VW_BASE_URL = get_env_var('VW_BASE_URL')
VW_CLIENT_USERNAME = get_env_var('VW_CLIENT_USERNAME')
VW_CLIENT_PASSWORD = get_env_var('VW_CLIENT_PASSWORD')
VW_ORGANIZATION_ID = get_env_var('VW_ORGANIZATION_ID')

# TESLA_BASE_URL = get_env_var('TESLA_BASE_URL')
# TESLA_TOKEN_URL = get_env_var('TESLA_TOKEN_URL')
# TESLA_CLIENT_ID = get_env_var('TESLA_CLIENT_ID')
