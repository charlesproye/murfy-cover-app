from dotenv import load_dotenv

from core.env_utils import get_env_var

load_dotenv()

# API Configuration
BMW_AUTH_URL = get_env_var("BMW_AUTH_URL")
BMW_BASE_URL = get_env_var("BMW_BASE_URL")
BMW_CLIENT_ID = get_env_var("BMW_CLIENT_ID")
BMW_FLEET_ID = get_env_var("BMW_FLEET_ID")
BMW_CLIENT_USERNAME = get_env_var("BMW_CLIENT_USERNAME")
BMW_CLIENT_PASSWORD = get_env_var("BMW_CLIENT_PASSWORD")

HM_BASE_URL = get_env_var("HM_BASE_URL")
HM_CLIENT_ID = get_env_var("HM_CLIENT_ID")
HM_CLIENT_SECRET = get_env_var("HM_CLIENT_SECRET")

STELLANTIS_BASE_URL = get_env_var("MS_BASE_URL")
STELLANTIS_EMAIL = get_env_var("MS_EMAIL")
STELLANTIS_PASSWORD = get_env_var("MS_PASSWORD")
STELLANTIS_FLEET_ID = get_env_var("MS_FLEET_ID")
STELLANTIS_COMPANY_ID = get_env_var("MS_COMPANY")

SPREADSHEET_ID = get_env_var("SPREADSHEET_ID")

RENAULT_KID = get_env_var("RENAULT_KID")
RENAULT_AUD = get_env_var("RENAULT_AUD")
RENAULT_CLIENT = get_env_var("RENAULT_CLIENT")
RENAULT_SCOPE = get_env_var("RENAULT_SCOPE")
RENAULT_PWD = get_env_var("RENAULT_PWD")

VW_AUTH_URL = get_env_var("VW_AUTH_URL")
VW_BASE_URL = get_env_var("VW_BASE_URL")
VW_CLIENT_USERNAME = get_env_var("VW_CLIENT_USERNAME")
VW_CLIENT_PASSWORD = get_env_var("VW_CLIENT_PASSWORD")
VW_ORGANIZATION_ID = get_env_var("VW_ORGANIZATION_ID")


KIA_AUTH_URL = get_env_var("KIA_AUTH_URL")
KIA_BASE_URL = get_env_var("KIA_BASE_URL")
KIA_API_USERNAME = get_env_var("KIA_API_USERNAME")
KIA_API_PWD = get_env_var("KIA_API_PWD")
KIA_API_KEY = get_env_var("KIA_API_KEY")

TESLA_CLIENT_ID = get_env_var("TESLA_CLIENT_ID")
TESLA_CLIENT_SECRET = get_env_var("TESLA_CLIENT_SECRET")

SLACK_TOKEN = get_env_var("OAUTH_TOKEN")
METRIC_SLACK_CHANNEL_ID = get_env_var("METRIC_SLACK_CHANNEL_ID")
