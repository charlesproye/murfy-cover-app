from dotenv import load_dotenv

from core.env_utils import get_env_var

load_dotenv()

TESLA_CLIENT_ID = get_env_var("TESLA_CLIENT_ID")
TESLA_CLIENT_SECRET = get_env_var("TESLA_CLIENT_SECRET")
TESLA_VEHICLE_COMMAND_CERT_PATH = get_env_var("TESLA_VEHICLE_COMMAND_CERT_PATH")
TESLA_VEHICLE_COMMAND_KEY_PATH = get_env_var("TESLA_VEHICLE_COMMAND_KEY_PATH")
FLEET_TELEMETRY_CERT_PATH = get_env_var("FLEET_TELEMETRY_CERT_PATH")
