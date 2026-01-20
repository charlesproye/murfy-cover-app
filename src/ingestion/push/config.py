from pydantic_settings import BaseSettings

KIA_KEYS_TO_IGNORE = [
    "*.Date",
    "meta",
    "received_date",
    "state.Electronics",
    "state.Green.Electric.SmartGrid",
    "state.Vehicle.Body",
    "state.Vehicle.Cabin",
    "state.Vehicle.Chassis",
    "state.Vehicle.Green.BatteryManagement.BatteryRemaining.Value",  # We already watch the ratio (SoC) and this value has a 1e-5kWh precision
    "state.Vehicle.Green.EnergyInformation",
    "state.Vehicle.Green.PlugAndCharge",
    "state.Vehicle.Green.PowerConsumption",
    "state.Vehicle.Location",
    "state.Vehicle.Offset",
    "state.Vehicle.RemoteControl",
    "state.Vehicle.Service",
    "state.Vehicle.Version",
]


class KafkaSettings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "127.0.0.1:9092"
    KAFKA_PUSH_TOPIC: str = "oem_push_data"


class RedisSettings(BaseSettings):
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_USER: str | None = None
    REDIS_PASSWORD: str | None = None
    REDIS_DB_KAFKA_CACHE: int = 2
