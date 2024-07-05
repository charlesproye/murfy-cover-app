import numpy as np
from datetime import datetime, timedelta
from datetime import timedelta as TD
import os

NOW = datetime.now()
RAW_VEHICLE_DF_DATE_MARGIN = timedelta(days=1)
PATH_TO_VEHICLES_INFO_DF = os.path.join("data_cache", "vehicles_info", "vehicles_info.parquet")
ELETRIC_VIN = "WBY71AW010FP87013"
DEFAULT_START_DATE = datetime(2021, 1, 1)
FIELDS = [
    "alert",
    "charging",
    "driving",
    "dtc",
    "electricEngine",
    "electricity",
    "engine",
    "fuel",
    "gps",
    "indoor",
    # "light",
    # "maintenance",
    "odometer",
    # "seatbelt",
    "setting",
    # "tire",
    # "typeOfRoad"
]
VALUES_TO_USE = [
    # "acceleration",
    # "accelerationLat",
    # "autonomy",
    # "autonomyMax",
    # "averageConsumption",
    # "avgElectricRangeConsumption",
    # "batteryCapacity",
    # "batteryResistance",
    # "batteryStatus",
    # "batteryVoltage",
    # "driver",
    # "electricalConsumptionSinceReset",
    # "electricalConsumptionSinceStart",
    # "endDate",
    # "endOfCharging",
    # "engineBatteryWarning",
    # "engineSpeed",
    "externalTemperature",
    # "heading",
    # "instantConsumption",
    "internalTemperature",
    # "latitude",
    # "limitSocMax",
    # "limitSocMin",
    # "limitSocStd",
    # "longitude",
    # "maxRangeChargeCounter",
    # "maxSpeed",
    # "mode",
    # "name",
    # "odometerRemaining",
    # "plugged",
    "rate",
    "power",
    "remainingTime",
    "speed",
    # "startDate",
    "status",
    "totalConsumption",
    # "turn",
    "usableBatteryLevel",
    # "value",
    # "voltage",
    # "warning",
    # "warnings",

    "batteryLevel",
    "odometer",
]

DF_TYPE_DICT = {
    "speed": np.float32,
    "heading": np.float32,
    "autonomy": np.float32,
    "batteryLevel": np.float32,
    "latitude": np.float32,
    "longitude": np.float32,
    "altitude": np.float32,
    "odometer": np.float32,
    "mode": "category",
    "status": "category",
    "plugged": bool,
}

PERF_PERIOD_MAX_POINT_INTERVAL = TD(minutes=10)

# Convertions
MILE_TO_KM = 1.60934
# Tesla
#model y rear drive
MODEL_Y_REAR_DRIVE_MIN_RANGE = 295 * MILE_TO_KM
MODEL_Y_REAR_DRIVE_MIN_KM_PER_SOC = MODEL_Y_REAR_DRIVE_MIN_RANGE / 100
MODEL_Y_REAR_DRIVE_STOCK_KJ = 291600
MODEL_Y_REAR_DRIVE_STOCK_KWH_PER_SOC = 0.81

