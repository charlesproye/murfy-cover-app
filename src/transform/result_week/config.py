import pandas as pd

UPDATE_FREQUENCY = pd.Timedelta(days=7)


# Arbitrary decision based on : Odometer(km) - 0.9 / 200,000 km - 0.65 - MVaz 01/2026
MIN_SOH_REGARDING_ODOMETER_PENT = -0.00000125
MIN_SOH_REGARDING_ODOMETER_INTERCEPT = 0.9
