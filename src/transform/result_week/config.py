import pandas as pd

UPDATE_FREQUENCY = pd.Timedelta(days=7)

# Eval strings to filter raw results SoH per brand 
# Use eval/where instead of query because we don't want to remove lines containing other results (such as odometer/charging levels).
SOH_FILTER_EVAL_STRINGS: dict[callable] = {
    "tesla": "SOH = SOH.where(SOC_DIFF > 40 & SOH.between(0.75, 1.05))",
    "volvo-cars": "SOH = SOH",
    "renault": "SOH = SOH",
    "ford": "SOH = SOH",
    "mercedes-benz": "SOH = SOH",
    "bmw": "SOH = SOH",
    "kia": "SOH = SOH",
    "stellantis": "SOH = SOH",
    "tesla-fleet-telemetry": "SOH = SOH.where(SOC_DIFF > 5 & SOH.between(0.91, 1.1))",
    "volkswagen": "SOH = SOH"
}
