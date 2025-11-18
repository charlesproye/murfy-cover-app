# Mercedes Data Analysis

This directory contains exploratory data analysis (EDA) for Mercedes vehicle data collected through High Mobility API.

## Raw
Location: `raw/mercedes_raw_ts_EDA.ipynb`
Purpose:
- Analyze data quality and completeness
- Investigate relationships between range and battery level
- Study charging patterns and efficiency
- Explore potential SOH estimation methods

Key Findings:
- Range estimation varies with battery level
- Charging patterns show consistent behaviors
- SOH estimation requires further investigation
### Charging Information
- `charging.battery_level`: Current battery level (0.0 to 1.0)
- `charging.charging_rate`: Current charging power in kW
- `charging.estimated_range`: Estimated range in km
- `charging.max_range`: Maximum range in km
- `charging.status`: Current charging status (charging, cable_unplugged, etc.)
- `charging.smart_charging_status`: Smart charging configuration
- `charging.preconditioning_remaining_time`: Time remaining for preconditioning
- `charging.starter_battery_state`: Health status of starter battery (green, yellow, red)

### Vehicle Diagnostics
- `diagnostics.odometer`: Total distance traveled in km

### Usage Statistics
- `usage.electric_consumption_rate_since_reset`: kWh/100km since last reset
- `usage.electric_consumption_rate_since_start`: kWh/100km for current trip

## SoH Estimation

Location: `Soh estimation/mercedes_soh_estimation.ipynb`
Purpose:
- Develop and test SOH estimation models
- Compare different estimation methods
- Validate results with ground truth data

## Future Work

1. Develop robust SOH estimation methodology
2. Improve charging pattern recognition
3. Create anomaly detection system
4. Implement automated quality checks

## Related Resources

- High Mobility API Documentation
- Mercedes EQ API Specifications
- TimescaleDB Schema Documentation

## Data Overview

The Mercedes dataset includes the following key metrics:

### Charging Information
- Battery level
- Charging status
- Charging rate
- Estimated range
- Maximum range
- Smart charging status
- Preconditioning information
- Starter battery state
- Plugged-in status

### Vehicle Diagnostics
- Odometer readings

### Usage Metrics
- Electric consumption rate since reset
- Electric consumption rate since start

## Key Files

- `raw/mercedes_raw_ts_EDA.ipynb`: Notebook analyzing raw time series data from Mercedes vehicles
  - Includes data quality checks
  - Visualizations of charging patterns
  - Analysis of range estimations
  - Battery level correlations

## Data Structure

The time series data includes:
- Timestamps in UTC
- Vehicle identification (VIN)
- Multiple charging and diagnostic metrics
- Usage statistics

## Analysis Goals

1. Understand charging patterns and behaviors
2. Analyze range estimation accuracy
3. Monitor battery performance
4. Track vehicle usage patterns
5. Identify potential state of health (SOH) indicators

## Additional Resources

For implementation details and data pipeline information, refer to:
- High Mobility integration code in `src/ingestion/high_mobility/`
- Raw time series transformations in `src/transform/raw_tss/` 
