# EV Database Module

This module is responsible for fetching and processing electric vehicle (EV) data from an external API and storing it in a database.

## Overview

The module fetches EV data from a configured API endpoint and processes it to store information about:
- Vehicle manufacturers (OEMs)
- Vehicle makes
- Vehicle models
- Battery specifications

## Features

- Fetches EV data from a configured API endpoint
- Processes and standardizes battery chemistry information
- Skips Tesla models (as per current implementation)
- Handles database operations for:
  - OEMs (Original Equipment Manufacturers)
  - Makes
  - Vehicle models
  - Battery specifications
- Includes error handling and logging

## Requirements

- Python environment with the following dependencies:
  - `requests`
  - Database connection utilities (from core.sql_utils)
- Environment variable:
  - `EV_DATABASE_URL`: Full URL of the EV data API endpoint (e.g., "https://api.example.com/ev-data")

## Usage

```python
from ev_database import fetch_ev_data

# Fetch and process EV data
data = fetch_ev_data()  # Returns the raw API data or None on failure
```

## API Response Format

The module expects the API to return a JSON array of vehicle objects with the following key fields:
- `Vehicle_Make`: Manufacturer name
- `Vehicle_Model`: Model name
- `Vehicle_Model_Version`: Model version/variant
- `Battery_Chemistry`: Battery chemistry type
- `Battery_Manufacturer`: Battery OEM
- `Battery_Capacity_Full`: Full battery capacity in kWh
- `Battery_Capacity_Useable`: Usable battery capacity in kWh
- `Range_WLTP`: WLTP range in km
- `Battery_Warranty_Period`: Battery warranty period
- `Battery_Warranty_Mileage`: Battery warranty mileage
- `EVDB_Detail_URL`: Source URL for the vehicle data

## Database Schema

The module interacts with the following database tables:

### oem
- `id` (UUID): Primary key
- `oem_name` (text): Original Equipment Manufacturer name

### make
- `id` (UUID): Primary key
- `make_name` (text): Vehicle make name
- `oem_id` (UUID): Foreign key to oem table

### vehicle_model
- `id` (UUID): Primary key
- `model_name` (text): Vehicle model name
- `type` (text): Model version/variant
- `make_id` (UUID): Foreign key to make table
- `oem_id` (UUID): Foreign key to oem table
- `autonomy` (numeric): WLTP range
- `warranty_date` (numeric): Battery warranty period
- `warranty_km` (numeric): Battery warranty mileage
- `source` (text): Source URL
- `battery_id` (UUID): Foreign key to battery table

### battery
- `id` (UUID): Primary key
- `battery_chemistry` (text): Standardized battery chemistry
- `battery_oem` (text): Battery manufacturer
- `capacity` (numeric): Full battery capacity
- `net_capacity` (numeric): Usable battery capacity
- `source` (text): Source URL

## Error Handling

The module handles the following error cases:
- API request failures (connection errors, timeouts)
- JSON decode errors
- Database operation failures
- General exceptions during vehicle processing

All errors are logged with appropriate error messages.

## Notes

- The module automatically skips Tesla models
- Battery chemistry is standardized (e.g., NMC/NCM variations are normalized)
- All text data is stored in lowercase for consistency
- The module includes detailed logging for tracking the data processing
