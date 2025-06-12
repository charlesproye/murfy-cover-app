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
  - `EV_DATABASE_URL`: URL of the EV data API endpoint

## Usage

```python
from ev_database import fetch_ev_data

# Fetch and process EV data
data = fetch_ev_data()
```

## Database Schema

The module interacts with the following database tables:
- `oem`: Stores Original Equipment Manufacturer information
- `make`: Stores vehicle make information
- `vehicle_model`: Stores vehicle model details
- `battery`: Stores battery specifications

## Notes

- The module automatically skips Tesla models
- Battery chemistry is standardized (e.g., NMC/NCM variations are normalized)
- All text data is stored in lowercase for consistency
- The module includes detailed logging for tracking the data processing

