# Production to Development Data Synchronization

This module provides functionality to synchronize vehicle model data from the production database to the development database, ensuring that development data stays up-to-date with production while preserving existing development configurations.

## Overview

The `prod_to_dev.py` script facilitates the synchronization of vehicle model information between production and development environments. It's designed to intelligently update development database records while maintaining data integrity and preventing unwanted overwrites.

## Features

### Data Matching Strategy
The script employs two different matching strategies to identify corresponding records:

1. **Version-Based Matching**
   - Primary matching method
   - Matches records based on the `version` field
   - Used when version numbers are available in both environments

2. **Model Name and Type Matching**
   - Fallback matching method
   - Matches records based on `model_name` and `type` fields
   - Used when version numbers are not available

### Field Updates
- Preserves critical fields:
  - `id`
  - `created_at`
  - `updated_at`
  - `oem_id`
  - `make_id`

- Updates the following vehicle information:
  - `version`
  - `model_name`
  - `type`
  - `capacity`
  - `net_capacity`
  - `autonomy`
  - `warranty_km`
  - `warranty_date`
  - `battery_oem`
  - `battery_chemistry`
  - `battery_name`
  - `source`

## Usage

### Running the Script


### Prerequisites
- Properly configured database connections in environment variables
- Required Python packages:
  - pandas
  - sqlalchemy
  - numpy
  - logging

## Technical Details

### Main Functions

1. `get_injection_data()`
   - Retrieves data from both production and development databases
   - Performs data matching and merging
   - Handles NULL values and data type conversions
   - Returns a DataFrame ready for injection

2. `update_vehicle_models()`
   - Performs bulk updates to the development database
   - Uses SQLAlchemy for database operations
   - Implements error handling and logging
   - Reports update statistics
