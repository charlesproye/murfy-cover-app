# Vehicle Information Ingestion

## Overview
This module handles the automated collection and processing of vehicle information from multiple OEM (Original Equipment Manufacturer) APIs. It supports BMW, HM, Stellantis, and Tesla vehicles.

## Features
- Connects to multiple OEM APIs (BMW, HM, Stellantis, Tesla)
- Processes vehicle data in bulk
- Handles authentication and API communication
- Stores vehicle information in a standardized format
- Supports filtering by owner

## Prerequisites
The following environment variables need to be set:
- BMW credentials (AUTH_URL, BASE_URL, CLIENT_ID, FLEET_ID, USERNAME, PASSWORD)
- HM credentials (BASE_URL, CLIENT_ID, CLIENT_SECRET)
- Stellantis credentials (BASE_URL, EMAIL, PASSWORD, FLEET_ID, COMPANY_ID)
- Tesla credentials (via Slack token)
- Renault credentials (requires a pkcs12.txt file for token generation, place it in src/ingestion/vehicle_info/api)
- Database connection details

## Usage

### Basic Usage
```python
# Run the ingestion for all vehicles
python3 -m src.ingestion.vehicle_info.main

# Run the ingestion for a specific owner [to be checked]
python main.py --owner "Ayvens"
```

### Process Flow
1. Connects to all OEM APIs
2. Retrieves fleet information
3. Processes vehicles through the activation service
4. Updates vehicle information in the database
5. Optionally retrieves model metadata

## Error Handling
- Graceful handling of interrupts (Ctrl+C)
- Comprehensive logging
- Cleanup of async tasks on exit

## Components
- `BMWApi`: Handles BMW vehicle data
- `HMApi`: Handles HM vehicle data
- `StellantisApi`: Handles Stellantis vehicle data
- `TeslaApi`: Handles Tesla vehicle data
- `VehicleActivationService`: Coordinates data collection from different APIs
- `VehicleProcessor`: Processes and standardizes vehicle data

## Key Functions

### Main Function
```python
async def main(owner_filter: Optional[str] = None)
```
The main entry point for vehicle processing. It:
- Initializes all API clients
- Retrieves fleet information
- Processes vehicles through the activation service
- Optionally filters by owner

### Vehicle Processing
```python
async def process_vehicles(df: pd.DataFrame)
```
Processes vehicle data by:
- Validating vehicle information
- Updating vehicle models in the database
- Handling vehicle activation status

### Model Metadata
```python
async def get_existing_model_metadata()
```
Retrieves and displays existing model metadata including:
- Make and model information
- Vehicle type
- Warranty information
- Vehicle capacity
- Image URLs

### Cleanup
```python
async def cleanup(task)
```
Handles graceful shutdown by:
- Cancelling running tasks
- Closing connections
- Logging cleanup status

## Logging
The module uses Python's built-in logging system with configurable settings through LOGGING_CONFIG.


