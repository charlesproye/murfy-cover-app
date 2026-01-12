# Vehicle Activation Module

## Overview
This module handles the automated activation, deactivation, and enrichment of vehicles across multiple OEM (Original Equipment Manufacturer) APIs. It manages the full lifecycle of vehicle activation status and ensures proper data enrichment for State of Health (SoH) calculations.

## Supported OEMs
- **Tesla**: Tesla fleet telemetry API
- **BMW**: BMW Connected Drive (includes Mini)
- **Renault**: Renault Group API (includes Dacia, Nissan)
- **Stellantis**: Mobilisights API (includes Peugeot, Citroën, Opel, Fiat, DS Automobiles)
- **Volkswagen**: VW Group API (includes Audi, Cupra, Seat, Skoda)
- **Kia**: Kia Connect API
- **High Mobility (HM)**: Third-party API for Mercedes-Benz, Ford, Volvo

## Features
- **OEM-Specific Activators**: Each OEM has a dedicated activator implementing custom business logic
- **Parallel Processing**: All OEM activators run concurrently for maximum efficiency
- **Activation Status Management**: Tracks and updates vehicle activation states in the database
- **Data Enrichment**: Associates vehicles with correct model/battery information for SoH calculations
- **Activation History**: Maintains complete audit trail of activation/deactivation events
- **Smart Filtering**: Automatically identifies vehicles needing activation changes or data processing
- **Slack Notifications**: Sends alerts for vehicles missing critical model information
- **Metrics Tracking**: Records daily activation metrics per OEM

## Prerequisites

### Environment Variables
Each OEM requires specific credentials to be set:

**Tesla**:
- `TESLA_CLIENT_ID`
- `TESLA_CLIENT_SECRET`

**BMW**:
- `BMW_AUTH_URL`
- `BMW_BASE_URL`
- `BMW_CLIENT_ID`
- `BMW_FLEET_ID`
- `BMW_CLIENT_USERNAME`
- `BMW_CLIENT_PASSWORD`

**Renault**:
- `RENAULT_KID`
- `RENAULT_AUD`
- `RENAULT_CLIENT`
- `RENAULT_SCOPE`
- `RENAULT_PWD`
- Requires `pkcs12.txt` file in `src/activation/api/` for JWT token generation

**Stellantis (Mobilisights)**:
- `STELLANTIS_BASE_URL`
- `STELLANTIS_EMAIL`
- `STELLANTIS_PASSWORD`
- `STELLANTIS_COMPANY_ID`

**Volkswagen**:
- `VW_AUTH_URL`
- `VW_BASE_URL`
- `VW_ORGANIZATION_ID`
- `VW_CLIENT_USERNAME`
- `VW_CLIENT_PASSWORD`

**Kia**:
- `KIA_AUTH_URL`
- `KIA_BASE_URL`
- `KIA_API_USERNAME`
- `KIA_API_PWD`
- `KIA_API_KEY`

**High Mobility**:
- `HM_BASE_URL`
- `HM_CLIENT_ID`
- `HM_CLIENT_SECRET`

**Database**:
- Standard database connection credentials (handled by `core.sql_utils`)

**Slack**:
- `METRIC_SLACK_CHANNEL_ID` for activation alerts

## Usage

### Run All OEM Activators
```bash
python -m src.activation.main
```

### Run Specific OEM(s)
```bash
# Single OEM
python -m src.activation.main --oem tesla

# Multiple OEMs
python -m src.activation.main --oem tesla --oem bmw

# Short form
python -m src.activation.main -o renault -o stellantis
```

### Tesla Individual Activation
For individual Tesla vehicle activations (separate from fleet telemetry):
```bash
python -m src.activation.tesla_individual.main
```

## Architecture

### Core Components

#### 1. Main Entry Point (`main.py`)
- Initializes all OEM API clients
- Retrieves vehicle command table with activation instructions
- Filters vehicles needing activation changes or processing
- Runs all activators in parallel using `asyncio.gather()`
- Writes activation metrics to database
- Ensures admin-fleet relationships are maintained

#### 2. Vehicle Command (`vehicle_command.py`)
- **`get_vehicle_table()`**: Fetches all vehicles from database
- **`add_oem_column()`**: Uses VIN decoder to identify vehicle make and map to OEM
- **`update_activation_status()`**: Updates activation status based on start/end dates
- **`get_vehicle_command()`**: Orchestrates the above to produce the command dataframe

#### 3. Base Activator (`activators/base_activator.py`)
Abstract base class that all OEM activators inherit from. Provides:
- **`activate()`**: Abstract method for OEM-specific activation/deactivation logic
- **`process()`**: Default implementation for data enrichment after activation
- **`run()`**: Orchestrates activation → processing workflow
- **Helper methods**: Database status updates, activation history tracking

#### 4. OEM-Specific Activators (`activators/*.py`)
Each activator implements:
- `get_oem_name()`: Returns the OEM identifier
- `activate()`: Calls OEM API to activate/deactivate vehicles based on requested status
- `process()`: (optional override) Custom enrichment logic for this OEM

Examples: `TeslaActivator`, `BMWActivator`, `StellantisActivator`, etc.

#### 5. API Clients (`api/*.py`)
Each OEM has a dedicated API client handling:
- Authentication and token management
- API request formatting
- Response parsing
- Error handling

### Configuration

#### `config/config.py`
- **`MAKES_TO_OEM`**: Mapping of vehicle makes to OEM names
- **`MAKES_WITH_SOH_BIB`**: OEMs that provide SoH data via their API
- **`MAKES_WITH_SOH_BIB_WO_MODEL_API`**: OEMs requiring manual model association for SoH
- **`REASON_MAPPING`**: Human-readable explanations for activation failure reasons

#### `config/credentials.py`
Loads all OEM credentials from environment variables.

#### `config/settings.py`
Logging configuration and other runtime settings.

### Utilities

#### `utils/metric_utils.py`
- **`write_metrics_to_db()`**: Records daily activation counts per OEM to `fct_activation_metric` table

#### `utils/check_utils.py`
- **`ensure_admins_linked_to_fleets()`**: Maintains admin-fleet relationship integrity

#### `utils/vin_decodeur.py`
VIN decoding utilities for make/model identification.

## Process Flow

```
1. Load Vehicle Command Table
   ├─ Query all vehicles from database
   ├─ Decode VINs to identify make/OEM
   └─ Update activation status based on dates

2. Filter Vehicles
   ├─ Vehicles needing activation changes: requested_status ≠ current_status
   └─ Vehicles needing processing: activated=True AND processed=False

3. Run OEM Activators (Parallel)
   │
   ├─ For each OEM:
   │  ├─ ACTIVATION PHASE
   │  │  ├─ Filter vehicles for this OEM
   │  │  ├─ Call OEM API to activate/deactivate
   │  │  ├─ Update vehicle activation_status in database
   │  │  └─ Record activation history
   │  │
   │  └─ PROCESSING PHASE
   │     ├─ Filter activated vehicles for this OEM
   │     ├─ Check for vehicle_model_id association
   │     ├─ Mark as processed if model exists
   │     └─ Send Slack alerts for missing models (if OEM requires it)
   │
4. Post-Processing
   ├─ Write activation metrics to data warehouse
   └─ Ensure admin-fleet relationships
```

## Error Handling

- **Graceful Shutdown**: SIGINT (Ctrl+C) handler cancels all running tasks cleanly
- **Per-OEM Error Isolation**: Failures in one OEM don't affect others (parallel execution)
- **Comprehensive Logging**: Structured logging at INFO/ERROR levels for all operations
- **Database Transactions**: Proper commit/rollback handling for data integrity
- **Activation History**: All activation attempts logged for audit trail

## Database Schema

### Key Tables
- **`vehicle`**: Core vehicle table with activation fields
  - `activation_requested_status`: Desired activation state
  - `activation_status`: Current activation state
  - `activation_start_date` / `activation_end_date`: Activation window
  - `activation_status_message`: Reason for status (especially failures)
  - `activation_comment`: Additional context
  - `is_processed`: Whether vehicle data has been enriched
  - `vehicle_model_id`: Link to vehicle model (required for some OEMs' SoH)

- **`vehicle_activation_history`**: Audit trail of all activation events
  - `vehicle_id`, `activation_status`, `activation_requested_status`
  - `activation_status_message`, `oem_detail`
  - Timestamped records

- **`vehicle_model`**: Vehicle model/battery capacity information
  - Used for SoH calculations

- **`fct_activation_metric`**: Daily metrics (data warehouse)
  - Date, OEM, count of activated vehicles

## Logging

The module uses Python's standard logging with custom configuration from `LOGGING_CONFIG`:
- Logger naming: `activation.<ClassName>` for easy filtering
- Log levels: INFO for normal operations, ERROR for failures
- Structured messages with OEM context

## Slack Integration

Sends notifications to `METRIC_SLACK_CHANNEL_ID` for:
- **Missing Model Associations**: Vehicles activated but lacking model info needed for SoH
- **Manual Model Entries**: Vehicles with manually-entered model data (uncertainty warning)
- **VIN Decoder Failures**: VINs that couldn't be mapped to a supported OEM

## Tesla Individual Module

Separate subsystem (`tesla_individual/`) for handling individual Tesla vehicles outside of fleet telemetry:
- Uses different authentication mechanism
- Supports per-vehicle activation via Tesla's vehicle command API
- Useful for non-fleet Tesla vehicles or special cases
