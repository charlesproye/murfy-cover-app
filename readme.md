# EV data analysis package

### Context:
The goal of this package is to handle every step of the data analytics service of Bib:
- data extraction
- data analysis
- data valorization

## Hierarchy of code base:
```
.
└── src
    ├── core
    ├── analysis
    ├── transform
    └── ingestion
```
### Core:
`core` implments the code that is common to any data pipeline.  
See [core documentation](core/readme.md).
### Analysis:
'analysis' contains the code to analyze the data(mostly notebooks).

### Ingestion:
'ingestion' contains the code to ingest the data from the data provider's API.
Two ways of ingestion are implemented:
-  High Mobility
-  BMW
-  Tesla
-  Mobilisight

### Transform:
>   Note:   
>       This more of an aspirational goal rather than an actual strict guideline.    
>       As of writting this readme, the pipelines are not exactly implemented in this way.    

Contains the data  pipelines implementations and a main script to run them:
```
transform
├── config.py
├── main.py
├── merge_prod_and_dev_s3_buckets.py
├── readme.md
├── ayvens
│   ├── ayvens_config.py
│   ├── ayvens_fleet_info.py
│   ├── ayvens_get_raw_tss.py
│   └── data_cache
├── bmw
│   └── bmw_raw_tss.py
├── ford_bug
├── high_mobility
│   └── high_mobility_raw_tss.py
├── stellantis
│   └── stellantis_raw_tss.py
└── tesla
    ├── tesla_config.py
    ├── tesla_fleet_info.py
    ├── tesla_processed_tss.py
    └── tesla_raw_tss.py
```
The pipelines consist in multiple steps each, ideally, implemented in a single module :
-  **Raw time series obtention step(get_raw_tss)**:
    Parse json responses into a single `raw_tss` dataframe where the values are stored as-is.  
- **Raw fleet info obtention step(fleet_info)**:
    This step consists in parsing/extracting a dataframe where each line represents a single vheicle.  
    The columns are variables that are stastic.  
    i.e: we don't need a per-timestamp value of this variable(the variable does not need to be actually static over time).  
-  **Process time series step**:
    -   Set the dtypes of raw_tss
    -   add missing columns (for ex: in_charge/dishcarge, age of vehicle)
    -   remove the useless ones.  
-  **Soh estimation step**: 
    This will most likely be different from one pipeline to another This is the aggregation step where we actually compute the soh to ceratin granularity level depending on the quality of the data.  

To launch all the the data pipelines use the following command:
```shell
python3 main.py
```
This script 

### Data provider sub packages:
The code specific to a certain data provider is put in a separate folder.
Each sub package (except `core`) has the following scripts (where XX is the name of the data provider):
```
.
├── XX_processed_ts.py
├── XX_raw_ts.py
├── XX_config.py
└── XX_fleet_info.py
```
- `XX_raw_ts`:  
    - Handles the extraction/parsing of raw time series.
    - Provides a `get_raw_tss` function that returns a single dataframe that contains all the time series.
- `XX_fleet_info`:
    - Computes a fleet_info dataframe which represents the profile of the fleet and its caching.
    - Each row corresponds to a vehicle and each row corresponds to a static property (e.g: `has_enough_data_to_compute_soh`).
    - Implements a `iterate_over_id/vin` iterator.
    - Computes the fleet_info_df when called as a script.
- `XX_processed_ts`: 
    - handles the processing of the raw time series and caching of the processed_time_series.
    - Implements a `raw_ts_of` function which returns a fully processed DF of a specific battery.
    - Implements a `iterate_over_processed_ts` on top of `XX_fleet_info.iterate_over_id/vin`.
    - Processes the raw_time_series of the fleet and caches the results when called as a script.
- `XX_config`:
    - Holds the constants values specific to that data provider like the path to a certain file.
