# Transform ETL pipeline
>   Note:   
>       This is more of an aspirational goal of how the pipeline should be structured rather than an actual strict guideline.    
>       As of writting this readme, the pipelines are not exactly implemented in this way.    

### Content:
```
transform
.
├── main.py
├── config.py
├── readme.md
├── fleet_info
│   ├── ayvens_fleet_info.py
│   ├── config.py
│   ├── main.py
│   └── tesla_fleet_info.py
├── processed_tss
│   ├── bmw_processed_tss.py
│   ├── config.py
│   ├── high_mobility_processed_tss.py
│   ├── main.py
│   └── tesla_processed_tss.py
├── raw_tss
│   ├── bmw_raw_tss.py
│   ├── config.py
│   ├── high_mobility_raw_tss.py
│   ├── main.py
│   └── tesla_raw_tss.py
└── watea
    ├── data_cache/
    ├── notes.md
    ├── readme.md
    ├── soh_estimation.py
    ├── watea_config.py
    ├── watea_fleet_info.py
    ├── watea_processed_tss.py
    └── watea_raw_tss.py
```
### Pipeline structure  
The entire pipeline can be considered as a single ETL.  
It takes in data of the vehicles from the data providers and outputs valuable informations(called `results`) in our database.  
The ETL consists in multiple sub ETLs, each implemented in a single module.    
For each step of the pipeline, there is a main function that orchestrates the execution of the step and performs some steps that are common to all the sub ETLs.
For this reason, you should always get the output of the step from the main module instead of a sub ETL module of the same step even if you won't use the other functions of the other sub ETLs modules.  
Here "XX" is the name of the data provider.   
- **main.py**:   
    goal:    
        Orchestrate the execution the pipeline.    
        Runs a blocking schedueler to execute the sub ETLs in the correct order once every day.  
-  **XX_raw_time_series.py**:  
    goal: Provide data in tabular format.  
    Input: Json responses from the data provider.  
    Output: A dataframe that contains unprocessed data. The data should be identical to the responses of the data providers.    
    Output location: `raw_tss/XX/time_series/raw_tss.parquet` (should just be `raw_tss/XX_raw_tss.parquet`...)  
    How: Parses json responses into DataFrames and concatenates them into a single one.  
- **raw_tss.main**:   
    goal: Implement a function:
        - `get_processed_tss(brand)`: provides access to any processed time series.
        - `update_all_processed_tss()`: updates all the processed time series.
    This module does not perform any extra step on the sub ETLs it orchestrates.  
    So you *could* directly use the output of the sub ETLs, but you *should* not in case a change is done in this main.
    Import `raw_tss` from this module to get the raw time series dataframe: `from transform.raw_tss.main import get_raw_tss`.
    Run it as a script to update all the raw time series.
- **XX_fleet_info.py**:
    goal: Provide a dataframe to get the model, version, capacity and range of the vehicles.  
    Input: (at least one)Table from each client on their fleet.    
    Output: A single dataframe where each line represents a single vehicle.    
    Output location: The output is not stored.
- **fleet_info.main**:  
    goal: Provide a single fleet info dataframe that represents all the fleets.  
    Input: fleet infos provided by the sub ETLs.  
    Output: A single fleet info dataframe that represents all the fleets.  
    Takes care of updating the "vehicle" table in the database.  
    How: Concatenates all the fleet info dataframes into a single one and left merges models_info onto the result.  
    Because of the extra steps performed on the fleet info, you *should* not use the output of the sub ETLs directly but the output of this main.
    Import `fleet_info` from this module to get the fleet info dataframe: `from transform.fleet_info.main import fleet_info`.
    Run it as a script to update the vehicle table in the database.
-  **ProcessedTimeSeries.py**:  
    Input: Raw time series and fleet info.  
    Output: A dataframe that contains processed data time series.  
            The dataframe should have a `date`, `soc` and `odometer` columns.  
    Output location: `processed_tss/time_series/XX/processed_tss.parquet`  
    Steps:  
        - Rename columns to be consistent across brands  
        - Drop unused columns
        - Set the dtypes of the time series
        - add missing columns (for ex: in_charge/dishcarge, cum energy added, cum energy spent, ...)  
        - Merge fleet info into the time series
- **results.py**:
    goal: Store the results in the database.
    Input: The processed time series dataframes.
    Output: None.
    Output location: The results table in the database.
    Steps:
        - For each vehicle, find the latest date of data in the processed time series.
        - Update the vehicle table in the database with the latest date of data for each vehicle.
- **VehicleInfoProcessor.py**:
    goal: Update the vehicle table in the database with the last date of data for each vehicle.
    Input: The processed time series dataframes.
    Output: None.
    Output location: The vehicle table in the database.
    Steps:
        - For each vehicle, find the latest date of data in the processed time series.
        - Update the vehicle table in the database with the latest date of data for each vehicle.

