# Transform ETL pipeline
>   Note:   
>       This more of an aspirational goal rather than an actual strict guideline.    
>       As of writting this readme, the pipelines are not exactly implemented in this way.    

### Content:
```
transform
.
├── ayvens
│   └── test.ipynb
├── bmw
│   └── test.ipynb
├── config.py
├── fleet_info
│   ├── ayvens_fleet_info.py
│   ├── fleet_info_config.py
│   └── test.ipynb
├── main.py
├── raw_tss
│   ├── bmw_raw_tss.py
│   ├── high_mobility_raw_tss.py
│   ├── raw_tss.py
│   ├── stellantis_config.py
│   ├── stellantis_raw_tss.py
│   └── tesla_raw_tss.py
├── readme.md
├── tesla
│   ├── tesla_config.py
│   ├── tesla_fleet_info.py
│   └── tesla_processed_tss.py
├── utils
│   └── merge_prod_and_dev_s3_buckets.py# since we can not run the pipeline on the dev bucket for limits on API we need some way to merge the prod and dev buckets
└── watea
    ├── data_cache
    ├── notes.md
    ├── readme.md
    ├── soh_estimation.py
    ├── test.ipynb
    ├── watea_config.py
    ├── watea_fleet_info.py
    ├── watea_processed_tss.py
    └── watea_raw_tss.py
```
### Pipeline structure  
The ETL consists in multiple sub ETLs, each implemented in a single module.    
Here "XX" is the name of the data provider.    
-  **XX_raw_time_series.py**:  
    Input: Json responses from the data provider.  
    Output: A dataframe that contains unprocessed data. The data should be identical to the responses of the data providers.    
    Output location: `raw_tss/XX/time_series/raw_tss.parquet` (should just be `raw_tss/XX_raw_tss.parquet`...)  
    How: Parses json responses into DataFrames and concatenates them into a single one.  
- **XX_fleet_info.py**:
    Input: (at least one)Table from each client on their fleet.    
    Output: A single dataframe where each line represents a single vehicle.    
    Output location: `fleet_info/XX/fleet_info.parquet`    
    Steps:    
        - Parses tabular data into DataFrames.    
        - concatenates them into a single one.    
        - Extracts extra data from data providers such as DAT.    
-  **XX_processed_tss.py**:  
    Input: Raw time series and fleet info.  
    Output: A dataframe that contains processed data time series.  
            The dataframe should have a `date`, `soc` and `odometer` columns.  
    Output location: `processed_tss/time_series/XX/processed_tss.parquet`  
    Steps:  
        - Rename columns to be consistent across brands  
        - Set the dtypes of raw_tss  
        - Merge fleet info into raw_tss  
        - add missing columns (for ex: in_charge/dishcarge, age of vehicle)  
        - remove the useless ones.    
-  **XX_results.py**(Still not implemented):  
    Output: One  or  multiple tables.
  
You can launch execute any module as a separate script if you want to run a sepcific step of the transform pipeline.    
  
### Running the pipelines  
Each sub ETL can be run as a separate script.  
To do so, run a main script in a sub directory of `transform`.  
Main scripts aggregate the results of the sub ETLs and run the results ETL.  
`transform/main.py` is the entry point to start a scheduler that launches the whole pipeline every day.
`transform/raw_tss/main.py` is a script that runs all the raw time series ETLs.  
`transform/fleet_info/main.py` is a script that runs all the fleet info ETLs.  
`transform/processed_tss/main.py` is a script that runs all the processed time series ETLs.  
`transform/results/main.py` is a script that runs the results ETL.  
