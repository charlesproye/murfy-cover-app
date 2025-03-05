# Transform ETL pipeline
### Content:
```
transform
├── main.py
├── config.py
├── readme.md
├── raw_tss
│   ├── bmw_raw_tss.py
│   ├── config.py
│   ├── high_mobility_raw_tss.py
│   ├── main.py
│   ├── mobilisight_raw_tss.py
│   └── tesla_raw_tss.py
├── fleet_info
│   ├── config.py
│   ├── main.py
├── processed_tss
│   ├── config.py
│   ├── data_cache
│   └── ProcessedTimeSeries.py
├── raw_results
│   ├── config.py
│   ├── data_cache
│   ├── ford_results.py
│   ├── main.py
│   ├── mercedes_results.py
│   ├── odometer_aggregation.py
│   ├── renault_results.py
│   ├── stellantis_results.py
│   ├── tesla_results.py
│   └── volvo_results.py
├── processed_results
│   ├── config.py
│   ├── data_cache
│   └── main.py
├── front_utils
│   ├── main.py
│   └── readme.md
└── vehicle_info
    ├── main.py
    └── readme.md
```
### Pipeline structure  
The entire pipeline can be considered as a single ETL.  
It takes in data time series and static vehicle data and outputs valuable time series data.  
Static data are data that remain the same throughout the vehicles lifes such as the model, default battery capacity, ect...  
Time series data... you should know what a time series is :left eye brow raised:.  
The ETL consists of multiple sub ETLs, each implemented in a single sub package.    
From a high level point of view, the ETL computes the following steps(each arrow is a step):
```
json API responses in S3         static data in DB    
         |                           |   
         V                           V   
parquet raw time series in S3     single fleet_info dataframe (not cached)   
         |                           |   
         \                           /   
          \                         /   
           \                       /   
            parquet processed_tss in S3   
                    |   
                    V   
            parquet raw_results in S3   
                    |   
                    V   
    parquet processed_results in vehicle_data   
```

> Here "XX" is the name of the data provider or a manufacturer.  

- **main.py**:   
    Runs a blocking schedueler to execute all the sub ETLs once every day.  
-  **raw_tss.XX_raw_time_series.py**:  
    goal: Provide time series data in tabular format.  
    Input: Json responses from the data providers, one json file per vehicle per day.  
    Output: A dataframe that contains unprocessed data, one dataframe(.parquet file) per brand/data provider.
    Output location: `raw_tss/XX/time_series/raw_tss.parquet` (should just be `raw_tss/XX_raw_tss.parquet`...)  
    How: Parses json responses into DataFrames and concatenates them into a single one.  
    Note: We nickname raw time series to raw_tss and not raw_ts because there are multiple time series per DF (one per vehicle).
- **raw_tss/main.py**:   
    Provides a function to:  
    - Update all the raw time series, `update_all_raw_tss`  
    - provide simple access to any raw_tss `get_raw_tss` on S3  
    Updates all the raw time series when ran as a script
- **fleet_info/main.py**:
    Goal: Provide the static data of the vehicles that BIB monitors in a single dataframe, one row per vehicle.
    Input: Multiple tables from the DB(`vehicle`, `vehicle_model`, ...).
    Output: Global `fleet_info` dataframe variable.
    Note: The output is not cached as the ETL is just a series of SQL joins and is fairly fast.
-  **processed_tss/ProcessedTimeSeries.py**:  
    Goal: Process the raw time series data.   
    Input: Raw time series and fleet info.     
    Output: A dataframe that contains processed time series, one per brand/data provider.     
    All the processed time series have:   
    -   Normalized names:   
            Raw time series columns `diagnostic.odometer` and `car.mileage` will be normalized to `odometer`.   
    -   Nomralized units: Imperial units will be converted to metric.   
    -   Segmentation columns: `in_charge`, `in_discharge`, ...   
    -   Segment indexing: `in_discahrge_idx`, `in_discharge_idx`, ...   
        Segment indexing columns should have the same name as the segment mask column they are indexing + "_idx"
        These columns are used for Split-Apply-Combine operations.
    -   Have static data joined to them (this might not be such a good idea as it first seemed...)
    Output location: `processed_tss/time_series/XX/processed_tss.parquet` on S3
- **raw_results/XX_results.py**:
    Goal: Compute "raw" valuable data from processed time series.
    Note: We call these results "raw" because they have some noise that we cannot directly show on our website.
        For example, the SoH estimation methods have some noise, 
        if we were to show the estimations right away, the SoH would some times increase which would look weird.
    Input: The processed time series.
    Output: A dataframe per data provider/manufacturer, with the columns `soh`, `level_1`, `level_2` and `level_3`.
    Level columns correspond to the soc gained in the corresponding charging level since the previous line.
    Output Location: `raw_results/XX.parquet` on S3
    Note: Most raw result implementations have a corresponding notebook that explains the reasoning steps taken to arrive to the implementation.
- **processed_results/main.py**:
    Goal: Make the results presentable.
    Input: raw results
    Output: Results aggregated by a fixed frequency (per week as of writting this readme) with outliers pruned out or clipped. 
    Also forces the SoH to be monotically decreasing per vin over odometer.
- **VehicleInfoProcessor.py**:
    goal: Update the vehicle table in the database with the last date of data for each vehicle.
    Input: The processed time series dataframes.
    Output: None.
    Output location: The vehicle table in the database.
    Steps:
        - For each vehicle, find the latest date of data in the processed time series.
        - Update the vehicle table in the database with the latest date of data for each vehicle.

