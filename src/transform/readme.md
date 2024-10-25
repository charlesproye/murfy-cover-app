# Transform pipeline
>   Note:   
>       This more of an aspirational goal rather than an actual strict guideline.    
>       As of writting this readme, the pipelines are not exactly implemented in this way.    

### Content:
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
 └── utils 
    ├── merge_prod_and_dev_s3_buckets.py # since we can not run the pipeline on the dev bucket for limits on API we need some way to merge the prod and dev buckets
```
### Pipelines structure
The pipelines consist in multiple steps each, ideally, implemented in a single module :
Here "XX" is the name of the data provider.  
-  **Raw time series obtention step**:
    - Result should be accessible from a `get_raw_tss` function.  
    - The implementation should be in each data group's module.  
    Parse json responses into a single `raw_tss` dataframe where the values are stored as-is.  
- **Raw fleet info obtention step**:
    - Result should be accessible from an importable `fleet_info` dataframe instance.  
    - The implementation should be in a `XX_fleet_info` module.  
    This step consists in parsing/extracting a dataframe where each line represents a single vheicle.  
    The columns are variables that are stastic.  
    i.e: we don't need a per-timestamp value of this variable(the variable does not need to be actually static over time).  
-  **Process time series and step**:
    - Result should be accessible from a `get_processed_tss` function.  
    - The implementation should be in a `XX_processed_tss` module.  
    -   Merge fleet info into raw_tss
    -   Set the dtypes of raw_tss
    -   add missing columns (for ex: in_charge/dishcarge, age of vehicle)
    -   remove the useless ones.  
-  **Result step**: 
    - Result should be accessible from a `get_result` function.  
    - The results are calculated for each brands
    - Steps :
        - Calculate the SoH 
 

You can launch execute any module as a separate script if you want to run a sepcific step of the transform pipeline.  

### Running the pipelines
The pipelines are orchestrated by a blocking apscheduler.  
Note: This is the first version of the scheduler and it is destined to be refactored (it will most likely be replaced by a chrontab).  
To launch the the data pipelines use the following command:  
```shell
python3 main.py
```
The pipelines are stored as a dataframe in `config`:  
```python
BRAND_PIPELINES_DICT:dict[str, dict[str, Callable]] = {
# bmw
    "BMW": {
        "raw_tss":bmw_get_raw_tss,
    },
# high mobility
    "kia":{
        "raw_tss":hm_get_raw_tss,
    },
    ...
# stellantis
    'opel': {
        "raw_tss": stellantis_get_raw_tss,
    },
    ...
    'fiat': {
        "raw_tss": stellantis_get_raw_tss,
    },
}

BRAND_PIPELINES = DF.from_dict(BRAND_PIPELINES_DICT, orient="index")
```
