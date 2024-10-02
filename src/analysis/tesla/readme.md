# Tesla analysis
## Content
This directory is composed of three main parts:
### The `data_cache` directory:
```
├── data_cache
│   └── api_responses
│   │   ├── data_2024-06-26.csv
│   │   ├── ...
│   │   ├── data_2024-06-28.csv
│   │   ├── raw_fleet_info.json
├── initial_fleet_info.parquet
├── processed_tss.parquet
└── raw_tss.parquet
```
All the data that is not in the `data_cache/api_response` will be automatically computed.  

### The tesla modules:
```
tesla_constants.py
tesla_fleet_info.py
tesla_processed_tss.py
tesla_raw_tss.py
```

### The notebooks
The notebooks are in the `notebook` directory.


## Setup
All you need to do is to create the data_cache and api_response diretories.
```
mkdir -p data_cache data_cache/api_response
```
Then you need to doenload the Google drive `Voiture - API` directory into the `data_cache/api_response` directory.  
Then when you will run any code that is built on top of the modules, all the necessary data will be computed and cached automatically.  
