# EV data analysis  package

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
    ├── tesla
    └── watea
```
### Core:
`core` contains the code that is common to any data provider.
```
core
├── argparse_utils.py
├── caching_utils.py
├── constant_variables.py
├── perf_agg_processing.py
├── plt_utils.py
└── time_series_processing.py
```
- `argparse_utils`: 
    Provides a `parse_args` function which converts the arguments passed to the script as a dict of primitives. 
    This is used to make the scripts modular without having to comment/comment out pieces of code.
- `caching_utils`:
    Abstract the implementation of caching operations away from data provider specific code.
- `perf_agg_processing`:
    Implements operation common to the data provider sub packages of period performances such as charging and motion periods.
- `time_series_processing`:
    Implements operation common to the data provider sub packages of time series such as charging and motion periods.  
The other modules are pretty self explanatory.

### Data provider sub packages:
The code specific to a certain data provider is put in a separate folder.
Each sub package (except `core`) has the following scripts (where XX is the name of the data provider):
```
.
├── data_cache
│   ├── fleet_info
│   ├── processed_time_series
│   └── raw_time_series
├── plt_XX.py
├── processed_XX_ts.py
├── raw_XX_ts.py
├── XX_constants.py
├── XX_fleet_info.py
└── XX_perfs.py
```
- `raw_XX_ts`:  
    - Handles the extraction of raw time series.
    - Implements a `raw_ts_of` function which returns a DF of a specific battery as provided by the data provider.
    - extracts the fleet's raw data when called as a script.
- `XX_fleet_info`:
    - Computes a fleet_info dataframe which represents the profile of the fleet and its caching.
    - Each row corresponds to a vehicle and each row corresponds to a static property (e.g: `has_enough_data_to_compute_soh`).
    - Implements a `iterate_over_id/vin` iterator.
    - Computes the fleet_info_df when called as a script.
- `processed_XX_ts`: 
    - handles the processing of the raw time series and caching of the processed_time_series.
    - Implements a `raw_ts_of` function which returns a fully processed DF of a specific battery.
    - Implements a `iterate_over_processed_ts` on top of `XX_fleet_info.iterate_over_id/vin`.
    - Processes the raw_time_series of the fleet and caches the results when called as a script.
- `XX_constants`:
    - Holds the constants specific to that data provider.
- `XX_perfs`:
    - Computes the periodic performances of the vehicle such as trip/motion, charging performances.
    - Computes the performances of the fleet when called as a script.
- `plt_XX`:
    - Implements all the plotting specific to that data provider.
    - Calls list of plotting functions on any vehicle passed as arguments when called as script.
    - (WIP)
