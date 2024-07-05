# Link by car package

### Context:
The goal of this package is to provide the tools to evaluate the performance of EV batteries to, in turn, evaluate their value.  
The package handles:
1. data fetching from the API
1. EDA of the raw data 
1. processing of the data
1. plotting of processing steps to properly interpret the results

### Setup:
Makes sure that link by car credentials are present in the .env file.  
The .env file should contain ``LINKBYCAR_USERNAME`` and ``LINKBYCAR_PASSWORD``.
```shell
mkdir -p data_cache
mkdir -p data_cache/html_plots
mdkir -p data_cache/jsons
mdkir -p data_cache/plots
mdkir -p data_cache/tesla_api
mdkir -p data_cache/vehicles_info
mdkir -p data_cache/vehicles_raw_time_series
python3 download_raw_api_time_time_series.py
```

### Scripts:

- ``constants_variables.py``  
- ``download_raw_api_time_time_series.py``:  
    Downloads raw time series from the API into `data_cache/raw_time_series`.  
    By default, all of the time series of the enabled EVs are downloaded.  
    You can provide VINs as arguments when calling the script to only download these specific cars time series.
    
- ``fleet_vehicles_info.py``:  
    Provides a dataframe representing the entire fleet.    
    Each row corresponds to a vehicle, the columns provide info on those   vehicles.  
    The dataframe can be accessed from other scripts with the following line:    
    ```python
    from fleet_vehicles_info import get_vehicles_info_df
    ```

- ``linkbycar_utils.py``  

- ``matplotlib_plt_time_series.py``  
    Plots tesla car or bmw car or other car if vin is provided as arg using matplotlib.

- ``plt_plotly_time_series.py``  
    Plots tesla car or bmw car or other car if vin is provided as arg using plotly.  
    The plot is exported as interactive html file into `data_cache/html_plots`.  
    

- ``process_tesla.py``:  
    Evaluates the performance of the tesla vehicles.  

- ``process_bmw.py``:  
    Started as copy of ``process_tesla.py``, evaluates the performance of the bmw vehicles.  
