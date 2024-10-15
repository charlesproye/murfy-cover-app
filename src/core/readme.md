# Data EV core
This sub package provides all the modules needed to devlope ETLs and perform EDAs.  

## Content:
```
core
├── caching_utils.py
├── config.py
├── console_utils.py
├── constants.py
├── data_cache
│   └── models_info.csv
├── ev_models_info.py
├── pandas_utils.py
├── plt_utils.py
├── readme.md
├── s3_utils.py
└── time_series_processing.py
```

Let's go through the modules one by one to summarize what they do:

### caching_utils:
Implements caching decorators:  
```python
@cache_results_to_s3(path_template: str, path_params: List[str]=[])
```
This decorator takes in a path that can be formatted by the arguments of the decoratated function.  
The function must take in an `S3_Bucket` instance like called `bucket` as parmater.  
The decorator first checks if the key (file in S3) formatted by the specified arguments exists.  
If so, the decorator returns the cached result.  
Otherwise, the decorator calls the wrapped function, caches its output for future calls and then returns the output.  
You can pass a `force_update` set to `True` to force the decorator to call the wrapped function an cache the result even if it was already cached.  
The formatting of the key through arguments allows to cache the result of different function inputs.  
For example we want to store a raw_tss for each EV brand.  
Sp we declare a key format in `core/confing.py`
```python
S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_tss.parquet"
```
and we dclare our get_raw_tss function as:
```python
@cache_result_in_s3(S3_RAW_TSS_KEY_FORMAT, ["brand"])
def get_raw_tss(brand:str, bucket: S3_Bucket=S3_Bucket()) -> DF:
```
If we wanted to store a time series by brand and by vehicle we would, for example, declare the path and the function as:  
```python
S3_RAW_TS_KEY_FORMAT = "raw_ts/{brand}/time_series/{vin}.parquet"
@cache_result_in_s3(S3_RAW_TSS_KEY_FORMAT, ["brand", "vin"])
def get_raw_ts_of_vehicle(brand:str, vin:str, bucket: S3_Bucket=S3_Bucket()) -> DF:
```
On the other hand if there can only be one output to the function don't specify any parameters in the key:
```python
S3_INITIAL_FLEET_INFO_KEY = "fleet_info/tesla/initial_fleet_info.parquet"
@cache_result_in_s3(S3_INITIAL_FLEET_INFO_KEY)
def get_fleet_info(bucket: S3_Bucket=S3_Bucket()) -> DF:
```
The second one will be for local storage.  
As of writting this readme, the local storage alternatives are `singleton_data_caching` and `instance_data_caching`.  
They will eventually be replace by a `cache_result_localy` once Wateas code gets refactored or removed.  
Ideally we would have only one decorator that can cache the result locally or remotly or both.  

### console_utils:
Implements functionalities for interacting with the CLI:  
1.  ```python
    @main_decorator
    ```
    This decorator wraps the function in a try block an will catch any  
    `KeyBordInterruptException` and print `exiting...` instead of a long traceback.
    It will also call the `install` function of `rich.traceback` to print prettier more readable tracebacks.  
    Here are a few comparaisons of a main with the decorator:
    ```
    ╭────────────────────────────────────────────── Traceback (most recent call last) ───────────────────────────────────────────────╮  
    │ /home/mauro/bib/data_ev/src/transform/ayvens/ayvens_fleet_info.py:57 in <module>                                               │  
    │                                                                                                                                │  
    │ ❱ 57 │   single_dataframe_script_main(get_fleet_info, force_update=True)                                                      │  
    │                                                                                                                                │  
    │ /home/mauro/bib/data_ev/src/core/console_utils.py:59 in wrapper                                                                │  
    │                                                                                                                                │  
    │ ❱ 59 │   │   │   main_func(*args, **kwargs)  # Pass the arguments to the original function                                     │  
    │                                                                                                                                │  
    │ /home/mauro/bib/data_ev/src/core/console_utils.py:66 in single_dataframe_script_main                                           │  
    │                                                                                                                                │  
    │ ❱ 66 │   df:DF = dataframe_gen(**kwargs)                                                                                       │  
    │                                                                                                                                │  
    │ /home/mauro/bib/data_ev/src/core/caching_utils.py:74 in wrapper                                                                │  
    │                                                                                                                                │  
    │ ❱  74 │   │   │   │   data: Union[DF, Series] = data_gen_func(*args, **kwargs)                                                 │  
    │                                                                                                                                │  
    │ /home/mauro/bib/data_ev/src/transform/ayvens/ayvens_fleet_info.py:27 in get_fleet_info                                         │  
    │                                                                                                                                │  
    │ ❱ 27 │   raise ValueError("tess")                                                                                              │  
    ╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯  
    ValueError: tess
    ```
    ```
    (data_ev) (base) ➜  watea git:(dev) ✗ python3 processed_watea_ts.py 
    Working... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   0% -:--:--
    KeyboardInterrupt, exiting...
    ```
1.  ```python
    @main_decorator
    def single_dataframe_script_main(dataframe_gen: Callable[[bool], DF], **kwargs):
    ```
    This function implements all the boiler plate code for a main function that generates a dataframe.  
    It prints:
    - The DF content.
    - The dtypes of the columns.
    - The density of the columns (ratio of notna / dataframe len).
    - The total memory usage of the DF.
    The `kwargs` will be passed the `dataframe_gen` function.  
    Example:
    ```python
    if __name__ == "__main__":
        single_dataframe_script_main(
            get_raw_tss,
            bucket=S3_Bucket(),
            force_update=True,
        )
    ```
    Output:
    ```
    key                date_of_value average_mileage_per_week_km  ... weighted_avg_energy_consumption_yesterday_kWh/100km                vin
    0           2024-08-18T20:26:16Z                         NaN  ...                                                NaN   WBY1Z610407A12415
    1    2024-09-27T07:03:06.846000Z                         NaN  ...                                                NaN   WBY1Z610407A12415
    2           2024-10-03T00:00:00Z                         NaN  ...                                               None   WBY1Z610407A12415
    3           2024-10-03T06:59:58Z                         NaN  ...                                                NaN   WBY1Z610407A12415
    4           2024-10-03T07:14:00Z                         NaN  ...                                                NaN   WBY1Z610407A12415
    ..                           ...                         ...  ...                                                ...                 ...
    0    2024-06-07T11:22:17.525000Z                         NaN  ...                                                NaN   WBY8P610X07D31659
    1           2024-09-11T10:58:04Z                         NaN  ...                                                NaN   WBY8P610X07D31659
    2           2024-10-01T11:55:04Z                         NaN  ...                                                NaN   WBY8P610X07D31659
    3           2024-10-03T00:00:00Z                         NaN  ...                                               None   WBY8P610X07D31659
    4                            NaN                           0  ...                                                NaN   WBY8P610X07D31659

    [3600 rows x 45 columns]
    all columns:
                                                        dtype   density
    key                                                                 
    date_of_value                                       object  0.903333
    average_mileage_per_week_km                         object  0.091667
    avg_electric_range_consumption_kWh/100km            object  0.043611
    basic_charging_modes                                object  0.096667
    battery_voltage_V                                   object  0.003333
    brand                                               object  0.096667
    ...
    power_kW                                            object  0.000000
    remaining_mileage_km                                object  0.000000
    soc_customer_target_percent                         object  0.137222
    teleservice_status                                  object  0.107222
    travelled_distance_yesterday                        object  0.000000
    weighted_avg_energy_consumption_yesterday_kWh/1...  object  0.000000
    vin                                                 object  1.000000
    total memory usage: 1.32MB.    
    ```

1. ```python
    def parse_kwargs(cli_args: dict[str, dict[str, str]] = [], **kwargs):
    ```
    This function implements the boiler plate argparse code and parses the CLI arguments to returns a dictionnary of string keys and primitive python objects.  
    This allows you to pass for example a list in python format in the CLI and get a list as a value in the dictionnary.  
    Example:
    ```python
    MAIN_KWARGS = {
        "--log-level": {
            "default": "INFO",
            "type": str,
            "help": "Set the logging level (e.g., DEBUG, INFO)",
        },
        "--pipelines": {
            "required": False,
            "help": "Specifies pipeline or list of pipelines to run."
        },
        "--steps": {
            "required": False,
            "help": "Specifies pipeline step or list of pipelines steps to run."
        },
        "--print_pipelines": {
            "default": False,
            "help": "Flag to print the pipelines that will be executed."
        },
    }
    ```
    Usage in python:
    ```python
    cli_kwargs = parse_kwargs(MAIN_KWARGS)
    print(cli_kwargs)
    ```
    First usage in CLI:
    ```
    (data_ev) (base) ➜  transform git:(dev) ✗ python3 main.py 
    {'log_level': 'INFO', 'print_pipelines': False}
    ```
    Second usage in CLI:
    ```
    (data_ev) (base) ➜  transform git:(dev) ✗ python3 main.py --level="DEBUG" print_pipelines=True
    {'level': 'DEBUG', 'print_pipelines': False, 'log_level': 'INFO'}
    ```
    
### constants:
Contains all the physics constants.

### data_cache/models_info:
Contains all the static data about EV models.

### ev_models_info:
Provides a dataframe already parsed from pd.read_csv.  
This module abstracts the exrtraction of the dataframe.  
This way if the csv was moved else where we wouldn't need to refactor all the code that uses this dataframe.  

### pandas_utils:
Provides a few helper functions to complete the pandas API.  

### plt_utils:
Provides a few helper functions to simplify the use of the plotly API.  

### s3_utils:
Implements an `S3_Bucket` class that acts as a wrapper around the `boto3.client` class to (greatly) simplify CRUD operations on the S3.    
You can pass the credentials of the S3 in the constructor call to cnnect to a specific S3 or leave it empty and it will default to the credentials in the dotenv.  

### time_series_processing:
Provides a bunch of, you guessed it, time series processing functions.  
Mainly:
1. ```python
    def compute_cum_integrals_of_current_vars(vehicle_df: DF) -> DF:
    ```
    Computes and adds to the dataframe cumulative energy (in kwh) and charge (in C).  
    If `power` is not present, `cum_energy` will not be computed.  
    If `current` is not present, `cum_charge` will not be computed.  

1. ```python
    def cum_integral(power_series: Series, date_series=None) -> Series:
    ```
    Description:
    Computes the cumulative of the time series by using cumulative trapezoid and a date series.
    Parameters:
    power_col: name of the power column, must be in kw.
    date_series: optional parameter to provide if the series is not indexed by date.
1.  ```python
    def high_freq_in_discharge_and_charge_from_soc_diff(vehicle_df: DF) -> DF:
    ```
    Computes and adds to the dataframe two boolean columns `in_charge` and `in_discharge`.  
    The reason why there is not just one column is because sometimes we are not sure that the vehicle is in charge nor in discharge.  
    In which case both of them are at `False`.  
    The columns are computed from soc and date.  
1.  ```python
    def low_freq_compute_charge_n_discharge_vars(vehicle_df: DF) -> DF:
    ```
    Same as the previous version with extra steps to handle time series with data points that have more than 6 hours in between.
1.  ```python
    def perf_mask_and_idx_from_condition_mask(
            vehicle_df: DF,
            src_mask:str,
            src_mask_idx_col_name="{src_mask}_idx",
            perf_mask_col_name="{src_mask}_perf_mask",
            max_time_diff:TD|None=None
        ) -> DF:
    ```
    This function implements a processing steps that aims at improving the results of aggregations of periods(charges, trips, ...).  
    The period must be represented by a mask/bool column.  
    It will add two columns with the names `src_mask_idx_col_name` and `perf_mask_col_name`.  
    `src_mask_idx_col_name` Will by used to `groupby` the dataframe.  
    `perf_mask_col_name` is a mask that equal to the `src_mask`(in_discharge for ex.) with the leading and trailing socs trimmed off.  
    The need for this processing step is caused by the quantization level of the soc which is usually 1%:  
    Let a A and B be the leading and trailing soc values respectively.  
    If we use the soc diff in our aggregation we have a +/- 2% soc margin of error.  
    By taking the next soc after A and the soc before B we are sure that the battery actually went trhoug those A + 1 and B - 1 values.  

