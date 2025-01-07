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
└── s3_utils.py
```

Let's go through the modules one by one to summarize what they do:

### caching_utils:
Implements caching decorators:  
```python
@cache_result(path: str, on: str, path_params: List[str] = [])
```
The decorator takes the following steps:
1.  Creates a dict of path arguments with path_params as keys and their corresponding value passed to the decorated function.  
1.  Defines the path to result cache as the path formated by this dictionnary.  
    This gives the possibility to have different cahces for different inputs to the function (see example).  
    *Note: The path must end in '.parquet'.*  
1.  If the decorated function received a kwargs `force_update` set to `True` or if there is no file at that path.   
    1.  The decorated function gets called to generate the data.  
    1.  The result is stored  
    Otherwise, the decorated function doesn't get called and the decorator returns the already cached result.  

The decorator can work both on s3 or on local storage.  
Set the argument `on` to:
-   `"s3"` to cache results on S3, if the decorated function takes in an arguement called `"bucket"` it will be used as an `S3_Bucket` instance.  
-   `"local_storage"` to sotre it locally. 

#### S3 storage example:
We declare a key format:
```python
S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_tss.parquet"
```
and we dclare our get_raw_tss function as:
```python
@cache_result(S3_RAW_TSS_KEY_FORMAT, on="s3", ["brand"])
def get_raw_tss(brand:str, bucket: S3_Bucket=S3_Bucket()) -> DF:
```
If we wanted to store a time series by brand and by vehicle we would, for example, declare the path and the function as:  
```python
S3_RAW_TS_KEY_FORMAT = "raw_ts/{brand}/time_series/{vin}.parquet"
@cache_result(S3_RAW_TSS_KEY_FORMAT, on="s3", ["brand", "vin"])
def get_raw_ts_of_vehicle(brand:str, vin:str, bucket: S3_Bucket=S3_Bucket()) -> DF:
```
On the other hand if there can only be one output to the function don't specify any parameters in the key:
```python
S3_INITIAL_FLEET_INFO_KEY = "fleet_info/tesla/initial_fleet_info.parquet"
@cache_result(S3_INITIAL_FLEET_INFO_KEY, on="s3")
def get_fleet_info(bucket: S3_Bucket=S3_Bucket()) -> DF:
```

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
