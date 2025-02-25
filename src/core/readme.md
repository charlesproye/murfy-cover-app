# Data EV core
This sub package provides all the modules needed to devlope ETLs and perform EDAs.  

## Content:
```
core
├── caching_utils.py
├── config.py
├── env_utils.py
├── console_utils.py
├── constants.py
├── ev_models_info.py
├── pandas_utils.py
├── plt_utils.py
├── readme.md
├── logging_utils.py
├── s3_utils.py
├── singleton_s3_bucket.py
├── stats_utils.py.py
└── time_utils.py
```

Let's go through the modules one by one and summarize what they do:

### caching_utils:
Provides utilities to cache results of ETLs.  
It provides a **function decorator**:  
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
It also provides a `CachedETL` **abstract base class**:
It provides the same functionalities as the decorator... but for a class.

### config:
This module stores all the variables used by the other core modules that would hurt the readablity of the code.

### console_utils:
Implements functionalities for interactions with the CLI:  
1.  ```python
    @main_decorator
    ```
    This decorator wraps the function in a try block an will catch any  
    `KeyBordInterruptException` and print `exiting...` instead of a long traceback.
    It will also call the `install` function of `rich.traceback` to print prettier, more readable, tracebacks.  
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
    This function implements all the boiler plate code for a main function that displays the output of an ETL.  
    It prints:
    - The data frame.
    - The dtypes of the columns.
    - The density of the columns (ratio of notna / dataframe len).
    - value counts
    - Descriptive statistics of numerical columns
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
        vin  soc  odometer  estimated_range                      date  charging_plug_connected charging_status charging_method  charging_ac_current  ...             version  capacity  net_capacity range  tesla_code  make  region_name  activation_status  age
        0       WBY1Z610407A12415  NaN       NaN              NaN 2024-08-18 20:26:16+00:00                    False             NaN             NaN                  NaN  ...  automaat (94ah) 5d       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        1       WBY1Z610407A12415  NaN       NaN              NaN 2024-09-27 07:03:06+00:00                    False             NaN             NaN                  NaN  ...  automaat (94ah) 5d       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        2       WBY1Z610407A12415  NaN       NaN              NaN 2024-10-03 00:00:00+00:00                    False             NaN             NaN                  NaN  ...  automaat (94ah) 5d       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        3       WBY1Z610407A12415  NaN  148854.0             71.0 2024-10-03 06:59:58+00:00                     True      nocharging      nocharging                  0.0  ...  automaat (94ah) 5d       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        4       WBY1Z610407A12415  NaN  148858.0             69.0 2024-10-03 07:14:00+00:00                     True      nocharging      nocharging                  0.0  ...  automaat (94ah) 5d       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        ...                   ...  ...       ...              ...                       ...                      ...             ...             ...                  ...  ...                 ...       ...           ...   ...         ...   ...          ...                ...  ...
        174567  WBY8P610X07F50573  NaN       NaN              NaN 2025-01-10 12:20:27+00:00                    False             NaN             NaN                  NaN  ...         i3s (120ah)       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        174568  WBY8P610X07F50573  NaN       NaN              NaN 2025-01-10 13:09:02+00:00                    False             NaN             NaN                  NaN  ...         i3s (120ah)       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        174569  WBY8P610X07F50573  NaN       NaN              NaN 2025-01-10 13:47:16+00:00                    False             NaN             NaN                  NaN  ...         i3s (120ah)       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        174570  WBY8P610X07F50573  NaN       NaN              NaN 2025-01-10 13:58:19+00:00                    False             NaN             NaN                  NaN  ...         i3s (120ah)       NaN           NaN   NaN         NaN   bmw           nl              False  NaT
        174571  WBY8P610X07F50573  NaN       NaN              NaN 2025-01-10 14:09:03+00:00                    False             NaN             NaN                  NaN  ...         i3s (120ah)       NaN           NaN   NaN         NaN   bmw           nl              False  NaT

        [174572 rows x 36 columns]
        sanity check:
                                            dtypes                                       value_counts  nuniques   count   density  memory_usage_in_MB          mean           std    min         max   median
        vin                                  category  {'WBY71AW000FM68170': 7417, 'WBY8P210607F46593...        41  174572  1.000000            0.178678           NaN           NaN    NaN         NaN      NaN
        soc                                   float32  {100.0: 1337, 99.0: 283, 98.0: 239, 97.0: 228,...       101   10602  0.060731            0.698288     68.485756     27.045305    0.0       100.0     74.0
        odometer                              float32  {151445.0: 52, 92393.0: 49, 108909.0: 29, 9241...      4102   10578  0.060594            0.698288  89825.843750  27214.189453    0.0    151649.0  88743.0
        estimated_range                       float32  {227.0: 57, 230.0: 47, 244.0: 47, 223.0: 46, 1...       361    7117  0.040768            0.698288    172.783188     77.418846    0.0       410.0    179.0
        date                      datetime64[ms, UTC]  {2024-10-03 00:00:00+00:00: 40, 2024-10-23 00:...    130616  174572  1.000000            1.396576           NaN           NaN    NaN         NaN      NaN
        charging_plug_connected                  bool                        {False: 167415, True: 7157}         2  174572  1.000000            0.174572      0.040997      0.198285  False        True      0.0
        charging_status                      category  {'nocharging': 5600, 'chargingactive': 1123, '...         6    7209  0.041295            0.175164           NaN           NaN    NaN         NaN      NaN
        charging_method                      category  {'nocharging': 5409, 'ac_type2plug': 1672, 'dc...         4    7210  0.041301            0.175025           NaN           NaN    NaN         NaN      NaN
        charging_ac_current                   float32  {0.0: 4599, 16.0: 635, 10.0: 202, 15.0: 141, 3...        19    6140  0.035172            0.698288      3.820195      7.191145    0.0        32.0      0.0
        charging_ac_voltage                   float32  {0.0: 5111, 228.0: 90, 230.0: 77, 231.0: 73, 2...        61    6124  0.035080            0.698288     37.483181     84.295624    0.0       247.0      0.0
        coolant_temperature                   float32                               {118.0: 417, 0.0: 6}         2     423  0.002423            0.698288    116.326241     13.970092    0.0       118.0    118.0
        power                                 float32                                                 {}         0       0  0.000000            0.698288           NaN           NaN    NaN         NaN      NaN
        ....
        time_diff                      timedelta64[s]  {0 days 00:00:20: 10781, 0 days 00:00:40: 7088...      9753  174531  0.999765            1.396576           NaN           NaN    NaN         NaN      NaN
        sec_time_diff                         float64  {20.0: 10781, 40.0: 7088, 60.0: 5091, 80.0: 35...      9753  174531  0.999765            1.396576   2664.136692  90738.587220    0.0  15260562.0    141.0
        in_charge                                bool                        {False: 173439, True: 1133}         2  174572  1.000000            0.174572      0.006490      0.080300  False        True      0.0
        in_discharge                             bool                        {False: 168950, True: 5622}         2  174572  1.000000            0.174572      0.032204      0.176543  False        True      0.0
        in_charge_idx                          uint16  {54: 2169, 66: 2014, 204: 1893, 60: 1845, 53: ...       459  174572  1.000000            0.349144    141.539439     73.126944      0         458    140.0    ...
        version                              category  {'i3 (120ah) 5d': 63749, 'i3 (120ah)': 19845, ...        13  153604  0.879889            0.365225           NaN           NaN    NaN         NaN      NaN
        capacity                              float64                                                 {}         0       0  0.000000            1.396576           NaN           NaN    NaN         NaN      NaN
        net_capacity                          float64                                                 {}         0       0  0.000000            1.396576           NaN           NaN    NaN         NaN      NaN
        range                                 float64                                                 {}         0       0  0.000000            1.396576           NaN           NaN    NaN         NaN      NaN
        tesla_code                           category  {'MT10A': 0, 'MT301': 0, 'MTS01': 0, 'MTS03': ...         0       0  0.000000            0.178374           NaN           NaN    NaN         NaN      NaN
        make                                 category  {'bmw': 153604, 'ford': 0, 'kia': 0, 'mercedes...         1  153604  0.879889            0.175373           NaN           NaN    NaN         NaN      NaN
        region_name                          category  {'nl': 146187, 'Germany': 7417, 'Denmark': 0, ...         2  153604  0.879889            0.175512           NaN           NaN    NaN         NaN      NaN
        activation_status                      object                           {False: 153600, True: 4}         2  153604  0.879889            6.032976           NaN           NaN    NaN         NaN      NaN
        age                           timedelta64[ns]                                                 {}         0       0  0.000000            1.396576           NaN           NaN    NaN         NaN      NaN
        total memory usage: 28.23MB.
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
Contains all the physics constants that we need.

### env_utils:
Calls `dotenv.loaddenv` when imported and provides a `get_env_var` that raises an error if the var is not in the env.  

### ev_models_info:
*Waring*:This module is deprecated, please use transform.fleet_info.main.fleet_info instead or the RDB "vehicle_model" table instead.  
Provides a dataframe that contains all the static data about EV models.
This module abstracts the exrtraction of the dataframe.  

### pandas_utils:
Provides a few helper functions to complete the pandas API.  
Please take a look at the source code where I left a bunch comments and doc strings.

### plt_utils:
Provides a few helper functions to simplify the use of the plotly API.  
Mainly, `plt_3d_df`.

### logging_utils:
Provides a `set_level_of_loggers_with_prefix` to set the log level of all the loggers whos name starts with a given prefix.  
Also makes them rich loggers which print prettier logs.  

### s3_utils:
Implements an `S3_Bucket` class that acts as a wrapper around the `boto3.client` class to (greatly) simplify CRUD operations on the S3.    
You can pass the credentials of the S3 in the constructor call to cnnect to a specific S3 or leave it empty and it will default to the credentials in the dotenv.  

### singleton_s3_bucket
This is a module that provides a default `S3_Bucket` instance called `bucket`.  
This avoids creating a new boto3 client instance when you simply need a connection from the .env creds.  

### sql_utils:
Creates a connection with the db creds provided in the env when imported.   
This connection can be imported with: `from core.sql_utils import con`.   
Provides a few utilities functions to perform actions on the RDB with pandas dataframes.  


### stats_utils:
Provides usefull stat operation impelemtation that directly input and ouput pandas objects instead of numpy ndarrays.  

### time_utils:
Provides:  
1. A contextmanager `time_to_exec` that can be used to measure the execution time of a task (must provide the name of the task).  
1. A dictionary `time_dict` that stores all the sum of the execution time for each task.  
1. A `print_time_dict` function to print all the execution times on te CLI.  
