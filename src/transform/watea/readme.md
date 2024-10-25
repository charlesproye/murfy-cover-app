# Watea

## Pipeline
```
soh_estimation.py
watea_config.py
watea_fleet_info.py
watea_processed_tss.py
watea_raw_tss.py
```
### watea_raw_tss
This module concatenates the seperate time series into a single one. It also drops useless columns and sets the dtypes for the columns. 
This module could (or should?) be merged within processed_tss.py as it already does some processing.  

### watea_processed_tss
This modules adds all the necessary columns for soh estimation and general EDA of the time series.  

### watea_fleet_info
This module aggegates information of the time series.  
This table can be used to report issues in the watea data extraction and to filter out time series that cannot be used in the soh estimation step.  

### soh_estimation
Implements soh estination see next chapter to understand how it works.  

### watea_config
Hold all the static variables.   

## SOH estimation

### Vocabulary:
- charging point: Aggregated time series samples over `CHARGING_POINTS_GRP_BY_SOC_QUANTIZATION` defined in `watea_constant`.
- `energy_added`: Energy received during a charging point.
- `default_100_soh energy_added`: `energy_added` of a battery with 100% soh.

### Assumptions:
Our main assumption is that: *a battery that requires less energy to gain a certain amount of soc than another battery has a lower soh*.  
Our second assumption is that: *The charges that were made at 3k odometer or less can be used to define the expected energy to gain a certain amount of soc for a 100% soh battery*.  

### Observations:
1.  The required energy to gain a certain amount of soc depends on multiple factors*.  
    **namely**:
    - voltage/soc
    - temperature
    - current
The relationship between the `energy_added` and the aforementioned factors is discontinous, forming different clusters of charging points.  
We call these clusters charging regimes as they are most likely representative of different charger types/brands and regimes (AC/DC and so on).

### Main idea:
We estimate the soh of a charging points as its `energy_added` divided by the `default_100_soh energy_added`.
The `default_100_soh energy_added` for a given charging point is estimated using Linear Regression.
Note: Ideally there would be one regressor per charging regime but here we implement only one regressor for one charging regime.

See the corresponding [notebook](../../analysis/watea/soh_estimation.ipynb) to visualize the soh estimation steps.  
