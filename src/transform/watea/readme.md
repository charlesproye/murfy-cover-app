# Watea

## SOH estimation

### pipeline with power data

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


### Implementation

1.  Extract all the charging points of vehicles that have power data.
1.  Clean the charging points, see `energy_distribution` preprocessing section (marked by the "preprocessing" comment).
    1.  Compute a "regime seperation feature" (this step is a bit random and it will most likely be removed)
    1.  Compute umap dimensionality reduction features with the energy_added as target.  
        These features will cluster together points that have the same input feature(soc, temperature, voltage, ect) to target feature (energy_added) relation.  
        Note: *These clusters correspond to the charging regimes.*  
    1.  Segment these clusters using DBSCAN.
    1.  Take the most common charging regime and prune out the rest.
1.  Train a polynomial LR to predict the `default_100_soh_energy_added`.
    1.  Train the model on the entirety of the charging points.
    1.  Adjust the intercept to the < 3k odometer points.
1.  Predict the `default_100_soh_energy_added`.
1.  Divide the actual `energy_added` by the predicted `default_100_soh_energy_added`.
