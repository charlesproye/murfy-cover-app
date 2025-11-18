# Results Quality

This folder contains exploratory analyses dedicated to evaluating the **quality of SOH (State of Health) results** calculated in the data processing pipeline.

## 📋 Objective

The main objective of this folder is to provide tools and analyses to:
- Evaluate the reliability and consistency of SOH estimates
- Identify anomalies and vehicles with unstable data
- Measure data coverage by manufacturer (OEM)
- Analyze the variance and confidence intervals of results
- Validate filtering criteria applied to results

## 📂 Folder Structure

### `confidence_interval/`
Notebooks analyzing **confidence intervals** of SOH estimates:
- `renault_confidence_intervals.ipynb`: Analysis specific to Renault vehicles
- `tesla_confidence_intervals.ipynb`: Analysis specific to Tesla vehicles
- `soh_variance.ipynb`: Analysis of SOH variance calculated before filtering

### `soh_quality/`
Notebooks analyzing the **intrinsic quality of results**:
- `check_monotonicty_of_results.ipynb`: Verification of SOH results monotonicity
- `correlation.ipynb`: Study of correlations between SOH and other variables (SOC, etc.)
- `real_autonomy_soh.ipynb`: Comparison between estimated SOH and observed real autonomy
- `soh_filtering.ipynb`: Validation and visualization of SOH results filtering criteria

### `vin_coverage/`
Notebooks analyzing **data coverage**:
- `check_vin.ipynb`: Verification of VIN validity and consistency
- `coverage_oem.ipynb`: Analysis of SOH data coverage by manufacturer (OEM)
- `nunique_vins_throughout_the_pipeline.ipynb`: Tracking of unique VINs throughout the different pipeline stages


## 🎯 Use Cases
### `quality_soh.py`
Python module containing utility functions for SOH data quality analysis:

**Main functions:**
- `analyze_coef_variation()`: Calculates SOH coefficient of variation per vehicle to identify unstable vehicles
- `check_soh_decresing()`: Verifies SOH monotonic decrease over time
- `coverage_oem()`: Calculates SOH data coverage by manufacturer
- `correlation_soc()`: Analyzes correlation between SOH and SOC (State of Charge)
- `proportion_soh_decreasing_per_km()`: Calculates the proportion of kilometer bins where SOH decreases
- `compute_ic_per_vin()`: Calculates confidence intervals per VIN
- `plot_soh()`: Generates interactive SOH vs odometer visualizations

## ⚠️ Important Notes

- The SOH results filtering criteria are currently **arbitrary** and can be modified/removed/supplemented




