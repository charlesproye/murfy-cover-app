# Degradation Parameters

This folder contains exploratory analyses studying the **impact of various parameters on battery degradation**.

## 📋 Objective

The main objective is to identify and quantify factors that influence battery State of Health (SOH) degradation over time:
- Analyze degradation patterns at both VIN level and individual charge level
- Study the impact of charging behavior (fast charging, charge frequency)
- Evaluate the influence of battery chemistry on degradation rates
- Assess the effect of usage patterns (speed, consumption, mean SoC)
- Understand environmental factors (temperature, odometer, vehicle age)

## 📂 Folder Structure

### `degradation_parameters.ipynb`
Initial study analyzing degradation parameters at the **VIN level**:
- Impact of vehicle age (lifetime)
- Impact of odometer (distance traveled)
- Impact of battery chemistry
- Impact of charge type
- Impact of temperature (no significant conclusions)

Results are documented in the [Notion page](https://www.notion.so/bib-batteries/Etude-des-param-tres-de-d-gradations-des-batteries-1a72de3b75c78054bdb1e9ae87c6670d?pvs=4)

### `degradation_parameters.ipynb`
More granular study analyzing degradation at the **individual charge level**.

### `fast_charge.ipynb`
Focused analysis on **fast charging impact**.

### `mean_soc.ipynb`
Analysis of the impact of **mean State of Charge (SoC)** on battery degradation.

### `chemistry.ipynb`
Analysis the degradation between the different **battery chemistry**.

### `speed_consommation.ipynb`
Analysis of the impact of **driving speed and energy consumption** on battery degradation.


