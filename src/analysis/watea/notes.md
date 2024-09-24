# Watea

## Soh estimation

**Observations**:
- soh estimation is not consistent enough:
    - over odometer.
    - per charges.
    - per point (although it seems like we have enough points to mitigate the noise per point).
- We don't have enough points for some regimes.
- KNN seems to be better than LR... but Martin says that the LR result are more coherent compared to the "norm of soh loss". 
- There doesn't seem to be much correlation between current and enerrgy added within most common regime.
- Our estimator states that: "A battery has an soh X because the median of its energy_added is X% of the energy_added of the default_100_soh batteries in the same charge region".

**Hypothesis**:
- An estimator who's total energy added over a charge regmie equals the battery capacity of the vehicle would have tbetter accuracy.

**Questions**:
- How does soc quantization effects soh estimation?
- Are there feature regions that contribute more to the noise of the output?
- Are there feature regions that have better soh correlation with odometer than others?
- Would estimators perform better without current?
- How does the knn distance influences the soh estimation?
- How to evaluate the soh estimation? 
- Why KNN seems to be better than LR?
- What other target feature could we use?

**Proposals**:
- estimation score metrics:
    - monotonicity score over nb charges
    - energy added entropy over the entire dataset
    - energy added entropy over dataset regions
    - soh
- Add another data cleaning step to retain only data points with decreasing energy_added over recharges/odometer/date
- Instead of evaluating the estimation based on the correlation betweem soh and fleet wise odometer, we could evaluate it based on vehicle wise odometer.


**Tasks**:
- Visualize all the charges sohs to get an intuative feel of the soh.
- Create a scorer/loss function to quantify/evaluate the soh estimations.
- Use odometer as target feature to see what is the best correlation possible between energy added and odometer.
- Implement estimator that takes in the entire charge.
- Iteratively:
    - Create a full sklearn Pipeline that we can hypertune from extraction to estimation.
    - Optimize the pipeline through hyperparameter tunning.
