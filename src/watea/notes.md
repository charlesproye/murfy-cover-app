# Watea

## Soh estimation

**Observations**:
- soh estimation is not consistent enough:
    - over odometer.
    - per charges.
    - per point (although it seems like we have enough points to mitigate the noise per point).
- We don't have enough points for some regimes.

**Questions**:
- How does the knn distance influences the soh estimation?
- Could we simply replace KNNregressor with LR:
    - Segment charging regimes
    - Does the shape of the energy distribution changes over odometer?
        - Yes: probably can't just use 

**Tasks**:
- Segment charging regimes
- Does the shape of the energy distribution changes over odometer (check for all the chargign regimes)?
- Try to use LR on most common charging regime with temp, current, voltage.
- Visualize nearest neighbours in 3d plot:
    - Check the relation between variance in soh and distances in knn.
    - Try to get a certainty estimation.
