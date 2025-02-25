# EV data analysis package

### Context:
The goal of this package is to handle every step of the data analytics service of Bib:
- data extraction
- data analysis
- data valorization

## Hierarchy of code base:
```
.
└── src
    ├── core
    ├── EDA
    ├── transform
    └── ingestion
```


## Install depenedencies

Conda is used for dedendency management.

if you want to manage python environment yourself with conda for example, the workflow is as follows:

* Create a conda env
* Activate conda env before performing any actions

You need to install all dependencies with conda

```bash
conda env update -f conda-env.yaml

#If you need to reset all the conda environnment
 conda env remove --name data_ev && conda env create --name data_ev --file conda-env.yaml
# to launch immediatly the environment:
conda env remove --name data_ev && conda env create --name data_ev --file conda-env.yaml && conda activate data_ev 

```

### Core:
`core` implments the code that is common to any data pipeline.  
See [core documentation](src/core/readme.md).
### EDA:
`EDA` contains the code to analyze the data(mostly notebooks) and to explain the reasoning steps behind the implementations in `transform`.

### Ingestion:
'ingestion' contains the code to ingest the data from the data provider's API.
Two ways of ingestion are implemented:
-  High Mobility
-  BMW
-  Tesla
-  Mobilisight

### Transform:
`tramsform` contains all the modules, for valorizing the data (cleaning, segmentation, ...).
See [transform readme](src/transform/readme.md).
