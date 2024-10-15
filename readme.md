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
    ├── analysis
    ├── transform
    └── ingestion
```
### Core:
`core` implments the code that is common to any data pipeline.  
See [core documentation](src/core/readme.md).
### Analysis:
'analysis' contains the code to analyze the data(mostly notebooks).

### Ingestion:
'ingestion' contains the code to ingest the data from the data provider's API.
Two ways of ingestion are implemented:
-  High Mobility
-  BMW
-  Tesla
-  Mobilisight

### Transform:
See [transform readme](src/transform/readme.md).
