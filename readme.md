# EValue project

Author: Bib Batteries

### Context:

This project is composed of 4 elements:

- EV data analysis package
- DB models: an ORM app managing how data is stored in dbs
- api_external: a RESTful API to share the results of data analysis
- a frontend to display th results

## 1) EV data analysis package

The goal of this package is to handle every step of the data analytics service of Bib:

- data extraction
- data analysis
- data valorization

### Hierarchy of code base:

```
.
└── src
    ├── core
    ├── EDA
    ├── transform
    └── ingestion
```

### Install depenedencies

Install [uv](https://github.com/astral-sh/uv?tab=readme-ov-file#installation)

```bash
# To install all deps
uv sync --locked --all-extras

# Only specific ones (like transform only)
uv sync --locked --extra transform
```

### Core:

`core` implements the code that is common to any data pipeline.
See [core documentation](src/core/readme.md).

### EDA:

`EDA` contains the code to analyze the data (mostly notebooks) and to explain the reasoning steps behind the implementations in `transform`.

### Ingestion:

'ingestion' contains the code to ingest the data from the data provider's API.
Two ways of ingestion are implemented:

- High Mobility
- BMW
- Tesla
- Mobilisight

### Transform:

`tramsform` contains all the modules, for valorizing the data (cleaning, segmentation, ...).
See [transform readme](src/transform/readme.md).

### Data Flow

1. Raw data from Tesla vehicles is ingested via Kafka
2. Data is temporarily stored in S3 in a buffer structure by VIN
3. Every night at midnight (UTC):
   - All data from the previous day is compressed
   - Compressed files are organized by date (YYYY/MM/DD)
   - Temporary files are cleaned up

### Storage Structure

```
s3://bucket/
├── temp/                 # Temporary storage during the day
│   └── {VIN}/           # One directory per vehicle
│       └── data.json    # Raw data files
└── compressed/          # Compressed historical data
    └── YYYY/           # Year
        └── MM/        # Month
            └── DD/    # Day
                └── {VIN}.parquet  # One file per vehicle per day
```

### Compression Strategy

- Compression runs daily at midnight UTC
- Only data from the previous day (UTC) is compressed
- Data is converted to Parquet format for efficient storage and querying
- Original JSON files are deleted after successful compression
- Each vehicle gets one compressed file per day

### Performance Considerations

- Buffer size: 1000 messages per vehicle
- Buffer flush interval: 30 seconds
- Daily compression reduces system load during operational hours
- Parquet format provides better query performance and compression ratio

### Testing strategy

- Tests are defined in the [tests](./tests/) folder.
- We use **pytest** and the settings defined in [.vscode/settings.json](./.vscode/settings.json) to run tests (either from the CLI or directly in VS Code).
- To debug scripts or APIs, you can use the VS Code launcher configured in [.vscode/launch.json](./.vscode/launch.json).
  We usually store API entries and scripts that require specific parameters in this file; otherwise, Python scripts that don’t need parameters can be run on the fly in VS Code.

## 2) DB Models

All database models are defined in [src/db_models](./src/db_models/) and defined with SQLAlchemy. It manages dbs/tables used by both data analyse and external_api.

You can check migrations with:

```bash
uv run alembic -c src/db_models/alembic.ini current
uv run alembic -c src/db_models/alembic.ini heads
```

Generate alembic commits:

```bash
uv run alembic -c src/db_models/alembic.ini revision --autogenerate -m "<COMMIT_MESSAGE>"
```

Run migrations with:

```bash
uv run alembic -c src/db_models/alembic.ini upgrade head
```

## 3) Backend external_api

Project can be found in ./src/external_api, it contains its own documentation.

## 4) Frontend (previously bib-leasing)

Project can be found in ./frontend, it contains its own documentation.

## 5) Other

### Code format

We use precommit (configuration in ./.pre-commit-config.yaml file) to apply linters/formatter before committing.

#### Python

We use ruff as linter and formatter:

```bash
uv run ruff format
uv run ruff check --format
```

You can add this to VSCode on save (`cmd + shift + P` -> `Open User Settings (JSON)`):

```json
    "[python]": {
        "editor.formatOnSave": true,
        "editor.defaultFormatter": "charliermarsh.ruff",
        "editor.codeActionsOnSave": {
            "source.organizeImports": "explicit"
        }
    }
```

##### Notebooks cleanup

Do not commit large notebooks to the repo, try to strip output before commit.
If you've enabled pre-commit (`pre-commit install`), `nbstripout` will run automatically.
Otherise, you can run `find . -name '*.ipynb' -exec uv run nbstripout {} +` to clean all notebook outputs in the repo.

#### Javascript

**Linter**: we use the eslint with `pnpm lint-staged` command.

**Prettier**: there is a prettier configuration file to be used locally but not explicitly in precommit.
Please configure your IDE to use it.
