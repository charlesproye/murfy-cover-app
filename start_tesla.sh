#!/bin/bash

# Load environment variables
set -a
source .env
set +a

ACCOUNTS_JSON='[
  {
    "access_token_key": "ACCESS_TOKEN_AYVENS",
    "professional_account": true,
    "code": "822157df8b98c65acbc49a224698a0f5b7774a491023c3e22c53cc683c5ed284ff99bd1e",
    "excel_url": "src/ingestion/tesla/excel/ayvens.csv"
  }
]'
# Run the main.py script with the accounts JSON
python3 src/ingestion/tesla/main.py --accounts "$ACCOUNTS_JSON"
