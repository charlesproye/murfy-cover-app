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
  },
  {
    "access_token_key": "ACCESS_TOKEN_OLINO",
    "professional_account": true,
    "code": "d77c548f8b98ca5acbc49e741498a0f1b4784a13152496e22f579f6a67088ad6f296b94f",
  },
  {
    "access_token_key": "ACCESS_TOKEN_ROADSTER",
    "refresh_token_key": "REFRESH_TOKEN_ROADSTER",
    "professional_account": true
  },
  {
    "access_token_key": "ACCESS_TOKEN_OLINO",
    "professional_account": true,
    "code": "d77c548f8b98ca5acbc49e741498a0f1b4784a13152496e22f579f6a67088ad6f296b94f"
  },
  {
    "access_token_key": "ACCESS_TOKEN_PA",
    "refresh_token_key": "REFRESH_TOKEN_PA",
    "professional_account": false,
    "vehicle_id": "LRWYGCFS6PC552861"
  },
  {
    "access_token_key": "ACCESS_TOKEN_BEN",
    "refresh_token_key": "REFRESH_TOKEN_BEN",
    "professional_account": false,
    "vehicle_id": "5YJ3E7EB1KF334219"
  },
  {
    "access_token_key": "ACCESS_TOKEN_EGGERMONT",
    "refresh_token_key": "REFRESH_TOKEN_EGGERMONT",
    "professional_account": false,
    "vehicle_id": "5YJ3E7EB7KF474436"
  },
  {
    "access_token_key": "ACCESS_TOKEN_BERNIE",
    "refresh_token_key": "REFRESH_TOKEN_BERNIE",
    "professional_account": false,
    "vehicle_id": "5YJSA7H11EFP63486"
  }
]'
# Run the main.py script with the accounts JSON
python3 src/ingestion/tesla/main.py --accounts "$ACCOUNTS_JSON"
