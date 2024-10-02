#!/bin/bash

# Load environment variables
set -a
source .env
set +a

ACCOUNTS_JSON='[
  {
    "access_token_key": "ACCESS_TOKEN_ROADSTER",
    "refresh_token_key": "REFRESH_TOKEN_ROADSTER",
    "professional_account": true
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
