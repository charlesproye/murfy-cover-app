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
    "access_token_key": "ACCESS_TOKEN_AYVENS_NV",
    "professional_account": true,
    "code": "862103da89cdc60dcb9f92774798a0a1b1264a48412a9ee27955993c3b09d884ffc9b94c",
  },
   {
    "access_token_key": "ACCESS_TOKEN_AYVENS_NVA",
    "professional_account": true,
    "code": "81295e8edfc0c75ccbc99f254198a0a2b6264a49417090e22f51983c69598ad6fe9ebe4c",
    "excel_url": "src/ingestion/tesla/excel/ayvens.csv"
  },
   {
    "access_token_key": "ACCESS_TOKEN_AYVENS_BLBV",
    "professional_account": true,
    "code": "847b51d9de9bcc01cbcdc8264398a0f3e5754a48442192e22857cf6a665f8fd5a296bd1c",
    "excel_url": "src/ingestion/tesla/excel/ayvens.csv"
  },
   {
    "access_token_key": "ACCESS_TOKEN_AYVENS_SLBV",
    "professional_account": true,
    "code": "d12d00dbd89b9b0bcbcf92774198a0f1b1224a49162291e27f51ce696759de84a69bbf17",
    "excel_url": "src/ingestion/tesla/excel/ayvens.csv"
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
