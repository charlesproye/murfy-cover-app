#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
import sys
import requests

def decode_vin(vin: str):
    """from a VIN queries the vindecoder.eu API to retrieve detailed vehicle information.
    Args:
        vin (str): vin 
    """
    # === Configuration ===
    api_prefix = "https://api.vindecoder.eu/3.2"
    api_key = os.environ.get('VIN_DECODER_KEY')
    secret_key = os.environ.get('VIN_DECODER_SECRET')
    endpoint_id = "decode"

    if not api_key or not secret_key:
        print("Environment variables VIN_DECODER_KEY and VIN_DECODER_SECRET are not set.")
        sys.exit(1)

    # === control sum ===
    hash_input = f"{vin}|{endpoint_id}|{api_key}|{secret_key}"
    control_sum = hashlib.sha1(hash_input.encode('utf-8')).hexdigest()[:10]

    url = f"{api_prefix}/{api_key}/{control_sum}/{endpoint_id}/{vin}.json"

    # === request ===
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        sys.exit(1)

    # === response ===
    try:
        result = response.json()
        print("VIN successfully decoded:\n")
        for k, v in result.items():
            print(f"{k}: {v}")

    except ValueError:
        print("Error decoding JSON response:")
        print(response.text)

