#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
import sys
import requests
import pandas as pd
from core.sql_utils import get_connection
from activation.config.mappings import mapping_vehicle_type


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
        for info in result["decode"]:
            if info['label'] == 'Make':
                make = info.get("value", '').lower()
                if make =='mercedes-benz':
                    make = "mercedes"
            if info['label'] == 'Model':
                model = info.get("value", '').lower()
            if info['label'] == 'Model Year':
                sale_year = info.get("value", '')
            if info['label'] == "Engine Power (kW)":
                engine_power = info.get("value", '')
        with get_connection() as con:
                cursor = con.cursor()
                cursor.execute("""SELECT vm.model_name, vm.id, vm.type, vm.commissioning_date, vm.end_of_life_date, m.make_name, b.capacity FROM vehicle_model vm
                                                            join make m on vm.make_id=m.id
                                                            join battery b on b.id=vm.battery_id;""")
                model_existing =  pd.DataFrame(cursor.fetchall(), columns=["model_name", "id", "type",  "commissioning_date", "vm.end_of_life_date", "make_name", "capacity"])

        vehicule_model_id = mapping_vehicle_type(model, make, model, model_existing, engine_power, sale_year)

        return vehicule_model_id

    except ValueError:
        print("Error decoding JSON response:")
        print(response.text)

