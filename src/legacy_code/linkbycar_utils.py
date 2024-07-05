from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from typing import Dict, Any
import os
import time
from contextlib import contextmanager
import requests

import traceback
from logging import raiseExceptions
from matplotlib.pyplot import xticks
import pandas as pd
import json 
import csv
import seaborn as sn
import numpy as np
from datetime import datetime, timedelta
import time
from rich import print
from rich.progress import Progress
from rich.traceback import Traceback
from constants_variables import *
from dotenv import load_dotenv

load_dotenv()

# Permet d'obtenir les tokens que l'on voulait 
def auth() -> dict[str, str]:
    ''' Retourne les headers avec token d'identitification'''
    assert os.getenv("LINKBYCAR_USERNAME"), "Could not get LINKBYCAR_USERNAME var in env. Please add credentials to connect to link by car API"
    assert os.getenv("LINKBYCAR_PASSWORD"), "Could not get LINKBYCAR_PASSWORD var in env. Please add credentials to connect to link by car API"
    identif = {
        "username" : os.getenv("LINKBYCAR_USERNAME"),
        "password" : os.getenv("LINKBYCAR_PASSWORD"),
        "scope" : 'read',
    }
    head = {
        "accept" : 'application/json',
        "Content-type" : 'application/x-www-form-urlencoded'
    }
    response = requests.post("https://api.linkbycar.com/v1/token", data = identif, headers = head ) #, params=parameters)
    lis = list(response.json().values())
    token = lis[0]
    # print(list)
    headers = {
        "Content-Type" : 'application/json',
        "Authorization": "Bearer " + str(token)
    } 
    return(headers)

try:
    headers = auth()
except:
    print('[red] Could get authentification headder for link by car api.')
    headers = None

def parse_url_as_dict(url: str) -> dict:
    """
    ### Description:
    Parses the url into dict with the following structure:
    scheme: str,
    netloc: str,
    path: str,
    params: str
    query: dict[str, str],
    fragment: str
    """
    parsed_url = urlparse(url)._asdict()
    parsed_url["query"] = parse_qs(parsed_url["query"])

    return parsed_url

def encode_parsed_url(parsed_url: dict) -> str:
    """
    ### Description:
    Takes in a dict with the same format as the one returned by parse_url_as_dict.
    Outputs a url string.
    """
    # <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
    url_tuple = (
        parsed_url['scheme'],
        parsed_url['netloc'],
        parsed_url['path'],
        parsed_url['params'],
        '&'.join([f"{k}={v}" for k, v in parsed_url['query'].items()]),  # Convert query dictionary to query string
        parsed_url['fragment']
    )
    # Convert tuple to URL string
    url_string = urlunparse(url_tuple)

    return url_string
