"""
This script converts High Mobility responses into raw parquet time series.
The script operates on S3 buckets. 
"""
import logging
from datetime import datetime as DT
from datetime import timedelta as TD
from dateutil import parser

from rich import print
import pandas as pd
from pandas import Series
from pandas import DataFrame as DF
from apscheduler.triggers.interval import IntervalTrigger

from core.s3_utils import S3_Bucket
from transform.base_jobs.job_interval import Jobinterval

class HighMobilityRawTS(Jobinterval):
    """
    ### Description:
    This ETL handles the parsing of High mobility responses for a single brand emitted by the ingestion module.  
    While we could have one ETL for all brands covered by high mobility, we prefer to create an instance of this ETL per brand as we will eventually access their API directly instead of relying on a third party.  
    Note: The responses from the ingestion module are high mobility responses preprocessed by the ingestion module, not the original high mobility responses.    
    1. Extracts json responses of brand's vins.
    1. Transforms them into time series dataframes.
    1. Loads them as parquet files in the raw_ts folder.
    """

    def __init__(self, brand:str) -> None:
        super().__init__()
        
        self.brand = brand
        self.name = brand + "-RawTS"
        self.id = self.name
        self.logger = logging.getLogger(self.name)
        self.trigger = IntervalTrigger(days=1, start_date=DT.now() - TD(days=1) + TD(seconds=1))

    async def func(self):
        self.bucket = S3_Bucket()
        self.parse_responses_of_all_vins_as_raw_tss()

    def parse_responses_of_all_vins_as_raw_tss(self) -> DF:
        """
        ### Description:
        Converts the responses of each vin into a single raw parquet series.
        """
        # Get list of objects in response folder
        keys = Series(self.bucket.list_keys(f"response/{self.brand}/"), dtype="string") # Make sure to leave the / at the end
        if len(keys) == 0:
            self.logger.info("""
                No responses found in the 'response' folder.
                No raw time series have been generated.
            """)
            return
        # print(keys)
        # Only retain .json responses
        keys = keys[keys.str.endswith(".json")]
        # Reponses are organized as follow response/brand_name/vin/date-of-response.json
        # We extract the brand and vin
        keys = pd.concat((keys, keys.str.split("/", expand=True).loc[:, 1:]), axis="columns").iloc[:, 0:-1]
        keys.columns = ["key", "brand", "vin", "file"] # Set column names
        # Check that the file name is a date
        keys['is_valid_file'] = (
            keys['file']
            .str.split(".", expand=True).loc[:, 0]
            .str.match(r'^\d{4}-\d{2}-\d{2}$')
        )
        keys["is_valid_file"] &= keys["vin"].str.len() != 0
        keys = keys.query(f"is_valid_file")
        # print(keys)
        # for each group of responses for a vin create a raw time series parquet and get the meta data
        meta_data: Series =  keys.groupby("vin").apply(self.parse_responses_of_single_vin_as_raw_ts, include_groups=False)
        meta_data = DF(meta_data.to_list(), index=meta_data.index)
        print(meta_data)
        

    def parse_responses_of_single_vin_as_raw_ts(self, src_keys:DF):
        """
        ### Description:
        Converts all the json responses of a vin into a single raw ts.
        """
        raw_jsons:Series = src_keys["key"].apply(self.bucket.read_json_file)                 # Read responses
        parsed_raw_tss = []
        for raw_json in raw_jsons:
            try:
                if self.brand == "stellantis":
                    parsed_raw_tss.append(self.parse_response_as_raw_ts_stellantis(raw_json))
                else:
                    parsed_raw_tss.append(self.parse_response_as_raw_ts(raw_json))
            except Exception as e:
                self.logger.warning(f"Caught exception wile parsing response of {src_keys.name}:\n{e}")
        if len(parsed_raw_tss) == 0:
            self.logger.warning(f"No data could be parsed from keys {src_keys['key'].values} for vin {src_keys.name[1]}")
            return
        raw_ts:DF = pd.concat(parsed_raw_tss, ignore_index=True)
        dest_key = "/".join(["raw_ts", self.brand, "time_series", src_keys.name]) + ".parquet"
        # print(dest_key)
        # print(raw_ts)
        # print("==============")
        self.bucket.save_df_as_parquet(raw_ts, dest_key)

        return raw_ts.columns

    def parse_response_as_raw_ts(self, response:dict) -> DF:
        """
        ### Description:
        Converts response dictionnary from the response folder into a raw time series.
        """
        # The responses are first indexed by "capability" (see any high mobility's air table data catalog).
        # We don't really need to know what capability but some variables that belong to different capabilities may have the same name.
        # To differentiate them, we will prepend their correspomding capability to their name.
        flattened_response:dict = {}
        for capability, variables in response.items():
            if not isinstance(variables, dict):
                # print(variables)
                continue
            for variable, elements in variables.items():
                for element in elements:
                    timestamp = parser.isoparse(element["timestamp"])
                    # timestamp = DT.strptime(element["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    variable_name = capability + "." + variable 
                    if isinstance(element["data"], dict):
                        if not "value" in element["data"]:
                            continue
                        value = element["data"]["value"]
                        if "unit" in element:
                            variable_name += "." + element["unit"]
                    else:
                        value = element["data"]
                    flattened_response[timestamp] = flattened_response.get(timestamp, {}) | {variable_name: value}
        raw_ts = (
            DF.from_records(flattened_response)
            .T
            .reset_index(drop=False, names="date")
        )

        return raw_ts
    
    def parse_response_as_raw_ts_stellantis(self, response:dict) -> DF:
        """
        ### Description:
        Converts response dictionnary from the response folder into a raw time series.
        """
        # The responses are first indexed by "capability" (see any stellantis' air table data catalog).
        # We don't really need to know what capability but some variables that belong to different capabilities may have the same name.
        # To differentiate them, we will prepend their correspomding capability to their name.
        flattened_response:dict = {}
        def flatten_dict(data, prefix=''):
            for key, value in data.items():
                new_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    for item in value:
                        if 'datetime' in item:
                            timestamp = parser.isoparse(item['datetime'])
                            for sub_key, sub_value in item.items():
                                if sub_key != 'datetime':
                                    variable_name = f"{new_key}.{sub_key}"
                                    if isinstance(sub_value, dict) and not sub_value:
                                        # Handle empty dict by adding a dummy field
                                        flattened_response.setdefault(timestamp, {})[f"{variable_name}.dummy"] = None
                                    else:
                                        flattened_response.setdefault(timestamp, {})[variable_name] = sub_value
                elif isinstance(value, dict):
                    if not value:
                        # Handle empty dict at top level
                        flattened_response.setdefault(DT.now(), {})[f"{new_key}.dummy"] = None
                    else:
                        flatten_dict(value, new_key)
                elif isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
                    # Handle single-item lists like in the 'alerts' field
                    flatten_dict(value[0], new_key)

        flatten_dict(response)

        raw_ts = (
            DF.from_dict(flattened_response, orient='index')
            .reset_index(names='date')
        )
        
        return raw_ts
