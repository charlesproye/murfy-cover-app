import pandas as pd

from core.sql_utils import get_sqlalchemy_engine
from external_api.services.flash_report.vin_decoder.config import (
    QUERY,
    QUERY_PARTIAL,
    VIN_DICTIONARY,
)


def is_tesla_vin(vin: str) -> bool:
    """
    Detects if a VIN belongs to a Tesla vehicle.

    Tesla VINs typically start with:
    - 5YJ for vehicles manufactured in the United States (Model S, 3, X, Y)
    - 7SA for vehicles manufactured in China
    - LRW for some Chinese vehicles
    - SFZ for some European vehicles (Berlin)

    Args:
        vin: Vehicle Identification Number

    Returns:
        bool: True if the VIN corresponds to a Tesla vehicle, False otherwise
    """
    if len(vin) < 3:
        return False

    tesla_prefixes = ["5YJ", "7SA", "LRW", "SFZ", "XP7"]
    return vin[:3].upper() in tesla_prefixes


class TeslaVinDecoder:
    def __init__(self):
        """
        Initialize the Tesla VIN decoder.

        Args:
            vin_dictionary (dict): Mapping of VIN segments to their decoded meanings.
            query_full (str): SQL query to retrieve full VIN discriminators.
            query_partial (str): SQL query for fallback when the full discriminator is not found.
        """
        self.vin_dict = VIN_DICTIONARY
        self.query_full = QUERY
        self.query_partial = QUERY_PARTIAL

    def _split_vin(self, vin: str, start: int, end=None) -> str:
        """
        Extract a substring from the VIN.

        Args:
            vin (str): The full vehicle identification number.
            start (int): Start index.
            end (int, optional): End index. Defaults to None.

        Returns:
            str: Extracted VIN substring.
        """
        return vin[start:end] if end is not None else vin[start]

    def _lookup(self, key: str, section: str):
        """
        Look up a decoded value from the VIN dictionary.

        Args:
            key (str): The VIN substring to decode.
            section (str): The dictionary section (e.g. "fourth", "seventh").

        Returns:
            Any: The decoded value, or None if not found.
        """
        return self.vin_dict.get(section, {}).get(key)

    def _fetch_type_version(self, vin: str):
        """
        Retrieve the Tesla model type, version, and net capacity using VIN patterns.

        Attempts a full VIN match first, then a reduced discriminator if not found.

        Args:
            vin (str): The full vehicle identification number.

        Returns:
            tuple: (type(s), version(s), net_capacities) or (None, None, None) if not found.
                type(s) and version(s) will be lists if they contain commas.
        """

        def split_and_trim(value, char):
            if value and isinstance(value, str) and char in value:
                return [v.strip() for v in value.split(char)]
            return value

        engine = get_sqlalchemy_engine()
        df = pd.read_sql(self.query_full, engine)

        disc_vin = self._split_vin(vin, 0, 11)
        match = df.loc[df["discriminative_vin"] == disc_vin]

        if not match.empty:
            row = match.iloc[0]
            models = split_and_trim(row["type_version_capa"], ";")

            if isinstance(models, str):
                types = models.split("|")[0]
                versions = models.split("|")[1]
                net_capacities = models.split("|")[2]
            else:
                types = [model.split("|")[0] for model in models]
                versions = [model.split("|")[1] for model in models]
                net_capacities = [model.split("|")[2] for model in models]

            #

            return (
                types,
                versions,
                net_capacities,
            )

        # Fallback with partial VIN discriminator
        engine = get_sqlalchemy_engine()
        df = pd.read_sql(self.query_partial, engine)

        reduced_vin = self._split_vin(vin, 3, 5) + self._split_vin(vin, 6, 10)
        match = df.loc[df["discriminative_vin"] == reduced_vin]

        if match.empty:
            return None, None, None

        row = match.iloc[0]
        models = split_and_trim(row["type_version_capa"], ";")

        if isinstance(models, str):
            types = models.split("|")[0]
            versions = models.split("|")[1]
            net_capacities = models.split("|")[2]
        else:
            types = [model.split("|")[0] for model in models]
            versions = [model.split("|")[1] for model in models]
            net_capacities = [model.split("|")[2] for model in models]

        return (
            types,
            versions,
            net_capacities,
        )

    def decode(self, vin: str) -> dict:
        """
        Decode a Tesla VIN and return structured vehicle information.

        Args:
            vin (str): The full vehicle identification number.

        Returns:
            dict: A dictionary containing decoded VIN information.
        """
        type_, version, net_capacity = self._fetch_type_version(vin)

        def select_type(type_):
            if isinstance(type_, list):
                if len(set(type_)) == 1:
                    return next(iter(set(type_)))
                else:
                    return list(set(type_))
            else:
                return type_

        def select_version(version, type_):
            if isinstance(type_, list):
                if len(set(type_)) == 1:
                    return version[0]
                else:
                    return version
            else:
                return version

        # Special case for MTY13B which has been mounted with two types of batteries
        if vin[9] == "P" and vin[7] == "S" and "standard range" in type_:
            version = ["MTY13C"]

        return {
            "VIN": vin,
            "Country": self._lookup(self._split_vin(vin, 0, 3), "first_to_third"),
            "Model": self._lookup(self._split_vin(vin, 3), "fourth"),
            "Type": select_type(type_),
            "Version": select_version(version, type_),
            "Battery": self._lookup(self._split_vin(vin, 6), "seventh"),
            "NetCapacity": net_capacity,
            "Propulsion": self._lookup(self._split_vin(vin, 7), "eighth"),
            "Year": self._lookup(self._split_vin(vin, 9), "tenth"),
            "FactoryLocation": self._lookup(self._split_vin(vin, 10), "eleventh"),
            "SpecialVehicle": self._lookup(self._split_vin(vin, 11), "twelfth"),
        }
