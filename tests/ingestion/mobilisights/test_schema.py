import json

import msgspec

from ingestion.mobilisights.schema import CarState


def test_carstate():
    decoder = msgspec.json.Decoder(CarState)

    json_data = {
        "vin": "1234567890",
        "datetime": "2021-01-01T00:00:00Z",
        "electricity": {"charging": {"remainingTime": {}}},
    }

    car_state = decoder.decode(json.dumps(json_data))

    assert car_state.vin == "1234567890"


def test_bool_parsing():
    decoder = msgspec.json.Decoder(CarState)

    json_data = b"""{
        "vin": "1234567890",
        "datetime": "2021-01-01T00:00:00Z",
        "seatbelt": {
            "driver": {"value": false, "datetime": "2025-10-02T08:50:19.613Z"},
            "passenger": {
                "rear": {
                    "left": {"value": false, "datetime": "2025-10-02T08:50:19.613Z"},
                    "right": {"value": true, "datetime": "2025-10-02T08:50:19.613Z"}
                }
            }
        }
    }"""

    car_state = decoder.decode(json_data)

    assert car_state.vin == "1234567890"

