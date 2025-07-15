from pandas import DataFrame as DF
from transform.raw_tss.parsing import parse_bmw, parse_mobilisight, parse_high_mobility, parse_fleet_telemetry
from pyspark.sql.types import *

S3_RAW_TSS_KEY_FORMAT = "raw_ts/{brand}/time_series/raw_ts_spark.parquet"
TESLA_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(brand="tesla")
DEFAULT_TESLA_RAW_TSS_DF = DF(columns=["vin", "readable_date"])

FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(
    brand="tesla-fleet-telemetry"
)

SPARK_FLEET_TELEMETRY_RAW_TSS_KEY = S3_RAW_TSS_KEY_FORMAT.format(
    brand="tesla-fleet-telemetry"
)


SCHEMAS = {
    "mercedes-benz": StructType([
                        StructField("diagnostics", StructType([
                            StructField("odometer", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("battery_voltage", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("engine_coolant_temperature", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ])))
                        ])),
                        StructField("charging", StructType([
                            StructField("battery_level", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", DoubleType())
                            ]))),
                            StructField("battery_level_at_departure", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", DoubleType())
                            ]))),
                            StructField("charging_rate", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("estimated_range", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("max_range", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("plugged_in", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("fully_charged_end_times", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("time", StructType([
                                        StructField("hour", IntegerType()),
                                        StructField("minute", IntegerType())
                                    ])),
                                    StructField("weekday", StringType())
                                ]))
                            ]))),
                            StructField("preconditioning_scheduled_time", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("preconditioning_remaining_time", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("preconditioning_departure_status", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("smart_charging_status", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("starter_battery_state", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("status", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ])))
                        ])),
                        StructField("usage", StructType([
                            StructField("electric_consumption_rate_since_reset", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("electric_consumption_rate_since_start", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("electric_distance_last_trip", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("electric_distance_since_reset", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", LongType())  # durée brute en secondes
                            ]))),
                            StructField("electric_duration_last_trip", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ])))
                        ])),
                        StructField("charging_session", StructType([
                            StructField("start_time", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", TimestampType())
                            ]))),
                            StructField("session_end_time", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", TimestampType())
                            ]))),
                            StructField("session_energy_delivered", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("session_duration", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", LongType())  # durée brute en secondes
                            ])))
                        ]))
                    ]),
    "tesla-fleet-telemetry": StructType([
                StructField("vin", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("readable_date", StringType(), True),
                StructField("createdAt", StringType(), True),
                StructField("data", ArrayType(
                    StructType([
                        StructField("key", StringType(), True),
                        StructField("value", StructType([
                            StructField("doubleValue", DoubleType(), True),
                            StructField("intValue", IntegerType(), True),
                            StructField("booleanValue", BooleanType(), True),
                            StructField("stringValue", StringType(), True),
                            StructField("carTypeValue", StringType(), True),
                            StructField("bmsStateValue", StringType(), True),
                            StructField("climateKeeperModeValue", StringType(), True),
                            StructField("chargePortValue", StringType(), True),
                            StructField("defrostModeValue", StringType(), True),
                            StructField("detailedChargeStateValue", StringType(), True),
                            StructField("fastChargerValue", StringType(), True),
                            StructField("hvacAutoModeValue", StringType(), True),
                            StructField("hvacPowerValue", StringType(), True),
                            StructField("sentryModeStateValue", StringType(), True),
                            StructField("invalid", BooleanType(), True),
                        ]))
                    ])
                ), True)
            ]),
    "kia": StructType([
            StructField("diagnostics", StructType([
                StructField("battery_level", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", DoubleType())
                ]))),
                StructField("odometer", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StructType([
                        StructField("unit", StringType()),
                        StructField("value", DoubleType())
                    ]))
                ]))),
                StructField("estimated_range", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StructType([
                        StructField("unit", StringType()),
                        StructField("value", DoubleType())
                    ]))
                ]))),
                StructField("estimated_mixed_powertrain_range", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StructType([
                        StructField("unit", StringType()),
                        StructField("value", DoubleType())
                    ]))
                ])))
            ])),
            StructField("charging", StructType([
                StructField("battery_level", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", DoubleType())
                ]))),
                StructField("charge_port_state", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StringType())
                ]))),
                StructField("estimated_range", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StructType([
                        StructField("unit", StringType()),
                        StructField("value", DoubleType())
                    ]))
                ]))),
                StructField("plugged_in", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StringType())
                ]))),
                StructField("preconditioning_immediate_status", ArrayType(StructType([
                    StructField("timestamp", TimestampType()),
                    StructField("failure", StringType()),
                    StructField("data", StringType())
                ])))
            ]))
        ]),
    "ford": StructType([
                StructField("diagnostics", StructType([
                    StructField("odometer", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StructType([
                            StructField("unit", StringType()),
                            StructField("value", DoubleType())
                        ]))
                    ])))
                ])),
                StructField("charging", StructType([
                    StructField("battery_energy", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StructType([
                            StructField("unit", StringType()),
                            StructField("value", DoubleType())
                        ]))
                    ]))),
                    StructField("battery_level", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", DoubleType())
                    ]))),
                    StructField("charge_limit", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", DoubleType())
                    ]))),
                    StructField("charger_voltage", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StructType([
                            StructField("unit", StringType()),
                            StructField("value", DoubleType())
                        ]))
                    ]))),
                    StructField("time_to_complete_charge", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StructType([
                            StructField("unit", StringType()),
                            StructField("value", DoubleType())
                        ]))
                    ]))),
                    StructField("status", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StringType())
                    ]))),
                    StructField("battery_performance_status", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StringType())
                    ])))
                ])),
                StructField("usage", StructType([
                    StructField("last_trip_battery_regenerated", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StructType([
                            StructField("unit", StringType()),
                            StructField("value", DoubleType())
                        ]))
                    ]))),
                    StructField("electric_distance_last_trip", ArrayType(StructType([
                        StructField("timestamp", TimestampType()),
                        StructField("failure", StringType()),
                        StructField("data", StructType([
                            StructField("unit", StringType()),
                            StructField("value", DoubleType())
                        ]))
                    ])))
                ]))
            ]),
        "renault": StructType([
                        StructField("diagnostics", StructType([
                            StructField("odometer", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("estimated_range", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("speed", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", DoubleType())
                            ])))
                        ])),
                        StructField("charging", StructType([
                            StructField("battery_energy", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("battery_level", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", DoubleType())
                            ]))),
                            StructField("charging_rate", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("distance_to_complete_charge", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("driving_mode_phev", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("estimated_range", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ]))),
                            StructField("plugged_in", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("battery_charge_type", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ]))),
                            StructField("status", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StringType())
                            ])))
                        ])),
                        StructField("climate", StructType([
                            StructField("outside_temperature", ArrayType(StructType([
                                StructField("timestamp", TimestampType()),
                                StructField("failure", StringType()),
                                StructField("data", StructType([
                                    StructField("unit", StringType()),
                                    StructField("value", DoubleType())
                                ]))
                            ])))
                        ]))
                    ]),
    "volvo-cars": StructType([
                    StructField("diagnostics", StructType([
                        StructField("odometer", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("distance_since_reset", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("estimated_range", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("fuel_volume", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ])))
                    ])),
                    StructField("charging", StructType([
                        StructField("status", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StringType())
                        ]))),
                        StructField("estimated_range", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("time_to_complete_charge", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("battery_level", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", DoubleType())
                        ]))),
                        StructField("plugged_in", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StringType())
                        ])))
                    ])),
                    StructField("usage", StructType([
                        StructField("average_speed", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("average_fuel_consumption", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ]))),
                        StructField("electric_consumption_average", ArrayType(StructType([
                            StructField("timestamp", TimestampType()),
                            StructField("failure", StringType()),
                            StructField("data", StructType([
                                StructField("unit", StringType()),
                                StructField("value", DoubleType())
                            ]))
                        ])))
                    ]))
                ]),
    "bmw": StructType([
                StructField("data", StructType([
                    StructField("vin", StringType(), True),
                    StructField("pushKeyValues", ArrayType(
                        StructType([
                            StructField("key", StringType(), True),
                            StructField("value", StringType(), True),  # ou DoubleType() si tu veux caster ensuite
                            StructField("unit", StringType(), True),
                            StructField("info", StringType(), True),
                            StructField("date_of_value", TimestampType(), True)  # à condition que tu passes le format ISO-8601
                        ])
                    ), True)
                ])
                )
            ])
}



ALL_MAKES = [
    "tesla-fleet-telemetry",
    "bmw",
    "mercedes-benz",
    "ford",
    "volvo-cars",
    "stellantis",
    "kia",
    "renault",
]

GET_PARSING_FUNCTIONS = {
    # Stellantis
    "stellantis": parse_mobilisight,
    # BMW
    "bmw": parse_bmw,
    # Tesla
    # "tesla": tesla_get_raw_tss,
    # Kia
    "kia": parse_high_mobility,
    # Mercedes-Benz
    "mercedes-benz": parse_high_mobility,
    # Ford
    "ford": parse_high_mobility,
    # Renault
    "renault": parse_high_mobility,
    # Volvo
    "volvo-cars": parse_high_mobility,
    # fleet-telemetry
    "tesla-fleet-telemetry": parse_fleet_telemetry,
}

