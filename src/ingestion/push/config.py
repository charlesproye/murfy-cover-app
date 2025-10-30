KIA_KEYS_TO_IGNORE = [
    "*.Date",
    "meta",
    "received_date",
    "state.Electronics",
    "state.Green.Electric.SmartGrid",
    "state.Vehicle.Body",
    "state.Vehicle.Cabin",
    "state.Vehicle.Chassis",
    "state.Vehicle.Green.BatteryManagement.BatteryRemaining.Value",  # We already watch the ratio (SoC) and this value has a 1e-5kWh precision
    "state.Vehicle.Green.EnergyInformation",
    "state.Vehicle.Green.PlugAndCharge",
    "state.Vehicle.Green.PowerConsumption",
    "state.Vehicle.Location",
    "state.Vehicle.Offset",
    "state.Vehicle.RemoteControl",
    "state.Vehicle.Service",
    "state.Vehicle.Version",
]

