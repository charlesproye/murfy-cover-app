MAKES = {
    'tesla-fleet-telemetry': {
        'mileage_data': 'Odometer',
    },
    'mercedes-benz': {
        'mileage_data': 'odometer'
    },
    'volvo-cars': {
        'mileage_data': 'odometer'
    },
    'kia': {
        'mileage_data': 'odometer'
    },
    'renault': {
        'mileage_data': 'odometer'
    },
    'ford': {
        'mileage_data': 'odometer'
    },
    'stellantis': {
        'mileage_data': 'odometer',
        'soh_oem_data': 'electricity.battery.stateOfHealth.percentage'
    },
    'bmw': {
        'mileage_data': 'mileage',
    }
}

TABLE_QUERY = """
                SELECT 
                    vin,
                    model_name, 
                    version,
                    type
                FROM vehicle_model vm2
                LEFT JOIN vehicle vm ON vm.vehicle_model_id = vm2.id
            """
