# Vehicle Eligibility Checker

This utility checks the eligibility of vehicles by verifying their VIN (Vehicle Identification Number) against various manufacturer APIs.

## Features

- Supports multiple vehicle manufacturers:
  - BMW
  - Citroën
  - DS
  - Volvo Cars
  - Fiat
  - Ford
  - Mini
  - Mercedes-Benz
  - Opel
  - Peugeot
  - Renault
  - Tesla (automatic eligibility)
- Interactive file selection through GUI
- Asynchronous API calls for better performance
- Detailed logging of the process
- Error handling and reporting

## Prerequisites

- Python 3.7+
- Required Python packages:
  - pandas
  - aiohttp
  - tkinter (built-in)

## Input File Format

The input CSV file must contain the following columns with these exact names:
- `vin`: Vehicle Identification Number (case-sensitive)
- `brand`: Vehicle brand name (case-insensitive)

Example CSV format:
```csv
vin,brand
WAUZZZ8T1BA123456,AUDI
WBA12345678901234,BMW
```

## Usage

1. Run the script:
   ```bash
   python src/ingestion/vehicle_info/utils/eligibility_checker.py
   ```

2. When prompted, select your CSV file containing VIN and brand information.

3. The script will process each vehicle and check its eligibility.

4. Results will be saved in a new CSV file with the following additional columns:
   - `Eligibility`: TRUE/FALSE/ERROR
   - `Comment`: Additional information or error messages

## Output

The script generates a new CSV file with the eligibility results for each vehicle. The output includes:
- Original VIN and brand information
- Eligibility status
- Any error messages or comments

## Error Handling

The script handles various error cases:
- Invalid VIN numbers
- Unsupported brands
- API connection issues
- File selection errors

## Common Issues and Solutions

1. **Missing Columns Error**
   - Error: `KeyError: 'vin'` or `KeyError: 'brand'`
   - Solution: Ensure your CSV file has columns named exactly 'vin' and 'brand' (case-sensitive for 'vin')

2. **File Format Issues**
   - Make sure your CSV file is properly formatted with commas as separators
   - Check that there are no hidden characters or BOM markers
   - Verify that the file is not corrupted

3. **Brand Name Issues**
   - Use the exact brand names as listed in the Features section
   - The script will automatically normalize some brand names (e.g., "citroën" → "citroen")

## Logging

Logs are generated with the following information:
- Progress updates (every 100 vehicles)
- Error messages
- Processing completion status

## Notes

- Tesla vehicles are automatically marked as eligible
- Brand names are automatically normalized (e.g., "citroën" → "citroen")
- The script uses asynchronous processing for better performance with large datasets
- The input CSV must use UTF-8 encoding

