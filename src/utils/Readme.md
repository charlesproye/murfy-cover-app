# Vehicle Eligibility Checker

A Python script to check vehicle eligibility using the High Mobility API.

## Prerequisites

- Python 3.x
- Required environment variables:
  - `CLIENT_ID_HM`
  - `CLIENT_SECRET_HM`

## Input File Requirements

The script expects a CSV file with the following columns:
- `VIN` (VIN number)
- `Make` (Brand name)

## Supported Brands

- Alfa Romeo (`alfa-romeo`)
- BMW (`bmw`)
- Citroën (`citroen`, `citroën`)
- DS (`ds`)
- Ford (`ford`)
- Fiat (`fiat`)
- Lexus (`lexus`)
- Mercedes-Benz (`mercedes`, `mercedes-benz`)
- MINI (`mini`)
- Opel (`opel`)
- Peugeot (`peugeot`)
- Renault (`renault`)
- Toyota (`toyota`)

## Usage

1. Run the script:
```bash
python3 src/utils/eligibility_check_hm.py
```

2. When prompted, select your input CSV file using the file dialog.

3. The script will:
   - Process each vehicle in the CSV
   - Check eligibility via the HM API
   - Display progress in real-time
   - Save results to `src/utils/eligibility_checker.csv`

## Output

The script will create a new CSV file with all original columns plus:
- `Activation`: 
  - `True` - Vehicle is eligible
  - `False` - Vehicle is not eligible
  - `Unsupported` - Brand not supported

## Example Output Log

```
[1/50] Processed VIN: WBAJC51060L571239 | Brand: bmw    | Eligible: True  | Time: 1.23s
[2/50] Processed VIN: WDD1770421J123456 | Brand: mercedes| Eligible: False | Time: 2.45s
```

