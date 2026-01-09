# CSV Data Explorer

A simple command-line tool for exploring CSV datasets using Python and pandas.

## Features
- Print summary statistics for a CSV file
- Describe a specific column
- Filter rows based on an expression (e.g. `heart_rate>120`) and save to a new file

## Usage

```bash
python explorer.py --file data/vitals.csv --summary
python explorer.py --file data/vitals.csv --column heart_rate
python explorer.py --file data/vitals.csv --filter "heart_rate>100" --out data/high_hr.csv
