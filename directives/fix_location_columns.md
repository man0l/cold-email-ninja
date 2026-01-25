## Fix Location Columns SOP

### Goal
Normalize scattered city/state/zip/country values in a Google Sheet tab and write cleaned values to a new tab in the same spreadsheet.

### Inputs
- **Source URL**: Google Sheet URL containing the leads (required)
- **Source Sheet**: Tab name to read from (optional; resolved from gid if omitted)
- **Output Sheet**: Tab name to write cleaned data (optional; defaults to `Fixed Locations`)

### Environment Variables / Files
- `credentials.json`: Google Service Account credentials (required)

### Tools
- `execution/fix_location_columns.py`

### Instructions

```bash
source .venv/bin/activate
python execution/fix_location_columns.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --sheet-name "Source Tab Name" \
  --output-sheet "Fixed Locations"
```

### Notes
- The script detects common column names for city/state/zip/country and will fail if required columns are missing.
- If `--sheet-name` is omitted, the script will attempt to resolve the tab from the `gid` in the URL.
