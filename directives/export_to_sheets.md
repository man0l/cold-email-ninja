# Export to Google Sheets SOP

## Goal
Export JSON data (from checkpoint files or other sources) to Google Sheets. Creates a new spreadsheet in a specified Drive folder with formatted headers.

## Inputs
- **Input File**: Path to a JSON file containing data to export
- **Output Sheet Name**: Name for the new Google Sheet
- **Folder ID**: (Optional) Google Drive folder ID (default: `0ADWgx-M8Z5r-Uk9PVA`)

## Supported Formats

The script handles two JSON formats:

1. **Checkpoint format** (from scraping scripts):
   ```json
   {
     "leads": [...],
     "processed_locations": [...],
     "current_keyword": "..."
   }
   ```

2. **Direct array format**:
   ```json
   [
     {"name": "...", "phone": "...", ...},
     {"name": "...", "phone": "...", ...}
   ]
   ```

## Tools
- `execution/export_to_sheets.py` - Export script

## Output
A Google Sheet URL containing the exported data with:
- Bold, gray header row
- Auto-resized columns
- All data from the JSON file

## Instructions

### Basic Usage

```bash
source .venv/bin/activate
python execution/export_to_sheets.py \
  --input .tmp/google_maps_checkpoint.json \
  --output-sheet "My Leads"
```

### Export to Specific Folder

```bash
python execution/export_to_sheets.py \
  --input data.json \
  --output-sheet "Leads Export" \
  --folder-id "YOUR_FOLDER_ID"
```

### Export to Drive Root

```bash
python execution/export_to_sheets.py \
  --input leads.json \
  --output-sheet "My Data" \
  --no-folder
```

### Options Reference
- `--input, -i`: Path to JSON file (required)
- `--output-sheet, -o`: Name for the Google Sheet (required)
- `--folder-id, -f`: Google Drive folder ID (default: `0ADWgx-M8Z5r-Uk9PVA`)
- `--no-folder`: Create in Drive root instead of default folder

## Common Use Cases

### Export Google Maps Scrape Results
```bash
python execution/export_to_sheets.py \
  --input .tmp/google_maps_checkpoint.json \
  --output-sheet "Contractors - Jan 2025"
```

### Export Clean Leads Checkpoint
```bash
python execution/export_to_sheets.py \
  --input .tmp/clean_leads_checkpoint.json \
  --output-sheet "Verified Leads"
```

## Requirements
- `credentials.json` - Google service account credentials
- Service account must have access to the target Drive folder
- Python packages: `google-api-python-client`, `google-auth`, `python-dotenv`

## Troubleshooting
- **"credentials.json not found"**: Ensure the service account file exists in the project root
- **Permission denied on folder**: The service account needs Editor access to the Drive folder
- **Empty export**: Check that the JSON file contains valid data in the expected format
