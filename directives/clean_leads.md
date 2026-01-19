# Clean Leads SOP

## Goal
Clean contact lists by filtering leads and validating website availability. This directive filters by category, and ensures all websites are active (200 OK) before further processing.

## Inputs
- **Source URL**: Google Sheet URL containing the leads (Required).
- **Output Sheet**: Name of the new Google Sheet to create (Required).
- **Category** (Optional): Filter leads by specific category/categories (substring match, OR logic).
- **Folder ID** (Optional): Google Drive Folder ID to create the output sheet in. Falls back to `GOOGLE_DRIVE_FOLDER_ID` env var if not specified.
- **Max Leads** (Optional): Limit validation count (for testing).
- **Workers** (Optional): Number of parallel workers for website validation (default: 10).

## Environment Variables
- `GOOGLE_DRIVE_FOLDER_ID`: Optional. Default folder ID for creating Google Sheets. Can be overridden with `--folder-id`.

## Algorithm
1. **Load Data**: Reads leads from the provided Google Sheet.
2. **Filter by Category**: (If specified) Keeps only leads where `Category` (or similar) field matches ANY of the provided categories.
3. **Clean URLs**:
    -   Extracts the root domain from the website URL.
    -   Removes leads with empty or invalid URLs.
4. **Validate Websites**:
    -   Checks each website in **parallel** using configurable workers (default: 10).
    -   Sends a `HEAD` (fallback to `GET`) request.
    -   **Keeps only websites returning 200 OK**.
    -   **Checkpoints**: Autosaves progress every 100 leads to `.tmp/clean_leads_checkpoint.json`. Resumes automatically if interrupted.
5. **Export**: Saves the cleaned and validated leads to a **new Google Sheet**.

## Tools
- `execution/clean_leads.py` - Python script for filtering and validation.

## Instructions

### Standard Cleaning Workflow

```bash
source .venv/bin/activate
python execution/clean_leads.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output-sheet "Cleaned Leads" \
  --folder-id "YOUR_FOLDER_ID" \
  --category "Plumber"
```

### Multiple Categories (OR logic)

```bash
python execution/clean_leads.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output-sheet "Cleaned Leads - Home Builders" \
  --folder-id "YOUR_FOLDER_ID" \
  --category "Custom home builder" "Home Builder"
```

### Options
- `--source-url`: Full URL of the source Google Sheet.
- `--output-sheet`: Name of the destination sheet.
- `--category`: One or more keywords to filter the Category column (case-insensitive, OR logic).
- `--folder-id`: Google Drive Folder ID for the output file (required for write access).
- `--max-leads`: Limit number of leads to validate (useful for testing).
- `--workers`: Number of parallel workers for validation (default: 10).
- `--verbose`: Show dropped leads (default: hidden).

## Features
- **Multiple Categories**: Pass multiple category values to match ANY of them (OR logic).
- **Progress Bar**: Shows real-time progress, percentage, and validation stats.
- **Checkpointing**: In case of interruption (Ctrl+C), re-run the command to resume validation from where it left off.
