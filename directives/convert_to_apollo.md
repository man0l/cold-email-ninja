# Convert Google Maps Leads to Apollo Format

## Goal
Convert scraped and enriched Google Maps leads (JSON or Google Sheets) into an Apollo-compatible format and upload to Google Sheets.
Uploads should write a new tab in the source spreadsheet (not a new spreadsheet) when a `spreadsheet_url` is used.

## Inputs
- `input_file`: Path to the source JSON file (Google Maps enriched leads), OR
- `spreadsheet_url`: Google Sheets URL containing the enriched leads
- `sheet_name`: (Optional) Name of the sheet within the spreadsheet
- `output_file`: Path to the destination JSON file (Apollo format)
- `target_spreadsheet_id`: (Optional) ID of spreadsheet to add Apollo formatted sheet to (defaults to source spreadsheet when using `spreadsheet_url`)
- Headers are always taken from the first row of the source sheet.

## Tools
- `execution/convert_to_apollo.py` - Converts JSON to Apollo format
- `execution/clean_leads.py` - Contains `load_from_google_sheets()` for downloading from Sheets
- `execution/export_to_sheets.py` - Uploads data to Google Sheets

## Execution Steps

### From JSON file:
1. Run the conversion script:
   ```bash
   python3 execution/convert_to_apollo.py --input <input_file> --output <output_file>
   ```

### From Google Sheets:
1. Convert and upload in one step:
   ```bash
   python3 execution/convert_to_apollo.py \
     --spreadsheet-url "<spreadsheet_url>" \
     --sheet-name "<sheet_name>" \
    --output .tmp/apollo_leads.json \
    --output-sheet "Apollo Export"
   ```

## Output
- A JSON file at `output_file` containing the converted leads.
- A new worksheet in the source Google Sheets file with the Apollo-formatted data.
- **Logic**:
    - **One lead per company**: Prioritizes `primary_email`. If missing, uses the first available email.
    - **Field Mapping**:
        - `decision_maker_name` is split into `first_name` and `last_name` (falls back to `full_name` if missing).
        - `company_country` is normalized to "United States".
        - `decision_maker_title` is used for `job_title` (falls back to other title fields).
    - **Email optional**: Rows are retained even if no email exists.
    - **Headers**: Output headers are taken from the first row of the source sheet.

## Edge Cases & Learnings
- **String-encoded fields**: When data comes from Google Sheets, fields like `socials` and `emails_raw` may be stored as string representations of dictionaries/lists. The script now handles both string and native Python types using `ast.literal_eval()`.
- **Field variations**: The script checks multiple field name variations (e.g., `social_linkedin`, `company_linkedin`) as fallbacks.
- **Headerless tabs**: If a sheet has no header row, it must be fixed upstream before conversion.
- **Sheet names with special characters**: Sheet tabs with spaces/parentheses require quoting in A1 ranges (e.g., `'Some Sheet (2026)'!A:ZZ`). The converter now handles this automatically.
- **Range parsing failures**: If Google Sheets rejects a quoted A1 range, the converter retries with an unquoted range before failing.
