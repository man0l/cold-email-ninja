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
- `headers_from_url`: (Optional) Spreadsheet URL to fetch output headers from (defaults to Emails Sample 20 sheet)
- `headers_sheet_name`: (Optional) Sheet name to fetch output headers from (defaults to Emails Sample 20 (Jan 24 2026))

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
        - `full_name` is split into `first_name` and `last_name`.
        - `company_country` is normalized to "United States".
        - `job_title` is extracted if available.
    - **Email required**: Leads without emails are excluded.
    - **Headers**: Output headers are pulled from the template sheet:
        - https://docs.google.com/spreadsheets/d/1B0dlnl-76PhdpYn5vgwI_m1KNL01zgyUzF2FsjMzDIA/edit
        - Sheet: `Emails Sample 20 (Jan 24 2026)`

## Edge Cases & Learnings
- **String-encoded fields**: When data comes from Google Sheets, fields like `socials` and `emails_raw` may be stored as string representations of dictionaries/lists. The script now handles both string and native Python types using `ast.literal_eval()`.
- **Field variations**: The script checks multiple field name variations (e.g., `social_linkedin`, `company_linkedin`) as fallbacks.
- **Headerless tabs**: Some sheets store data rows without a header row (e.g., a filtered "first 100" tab). In that case, pull headers from a source sheet that has them and rebuild each row by index before converting, otherwise every field maps to empty values.
