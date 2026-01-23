# Anymail Finder Decision Maker Email SOP

## Goal
Find decision maker email addresses for each lead using the Anymail Finder API. Accepts leads from CSV/JSON or Google Sheets and produces a new sheet or JSON with decision maker email data.

## Inputs
- **Input Source**: ONE of the following:
  - `source-file`: Path to a CSV or JSON file
  - `source-url`: Full URL of a Google Sheet
- **Output Destination**: ONE of the following:
  - `output`: Path for JSON output
  - `output-sheet`: Name of the new Google Sheet to create
- **Folder ID** (Optional): `--folder-id` to create the sheet in a specific Drive folder. Falls back to `GOOGLE_DRIVE_FOLDER_ID` env var if not specified.
- **Decision Maker Categories**: One or more categories, in priority order (e.g., `ceo`, `finance`, `sales`).
- **Max Leads** (Optional): Limit processing count (default: 100)
- **Include Existing** (Optional): `--include-existing` to reprocess leads with existing decision maker emails

## Environment Variables
- `ANYMAIL_FINDER_API_KEY`: Required. API key for Anymail Finder.
- `GOOGLE_DRIVE_FOLDER_ID`: Optional default Drive folder ID.

## Algorithm
1. **Load Data**: Read leads from CSV/JSON or Google Sheet.
2. **Normalize Domain**: Extract domain from fields like `website`, `companyWebsite`, `company_website`, or `domain`.
3. **Fallback Company Name**: If domain is missing, use `company_name`, `company`, or `name`.
4. **Skip Existing**: By default, process only leads missing decision maker email fields.
5. **Validate Input**: Ensure each lead has a domain or company name.
6. **Permission Check**: Ask for confirmation before running, showing:
   - Total leads
   - Leads that will be processed
   - Max leads limit
   - Estimated cost range (best effort)
7. **API Calls**: Send POST requests to Anymail Finder with the selected categories in priority order.
8. **Save Results**: Write response fields to output columns:
   - `decision_maker_email`
   - `decision_maker_email_status`
   - `decision_maker_name`
   - `decision_maker_title`
   - `decision_maker_linkedin`
9. **Output**: Save to a **new** Google Sheet or JSON file, preserving all original fields.

## Tools
- `execution/anymail_find_emails.py` - Anymail Finder decision maker email enrichment script

## Safety Rules

> [!IMPORTANT]
> **PAID API SAFETY**
> - Anymail Finder usage costs credits
> - Always show a pre-run summary and require explicit "yes" confirmation
> - Default to processing only leads without an existing decision maker email
> - Use `--max-leads` to cap usage

> [!WARNING]
> **NEVER OVERWRITE DATA**
> - Always create a **NEW** file or **NEW** sheet for output
> - Do not use the same filename for input and output
> - Preserve all original fields

## Instructions

### Option 1: From Google Sheet → New Google Sheet (Recommended)

```bash
source .venv/bin/activate
python execution/anymail_find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output-sheet "Decision Maker Emails (Jan 21)" \
  --decision-maker-category "ceo" \
  --decision-maker-category "finance" \
  --max-leads 100
```

### Option 2: From Google Sheet → New JSON

```bash
source .venv/bin/activate
python execution/anymail_find_emails.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output .tmp/decision_maker_emails.json \
  --decision-maker-category "ceo" \
  --max-leads 100
```

### Option 3: From CSV/JSON → New Google Sheet

```bash
source .venv/bin/activate
python execution/anymail_find_emails.py \
  --source-file ".tmp/leads.json" \
  --output-sheet "Decision Maker Emails (Jan 21)" \
  --decision-maker-category "ceo" \
  --max-leads 100
```

### Optional Flags
- `--sheet-name "Sheet1"`: Source sheet name (default: first sheet)
- `--folder-id "FOLDER_ID"`: Drive folder for output
- `--include-existing`: Process leads that already have decision maker email fields
- `--skip-first 1000`: Skip the first N leads from the source
- `--verbose`: Log per-lead decisions

## API Details

Anymail Finder Decision Maker endpoint:
- **Method**: POST
- **Endpoint**: `https://api.anymailfinder.com/v5.1/find-email/decision-maker`
- **Authentication**: `Authorization: ANYMAIL_FINDER_API_KEY`
- **Content-Type**: `application/json`
- **Timeout**: 180 seconds recommended
- **Cost**: 2 credits per valid email; free for risky, blacklisted, or not found

Request body:
```json
{
  "domain": "microsoft.com",
  "decision_maker_category": ["ceo"]
}
```

Response fields:
```json
{
  "email": "satyan@microsoft.com",
  "email_status": "valid",
  "person_full_name": "Satya Nadella",
  "person_job_title": "Chairman and CEO",
  "person_linkedin_url": "https://www.linkedin.com/in/satyanadella/",
  "valid_email": "satyan@microsoft.com"
}
```

## Required Fields
- **Domain** (preferred): `website`, `companyWebsite`, `company_website`, or `domain`
- **Company name** (fallback): `company`, `company_name`, or `name`

Leads missing both domain and company name are skipped.

## Troubleshooting
- **Missing ANYMAIL_FINDER_API_KEY**: Add to `.env`
- **No email found**: `email_status` will be `not_found`, and `decision_maker_email` should remain empty
- **Risky email**: Store the email but keep `decision_maker_email` empty unless explicitly allowed
- **Unauthorized**: Verify API key and header formatting

## Notes
- All intermediate files must live in `.tmp/`
- Preserve all original fields in output
- Use `valid_email` when `email_status` is `valid`
