# Find Decision Makers SOP

## Goal
Identify the most likely decision maker (owner/founder/executive) for each lead using a **waterfall enrichment** approach. Accepts leads from JSON/CSV or Google Sheets output from `execution/scrape_google_maps.py`, and produces a new sheet or JSON with decision maker data similar to the provided sample sheet.

## Inputs
- **Input Source**: ONE of the following:
  - `source-file`: Path to a JSON or CSV file
  - `source-url`: Full URL of a Google Sheet
- **Output Destination**: ONE of the following:
  - `output`: Path for JSON output
  - `output-sheet`: Name of the new Google Sheet to create
- **Folder ID** (Optional): `--folder-id` to create the sheet in a specific Drive folder. Falls back to `GOOGLE_DRIVE_FOLDER_ID` env var if not specified.
- **Max Leads** (Optional): Limit processing count (default: 100)
- **Include Existing** (Optional): `--include-existing` to reprocess leads that already have a decision maker

## Environment Variables
- `OPENAI_API_KEY`: Required for OpenAI waterfall extraction
- `DATAFORSEO_USERNAME`: Required for DataForSEO LinkedIn search
- `DATAFORSEO_PASSWORD`: Required for DataForSEO LinkedIn search
- `GOOGLE_DRIVE_FOLDER_ID`: Optional default Drive folder ID

## Algorithm (Waterfall Enrichment)
1. **Load Data**: Read leads from JSON/CSV or Google Sheet.
2. **Normalize Website**: Extract root domain from `website`, `companyWebsite`, `company_website`, or `domain`.
3. **Skip Existing**: By default, process only leads missing decision maker fields.
4. **Waterfall Steps** (stop at first strong match):
   1. **About/Contact Pages**:
      - Crawl the homepage for `About`, `About Us`, `Contact`, `Team`, `Leadership`.
      - Fetch page text and use OpenAI to extract owner/leadership names and titles.
   2. **Terms of Service / Legal Pages**:
      - Find `Terms`, `Terms of Service`, `Legal`, `Imprint`, `Privacy`.
      - Use OpenAI to extract business owner or responsible person.
   3. **LinkedIn Company Profile (via DataForSEO)**:
      - Search LinkedIn company page using company name + domain.
      - Save company LinkedIn URL if found.
   4. **LinkedIn Employees (via DataForSEO)**:
      - Pull employee list from the LinkedIn company.
      - Filter titles for decision makers: `Owner`, `Founder`, `CEO`, `President`,
        `Managing Director`, `Managing Partner`, `Principal`, `Director`, `Partner`,
        `COO`, `CFO`, `CTO`, `CMO`, `CIO`, `GM`.
      - Use OpenAI to rank the best candidate if multiple matches.
5. **Scoring**:
   - Assign a confidence score and source (`about_page`, `terms_page`, `linkedin_employees`).
6. **Output**:
   - Save results to a **new** Google Sheet or JSON file, preserving all original fields.
   - Add fields: `decision_maker_name`, `decision_maker_title`,
     `decision_maker_source`, `decision_maker_confidence`,
     `decision_maker_linkedin`, `company_linkedin`.

## Tools
- `execution/find_decision_makers.py` - Decision maker waterfall enrichment script

## Safety Rules

> [!IMPORTANT]
> **PAID API SAFETY**
> - OpenAI and DataForSEO usage costs money
> - Always show a pre-run summary:
>   - Total leads
>   - Leads that will be processed
>   - Max leads limit
>   - Estimated cost range (best effort)
> - Require explicit "yes" confirmation before running

> [!WARNING]
> **NEVER OVERWRITE DATA**
> - Always create a **NEW** file or **NEW** sheet for output
> - Do not use the same filename for input and output
> - Preserve all original fields

## Instructions

### Option 1: From Google Sheet → New Google Sheet (Recommended)

```bash
source .venv/bin/activate
python execution/find_decision_makers.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output-sheet "Decision Makers (Jan 19)" \
  --max-leads 100
```

### Option 2: From Google Sheet → New JSON

```bash
source .venv/bin/activate
python execution/find_decision_makers.py \
  --source-url "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit" \
  --output .tmp/decision_makers.json \
  --max-leads 100
```

### Option 3: From JSON/CSV → New Google Sheet

```bash
source .venv/bin/activate
python execution/find_decision_makers.py \
  --source-file ".tmp/leads.json" \
  --output-sheet "Decision Makers (Jan 19)" \
  --max-leads 100
```

### Optional Flags
- `--sheet-name "Sheet1"`: Source sheet name (default: first sheet)
- `--folder-id "FOLDER_ID"`: Drive folder for output
- `--include-existing`: Process leads that already have decision maker fields
- `--skip-first 1000`: Skip the first N leads from the source
- `--skip-dataforseo`: Skip DataForSEO Google search for LinkedIn company pages
- `--log-file ".tmp/find_decision_makers.log"`: Save console output to a log file
- `--verbose`: Log per-lead decisions

## Output Schema (Added Columns)
- `decision_maker_name`
- `decision_maker_title`
- `decision_maker_source`
- `decision_maker_confidence`
- `decision_maker_linkedin`
- `company_linkedin`

## Troubleshooting
- **Missing OPENAI_API_KEY**: Add to `.env`
- **Missing DataForSEO credentials**: Add `DATAFORSEO_USERNAME` and `DATAFORSEO_PASSWORD` to `.env`
- **No decision maker found**: Leave decision maker fields empty and continue
- **Rate limits**: Script should retry with backoff and resume from checkpoint

## Notes
- All intermediate files must live in `.tmp/`
- Checkpoint file: `.tmp/find_decision_makers_checkpoint.json`
- Similar output layout to the provided Google Sheet template
