# Create Campaign + Upload CSV SOP

## Goal
Create a new campaign via the API and upload a CSV of leads to that campaign.

## Inputs
- **Campaign fields** (required):
  - `name`: Campaign name
  - `service_line`: Service line description (used in icebreaker)
  - `summarize_prompt`: Prompt template for summarizing company pages
  - `icebreaker_prompt`: Prompt template for generating icebreaker messages
- **CSV file**: Path to a CSV file with lead data
- **API base URL**: Default `http://localhost:3000`

## Endpoints
- `POST /api/campaigns`
  - Body: JSON with required campaign fields
  - Response: `{ campaign: { id: string, ... } }`
- `POST /api/campaigns/{id}/upload`
  - Body: `multipart/form-data` with `file=@leads.csv`
  - Response: `{ inserted: number }`

## Tools
- `execution/create_campaign.py` - Create campaign and upload mapped CSV

## CSV Column Mapping
Accepts both CamelCase and snake_case:
- `First Name` or `first_name`
- `Last Name` or `last_name`
- `Full Name` or `full_name` (optional; generated if missing)
- `Email` or `email`
- `Personal Email` or `personal_email`
- `Company Name` or `company_name`
- `Company Website` or `company_website` or `Company Domain` or `company_domain`
- `LinkedIn` or `linkedin`
- `Title` or `title`
- `Industry` or `industry`
- `City` or `city`
- `State` or `state`
- `Country` or `country`

### Apollo Export (Observed Columns)
Current sheet headers from the Apollo Export tab:
- `first_name`, `last_name`, `full_name`, `job_title`
- `email`
- `company_name`, `company_domain`, `company_website`
- `company_phone`, `company_linkedin`, `person_linkedin`
- `company_address`, `company_city`, `company_state`, `company_zip`, `company_country`
- `company_category`, `source`

Unrecognized columns are preserved in `raw` during upload.

### Apollo Export (Recommended Mapping)
Some API instances reject the raw Apollo headers. If upload returns `500`, map to supported fields:
- `job_title` → `title`
- `person_linkedin` → `linkedin`
- `company_category` → `industry`
- `company_city` → `city`
- `company_state` → `state`
- `company_country` → `country`

Minimal mapped header set:
```
first_name,last_name,full_name,email,company_name,company_website,company_domain,linkedin,title,industry,city,state,country
```

## Algorithm
1. Create the campaign with `POST /api/campaigns`.
2. Capture the returned `campaign.id`.
3. Upload the CSV with `POST /api/campaigns/{id}/upload` as `multipart/form-data`.
4. Confirm the response `inserted` count.

## Instructions

### Option 1: Run via Execution Tool (Recommended)
```bash
source .venv/bin/activate
python execution/create_campaign.py \
  --name "Apollo Export - Jan 2026" \
  --service-line "To make a long story short, we help agencies scale their SEO operations with AI-powered content strategies." \
  --summarize-prompt "You are summarizing a company's page for sales research. Write a tight 3-5 bullet summary.\nPage URL: {url}\nContent (markdown):\n{markdown}" \
  --icebreaker-prompt "Write a 2-3 paragraph, personable cold open for {firstName} at {companyName}.\nUse insights from these page summaries:\n{pageSummaries}\nInclude this service line: {serviceLine}\nTone: curious, succinct, no fluff. No subject line. Output only the message." \
  --source-url "https://docs.google.com/spreadsheets/d/1GlqtVPyui1eWg4p0P7fe--VZK3K8bDXjCbPnOOx-vSc/edit?gid=1274498018#gid=1274498018" \
  --sheet-name "Apollo Export" \
  --limit 100
```

### Step 1: Create Campaign (cURL)
```bash
curl -X POST http://localhost:3000/api/campaigns \
  -H "Content-Type: application/json" \
  -d '{
    "name": "SEO Agencies - Jan 2026",
    "service_line": "To make a long story short, we help agencies scale their SEO operations with AI-powered content strategies.",
    "summarize_prompt": "You are summarizing a company'\''s page for sales research. Write a tight 3–5 bullet summary.\nPage URL: {url}\nContent (markdown):\n{markdown}",
    "icebreaker_prompt": "Write a 2–3 paragraph, personable cold open for {firstName} at {companyName}.\nUse insights from these page summaries:\n{pageSummaries}\nInclude this service line: {serviceLine}\nTone: curious, succinct, no fluff. No subject line. Output only the message."
  }'
```

### Step 2: Upload CSV (cURL)
```bash
CAMPAIGN_ID="<campaign_id_from_step_1>"
curl -X POST "http://localhost:3000/api/campaigns/${CAMPAIGN_ID}/upload" \
  -F "file=@leads.csv"
```

## Optional Examples

### JavaScript (fetch)
```javascript
async function createCampaign() {
  const response = await fetch("/api/campaigns", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: "SEO Agencies - Jan 2026",
      service_line: "To make a long story short, we help agencies scale their SEO operations with AI-powered content strategies.",
      summarize_prompt: "You are summarizing a company's page for sales research. Write a tight 3–5 bullet summary.\nPage URL: {url}\nContent (markdown):\n{markdown}",
      icebreaker_prompt: "Write a 2–3 paragraph, personable cold open for {firstName} at {companyName}.\nUse insights from these page summaries:\n{pageSummaries}\nInclude this service line: {serviceLine}\nTone: curious, succinct, no fluff. No subject line. Output only the message."
    })
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || "Failed to create campaign");
  }

  const data = await response.json();
  return data.campaign.id;
}

async function uploadCSV(campaignId, csvFile) {
  const formData = new FormData();
  formData.append("file", csvFile);

  const response = await fetch(`/api/campaigns/${campaignId}/upload`, {
    method: "POST",
    body: formData
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || "Failed to upload CSV");
  }

  const data = await response.json();
  return data.inserted;
}
```

### Python (requests)
```python
import requests

def create_campaign():
    url = "http://localhost:3000/api/campaigns"
    payload = {
        "name": "SEO Agencies - Jan 2026",
        "service_line": "To make a long story short, we help agencies scale their SEO operations with AI-powered content strategies.",
        "summarize_prompt": "You are summarizing a company's page for sales research. Write a tight 3–5 bullet summary.\nPage URL: {url}\nContent (markdown):\n{markdown}",
        "icebreaker_prompt": "Write a 2–3 paragraph, personable cold open for {firstName} at {companyName}.\nUse insights from these page summaries:\n{pageSummaries}\nInclude this service line: {serviceLine}\nTone: curious, succinct, no fluff. No subject line. Output only the message."
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["campaign"]["id"]

def upload_csv(campaign_id, csv_file_path):
    url = f"http://localhost:3000/api/campaigns/{campaign_id}/upload"
    with open(csv_file_path, "rb") as f:
        response = requests.post(url, files={"file": f})
    response.raise_for_status()
    return response.json()["inserted"]
```

## Notes
- Upload endpoint deduplicates by `(campaign_id, email)` and normalizes emails to lowercase.
- Company websites are normalized by stripping `http://` and `https://`.
- If `Full Name` is missing, it is generated from `First Name` + `Last Name`.
- CSVs are processed in chunks of 500 rows.
- All original CSV data is stored in the `raw` field.
