# Casualise Company Name SOP

## Goal
Shorten company names to their casual, conversational form by removing common suffixes and unnecessary words. This makes company names more approachable and natural-sounding in cold outreach emails.

## Inputs
- **Company Name**: The full, formal company name (e.g., "XYZ Agency", "Love Mayo Inc.")

## Algorithm
1. **Identify Core Name**: Extract the main brand/company name before common suffixes
2. **Remove Suffixes**: Strip away common business entity markers and descriptors:
   - Legal entity types: "Inc.", "Inc", "LLC", "Ltd", "Limited", "Corp", "Corporation", "Co.", "Company"
   - Business descriptors: "Agency", "Professional Services", "Services", "Group", "Partners", "Consulting", "Solutions", "Technologies", "Tech", "Media", "Studio", "Studios", "Productions", "Digital"
3. **Preserve Brand Identity**: Keep the core brand name intact (e.g., "Love AMS" stays "Love AMS", not just "Love")
4. **Handle Edge Cases**:
   - If the entire name is a descriptor (e.g., "The Agency"), keep it as-is
   - If removing suffixes leaves less than 2 characters, keep the original
   - Preserve acronyms and intentional capitalization

## Examples
- "XYZ Agency" → "XYZ"
- "Love AMS Professional Services" → "Love AMS"
- "Love Mayo Inc." → "Love Mayo"
- "Smith & Co. LLC" → "Smith & Co."
- "TechCorp Solutions" → "TechCorp"
- "Digital Media Studios" → "Digital Media"
- "Acme Corporation" → "Acme"
- "The Creative Group" → "The Creative Group" (or "Creative" if removing "The" and "Group")

## Tools
- `execution/casualise_company_name.py` - Main casualization script

## Output
- **Casualised Name**: The shortened, conversational version of the company name
- Can be applied to a single name or batch process a list/spreadsheet of companies

## Instructions

### Option 1: Casualise a Single Company Name
```bash
.venv/bin/python execution/casualise_company_name.py \
  --name "XYZ Agency"
```

### Option 2: Batch Process from JSON File
```bash
.venv/bin/python execution/casualise_company_name.py \
  --source-file .tmp/leads.json \
  --output .tmp/leads_casualised.json
```

### Option 3: Process Google Sheet Column
```bash
.venv/bin/python execution/casualise_company_name.py \
  --source-url "https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit" \
  --column "Company Name" \
  --output-column "Casual Name"
```

## Field Mapping
The script looks for company names in the following fields (in order of priority):
- `companyName`, `company_name`, `Company Name`
- `name`, `Name`
- `business_name`, `Business Name`

## Suffix Patterns to Remove
The script removes these common patterns (case-insensitive):
- **Legal**: Inc, Inc., LLC, Ltd, Ltd., Limited, Corp, Corp., Corporation, Co., Co, Company
- **Services**: Agency, Agencies, Professional Services, Services, Service, Consulting, Consultancy, Consultant, Consultants
- **Structure**: Group, Partners, Partnership, Associates
- **Tech/Digital**: Solutions, Technologies, Technology, Tech, Software, Digital, Media, Studio, Studios
- **Production**: Productions, Production, Creative, Creatives

## Edge Cases & Notes
- **Ambiguous Names**: If the core name is unclear, the script will preserve more of the original (e.g., "Professional Services Group" might stay as-is)
- **Acronyms**: Preserved intact (e.g., "AMS" in "Love AMS Professional Services")
- **Ampersands**: Preserved (e.g., "Smith & Jones LLC" → "Smith & Jones")
- **The**: Leading "The" is typically preserved if it's part of the brand identity
- **Multi-word Brands**: Keep together (e.g., "Blue Ocean Technologies" → "Blue Ocean")

## Use Cases
- **Cold Email Personalization**: Use casual names in email subject lines and greetings for a warmer, less formal tone
- **CRM Data Cleanup**: Standardize company names for better deduplication
- **Display Names**: Shorten names for UI display where space is limited
- **Conversational AI**: Use in chatbot responses for more natural language

## Troubleshooting
- **Name Too Short**: If output is < 2 characters, original is preserved
- **Entire Name Removed**: Script will fall back to original if casualization removes everything
- **Unexpected Results**: Use `--verbose` flag to see the casualization logic applied

## Notes
- This is a heuristic-based approach, not ML-based
- Some edge cases may require manual review
- The script is conservative - it won't casualize if it's uncertain
- All original data is preserved; casualized names are added as new fields/columns
