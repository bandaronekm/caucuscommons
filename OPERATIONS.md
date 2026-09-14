# Production Operations Notes

Project root: `/Users/marcbandaronek/dsa-scraper`

Site output root: `/Users/marcbandaronek/Documents/fun/my website/onquarryrd mov`

LaunchAgent: `~/Library/LaunchAgents/com.marc.dsa.plist`

Production database: `/Users/marcbandaronek/dsa-scraper/data/dsa_intel.db`

The Cloudflare token must remain outside Git at:
`/Users/marcbandaronek/dsa-scraper/cloudflare_api_token.txt`

Restore Python packages with:
`python3 -m venv venv && venv/bin/python3 -m pip install -r requirements.txt`

Restore Node packages with:
`npm ci`
