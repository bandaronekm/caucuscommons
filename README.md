# DSA Scraper Production Source

This repository contains the verified active-production DSA scraper baseline.

## Active source

- `dsa.py` is the production entry point.
- `cleanup_cloudflare_deployments.py` is its only detected local Python dependency.
- `run_dsa_hourly.sh` is the active launch wrapper.
- `deployment/com.marc.dsa.plist` records the active LaunchAgent.
- `package.json` and `package-lock.json` declare Wrangler.
- `requirements.txt` records the audited Python packages.

## Audited environment

- Python 3.14.6
- Wrangler 4.110.0
- Beautiful Soup 4.15.0
- lxml 6.1.3

The recorded environment includes lxml for Beautiful Soup XML fallback parsing.

## Exclusions

Credentials, databases, logs, generated site files, virtual environments,
node_modules, and July experimental material are intentionally excluded.

## Machine-specific paths

The committed wrapper and LaunchAgent document current operation on this Mac and
contain absolute paths under `/Users/marcbandaronek`.

## License

Copyright 2026 Marc Bandaronek.

Caucus Commons is source-available under the PolyForm Noncommercial License
1.0.0. Noncommercial use, modification, and distribution are permitted subject
to the terms in LICENSE.

Commercial use is not permitted without a separate license from the copyright
holder.

Third-party dependencies and components remain subject to their respective
licenses.
