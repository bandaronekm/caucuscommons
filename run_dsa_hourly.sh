#!/bin/zsh

cd "/Users/marcbandaronek/dsa-scraper" || exit 1

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

"/Users/marcbandaronek/dsa-scraper/venv/bin/python3" \
"/Users/marcbandaronek/dsa-scraper/dsa.py" \
>> "/Users/marcbandaronek/dsa-scraper/dsa_scheduler.log" 2>&1
