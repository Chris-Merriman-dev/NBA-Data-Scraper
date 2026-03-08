# NBA Data Project

This project provides an automated pipeline for collecting and processing historical basketball statistics sourced from [Basketball-Reference](https://www.basketball-reference.com/).

## Files
* **nba_data_scraper.py**: Orchestrates the data collection process. It utilizes **Playwright** to navigate the site, interact with dynamic elements, and trigger "Share & Export" menus to extract raw CSV data directly from the browser.
* **clean_data.py**: Handles data transformation and quality control. It uses **Pandas** to aggregate raw files, resolve formatting inconsistencies, and sort records into structured historical datasets.

## Technical Details
* **Scraping Logic**: 
    * The scraper uses an asynchronous **Playwright (Chromium)** workflow to handle modern web elements. 
    * It operates in phases: first indexing the alphabet-based player lists and then drilling down into individual career stats.
    * Built-in randomized delays and retry logic are used to maintain stable interactions with the site's server.
* **Data Organization & Sorting**: 
    * **Pandas** is used to parse raw CSV exports into DataFrames for efficient manipulation.
    * The logic automatically cleans up header noise (like "cite us" text) and handles the sorting of multi-year data by player ID and season age.
    * Data is organized into a directory hierarchy, sorting players into subfolders based on the first letter of their unique ID for easier file management.

## Setup & Installation
1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt

2. **Install Browser Binaries**:
    ```bash
    playwright install chromium

## Usage
1. **Scrape Data: Run the scraper to pull alphabet indices and individual player stats (regular season and playoffs).**:
    ```bash
    python nba_data_scraper.py

2. **Clean Data: Run the cleaning script to process the raw CSVs into a structured format.**:
    ```bash
    python clean_data.py