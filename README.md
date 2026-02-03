# Flagma Scraper
[![Ask DeepWiki](https://devin.ai/assets/askdeepwiki.png)](https://deepwiki.com/12farit21/flagma_scraper)

This repository contains a Python-based web scraper designed to extract company information from the Flagma business portal (e.g., `flagma.kz`). It is built to handle anti-scraping measures by leveraging dynamic IP rotation through various proxy solutions. The collected data is saved into a local SQLite database for persistence and easy access.

## Features

*   **Company Data Extraction**: Scrapes company ID, name, legal type, and city from specified Flagma categories.
*   **Robust Proxy Integration**: Built to bypass IP-based blocking with support for multiple proxy backends.
*   **Recommended Proxy**: Natively integrates with [DataImpulse](https://dataimpulse.com/) for reliable residential proxies.
*   **Alternative Proxies**: Includes support for Tor and free public proxies as alternatives.
*   **Resilient Scraping**: Automatically rotates proxies and retries failed requests.
*   **Persistent Storage**: Saves scraped data efficiently into a SQLite database (`flagma_companies.db`), avoiding duplicates.
*   **Error Reporting**: Logs progress and generates a `skipped_pages.json` report for any pages that failed to process.
*   **Configurable**: All sensitive credentials and paths are managed via a `.env` file.

## Requirements

*   Python 3.x
*   Dependencies listed in `requirements.txt`

## Setup and Configuration

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/12farit21/flagma_scraper.git
    cd flagma_scraper
    ```

2.  **Install the required Python packages:**
    ```sh
    pip install -r requirements.txt
    ```

3.  **Create and configure your environment file:**
    Copy the example file to create your own configuration.
    ```sh
    cp .env.example .env
    ```
    Open the `.env` file and fill in the necessary credentials. The scraper is primarily configured to use DataImpulse residential proxies.

    **For DataImpulse (Recommended):**
    *   Sign up at [https://dataimpulse.com/](https://dataimpulse.com/).
    *   Fill in your credentials in the `.env` file:
        ```ini
        # DataImpulse proxy credentials
        DATAIMPULSE_USERNAME=your_username
        DATAIMPULSE_PASSWORD=your_password
        DATAIMPULSE_HOST=gw.dataimpulse.com
        DATAIMPULSE_PORT=823
        # Comma-separated list of country codes for proxy location
        DATAIMPULSE_COUNTRIES=ua,kz,ru
        ```

    **For Tor (Alternative):**
    *   Ensure you have Tor installed and provide the path to the executable.
        ```ini
        # Path to the Tor executable
        TOR_EXECUTABLE_PATH=C:/Tor/Tor/tor.exe
        ```
    *   To use Tor, you must modify `flagma_scraper.py` to set `proxies=PROXY_TYPE_TOR` when creating the `HttpRequest` object.

## Usage

1.  **Select Categories to Scrape:**
    Open `flagma_scraper.py` and edit the `all_base_urls` list to include the Flagma category URLs you wish to target.

    ```python
    # Example:
    all_base_urls = [
            "https://flagma.kz/companies/kompyutery-ofis-kompanii/",
            "https://flagma.kz/companies/metally-metalloprokat-kompanii/",
    ]
    ```

2.  **Run the Scraper:**
    Execute the main script from your terminal.
    ```sh
    python flagma_scraper.py
    ```
    The scraper will begin processing each category, displaying progress in the console and saving logs to the `logs/` directory.

3.  **Check the Output:**
    *   **Database**: All successfully scraped company data will be stored in `flagma_companies.db`. You can use any SQLite browser to view the data.
    *   **Skipped Pages Report**: A file named `skipped_pages.json` will be created, detailing any pages that could not be processed, which can be useful for retrying later.

## Code Structure

The project is organized into a main script and a `utils` directory containing modular components.

```
.
├── .env.example          # Environment variable template for credentials
├── flagma_scraper.py     # Main scraper script and entry point
├── requirements.txt      # Project dependencies
└── utils/
    ├── http_request.py       # Core class for making HTTP requests with proxy rotation
    ├── scraping_utils.py     # Helpers for database (SQLite), logging, and I/O
    ├── dataimpulse_proxy.py  # Integration logic for DataImpulse proxies
    ├── tor_proxy.py          # Integration logic for using Tor as a proxy
    └── free_proxy.py         # Integration logic for using free public proxies
