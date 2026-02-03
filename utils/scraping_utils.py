import csv
import json
import re
import os
import os.path
import logging
import logging.handlers
import unicodedata
import sqlite3
from datetime import datetime

# Directory name for saving log files
LOG_FOLDER = 'logs'

# Log file name
LOG_NAME = 'scraper.log'

# Full path to the log file
LOG_PATH = os.path.join(LOG_FOLDER, LOG_NAME)

# Maximum log file size
LOG_SIZE = 2 * 1024 * 1024

# Log files count for cyclic rotation
LOG_BACKUPS = 2

# Common text for displaying while script is shutting down
FATAL_ERROR_STR = 'Fatal error. Shutting down.'

# Characters not allowed in filenames
FORBIDDEN_CHAR_RE = r'[<>:"\/\\\|\?\*]'

NL = '\r\n'
LT = '\r\n'
CSV_DELIMITER = ','

LAST_PROCESSED_PAGE_FILENAME = 'last_processed_page.txt'

# Database configuration
DATABASE_NAME = 'flagma_companies.db'

# Setting up configuration for logging
def setup_logging():
    logFormatter = logging.Formatter(
        fmt='[%(asctime)s] %(filename)s:%(lineno)d %(levelname)s - %(message)s',
        datefmt='%d.%m.%Y %H:%M:%S')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    if not os.path.exists(LOG_FOLDER):
        try:
            os.mkdir(LOG_FOLDER)
        except OSError:
            logging.warning("Can't create log folder.")

    if os.path.exists(LOG_FOLDER):
        fileHandler = logging.handlers.RotatingFileHandler(
            LOG_PATH, mode='a', encoding='utf-8', maxBytes=LOG_SIZE,
            backupCount=LOG_BACKUPS)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)

def fix_filename(filename: str, subst_char: str='_') -> str:
    return re.sub(FORBIDDEN_CHAR_RE, subst_char, filename)

def remove_umlauts(text: str) -> str:
    return (unicodedata.normalize('NFKD', text)
            .encode('ASCII', 'ignore')
            .decode('utf-8'))

def clean_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text.strip())

# Saves last processed page
def save_last_page(page: int) -> bool:
    try:
        with open(LAST_PROCESSED_PAGE_FILENAME, 'w') as f:
            f.write(str(page))
    except OSError:
        logging.exception("Can't save last processed page to a file.")
        return False
    return True

# Loads previously saved last processed page
def load_last_page() -> int:
    page = 0
    if os.path.exists(LAST_PROCESSED_PAGE_FILENAME):
        try:
            with open(LAST_PROCESSED_PAGE_FILENAME, 'r') as f:
                page = int(f.read())
        except OSError:
            logging.warning("Can't load last processed page from file.")
        except ValueError:
            logging.exception(f'File {LAST_PROCESSED_PAGE_FILENAME} '
                              'is currupted.')
    return page

# Saving prepared item data to a CSV file
def save_item_csv(item: dict, columns: list, filename: str,
                  first_item=False) -> bool:
    try:
        with open(filename, 'w' if first_item else 'a',
                  newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=CSV_DELIMITER, lineterminator=LT)
            if first_item:
                writer.writerow(columns)
            writer.writerow([item[key] for key in columns])
    except OSError:
        logging.exception(f"Can't write to CSV file {filename}.")
        return False
    except Exception as e:
        logging.exception('Scraped data saving fault.')
        return False

    return True

# Saves prepared items list to a CSV file
def save_items_csv(items: list, columns: list, filename: str) -> bool:
    for index, item in enumerate(items):
        if not save_item_csv(item, columns, filename,
                             first_item = (index == 0)):
            return False

    return True

def load_items_csv(filename: str, columns: list) -> list:
    if not os.path.exists(filename):
        return []

    items = []

    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=CSV_DELIMITER, lineterminator=LT)
            next(reader)
            for row in reader:
                item = {}
                for index, key in enumerate(columns):
                    item[key] = row[index]
                items.append(item)
    except OSError:
        logging.exception(f"Can't read CSV file {filename}.")
    except Exception:
        logging.exception('CVS file reading fault.')

    return items

# Saves item list to a JSON file
def save_items_json(items: list, filename: str) -> bool:
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
    except OSError:
        logging.exception(f"Can't write to the file {filename}.")
        return False

    return True

def load_items_json(filename: str) -> list:
    try:
        with open(filename, encoding='utf-8') as f:
            items = json.load(f)
    except OSError:
        logging.warning(f"Can't load the file {filename}.")
        return []

    return items

# Database functions
def init_database(db_name: str = DATABASE_NAME) -> bool:
    """Creates SQLite database and companies table if not exists.

    Args:
        db_name: Database filename

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id TEXT UNIQUE,
                company_name TEXT,
                company_type TEXT,
                city TEXT,
                category_url TEXT,
                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()
        logging.info(f'Database initialized: {db_name}')
        return True

    except sqlite3.Error:
        logging.exception(f"Can't initialize database {db_name}.")
        return False

def save_companies_batch_to_db(companies: list, category_url: str,
                                db_name: str = DATABASE_NAME) -> bool:
    """Saves multiple companies in a single transaction.

    Args:
        companies: List of company dicts
        category_url: The category URL being scraped
        db_name: Database filename

    Returns:
        bool: True if successful, False otherwise
    """
    if not companies:
        return True

    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        data = [
            (
                company.get('company_id', ''),
                company.get('company_name', ''),
                company.get('company_type', ''),
                company.get('city', ''),
                category_url
            )
            for company in companies
        ]

        cursor.executemany('''
            INSERT OR IGNORE INTO companies
            (company_id, company_name, company_type, city, category_url)
            VALUES (?, ?, ?, ?, ?)
        ''', data)

        conn.commit()
        rows_affected = cursor.rowcount
        conn.close()

        logging.info(f"Saved {rows_affected} new companies to database.")
        return True

    except sqlite3.Error:
        logging.exception(f"Can't save companies batch to database.")
        return False
