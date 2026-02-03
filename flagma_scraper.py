"""The script scrapes https://flagma.ua/ site. It collects contact data for
each company from the specified category. The results are saved to a CSV file.

The scraping process requires dynamic IP change, for the site has anti-scrape
protection (IP ban). Therefore the script uses DataImpulse residential proxy
service. To use this script, you need to:

1. Sign up for DataImpulse at https://dataimpulse.com/
2. Copy .env.example to .env and fill in your credentials:
   - DATAIMPULSE_USERNAME, DATAIMPULSE_PASSWORD, etc.
3. Optionally adjust DATAIMPULSE_COUNTRIES in .env for targeting specific countries

Documentation: https://docs.dataimpulse.com/
"""
import logging
import json

from bs4 import BeautifulSoup

from utils.http_request import HttpRequest, PROXY_TYPE_DATAIMPULSE
from utils.scraping_utils import (
    FATAL_ERROR_STR,

    setup_logging,
    clean_text,

    # Database functions
    init_database,
    save_companies_batch_to_db,
    DATABASE_NAME,
)

# List of category URLs to scrape
all_base_urls = [
        # # # "https://flagma.kz/companies/avia-avto-zhd-more-tehnika-kompanii/",
        # # "https://flagma.kz/companies/bytovaya-tehnika-kompanii/",
        # # "https://flagma.kz/companies/bytovye-tovary-odezhda-kompanii/",
        # # "https://flagma.kz/companies/drevesina-bumaga-kompanii/",
        "https://flagma.kz/companies/kompyutery-ofis-kompanii/", # !
        "https://flagma.kz/companies/metally-metalloprokat-kompanii/", # Остановлен ошибок 400 при обработке 54 стр
        # "https://flagma.kz/companies/oborudovanie-kompanii/",
        # "https://flagma.kz/companies/produkty-pitaniya-kompanii/",
        # "https://flagma.kz/companies/selskoe-hozyaystvo-kompanii/",
        # "https://flagma.kz/companies/stroitelnye-materialy-kompanii/",
        # "https://flagma.kz/companies/tara-i-upakovka-kompanii/",
        # "https://flagma.kz/companies/himiya-neftehimiya-ugol-kompanii/",
        # "https://flagma.kz/companies/elektrotehnicheskie-kompanii/"
]

SKIPPED_PAGES_REPORT = 'skipped_pages.json'

setup_logging()
request = HttpRequest(proxies=PROXY_TYPE_DATAIMPULSE)

def get_html(url: str) -> str:
    r = request.get(url)
    if r == None:
        return None
    return r.text

def save_skipped_pages_report(skipped_info: dict, filename: str = SKIPPED_PAGES_REPORT) -> bool:
    """Saves skipped pages report to JSON file.

    Args:
        skipped_info: Dict with format {category_url: {max_pages: int, skipped_pages: list}}
        filename: Output filename

    Returns:
        bool: True if successful
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(skipped_info, f, ensure_ascii=False, indent=2)
        logging.info(f'Skipped pages report saved to: {filename}')
        return True
    except OSError:
        logging.exception(f"Can't save skipped pages report to {filename}.")
        return False

def parse_company_list(page: int, page_url_template: str) -> list:
    """Parses company data from a list page.

    Extracts company_id, company_name, company_type, and city directly
    from list page without visiting detail pages.

    Args:
        page: Page number to scrape
        page_url_template: URL template with {} for page number

    Returns:
        list: List of company dicts, or None on error
    """
    html = get_html(page_url_template.format(page))
    if not html:
        return None

    companies = []

    try:
        soup = BeautifulSoup(html, 'lxml')

        # Find all company blocks
        company_divs = soup.find_all('div', class_='page-list-item container job')

        # Fallback to alternative selector if primary fails
        if not company_divs:
            logging.warning('Primary selector failed, trying fallback selector.')
            company_divs = soup.find_all('div', class_='page-list-item')

        for company_div in company_divs:
            company = {}

            # Extract company_id from URL
            try:
                header_div = company_div.find('div', class_='header')
                if header_div:
                    link = header_div.find('a', href=True)
                    if link:
                        href = link['href']
                        # Extract ID from URL: https://flagma.kz/737522/ -> 737522
                        url_parts = href.rstrip('/').split('/')
                        company_id = url_parts[-1] if url_parts else ''
                        if company_id.isdigit():
                            company['company_id'] = company_id
                        else:
                            company['company_id'] = ''
                    else:
                        company['company_id'] = ''
                else:
                    company['company_id'] = ''
            except (AttributeError, KeyError, IndexError):
                company['company_id'] = ''

            # Extract company_name and company_type from header text
            try:
                header_div = company_div.find('div', class_='header')
                if header_div:
                    # Get full text: "Shini, ТОО"
                    full_text = clean_text(header_div.get_text())

                    # Split by comma to separate name and type
                    if ',' in full_text:
                        parts = full_text.split(',', 1)
                        company['company_name'] = parts[0].strip()
                        company['company_type'] = parts[1].strip()
                    else:
                        company['company_name'] = full_text
                        company['company_type'] = ''
                else:
                    company['company_name'] = ''
                    company['company_type'] = ''
            except AttributeError:
                company['company_name'] = ''
                company['company_type'] = ''

            # Extract city from location span
            try:
                location_span = company_div.find('span', itemprop='location')
                if location_span:
                    city_span = location_span.find('span', itemprop='name')
                    if city_span:
                        company['city'] = clean_text(city_span.get_text())
                    else:
                        company['city'] = ''
                else:
                    company['city'] = ''
            except AttributeError:
                company['city'] = ''

            # Only add if we have at least company_id or company_name
            if company.get('company_id') or company.get('company_name'):
                companies.append(company)
            else:
                logging.warning(f'Skipped company entry with no ID or name on page {page}.')

    except (AttributeError, KeyError) as e:
        logging.exception(f'Error while parsing company list on page {page}.')
        return None

    logging.info(f'Parsed {len(companies)} companies from page {page}.')
    return companies

def get_page_count(base_url: str) -> int:
    """Gets total page count for a category.

    Args:
        base_url: Base URL of the category

    Returns:
        int: Page count or None on error
    """
    html = get_html(base_url)
    if not html:
        return None

    try:
        page_count = int(
            BeautifulSoup(html, 'lxml')
            .find('li', class_='page notactive').span.get_text())
    except (AttributeError, ValueError):
        logging.exception('Error while parsing page count.')
        return None

    return page_count

def scrape_page_companies(page: int, page_url_template: str, category_url: str) -> int:
    """Scrapes companies from a single page and saves to database.

    Args:
        page: Page number to scrape
        page_url_template: URL template for pages
        category_url: Category URL for database tracking

    Returns:
        int: Number of companies saved, or -1 on error
    """
    logging.info(f'Scraping companies for page {page}.')

    companies = parse_company_list(page, page_url_template)
    if companies is None:
        logging.error(f'Failed to parse page {page}.')
        return -1

    if not companies:
        logging.warning(f'No companies found on page {page}.')
        return 0

    if save_companies_batch_to_db(companies, category_url):
        logging.info(f'Successfully saved page {page} to database.')
        return len(companies)
    else:
        logging.error(f'Failed to save page {page} to database.')
        return -1

def scrape_category(base_url: str) -> dict:
    """Scrapes all pages from a single category.

    Args:
        base_url: Base URL of the category

    Returns:
        dict: Statistics {
            'max_pages': int,
            'successful_pages': list,
            'skipped_pages': list,
            'total_companies': int
        }
    """
    logging.info('='*80)
    logging.info(f'Processing category: {base_url}')
    logging.info('='*80)

    # Rotate proxy before getting page count
    request.rotate_proxy()

    page_count = get_page_count(base_url)
    if page_count is None:
        logging.error(f'Failed to get page count for {base_url}.')
        return {
            'max_pages': 0,
            'successful_pages': [],
            'skipped_pages': [],
            'total_companies': 0
        }

    logging.info(f'Total page count: {page_count}.')

    # Build page URL template
    page_url_template = base_url.rstrip('/') + '/page-{}/?sort=date'

    total_companies = 0
    successful_pages = []
    skipped_pages = []

    for page in range(1, page_count + 1):
        # Rotate proxy before each page
        request.rotate_proxy()

        result = scrape_page_companies(page, page_url_template, base_url)

        if result == -1:
            skipped_pages.append(page)
            logging.error(f'Page {page} failed.')
        elif result >= 0:
            successful_pages.append(page)
            total_companies += result

    logging.info(f'Category complete. Total companies: {total_companies}')
    logging.info(f'Successful pages: {len(successful_pages)}/{page_count}')

    if skipped_pages:
        logging.warning(f'Skipped pages: {skipped_pages}')

    return {
        'max_pages': page_count,
        'successful_pages': successful_pages,
        'skipped_pages': skipped_pages,
        'total_companies': total_companies
    }

def scrape_all_categories(base_urls: list) -> dict:
    """Scrapes all categories from the list.

    Args:
        base_urls: List of category base URLs

    Returns:
        dict: Report with format {category_url: {max_pages: int, skipped_pages: list}}
    """
    logging.info(f'Starting scraping process for {len(base_urls)} categories.')

    skipped_report = {}
    total_companies_all = 0

    for idx, base_url in enumerate(base_urls, 1):
        logging.info(f'\n{"="*80}')
        logging.info(f'Category {idx}/{len(base_urls)}: {base_url}')
        logging.info(f'{"="*80}\n')

        stats = scrape_category(base_url)

        # Add to report
        skipped_report[base_url] = {
            'max_pages': stats['max_pages'],
            'skipped_pages': stats['skipped_pages']
        }

        total_companies_all += stats['total_companies']

    # Summary
    logging.info('\n' + '='*80)
    logging.info('SCRAPING SUMMARY')
    logging.info('='*80)
    logging.info(f'Total categories processed: {len(base_urls)}')
    logging.info(f'Total companies saved: {total_companies_all}')

    # Count total skipped pages
    total_skipped = sum(len(info['skipped_pages']) for info in skipped_report.values())
    logging.info(f'Total skipped pages across all categories: {total_skipped}')

    return skipped_report

def main():
    logging.info('Starting scraping process.')

    # Initialize database
    if not init_database():
        logging.error(FATAL_ERROR_STR)
        return

    # Scrape all categories
    skipped_report = scrape_all_categories(all_base_urls)

    # Save skipped pages report
    if save_skipped_pages_report(skipped_report):
        logging.info(f'Skipped pages report saved to: {SKIPPED_PAGES_REPORT}')
    else:
        logging.warning('Failed to save skipped pages report.')

    logging.info('Scraping process complete.')
    logging.info(f'Database file: {DATABASE_NAME}')
    logging.info(f'Skipped pages report: {SKIPPED_PAGES_REPORT}')

if __name__ == '__main__':
    main()
