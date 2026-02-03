"""DataImpulse residential proxy service integration.

DataImpulse provides residential proxies with country targeting.
Documentation: https://docs.dataimpulse.com/

Basic authentication format:
    USERNAME__cr.COUNTRY1,COUNTRY2:PASSWORD@gw.dataimpulse.com:823

Example:
    myusername__cr.kz,ru,ua:mypassword@gw.dataimpulse.com:823

All credentials and settings are loaded from the .env file in the project root.
"""

import os
from dotenv import load_dotenv

load_dotenv()

DATAIMPULSE_USERNAME = os.environ['DATAIMPULSE_USERNAME']
DATAIMPULSE_PASSWORD = os.environ['DATAIMPULSE_PASSWORD']
DATAIMPULSE_HOST = os.environ['DATAIMPULSE_HOST']
DATAIMPULSE_PORT = int(os.environ['DATAIMPULSE_PORT'])
DATAIMPULSE_COUNTRIES = os.environ['DATAIMPULSE_COUNTRIES'].split(',')

# Constructed proxy URL
def get_dataimpulse_proxy_url(username: str = DATAIMPULSE_USERNAME,
                               password: str = DATAIMPULSE_PASSWORD,
                               countries: list = DATAIMPULSE_COUNTRIES,
                               host: str = DATAIMPULSE_HOST,
                               port: int = DATAIMPULSE_PORT) -> str:
    """Constructs DataImpulse proxy URL with country filtering.

    Args:
        username: DataImpulse account username
        password: DataImpulse account password
        countries: List of country codes (e.g., ['ua', 'kz', 'ru'])
        host: DataImpulse proxy server host
        port: DataImpulse proxy server port

    Returns:
        Proxy URL in format: http://username__cr.countries:password@host:port
    """
    country_filter = ','.join(countries)
    proxy_url = f'http://{username}__cr.{country_filter}:{password}@{host}:{port}'
    return proxy_url


def get_dataimpulse_proxies(username: str = DATAIMPULSE_USERNAME,
                             password: str = DATAIMPULSE_PASSWORD,
                             countries: list = DATAIMPULSE_COUNTRIES,
                             host: str = DATAIMPULSE_HOST,
                             port: int = DATAIMPULSE_PORT) -> dict:
    """Returns proxies dict for use with requests library.

    Returns:
        Dict with 'http' and 'https' proxy URLs for requests
    """
    proxy_url = get_dataimpulse_proxy_url(username, password, countries, host, port)
    return {
        'http': proxy_url,
        'https': proxy_url
    }


class DataImpulseProxy:
    """DataImpulse proxy manager for rotating residential proxies."""

    def __init__(self, username: str = DATAIMPULSE_USERNAME,
                 password: str = DATAIMPULSE_PASSWORD,
                 countries: list = DATAIMPULSE_COUNTRIES,
                 host: str = DATAIMPULSE_HOST,
                 port: int = DATAIMPULSE_PORT):
        """Initialize DataImpulse proxy manager.

        Args:
            username: DataImpulse account username
            password: DataImpulse account password
            countries: List of country codes for targeting
            host: Proxy server host
            port: Proxy server port
        """
        self.username = username
        self.password = password
        self.countries = countries
        self.host = host
        self.port = port

    def get_proxy_url(self) -> str:
        """Get formatted proxy URL."""
        return get_dataimpulse_proxy_url(
            self.username, self.password, self.countries, self.host, self.port
        )

    def get_proxies(self) -> dict:
        """Get proxies dict for requests library."""
        return get_dataimpulse_proxies(
            self.username, self.password, self.countries, self.host, self.port
        )


# For testing
if __name__ == '__main__':
    import requests

    # Test configuration
    print("DataImpulse Proxy Configuration:")
    print(f"Host: {DATAIMPULSE_HOST}:{DATAIMPULSE_PORT}")
    print(f"Countries: {', '.join(DATAIMPULSE_COUNTRIES)}")

    proxy_url = get_dataimpulse_proxy_url()
    print(f"\nProxy URL: {proxy_url}")

    proxies = get_dataimpulse_proxies()
    print(f"\nProxies dict: {proxies}")

    # Test connection (uncomment when credentials are configured)
    # print("\nTesting proxy connection...")
    # try:
    #     r = requests.get('https://api.ipify.org/', proxies=proxies, timeout=10)
    #     print(f"Current IP: {r.text}")
    #     print("Proxy connection successful!")
    # except Exception as e:
    #     print(f"Proxy connection failed: {e}")
