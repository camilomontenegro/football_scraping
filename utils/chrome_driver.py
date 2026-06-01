"""Chrome WebDriver setup for Selenium scrapers.

Selenium 4.6+ ships Selenium Manager, which downloads a ChromeDriver that matches
the installed Chrome and the host OS/architecture.

webdriver-manager 4.0.1 must not be used on 64-bit Windows: it always requests
the win32 ChromeDriver while caching under win64, which triggers WinError 193.
"""

from __future__ import annotations

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def create_chrome_driver(options: Options) -> webdriver.Chrome:
    """Launch Chrome; Selenium Manager resolves the ChromeDriver path."""
    return webdriver.Chrome(options=options)
