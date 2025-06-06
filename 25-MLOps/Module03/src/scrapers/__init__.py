"""
Web Scraping Module for Horse Racing Data

Learning objectives:
1. Understand ethical web scraping practices
2. Handle different website structures (static vs dynamic)
3. Implement rate limiting and respectful scraping
4. Parse HTML/JSON responses effectively
5. Handle errors and edge cases gracefully

Available scrapers:
- TABScraper: For TAB.nz (New Zealand)
- TracksideScraper: For Trackside.co.nz
- RacingScraper: For Racing.com (backup/international)
"""

from .base import BaseScraper
from .tab_scraper import TABScraper

__all__ = ["BaseScraper", "TABScraper"]