"""
Base Scraper Class - Learning Foundation

LEARNING OBJECTIVES:
1. Understand abstract base classes and inheritance
2. Learn about rate limiting and ethical scraping
3. Practice error handling and logging patterns
4. Implement session management for web scraping

TODO FOR STUDENT: Study this base class before implementing specific scrapers
"""

import requests
import time
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
import random
from dataclasses import dataclass

# Configure logging for learning visibility
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ScrapingConfig:
    """Configuration for scraping behavior - learn about dataclasses!"""
    base_delay: float = 1.0  # Minimum delay between requests
    max_delay: float = 3.0   # Maximum delay (for randomization)
    timeout: int = 30        # Request timeout in seconds
    max_retries: int = 3     # Number of retry attempts
    respect_robots: bool = True  # Check robots.txt
    user_agent: str = "Mozilla/5.0 (compatible; HorseRacing-MLOps-Student/1.0)"


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    
    LEARNING NOTES:
    - ABC (Abstract Base Class) ensures all scrapers implement required methods
    - Session management improves performance by reusing connections
    - Rate limiting shows respect for target websites
    - Error handling prevents crashes and provides useful debugging info
    
    TODO FOR STUDENT:
    1. Research what robots.txt is and why it matters
    2. Understand why we use sessions vs individual requests
    3. Learn about User-Agent strings and their importance
    4. Study the retry mechanism implementation
    """
    
    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.config = config or ScrapingConfig()
        self.session = self._create_session()
        self.robots_parser = None
        
        # Track request statistics for learning
        self.request_count = 0
        self.failed_requests = 0
        
        logger.info(f"Initialized {self.__class__.__name__} with config: {self.config}")
    
    def _create_session(self) -> requests.Session:
        """
        Create a configured requests session.
        
        LEARNING NOTE: Sessions maintain cookies, connection pooling,
        and other state across requests - much more efficient!
        """
        session = requests.Session()
        session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        return session
    
    def check_robots_txt(self, base_url: str) -> bool:
        """
        Check if scraping is allowed according to robots.txt
        
        TODO FOR STUDENT:
        1. Visit a few websites and check their /robots.txt
        2. Understand the difference between allowed and disallowed paths
        3. Research why respecting robots.txt is important legally/ethically
        """
        if not self.config.respect_robots:
            return True
            
        try:
            robots_url = urljoin(base_url, '/robots.txt')
            self.robots_parser = RobotFileParser()
            self.robots_parser.set_url(robots_url)
            self.robots_parser.read()
            
            # Check if our user agent can fetch the base URL
            can_fetch = self.robots_parser.can_fetch(self.config.user_agent, base_url)
            logger.info(f"Robots.txt check for {base_url}: {'Allowed' if can_fetch else 'Blocked'}")
            return can_fetch
            
        except Exception as e:
            logger.warning(f"Could not check robots.txt for {base_url}: {e}")
            return True  # Assume allowed if we can't check
    
    def _wait_between_requests(self):
        """
        Implement respectful delays between requests.
        
        LEARNING NOTE: Random delays help avoid appearing like a bot
        and reduce server load. This is basic 'good citizen' behavior.
        """
        delay = random.uniform(self.config.base_delay, self.config.max_delay)
        logger.debug(f"Waiting {delay:.2f} seconds before next request...")
        time.sleep(delay)
    
    def make_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make a HTTP request with error handling and retries.
        
        TODO FOR STUDENT:
        1. Study the retry mechanism - when should you retry vs give up?
        2. Learn about different HTTP status codes and what they mean
        3. Understand why we track request statistics
        4. Research rate limiting and how websites detect/block scrapers
        """
        if not self.check_robots_txt(url):
            logger.error(f"Robots.txt disallows scraping {url}")
            return None
        
        for attempt in range(self.config.max_retries):
            try:
                # Wait between requests (except first request)
                if self.request_count > 0:
                    self._wait_between_requests()
                
                logger.debug(f"Making request to {url} (attempt {attempt + 1})")
                
                response = self.session.get(
                    url, 
                    timeout=self.config.timeout,
                    **kwargs
                )
                
                # Track statistics
                self.request_count += 1
                
                # Check for successful response
                response.raise_for_status()
                
                logger.debug(f"Successful request to {url} (status: {response.status_code})")
                return response
                
            except requests.exceptions.RequestException as e:
                self.failed_requests += 1
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.config.max_retries}): {e}")
                
                if attempt < self.config.max_retries - 1:
                    # Exponential backoff for retries
                    wait_time = (2 ** attempt) * self.config.base_delay
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {self.config.max_retries} attempts")
        
        return None
    
    def get_request_stats(self) -> Dict[str, int]:
        """Get scraping statistics for monitoring and debugging."""
        return {
            "total_requests": self.request_count,
            "failed_requests": self.failed_requests,
            "success_rate": (self.request_count - self.failed_requests) / max(1, self.request_count)
        }
    
    @abstractmethod
    def scrape_daily_races(self) -> List[Dict[str, Any]]:
        """
        Abstract method - each scraper must implement this.
        
        TODO FOR STUDENT: 
        Implement this method in your specific scraper classes.
        Should return a list of race dictionaries with standardized fields.
        """
        pass
    
    @abstractmethod
    def get_race_results(self, race_id: str) -> Dict[str, Any]:
        """
        Abstract method for getting specific race results.
        
        TODO FOR STUDENT:
        Implement detailed race result scraping for individual races.
        """
        pass
    
    def __enter__(self):
        """Context manager entry - for learning about 'with' statements"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit - cleanup resources.
        
        LEARNING NOTE: Always close sessions to free up resources!
        """
        self.session.close()
        logger.info(f"Scraper session closed. Final stats: {self.get_request_stats()}")