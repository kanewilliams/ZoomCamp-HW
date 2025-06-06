"""
TAB New Zealand Scraper - Your First Real Web Scraping Challenge!

LEARNING MISSION:
This file contains the skeleton for scraping TAB.nz - New Zealand's official betting site.
Your job is to fill in the actual implementation by inspecting the website structure.

LEARNING STEPS:
1. Visit https://www.tab.co.nz/racing in your browser
2. Open Developer Tools (F12) and examine the HTML structure
3. Find the CSS selectors for race information
4. Test your selectors in the browser console
5. Implement the parsing logic below

ETHICAL REMINDER:
- Always check robots.txt first: https://www.tab.co.nz/robots.txt
- Use reasonable delays between requests
- Don't overload their servers
- Consider if there's a public API before scraping
"""

from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from datetime import datetime, date
import logging
from .base import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)


class TABScraper(BaseScraper):
    """
    Scraper for TAB New Zealand racing data.
    
    TODO FOR STUDENT - MAJOR IMPLEMENTATION NEEDED:
    This class needs you to figure out the actual HTML structure of TAB.nz!
    
    YOUR LEARNING TASKS:
    1. [ ] Visit https://www.tab.co.nz/racing/today
    2. [ ] Inspect HTML structure using browser dev tools
    3. [ ] Identify CSS selectors for race cards
    4. [ ] Find selectors for horse information
    5. [ ] Implement parse_race_card() method
    6. [ ] Implement parse_horse_data() method
    7. [ ] Test with small amounts of data first
    8. [ ] Add error handling for missing elements
    """
    
    def __init__(self):
        # TAB.nz specific configuration
        config = ScrapingConfig(
            base_delay=2.0,  # Be extra respectful - it's an official betting site
            max_delay=5.0,
            timeout=30,
            respect_robots=True,
            user_agent="Mozilla/5.0 (compatible; HorseRacing-Student-Project/1.0)"
        )
        super().__init__(config)
        self.base_url = "https://www.tab.co.nz"
        
        # TODO: You might need to handle authentication or cookies
        # Research if TAB requires any special headers
    
    def scrape_daily_races(self, target_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Scrape today's race information from TAB.
        
        TODO FOR STUDENT - IMPLEMENTATION REQUIRED:
        1. Build the correct URL for the target date
        2. Handle the response and parse HTML
        3. Extract race cards from the page
        4. Return standardized race data
        
        HINT: Start by printing the HTML to understand the structure!
        """
        if target_date is None:
            target_date = date.today()
        
        # TODO: Figure out the correct URL format for TAB
        # Examples to try:
        # - https://www.tab.co.nz/racing/today
        # - https://www.tab.co.nz/racing/2024-01-15
        # - https://www.tab.co.nz/racing/meetings
        
        # STEP 1: Build URL (you need to find the right format)
        url = f"{self.base_url}/racing/today"  # TODO: Update this based on your research
        
        logger.info(f"Scraping TAB races for {target_date} from {url}")
        
        # STEP 2: Make the request
        response = self.make_request(url)
        if not response:
            logger.error(f"Failed to fetch TAB racing page")
            return []
        
        # STEP 3: Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # TODO FOR STUDENT: Debug by printing the HTML structure
        # Uncomment this line to see what you're working with:
        # print("First 2000 characters of HTML:", response.text[:2000])
        
        # STEP 4: Find race containers
        races_data = []
        
        # TODO FOR STUDENT: Replace these placeholder selectors with real ones
        # You need to inspect TAB.nz to find the actual CSS classes/IDs
        race_cards = soup.find_all('div', class_='REPLACE_WITH_ACTUAL_RACE_CARD_CLASS')
        
        if not race_cards:
            logger.warning("No race cards found - check your CSS selectors!")
            logger.info("Available CSS classes in HTML:")
            # TODO: Add debugging code to help you find the right selectors
            all_divs = soup.find_all('div', class_=True)
            classes = set()
            for div in all_divs[:20]:  # Just first 20 to avoid spam
                if div.get('class'):
                    classes.update(div.get('class'))
            logger.info(f"Sample CSS classes found: {list(classes)[:10]}")
            return []
        
        # STEP 5: Parse each race card
        for race_card in race_cards:
            try:
                race_data = self._parse_race_card(race_card, target_date)
                if race_data:
                    races_data.append(race_data)
            except Exception as e:
                logger.error(f"Error parsing race card: {e}")
                # TODO: Add more specific error handling
        
        logger.info(f"Successfully scraped {len(races_data)} races from TAB")
        return races_data
    
    def _parse_race_card(self, race_card_element, race_date: date) -> Optional[Dict[str, Any]]:
        """
        Parse individual race card HTML element.
        
        TODO FOR STUDENT - CRITICAL IMPLEMENTATION:
        This is where the real work happens! You need to:
        1. Extract race time, number, distance
        2. Extract track/venue information  
        3. Get list of horses with details
        4. Handle missing or malformed data gracefully
        
        DEBUGGING TIP: Print the race_card_element HTML to see its structure
        """
        try:
            # TODO: Replace these with actual selectors from TAB.nz
            
            # Example of what you need to find:
            race_time_element = race_card_element.find('span', class_='REPLACE_WITH_TIME_CLASS')
            race_number_element = race_card_element.find('span', class_='REPLACE_WITH_NUMBER_CLASS') 
            track_element = race_card_element.find('span', class_='REPLACE_WITH_TRACK_CLASS')
            distance_element = race_card_element.find('span', class_='REPLACE_WITH_DISTANCE_CLASS')
            
            # TODO: Implement null checking and data extraction
            race_time = race_time_element.text.strip() if race_time_element else "Unknown"
            race_number = race_number_element.text.strip() if race_number_element else "Unknown"
            track = track_element.text.strip() if track_element else "Unknown"
            distance = distance_element.text.strip() if distance_element else "Unknown"
            
            # TODO: Extract horse data
            horses = self._parse_horses_in_race(race_card_element)
            
            race_data = {
                'date': race_date.isoformat(),
                'race_time': race_time,
                'race_number': race_number,
                'track': track,
                'distance': distance,
                'horses': horses,
                'source': 'tab_nz',
                'scraped_at': datetime.now().isoformat()
            }
            
            return race_data
            
        except Exception as e:
            logger.error(f"Error parsing race card: {e}")
            return None
    
    def _parse_horses_in_race(self, race_card_element) -> List[Dict[str, Any]]:
        """
        Extract horse information from a race card.
        
        TODO FOR STUDENT - HORSE DATA EXTRACTION:
        Find the section with horse information and extract:
        - Horse name
        - Jockey name  
        - Barrier number
        - Current odds
        - Trainer name (if available)
        - Weight (if available)
        """
        horses = []
        
        # TODO: Find the container with horse information
        horse_elements = race_card_element.find_all('div', class_='REPLACE_WITH_HORSE_CLASS')
        
        for horse_element in horse_elements:
            try:
                # TODO: Extract horse data - replace with actual selectors
                horse_name = horse_element.find('span', class_='HORSE_NAME_CLASS')
                jockey_name = horse_element.find('span', class_='JOCKEY_NAME_CLASS')
                barrier = horse_element.find('span', class_='BARRIER_CLASS')
                odds = horse_element.find('span', class_='ODDS_CLASS')
                
                horse_data = {
                    'name': horse_name.text.strip() if horse_name else 'Unknown',
                    'jockey': jockey_name.text.strip() if jockey_name else 'Unknown',
                    'barrier': barrier.text.strip() if barrier else 'Unknown',
                    'odds': odds.text.strip() if odds else 'Unknown',
                    # TODO: Add more fields as you find them
                }
                
                horses.append(horse_data)
                
            except Exception as e:
                logger.error(f"Error parsing horse data: {e}")
                continue
        
        return horses
    
    def get_race_results(self, race_id: str) -> Dict[str, Any]:
        """
        Get detailed results for a specific race.
        
        TODO FOR STUDENT - RESULTS SCRAPING:
        1. Figure out how TAB.nz identifies individual races
        2. Find the results page URL format
        3. Extract finishing positions, margins, dividends
        4. Handle races that haven't finished yet
        """
        # TODO: Implement race results scraping
        # This might require a different URL pattern like:
        # https://www.tab.co.nz/racing/results/{race_id}
        
        logger.warning("get_race_results not yet implemented - TODO for student!")
        return {}
    
    def search_for_free_apis(self) -> Dict[str, str]:
        """
        Research alternative data sources before heavy scraping.
        
        TODO FOR STUDENT - API RESEARCH:
        Before implementing full scraping, research if TAB or other
        NZ racing authorities provide:
        1. Free APIs for race data
        2. CSV downloads
        3. RSS feeds
        4. Public data dumps
        
        Check these sources:
        - https://www.racing.nz (Racing NZ official)
        - https://www.trackside.co.nz (might have feeds)
        - Any government open data portals
        """
        api_sources = {
            "racing_nz_api": "TODO: Research if Racing NZ has an API",
            "trackside_feeds": "TODO: Check for RSS/XML feeds",
            "open_data": "TODO: Search NZ government open data",
            "csv_downloads": "TODO: Look for downloadable datasets"
        }
        
        logger.info("Research these API alternatives before scraping:")
        for source, todo in api_sources.items():
            logger.info(f"  {source}: {todo}")
        
        return api_sources


# TODO FOR STUDENT - TESTING SECTION:
# Add this code to test your implementation

if __name__ == "__main__":
    """
    Test your TAB scraper implementation.
    
    TESTING STRATEGY:
    1. Start with small tests (single page)
    2. Add debugging prints to understand HTML structure
    3. Test error handling with invalid URLs
    4. Verify data quality and completeness
    """
    
    print("🏇 Testing TAB.nz Scraper Implementation")
    print("=" * 50)
    
    # TODO: Uncomment and test as you implement
    
    with TABScraper() as scraper:
        print("1. Testing robots.txt compliance...")
        # TODO: Test robots.txt checking
        
        print("2. Testing daily races scraping...")
        # TODO: Test with a specific date
        races = scraper.scrape_daily_races()
        print(f"Found {len(races)} races")
        
        if races:
            print("3. Sample race data:")
            print(races[0])  # Show first race
        else:
            print("3. No races found - check your implementation!")
        
        print("4. Scraper statistics:")
        print(scraper.get_request_stats())
    
    print("\\n🎯 Next Steps:")
    print("1. Open TAB.nz in browser and inspect HTML")
    print("2. Update CSS selectors in this file")
    print("3. Test with small amounts of data")
    print("4. Add proper error handling")
    print("5. Research API alternatives before scaling up")