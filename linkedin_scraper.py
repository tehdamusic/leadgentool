import os
import time
import random
import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='linkedin_scraper.log'
)
logger = logging.getLogger('linkedin_scraper')

# Load environment variables
load_dotenv()

class LinkedInScraper:
    """LinkedIn scraper class for extracting leads data."""
    
    def __init__(self, headless: bool = False, timeout: int = 20):
        """
        Initialize the LinkedIn scraper.
        
        Args:
            headless: Whether to run the browser in headless mode
            timeout: Default timeout for waiting for elements
        """
        self.timeout = timeout
        self.base_url = "https://www.linkedin.com"
        self.login_url = f"{self.base_url}/login"
        self.driver = self._setup_driver(headless)
        self.wait = WebDriverWait(self.driver, timeout)
        
        # Get credentials from environment variables
        self.username = os.getenv('LINKEDIN_USERNAME')
        self.password = os.getenv('LINKEDIN_PASSWORD')
        
        if not self.username or not self.password:
            raise ValueError("LinkedIn credentials not found in environment variables")
        
        # Target industries and roles for searching
        self.target_industries = [
            "Tech", "Finance", "Consulting", "Startups", 
            "Healthcare", "Law"
        ]
        self.target_roles = [
            "CEO", "Founder", "Co-Founder", "Business Owner", 
            "Managing Director", "Partner"
        ]
        
        logger.info("LinkedIn scraper initialized")
        
    def _setup_driver(self, headless: bool) -> webdriver.Chrome:
        """
        Set up and configure the Chrome webdriver.
        
        Args:
            headless: Whether to run the browser in headless mode
            
        Returns:
            Configured Chrome webdriver
        """
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
            
        # Add additional options to make scraping more robust
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # User agent to appear more like a real user
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set window size
        driver.set_window_size(1920, 1080)
        
        return driver
    
    def random_sleep(self, min_seconds: float = 2.0, max_seconds: float = 5.0) -> None:
        """
        Sleep for a random amount of time to avoid detection.
        
        Args:
            min_seconds: Minimum sleep time in seconds
            max_seconds: Maximum sleep time in seconds
        """
        time.sleep(random.uniform(min_seconds, max_seconds))
        
    def login(self) -> bool:
        """
        Log in to LinkedIn.
        
        Returns:
            True if login was successful, False otherwise
        """
        try:
            logger.info("Attempting to log in to LinkedIn")
            self.driver.get(self.login_url)
            
            # Wait for login page to load
            self.random_sleep(3, 5)
            
            # Enter username
            username_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            username_field.clear()
            self._slow_type(username_field, self.username)
            
            # Enter password
            password_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "password"))
            )
            password_field.clear()
            self._slow_type(password_field, self.password)
            
            # Click login button
            self.random_sleep(1, 2)
            login_button = self.wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[type='submit']")
                )
            )
            login_button.click()
            
            # Wait for homepage to load
            self.random_sleep(5, 8)
            
            # Check if login was successful
            if "/feed" in self.driver.current_url:
                logger.info("Successfully logged in to LinkedIn")
                return True
            else:
                # Check for verification or other issues
                if "checkpoint" in self.driver.current_url:
                    logger.error("LinkedIn security verification required")
                else:
                    logger.error("Failed to log in to LinkedIn")
                return False
                
        except Exception as e:
            logger.error(f"Error during login: {str(e)}")
            return False
    
    def _slow_type(self, element, text: str) -> None:
        """
        Type text slowly into an element like a human would.
        
        Args:
            element: The web element to type into
            text: The text to type
        """
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))
            
    def search_for_leads(self, industry: str, role: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        """
        Search for leads based on industry and role.
        
        Args:
            industry: The industry to search for
            role: The role to search for
            max_pages: Maximum number of search result pages to scrape
            
        Returns:
            List of lead profile URLs
        """
        leads = []
        search_query = f"{role} {industry}"
        search_url = f"{self.base_url}/search/results/people/?keywords={search_query.replace(' ', '%20')}"
        
        try:
            logger.info(f"Searching for: {search_query}")
            self.driver.get(search_url)
            self.random_sleep(3, 5)
            
            # Process search results pages
            for page in range(1, max_pages + 1):
                logger.info(f"Processing search results page {page}")
                
                # Wait for search results to load
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".reusable-search__result-container"))
                )
                
                # Extract profile data from current page
                page_leads = self._extract_leads_from_search_page()
                leads.extend(page_leads)
                logger.info(f"Extracted {len(page_leads)} leads from page {page}")
                
                # Add industry and role tags to each lead
                for lead in page_leads:
                    lead["industry"] = industry
                    lead["searched_role"] = role
                
                # Check if there's a next page and navigate to it
                if page < max_pages:
                    try:
                        next_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='Next']")
                        if next_button.is_enabled():
                            self.random_sleep(2, 4)
                            next_button.click()
                            self.random_sleep(3, 5)
                        else:
                            # No more pages
                            logger.info(f"No more search result pages after page {page}")
                            break
                    except NoSuchElementException:
                        logger.info(f"No more search result pages after page {page}")
                        break
                    
        except Exception as e:
            logger.error(f"Error during search: {str(e)}")
            
        return leads
    
    def _extract_leads_from_search_page(self) -> List[Dict[str, Any]]:
        """
        Extract lead information from a search results page.
        
        Returns:
            List of lead dictionaries with basic information
        """
        leads = []
        
        try:
            # Get all search result containers
            result_containers = self.driver.find_elements(By.CSS_SELECTOR, ".reusable-search__result-container")
            
            for container in result_containers:
                try:
                    # Extract basic information from search result
                    profile_link_elem = container.find_element(By.CSS_SELECTOR, "a.app-aware-link")
                    profile_url = profile_link_elem.get_attribute("href").split("?")[0]  # Remove URL parameters
                    
                    # Extract name
                    name_elem = container.find_element(By.CSS_SELECTOR, "span.entity-result__title-text")
                    name = name_elem.text.strip().split("\n")[0]  # Remove connection level text
                    
                    # Extract job title and company if available
                    try:
                        title_elem = container.find_element(By.CSS_SELECTOR, ".entity-result__primary-subtitle")
                        job_title = title_elem.text.strip()
                    except NoSuchElementException:
                        job_title = ""
                    
                    # Basic lead information
                    lead = {
                        "name": name,
                        "job_title": job_title,
                        "profile_url": profile_url,
                        "date_added": datetime.now().strftime("%Y-%m-%d"),
                        "bio_snippet": "",
                        "recent_posts": []
                    }
                    
                    leads.append(lead)
                    
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    logger.warning(f"Error extracting lead from search result: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting leads from search page: {str(e)}")
            
        return leads
    
    def enrich_lead_data(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Visit a lead's profile to enrich the data with additional information.
        
        Args:
            lead: Basic lead information dictionary
            
        Returns:
            Enriched lead information dictionary
        """
        try:
            logger.info(f"Enriching data for: {lead['name']}")
            profile_url = lead['profile_url']
            self.driver.get(profile_url)
            self.random_sleep(4, 7)
            
            # Wait for profile page to load
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".pv-top-card"))
            )
            
            # Extract bio snippet
            try:
                about_section = self.driver.find_element(By.CSS_SELECTOR, "section.pv-about-section")
                bio_snippet = about_section.find_element(By.CSS_SELECTOR, ".pv-about__summary-text").text.strip()
                lead["bio_snippet"] = bio_snippet
            except NoSuchElementException:
                logger.info(f"No bio found for {lead['name']}")
                lead["bio_snippet"] = ""
            
            # Extract recent posts (if any)
            try:
                # Look for activity section or navigate to it
                try:
                    activity_button = self.driver.find_element(By.CSS_SELECTOR, "a.pv-top-card__tab--activity")
                    activity_url = activity_button.get_attribute("href")
                    self.driver.get(activity_url)
                    self.random_sleep(3, 5)
                except NoSuchElementException:
                    pass  # No activity tab, might already be on activity page
                
                # Try to locate posts
                post_elements = self.driver.find_elements(By.CSS_SELECTOR, ".feed-shared-update-v2__description")
                recent_posts = []
                
                for i, post_elem in enumerate(post_elements[:3]):  # Only take the most recent 3 posts
                    post_text = post_elem.text.strip()
                    if post_text:
                        recent_posts.append(post_text)
                
                lead["recent_posts"] = recent_posts
                
            except (NoSuchElementException, ElementClickInterceptedException) as e:
                logger.info(f"Could not extract recent posts for {lead['name']}: {str(e)}")
                lead["recent_posts"] = []
                
            # Extract additional contact information if available
            try:
                # Click "Contact info" button if it exists
                contact_btn = self.driver.find_element(By.CSS_SELECTOR, "a[data-control-name='contact_see_more']")
                contact_btn.click()
                self.random_sleep(1, 2)
                
                # Extract email if available
                contact_info = {}
                try:
                    email_section = self.driver.find_element(By.CSS_SELECTOR, "section.ci-email")
                    contact_info["email"] = email_section.find_element(By.CSS_SELECTOR, ".pv-contact-info__ci-container").text.strip()
                except NoSuchElementException:
                    pass
                
                lead["contact_info"] = contact_info
                
                # Close the modal
                close_btn = self.driver.find_element(By.CSS_SELECTOR, "button.artdeco-modal__dismiss")
                close_btn.click()
                
            except (NoSuchElementException, ElementClickInterceptedException):
                lead["contact_info"] = {}
                
            return lead
            
        except Exception as e:
            logger.error(f"Error enriching lead data for {lead['name']}: {str(e)}")
            return lead
    
    def save_leads_to_google_sheets(self, leads: List[Dict[str, Any]], sheets_client) -> bool:
        """
        Save leads to Google Sheets.
        
        Args:
            leads: List of lead information dictionaries
            sheets_client: Google Sheets client
            
        Returns:
            True if saving was successful, False otherwise
        """
        try:
            logger.info(f"Saving {len(leads)} leads to Google Sheets")
            
            # Prepare data for sheets
            rows = []
            for lead in leads:
                # Format recent posts as a string
                recent_posts_text = "; ".join(lead.get('recent_posts', []))
                
                # Format contact info
                contact_info = json.dumps(lead.get('contact_info', {}))
                
                row = [
                    lead.get('name', ''),
                    lead.get('job_title', ''),
                    lead.get('industry', ''),
                    lead.get('profile_url', ''),
                    lead.get('bio_snippet', '')[:500],  # Limit length for Google Sheets
                    recent_posts_text[:500],  # Limit length for Google Sheets
                    contact_info,
                    lead.get('date_added', '')
                ]
                rows.append(row)
            
            # Append to Google Sheet
            if sheets_client and rows:
                for row in rows:
                    sheets_client.append_row(row)
                logger.info(f"Successfully saved {len(rows)} leads to Google Sheets")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error saving leads to Google Sheets: {str(e)}")
            return False
    
    def scrape_by_industry_and_role(self, sheets_client, max_leads: int = 50) -> List[Dict[str, Any]]:
        """
        Scrape leads by iterating through target industries and roles.
        
        Args:
            sheets_client: Google Sheets client
            max_leads: Maximum number of leads to collect
            
        Returns:
            List of all leads collected
        """
        all_leads = []
        
        # Login first
        if not self.login():
            logger.error("Login failed, aborting scrape operation")
            return all_leads
        
        # Iterate through combinations of industries and roles
        for industry in self.target_industries:
            for role in self.target_roles:
                # Check if we've reached the maximum number of leads
                if len(all_leads) >= max_leads:
                    logger.info(f"Reached maximum number of leads ({max_leads})")
                    break
                
                # Search for leads with current industry and role
                leads = self.search_for_leads(industry, role, max_pages=2)
                logger.info(f"Found {len(leads)} leads for {role} in {industry}")
                
                # Enrich a subset of leads with detailed profile information
                enriched_leads = []
                for lead in leads[:5]:  # Only enrich up to 5 leads per search to avoid rate limiting
                    enriched_lead = self.enrich_lead_data(lead)
                    enriched_leads.append(enriched_lead)
                    self.random_sleep(5, 8)  # Longer sleep between profile visits
                    
                    # Check if we've reached the maximum
                    if len(all_leads) + len(enriched_leads) >= max_leads:
                        break
                
                # Save this batch to Google Sheets
                self.save_leads_to_google_sheets(enriched_leads, sheets_client)
                
                # Add to overall list
                all_leads.extend(enriched_leads)
                
                # Take a longer break between searches
                self.random_sleep(10, 15)
                
            # Check outer loop as well
            if len(all_leads) >= max_leads:
                break
                
        return all_leads
    
    def close(self) -> None:
        """Close the browser and release resources."""
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")


def run_linkedin_scraper(sheets_client, max_leads: int = 50, headless: bool = False) -> List[Dict[str, Any]]:
    """
    Run the LinkedIn scraper as a standalone function.
    
    Args:
        sheets_client: Google Sheets client for saving results
        max_leads: Maximum number of leads to collect
        headless: Whether to run in headless mode
        
    Returns:
        List of leads collected
    """
    scraper = LinkedInScraper(headless=headless)
    
    try:
        leads = scraper.scrape_by_industry_and_role(sheets_client, max_leads)
        return leads
    finally:
        scraper.close()


if __name__ == "__main__":
    # This allows the script to be run directly for testing
    from utils.sheets_manager import get_sheets_client
    
    # Get Google Sheets client
    sheets_client = get_sheets_client()
    worksheet = sheets_client.open('LeadGenerationData').worksheet('Leads')
    
    # Run the scraper
    leads = run_linkedin_scraper(worksheet, max_leads=20, headless=False)
    print(f"Collected {len(leads)} leads")
