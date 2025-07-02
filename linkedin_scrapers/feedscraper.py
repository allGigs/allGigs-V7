#!/usr/bin/env python3
"""
LinkedIn Selenium Scraper
=========================

This script uses Selenium and undetected-chromedriver to scrape the LinkedIn feed
in a way that mimics human behavior to avoid detection.
"""

import time
import random
import logging
import pandas as pd
from datetime import datetime, date, timedelta
import os
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import argparse
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from linkedin_scrapers.freelance_keywords import FREELANCE_KEYWORDS, INDUSTRY_KEYWORDS  # centralised keyword list

# Configure logging
LOG_PATH = os.path.join(os.path.dirname(__file__), 'linkedin_selenium_scraper.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LinkedInSeleniumScraper:
    """
    A class to scrape LinkedIn using Selenium, with human-like behavior.
    """

    def __init__(self):
        """
        Initializes the scraper, setting up the WebDriver.
        """
        logger.info("Initializing the scraper...")
        try:
            options = uc.ChromeOptions()
            # You can add options here if needed, e.g.:
            # options.add_argument('--headless') # Not recommended for avoiding detection
            self.driver = uc.Chrome(options=options, version_main=137)
            logger.info("WebDriver initialized successfully for Chrome version 137.")
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise

    def human_like_delay(self, min_seconds=1.0, max_seconds=3.0):
        """
        Pauses for a random duration to mimic human thinking time.
        """
        time.sleep(random.uniform(min_seconds, max_seconds))

    def human_like_scroll(self, scrolls=3, scroll_pause_time=0.2):
        """
        Scrolls down the page in small, human-like increments.
        """
        body = self.driver.find_element(By.TAG_NAME, 'body')
        for _ in range(scrolls):
            # Scroll down by a fraction of the window height
            if random.random() < 0.1:  # 10% chance to scroll up
                scroll_increment = -random.uniform(200, 400)
                logger.info("Scrolling up to simulate human behavior...")
            else:
                scroll_increment = random.uniform(200, 400)
                logger.info("Scrolling down to simulate human behavior...")
            self.driver.execute_script(f"window.scrollBy(0, {scroll_increment});")
            self.human_like_delay(min_seconds=scroll_pause_time, max_seconds=scroll_pause_time + 0.3)

    def dumb_user_action(self):
        """
        With a small probability, performs a 'dumb' action like clicking a profile
        and navigating back, to break the scraping pattern.
        """
        if random.random() < 0.1:  # 10% chance to perform this action
            logger.info("Performing a 'dumb user' action to appear more human...")
            try:
                # Find a random profile link in a post
                profile_links = self.driver.find_elements(By.CSS_SELECTOR, "span.feed-shared-actor__name a")
                if profile_links:
                    random_profile = random.choice(profile_links)
                    logger.info(f"Navigating to profile: {random_profile.text}")
                    random_profile.click()
                    self.human_like_delay(min_seconds=4, max_seconds=8)  # "Read" the profile
                    self.driver.back()
                    logger.info("Navigated back to the feed.")
                    self.human_like_delay(min_seconds=2, max_seconds=5)
            except Exception as e:
                logger.warning(f"Dumb user action failed: {e}")
                # Don't crash the script, just go back if possible
                try:
                    self.driver.back()
                except:
                    pass

    def scrape_feed(self, duration_minutes=5):
        """
        Main scraping loop. Scrolls, captures posts, and performs human-like actions.
        """
        logger.info("Navigating to LinkedIn feed...")
        self.driver.get("https://www.linkedin.com/feed/")
        self.human_like_delay(3, 5) # "Thinking time" after page load

        input("Please log in to LinkedIn in the browser window, then press Enter here to continue...")

        logger.info(f"Starting to scrape for {duration_minutes} minutes.")
        
        # Extract URLs for posts and profiles using Selenium
        def extract_urls(post_element):
            try:
                # Extract the URL of the post
                post_url = post_element.find_element(By.CSS_SELECTOR, 'a[href*="linkedin.com/feed/update/"]').get_attribute('href')
                # Extract the URL of the profile associated with the post
                profile_url = post_element.find_element(By.CSS_SELECTOR, 'a[href*="linkedin.com/in/"]').get_attribute('href')
                return post_url, profile_url
            except NoSuchElementException as e:
                logger.warning(f"Could not extract URLs: {e}")
                return None, None

        # Update the scraping loop to collect URLs
        all_posts_data = []
        end_time = time.time() + duration_minutes * 60

        while time.time() < end_time:
            self.human_like_scroll()
            posts = self.driver.find_elements(By.CSS_SELECTOR, ".feed-shared-update-v2")
            for post in posts:
                try:
                    raw_text = post.text
                    post_url, profile_url = extract_urls(post)
                    sector_name = self.extract_sector_name(post)
                    all_posts_data.append({
                        'raw_post_text': raw_text,
                        'scraped_at': datetime.now().isoformat(),
                        'URL_post': post_url,
                        'URL_profile': profile_url,
                        'sector_name': sector_name
                    })
                except Exception as e:
                    logger.warning(f"Could not extract text, URLs, or sector name from a post element: {e}")

        # Convert to DataFrame and continue with existing processing
        df = pd.DataFrame(all_posts_data)
        df.drop_duplicates(subset=["raw_post_text"], keep="first", inplace=True)

        initial_count = len(df)
        logger.info(f"[Filter] Posts before keyword filtering: {initial_count}")

        # Filter based on freelance keywords
        df['matched_keywords'] = df['raw_post_text'].apply(lambda text: ', '.join([kw for kw in FREELANCE_KEYWORDS if kw in text.lower()]))
        df = df[df['matched_keywords'].str.len() > 0]

        after_count = len(df)
        removed_count = initial_count - after_count
        logger.info(f"[Filter] Removed {removed_count} posts without keywords; {after_count} remain.")

        if after_count == 0:
            logger.warning("No posts left after keyword filtering; nothing will be saved.")
            return

        # Add logic to split 'raw_post_text' into 'header' and 'body'
        df['header'] = df['raw_post_text'].apply(lambda text: text.split('Visible to anyone on or off LinkedIn')[0].strip() if 'Visible to anyone on or off LinkedIn' in text else text)
        df['body'] = df['raw_post_text'].apply(lambda text: text.split('Visible to anyone on or off LinkedIn')[1].strip() if 'Visible to anyone on or off LinkedIn' in text else '')

        # Trim the content inside the 'body' column
        def trim_body_content(text):
            # Remove content below and including the marker '…more'
            trimmed_text = text.split('…more')[0].strip() if '…more' in text else text
            # Remove lines with no text
            lines = [line for line in trimmed_text.splitlines() if line.strip()]
            return '\n'.join(lines)

        df['body'] = df['body'].apply(trim_body_content)

        # Add logic to create 'post_owner' column
        def extract_post_owner(text):
            markers = ["2nd", "1st", "3rd+"]
            lines = text.splitlines()
            for i in range(len(lines) - 1, -1, -1):
                for marker in markers:
                    if marker in lines[i]:
                        # Look for the second occurrence of the marker
                        for j in range(i - 1, -1, -1):
                            if marker in lines[j]:
                                # Extract the next two words after the marker
                                words = lines[j].split()
                                marker_index = words.index(marker)
                                if marker_index + 2 < len(words):
                                    return ' '.join(words[marker_index + 1:marker_index + 3])
                                else:
                                    logger.warning("Cannot find two words after the second marker")
                                    return None
                        logger.warning("Cannot find the second marker")
                        return None
            logger.info("Delete advertisement")
            return None

        df['post_owner'] = df['header'].apply(extract_post_owner)

        # Remove rows where post_owner is None and log the deletion
        initial_count = len(df)
        df.dropna(subset=['post_owner'], inplace=True)
        removed_count = initial_count - len(df)
        logger.info(f"Deleted {removed_count} advertisements")

        # Create a 'dateposted' column
        def extract_date_posted(text):
            marker = 'Visible to anyone on or off LinkedIn'
            if marker in text:
                parts = text.split(marker)[0].strip().split()
                if len(parts) >= 3:
                    number = int(parts[-3])
                    unit = parts[-2].lower()
                    if unit in ['day', 'days']:
                        return (datetime.now() - timedelta(hours=number * 24)).isoformat()
                    elif unit in ['week', 'weeks']:
                        return (datetime.now() - timedelta(hours=number * 24 * 7)).isoformat()
            # Assume posted today if no valid time found
            return datetime.now().isoformat()

        df['dateposted'] = df['raw_post_text'].apply(extract_date_posted)

        # Add new columns with specified default values or content
        df['Source'] = 'Feed Jobs'
        df['Company'] = 'No company, feed job'
        df['rate'] = 'no rate mentioned'
        df['Summary'] = df['body']
        df['Location'] = 'not mentioned'

        # Modify the CSV filename to have the date and time in front
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
        filename = f"{timestamp}_Feed_jobs.csv"
        filepath = os.path.join(os.getcwd(), filename)

        # Save the updated DataFrame with the new filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved data to {filename}")

    def close(self):
        """
        Closes the WebDriver session.
        """
        logger.info("Closing the WebDriver.")
        self.driver.quit()

    def extract_sector_name(self, post_element):
        try:
            # Extract the sector name from the span element
            sector_name_element = post_element.find_element(By.CSS_SELECTOR, 'span[aria-hidden="true"]')
            return sector_name_element.text
        except NoSuchElementException as e:
            logger.warning(f"Could not extract sector name: {e}")
            return None

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Scrape the LinkedIn feed for a specified duration (in minutes)."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=15,
        help="How long to scrape the feed, in minutes. Default is 30 minutes.",
    )
    parser.add_argument(
        "--no-login-prompt",
        action="store_true",
        help="Skip the interactive login confirmation prompt (use only if you are already logged in).",
    )
    return parser.parse_args()

def main():
    """Main function to run the scraper."""
    args = parse_args()
    scraper = None
    try:
        scraper = LinkedInSeleniumScraper()
        if args.no_login_prompt:
            import builtins
            _orig_input = builtins.input
            builtins.input = lambda *a, **k: None  # skip prompt
            try:
                scraper.scrape_feed(duration_minutes=args.duration)
            finally:
                builtins.input = _orig_input  # restore
        else:
            scraper.scrape_feed(duration_minutes=args.duration)
    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}")
    finally:
        if scraper:
            scraper.close()
        logger.info("Script finished.")

if __name__ == "__main__":
    main()