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
        
        all_posts_data = []
        end_time = time.time() + duration_minutes * 60

        while time.time() < end_time:
            # Perform a scroll action
            self.human_like_scroll()
            
            # Extract posts currently visible on the page
            posts = self.driver.find_elements(By.CSS_SELECTOR, ".feed-shared-update-v2")
            for post in posts:
                try:
                    raw_text = post.text
                    cleaned_text = self.clean_post_text(raw_text)

                    # Use the shared list of keywords/phrases that indicate freelance or contract work
                    KEYWORDS_OF_INTEREST = FREELANCE_KEYWORDS

                    # Decide whether to keep a post: either long enough or contains key terms
                    matched_kw = [kw for kw in KEYWORDS_OF_INTEREST if kw in cleaned_text.lower()]

                    if cleaned_text and (
                        len(cleaned_text) > 50 or matched_kw
                    ):
                        all_posts_data.append({
                            'cleaned_post_text': f"---- NEW POST ({', '.join(matched_kw)}) ----\n{cleaned_text}",
                            'raw_post_text': raw_text,
                            'matched_keywords': ", ".join(matched_kw),
                            'scraped_at': datetime.now().isoformat()
                        })
                except Exception as e:
                    logger.warning(f"Could not extract text from a post element: {e}")

            # Occasionally perform a "dumb user" action
            self.dumb_user_action()

            # Add an extra long pause sometimes to simulate reading/distraction
            if random.random() < 0.15: # 15% chance
                logger.info("Taking a longer, human-like pause...")
                self.human_like_delay(min_seconds=5, max_seconds=12)

        logger.info("Scraping finished. Processing results...")
        self.save_to_csv(all_posts_data)

    def save_to_csv(self, data: list):
        """
        Saves the scraped data to a CSV file.
        """
        if not data:
            logger.warning("No data scraped to save.")
            return

        # Save the scraped data to a CSV file with the URL column
        df = pd.DataFrame(data)
        # Deduplicate raw posts
        df.drop_duplicates(subset=["raw_post_text"], keep="first", inplace=True)

        initial_count = len(df)
        logger.info(f"[Filter] Posts before keyword filtering: {initial_count}")

        # Keep rows that actually matched at least one keyword during scraping
        df = df[df["matched_keywords"].str.len() > 0]

        after_count = len(df)
        removed_count = initial_count - after_count
        logger.info(f"[Filter] Removed {removed_count} posts without keywords; {after_count} remain.")

        if after_count == 0:
            logger.warning("No posts left after keyword filtering; nothing will be saved.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"linkedin_selenium_feed_{timestamp}.csv"
        filepath = os.path.join(os.getcwd(), filename)
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {after_count} keyword-matching posts to {filename}")

        # Ensure the URL column is included in the CSV
        df["URL"] = df.apply(lambda row: _extract_field(URL_PATTERN, row["raw_post_text"]) or re.search(r"https?://www\.linkedin\.com/in/[\w-]+", row["raw_post_text"]).group(0) if re.search(r"https?://www\.linkedin\.com/in/[\w-]+", row["raw_post_text"]) else "", axis=1)

        # Save the updated DataFrame with the URL column
        df.to_csv(filepath, index=False)
        logger.info(f"Updated {filename} with URL column")

        # ------------------------------------------------------------------
        # Derive "Freelance scraped jobs.csv" from the posts kept this run
        # ------------------------------------------------------------------

        LINKEDIN_DIR = os.path.dirname(__file__)
        jobs_csv_path = os.path.join(LINKEDIN_DIR, "Freelance scraped jobs.csv")

        # Regex helpers (simple heuristics)
        ROLE_PATTERNS = [
            r"(?:looking for|seeking|need(?:ing)?|hiring|require|vacancy for|recruiting)\s+([A-Za-z0-9 /\\-]{3,80}?)\b(?:in|for|with|at|to|,|\.|$)",
        ]
        LOCATION_PATTERNS = [
            r"\bin\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)",
            r"\b(Remote)\b",
        ]
        RATE_PATTERN = r"(\$|£|€)\s?\d+[\d,.]*(?:[kK]|/h|/hr| per hour| per day|/day| per annum|/month)?"
        # Company pattern fallback (two words before connection degree)
        COMPANY_DEGREE_PATTERN = r"(\b\w+)\s+(\w+)\s+(?:•\s*)?(?:1st|2nd|3rd)\b"
        DATE_PATTERN = r"\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b"
        URL_PATTERN = r"https?://www\.linkedin\.com/[\w\-/]+"

        def _extract_first(patterns, text):
            for pat in patterns:
                m = re.search(pat, text, re.I)
                if m:
                    return m.group(1).strip()
            return ""

        def _extract_field(pattern, text):
            m = re.search(pattern, text, re.I)
            return m.group(1).strip() if m else ""

        job_rows = []
        for _, post_row in df.iterrows():
            raw_post = post_row["raw_post_text"]
            matched_kw_raw = post_row.get("matched_keywords", "")
            first_matched_kw = matched_kw_raw.split(",")[0].strip() if matched_kw_raw else ""

            # Split the raw text into title and body
            marker = "visible to anyone on or off linkedin"
            idx = raw_post.lower().find(marker)
            if idx != -1:
                title = raw_post[:idx].strip()
                body = raw_post[idx + len(marker):].strip()
            else:
                title = raw_post
                body = ""

            # Remove trailing boiler-plate phrases and everything after them
            for tail in [
                "your document has finished loading",
                "link below",
                "reply",
                "see content",
                "show translation",
            ]:
                pos = body.lower().find(tail)
                if pos != -1:
                    body = body[:pos].rstrip()
                    break

            # Detect industry keyword in body
            industry = None
            for kw, ind in INDUSTRY_KEYWORDS.items():
                if re.search(rf"\b{re.escape(kw)}\b", body, re.I):
                    industry = ind
                    break

            if industry:
                title = f"Freelance {industry}"
            else:
                title = _extract_first(ROLE_PATTERNS, body) or " ".join(body.split()[:8])
            location = _extract_first(LOCATION_PATTERNS, body)
            rate = _extract_field(RATE_PATTERN, body)
            # Company: two words before 1st/2nd/3rd badge (• 1st / 2nd / 3rd) searched in raw post first
            cmp_match = re.search(r"(\b\w+)\s+(\w+)\s+(?:•\s*)?(?:1st|2nd|3rd)\b", raw_post, re.I)
            if not cmp_match:
                # fallback to cleaned body without bullet
                cmp_match = re.search(COMPANY_DEGREE_PATTERN, body, re.I)
            if cmp_match:
                company = f"{cmp_match.group(1)} {cmp_match.group(2)}".strip()
            else:
                company = ""
            start = _extract_field(DATE_PATTERN, body)
            if not start:
                start = "ASAP"
            # Extract the post URL
            post_url = _extract_field(URL_PATTERN, raw_post)

            # Fallback to profile URL if post URL is not available
            if not post_url:
                profile_url_match = re.search(r"https?://www\.linkedin\.com/in/[\w-]+", raw_post)
                post_url = profile_url_match.group(0) if profile_url_match else ""

            # date posted calculation based on "days" tag
            days_match = re.search(r"(\d+)\s+days?\b", body, re.I)
            if days_match:
                days_ago = int(days_match)
                posted_date = (datetime.now() - timedelta(days=days_ago)).isoformat(timespec="seconds")
            else:
                posted_date = datetime.now().isoformat(timespec="seconds")

            job_rows.append({
                "Title": title,
                "Body": body.strip(),
                "Raw Text": raw_post,
                "URL": post_url,  # Use the extracted post or profile URL
                "Start": start,
                "rate": rate,
                "Company": company,
                "Source": "LinkedIn Feed",
                "Matched Keyword": first_matched_kw,
                "Industry": industry if industry else "",
                "date posted": posted_date,
            })

        jobs_df = pd.DataFrame(job_rows)

        # --------------------------------------------------------------
        # Save to historical record 'allgigs Freelance feed.csv'
        # --------------------------------------------------------------

        historical_csv_path = os.path.join(LINKEDIN_DIR, "allgigs Freelance feed.csv")

        if os.path.exists(historical_csv_path):
            try:
                historical_df = pd.read_csv(historical_csv_path)
            except Exception:
                historical_df = pd.DataFrame()
            combined_historical_df = pd.concat([historical_df, jobs_df], ignore_index=True)
        else:
            combined_historical_df = jobs_df

        # Drop exact duplicates by Summary
        combined_historical_df.drop_duplicates(subset=["Summary"], keep="first", inplace=True)

        combined_historical_df.to_csv(historical_csv_path, index=False)

        logger.info(
            f"Updated {os.path.basename(historical_csv_path)} with {len(jobs_df)} new rows; total {len(combined_historical_df)} rows"
        )

        # --------------------------------------------------------------
        # Merge with any existing jobs, de-duplicate, sort newest first
        # --------------------------------------------------------------

        if os.path.exists(jobs_csv_path):
            try:
                existing_df = pd.read_csv(jobs_csv_path)
            except Exception:
                existing_df = pd.DataFrame()
            combined_df = pd.concat([existing_df, jobs_df], ignore_index=True)
        else:
            combined_df = jobs_df

        # Drop exact duplicates by Summary (fallback all columns)
        subset_cols = ["Summary"] if "Summary" in combined_df.columns else None
        combined_df.drop_duplicates(subset=subset_cols, keep="first", inplace=True)

        # Ensure date posted is sort-able
        if "date posted" in combined_df.columns:
            combined_df.sort_values(by="date posted", ascending=False, inplace=True)

        combined_df.to_csv(jobs_csv_path, index=False)

        logger.info(
            f"Updated {os.path.basename(jobs_csv_path)} with {len(jobs_df)} new rows; total {len(combined_df)} rows"
        )

    def close(self):
        """
        Closes the WebDriver session.
        """
        logger.info("Closing the WebDriver.")
        self.driver.quit()

    # ------------------------------------------------------------------
    # Text cleaning helpers
    # ------------------------------------------------------------------

    # Common phrases/words that are not relevant to the content of a post
    _UNINTERESTING_PHRASES = [
        # Interaction keywords
        "comment", "comments", "like", "likes", "repost", "reposts", "share", "send",
        # UI / action keywords
        "follow", "promoted", "apply", "play", "download", "view", "learn more",
        # Misc separators or UI glyphs
        "·", "•", "see more", "...more", "…more",
    ]

    @classmethod
    def _is_uninteresting_line(cls, line: str) -> bool:
        """Returns True if a line is deemed uninteresting (meta info, actions, etc.)."""
        ll = line.lower().strip()
        # Skip completely empty lines quickly
        if not ll:
            return True

        # If the line is very short and mostly meta, drop it
        if len(ll) < 10 and any(ph in ll for ph in cls._UNINTERESTING_PHRASES):
            return True

        # Drop lines that only contain these actionable keywords
        for ph in cls._UNINTERESTING_PHRASES:
            # Match whole word or occurrences
            if ll == ph or re.fullmatch(rf".*\b{re.escape(ph)}\b.*", ll):
                return True

        # Lines that are only a number (e.g., like count) or numeric + meta word
        if re.fullmatch(r"\d+", ll):
            return True
        if re.fullmatch(r"\d+\s*(likes?|comments?|reposts?)?", ll):
            return True
        return False

    @classmethod
    def clean_post_text(cls, raw_text: str) -> str:
        """Cleans a raw LinkedIn post text by removing uninteresting meta lines."""
        if not raw_text:
            return ""

        lines = raw_text.splitlines()
        kept_lines = [line for line in lines if not cls._is_uninteresting_line(line)]

        # Collapse multiple spaces and rejoin
        cleaned = " ".join([re.sub(r"\s+", " ", ln).strip() for ln in kept_lines]).strip()
        return cleaned

    def process_and_save_jobs(self, feed_file_path):
        """
        Processes the linkedin_selenium_feed CSV file to extract job information
        and saves it to Freelance scraped jobs.csv.
        """
        try:
            feed_df = pd.read_csv(feed_file_path)
        except Exception as e:
            logger.error(f"Failed to read {feed_file_path}: {e}")
            return

        job_rows = []
        for _, post_row in feed_df.iterrows():
            raw_post = post_row["raw_post_text"]
            matched_kw_raw = post_row.get("matched_keywords", "")
            first_matched_kw = matched_kw_raw.split(",")[0].strip() if matched_kw_raw else ""

            # Split the raw text into title and body
            marker = "visible to anyone on or off linkedin"
            idx = raw_post.lower().find(marker)
            if idx != -1:
                title = raw_post[:idx].strip()
                body = raw_post[idx + len(marker):].strip()
            else:
                title = raw_post
                body = ""

            # Remove trailing boiler-plate phrases and everything after them
            for tail in [
                "your document has finished loading",
                "link below",
                "reply",
                "see content",
                "show translation",
            ]:
                pos = body.lower().find(tail)
                if pos != -1:
                    body = body[:pos].rstrip()
                    break

            # Detect industry keyword in body
            industry = None
            for kw, ind in INDUSTRY_KEYWORDS.items():
                if re.search(rf"\b{re.escape(kw)}\b", body, re.I):
                    industry = ind
                    break

            if industry:
                title = f"Freelance {industry}"
            else:
                title = _extract_first(ROLE_PATTERNS, body) or " ".join(body.split()[:8])
            location = _extract_first(LOCATION_PATTERNS, body)
            rate = _extract_field(RATE_PATTERN, body)
            # Company: two words before 1st/2nd/3rd badge (• 1st / 2nd / 3rd) searched in raw post first
            cmp_match = re.search(r"(\b\w+)\s+(\w+)\s+(?:•\s*)?(?:1st|2nd|3rd)\b", raw_post, re.I)
            if not cmp_match:
                # fallback to cleaned body without bullet
                cmp_match = re.search(COMPANY_DEGREE_PATTERN, body, re.I)
            if cmp_match:
                company = f"{cmp_match.group(1)} {cmp_match.group(2)}".strip()
            else:
                company = ""
            start = _extract_field(DATE_PATTERN, body)
            if not start:
                start = "ASAP"
            # Extract the post URL
            post_url = post_row["URL"]

            # date posted calculation based on "days" tag
            days_match = re.search(r"(\d+)\s+days?\b", body, re.I)
            if days_match:
                days_ago = int(days_match.group(1))
                posted_date = (datetime.now() - timedelta(days=days_ago)).isoformat(timespec="seconds")
            else:
                posted_date = datetime.now().isoformat(timespec="seconds")

            job_rows.append({
                "Title": title,
                "Body": body.strip(),
                "Raw Text": raw_post,
                "URL": post_url,  # Use the extracted post or profile URL
                "Start": start,
                "rate": rate,
                "Company": company,
                "Source": "LinkedIn Feed",
                "Matched Keyword": first_matched_kw,
                "Industry": industry if industry else "",
                "date posted": posted_date,
            })

        jobs_df = pd.DataFrame(job_rows)

        # --------------------------------------------------------------
        # Merge with any existing jobs, de-duplicate, sort newest first
        # --------------------------------------------------------------

        LINKEDIN_DIR = os.path.dirname(__file__)
        jobs_csv_path = os.path.join(LINKEDIN_DIR, "Freelance scraped jobs.csv")

        if os.path.exists(jobs_csv_path):
            try:
                existing_df = pd.read_csv(jobs_csv_path)
            except Exception:
                existing_df = pd.DataFrame()
            combined_df = pd.concat([existing_df, jobs_df], ignore_index=True)
        else:
            combined_df = jobs_df

        # Drop exact duplicates by Summary (fallback all columns)
        subset_cols = ["Summary"] if "Summary" in combined_df.columns else None
        combined_df.drop_duplicates(subset=subset_cols, keep="first", inplace=True)

        # Ensure date posted is sort-able
        if "date posted" in combined_df.columns:
            combined_df.sort_values(by="date posted", ascending=False, inplace=True)

        combined_df.to_csv(jobs_csv_path, index=False)

        logger.info(
            f"Updated {os.path.basename(jobs_csv_path)} with {len(jobs_df)} new rows; total {len(combined_df)} rows"
        )

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Scrape the LinkedIn feed for a specified duration (in minutes)."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
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