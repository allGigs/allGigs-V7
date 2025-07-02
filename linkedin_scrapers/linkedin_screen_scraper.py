#!/usr/bin/env python3
"""
LinkedIn Screen Reader
=======================

This script reads the screen of the LinkedIn feed while scrolling,
then uses OCR to extract text and convert to CSV format.

No login or saved screenshots required - just in-memory screen capture and text extraction.
"""

import cv2
import numpy as np
import pytesseract
import pandas as pd
import time
import os
from datetime import datetime
from PIL import Image
import pyautogui
import logging
from typing import List, Dict, Tuple
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('linkedin_screen_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LinkedInScreenScraper:
    """
    LinkedIn Screen Reader - reads the screen and extracts text via OCR
    """
    
    def __init__(self):
        """
        Initialize the screen scraper
        """
        self.data = []
        self.focus_region = None  # Will store the LinkedIn window region
        
        # Configure pytesseract (adjust path if needed)
        # pytesseract.pytesseract.tesseract_cmd = r'/usr/local/bin/tesseract'  # macOS
        # pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'  # Windows
        
        logger.info("LinkedIn Screen Scraper initialized")
    
    def set_focus_region(self, x: int, y: int, width: int, height: int):
        """
        Set the focus region for screen capture (LinkedIn window area)
        
        Args:
            x: X coordinate of top-left corner
            y: Y coordinate of top-left corner
            width: Width of the region
            height: Height of the region
        """
        self.focus_region = (x, y, width, height)
        logger.info(f"Focus region set to: ({x}, {y}, {width}, {height})")
    
    def get_screen_region(self):
        """
        Sets the screen region to a hard-coded value based on user input.
        This removes the need for manual entry on each run.
        """
        # Hard-coded coordinates for the LinkedIn feed area
        x = 393
        y = 360
        width = 551
        height = 440
        
        self.set_focus_region(x, y, width, height)
        logger.info("Using hard-coded screen region.")
        return self.focus_region
    
    def extract_text_from_screen(self) -> str:
        """
        Extract text from LinkedIn window directly without saving any files
        Uses simplified preprocessing for better text quality
        
        Returns:
            Extracted text from the LinkedIn window
        """
        try:
            # Use the focus_region that has been set
            if self.focus_region:
                x, y, width, height = self.focus_region
                screenshot = pyautogui.screenshot(region=(x, y, width, height))
                logger.debug(f"Captured focused region: {width}x{height} at ({x},{y})")
            else:
                # Fallback to full screen if no region is set
                screenshot = pyautogui.screenshot()
                logger.warning("No focus region set. Capturing full screen.")
            
            # Convert to numpy array for OpenCV processing
            img_array = np.array(screenshot)
            
            # Convert to grayscale
            if len(img_array.shape) == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            else:
                gray = img_array
            
            # Simplified preprocessing for better text quality
            # 1. Resize image moderately (2x instead of 3x)
            height, width = gray.shape
            gray = cv2.resize(gray, (width * 2, height * 2), interpolation=cv2.INTER_CUBIC)
            
            # 2. Apply gentle Gaussian blur to reduce noise
            gray = cv2.GaussianBlur(gray, (1, 1), 0)
            
            # 3. Apply simple thresholding (less aggressive than adaptive)
            _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            
            # 4. Convert back to PIL Image
            processed_image = Image.fromarray(binary)
            
            # Use single, optimized OCR configuration
            try:
                # Configuration optimized for LinkedIn text (without problematic whitelist)
                custom_config = r'--oem 3 --psm 6'
                text = pytesseract.image_to_string(processed_image, config=custom_config)
                
                if text.strip():
                    # Clean up the text more gently
                    lines = text.split('\n')
                    cleaned_lines = []
                    for line in lines:
                        line = line.strip()
                        # Keep lines that have reasonable length and content
                        if len(line) > 3 and len([c for c in line if c.isalnum()]) > len(line) * 0.2:
                            cleaned_lines.append(line)
                    
                    final_text = '\n'.join(cleaned_lines)
                    return final_text
                
            except Exception as e:
                logger.error(f"OCR error: {str(e)}")
            
            return ""
            
        except Exception as e:
            logger.error(f"Error extracting text from screen: {str(e)}")
            return ""
    
    def scrape_feed(self, duration_minutes: int = 1):
        """
        Scrapes the feed, capturing all text, then separates it into posts
        without any keyword filtering.
        """
        logger.info(f"Starting to scrape LinkedIn feed for {duration_minutes} minutes...")
        logger.info("This will use continuous slow scrolling.")
        
        all_text = []
        end_time = time.time() + duration_minutes * 60
        
        # --- Continuous scrolling logic ---
        scroll_amount = -20      # A smaller amount for a smoother scroll
        scroll_interval = 1.6     # Time between each small scroll (slowed down again)
        capture_interval = 5      # Time between screen captures
        
        last_capture_time = time.time()

        logger.info(f"Starting continuous scroll. Capturing all text for later analysis.")

        while time.time() < end_time:
            # Perform a small scroll
            self.scroll_down(amount=scroll_amount)
            
            # Check if it's time to capture the screen
            current_time = time.time()
            if current_time - last_capture_time >= capture_interval:
                last_capture_time = current_time
                
                # Let the screen settle for a moment before capturing
                time.sleep(0.5)

                # Extract text from the current screen
                text = self.extract_text_from_screen()
                all_text.append(text)
                logger.info("Captured a screen's worth of text.")
            
            # Wait before the next small scroll
            time.sleep(scroll_interval)
            
        logger.info("Finished capturing text. Now separating into posts...")

        full_text_blob = "\n".join(all_text)
        
        with open("ocr_debug_output.txt", "w", encoding="utf-8") as debug_file:
            debug_file.write(full_text_blob)

        # --- Split the text blob into posts using the button bar as a divider ---
        # This regex looks for "Comment...Repost...Send" to be robust against OCR errors
        posts_text = re.split(r'Comment\s*.*?\s*Repost\s*.*?\s*Send', full_text_blob)
        
        found_posts = []
        for post_text in posts_text:
            cleaned_text = post_text.strip()
            if cleaned_text: # Make sure we don't save empty posts
                found_posts.append({
                    'raw_post_text': cleaned_text,
                    'scraped_at': datetime.now().isoformat()
                })
        
        if found_posts:
            self.data.extend(found_posts)
            logger.info(f"Analysis complete. Separated {len(found_posts)} posts using the button bar as a divider.")
        else:
            logger.info("Analysis complete. No posts could be separated.")
    
    def save_to_csv(self, filename: str = None):
        """
        Saves the raw, separated post text to a CSV file.
        """
        if not self.data:
            logger.warning("No data to save.")
            return
        
        df = pd.DataFrame(self.data)
        df.drop_duplicates(subset=['raw_post_text'], keep='first', inplace=True)
        
        num_posts = len(df)
        summary_message = f"\nScraping Summary:\n- Total posts separated: {num_posts}\n"
        logger.info(summary_message)
        
        if not df.empty:
            logger.info("Sample of separated posts (raw text):\n" + df.head(2).to_string())
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"linkedin_feed_posts_raw_{timestamp}.csv"
        
        filepath = os.path.join(os.getcwd(), filename)
        
        final_columns = ['raw_post_text', 'scraped_at']
        df = df.reindex(columns=final_columns)
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(df)} raw posts to {filename}")

    def scroll_down(self, amount: int = -1000):
        """
        Scroll down the LinkedIn feed by a given amount.
        """
        try:
            # Scroll down using pyautogui
            pyautogui.scroll(amount)  # Scroll down (negative value) or up (positive value)
            logger.debug(f"Scrolled by {amount} units")
        except Exception as e:
            logger.error(f"Error scrolling: {str(e)}")

def main():
    """Main function to run the scraper"""
    print("========================================")
    print(" LinkedIn Feed Screen Scraper (OCR)")
    print("========================================")
    print("This tool will read your screen to scrape the LinkedIn feed.")
    print("Ensure the LinkedIn feed is visible on your screen.")
    print("No login or browser automation is performed.")
    print("========================================")
    
    try:
        # Initialize the scraper
        scraper = LinkedInScreenScraper()
        
        # Set the hard-coded screen region
        scraper.get_screen_region()
        
        # Get user input for configuration
        print("\nConfiguration:")
        duration = input("How many minutes to run? (default: 1.0): ").strip()
        duration = float(duration) if duration.replace('.', '').isdigit() else 1.0
        
        # Wait for user to be ready
        input(f"\nPress Enter when ready to start scraping for {duration} minutes...")
        
        # Start scraping for specified duration
        scraper.scrape_feed(duration_minutes=duration)
        
        # Save results to CSV
        scraper.save_to_csv()
        
    except KeyboardInterrupt:
        logger.info("\nScraping interrupted by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main() 