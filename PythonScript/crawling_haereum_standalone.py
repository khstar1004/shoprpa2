import asyncio
import pandas as pd
from playwright.async_api import Browser, Page, Error as PlaywrightError # Import specific types
# import requests # Comment out requests related imports
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
from bs4 import BeautifulSoup # Keep BeautifulSoup for potential small parsing tasks if needed
import random
import logging
from urllib.parse import urlparse, urljoin
import re
import sys
import os
import time
from typing import Optional, Dict, Tuple
import configparser # Import configparser
import json # Import json for parsing selectors
import aiohttp
import aiofiles
import hashlib

# Ensure utils can be imported if run directly
# Assuming utils.py is in the same directory or Python path is set correctly
# script_dir = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(script_dir)
# from utils import generate_keyword_variations # Example if needed

# Import helper from Kogift scraper (or move to utils)
from crowling_kogift import should_block_request, setup_page_optimizations 

# Recent Changes (2024-05-21):
# 1. Updated scrape_haereum_data function to return a dictionary with URL, local path, and source info
# 2. Added download_image_to_main function to save images directly to main folder
# 3. Images saved with source identification prefix "haereum_" in filename
# 4. Enhanced file naming to include product code and URL hash
# 5. Uses image_main_dir from config.ini [Paths] section

# 로거 설정
logger = logging.getLogger(__name__)

# Constants moved to config or passed in scrape_haereum_data
# HAEREUM_MAIN_URL = "https://www.jclgift.com/"
# HAEREUM_IMAGE_BASE_URL = "http://i.jclgift.com/" 
# HAEREUM_PAGE_BASE_URL = "https://www.jclgift.com/" 
# USER_AGENT = ...
# SELECTORS = ...
# PATTERNS = ...

def _normalize_text(text: str) -> str:
    """Normalizes text (remove extra whitespace)."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

# Note: _extract_price is not used in the current logic focused on image URL extraction
# def _extract_price(price_text: str) -> float:
#     ...

# Updated main scraping function to accept browser and ConfigParser
async def scrape_haereum_data(browser: Browser, keyword: str, config: configparser.ConfigParser = None) -> Optional[Dict[str, str]]:
    """Find the first product with an exact name match and return its image URL and local path, using Playwright.
       Uses a shared Browser instance. Accepts ConfigParser object.
    
    Args:
        browser: An active Playwright Browser instance.
        keyword: The product name (keyword) to search for.
        config: ConfigParser object containing configuration settings.
        
    Returns:
        Dictionary with image URL and local path if found, otherwise None.
    """
    if config is None:
        logger.error("🟡 Configuration object (ConfigParser) is missing for Haereum scrape.")
        return None # Return None on critical config error

    # Get settings from config using ConfigParser methods
    try:
        haereum_main_url = config.get('ScraperSettings', 'haereum_main_url', fallback="https://www.jclgift.com/")
        haereum_image_base_url = config.get('ScraperSettings', 'haereum_image_base_url', fallback="http://i.jclgift.com/")
        user_agent = config.get('ScraperSettings', 'user_agent', fallback="Mozilla/5.0 ...")
        
        default_timeout = config.getint('Playwright', 'playwright_default_timeout_ms', fallback=60000)
        navigation_timeout = config.getint('Playwright', 'playwright_navigation_timeout_ms', fallback=60000)
        action_timeout = config.getint('Playwright', 'playwright_action_timeout_ms', fallback=15000)
        block_resources = config.getboolean('Playwright', 'playwright_block_resources', fallback=True)
        
        # Get download retry settings
        max_download_retries = config.getint('Matching', 'max_retries_downloads', fallback=3)

        # Load selectors from JSON string in config
        selectors_json = config.get('ScraperSettings', 'haereum_selectors_json', fallback='{}')
        try:
            selectors = json.loads(selectors_json)
            if not isinstance(selectors, dict):
                 raise ValueError("Selectors JSON did not parse into a dictionary.")
        except (json.JSONDecodeError, ValueError) as json_err:
            logger.error(f"🟡 Error parsing Haereum selectors JSON from config: {json_err}. Using default selectors. JSON string: {selectors_json}")
            # Define default selectors here if parsing fails
            selectors = {
                "search_input": 'input[name="keyword"]',
                "search_button": 'input[type="image"][src*="b_search.gif"]',
                "product_list_item": 'td[width="160"][bgcolor="ffffff"]',
                "product_name_list": 'td[align="center"][style*="line-height:130%"] > a',
                "product_image_list": 'td[align="center"] > a > img',
                "product_list_wrapper": 'form[name="ListForm"]'
            }
            
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        logger.error(f"🟡 Error reading Haereum/Playwright config: {e}. Cannot proceed.")
        return None # Cannot proceed without basic config

    logger.info(f"Starting Haereum scrape for keyword: '{keyword}'")
    found_image_url = None 
    normalized_keyword = _normalize_text(keyword)

    context = None
    page = None
    try:
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()
        page.set_default_timeout(default_timeout)
        page.set_default_navigation_timeout(navigation_timeout)

        if block_resources:
            await setup_page_optimizations(page)

        logger.debug(f"Navigating to {haereum_main_url}")
        await page.goto(haereum_main_url, wait_until="domcontentloaded")
        # Increase explicit wait after navigation
        await page.wait_for_timeout(5000) 
        logger.debug("Initial page load wait finished.")

        # --- Locate and interact with search elements --- 
        search_input_selector = selectors.get("search_input", 'input[name="keyword"]')
        search_button_selector = selectors.get("search_button", 'input[type="image"][src*="b_search.gif"]')

        logger.debug(f"Looking for search input with selector: {search_input_selector}")
        # Wait for the selector to be attached
        await page.locator(search_input_selector).wait_for(state="attached", timeout=action_timeout)
        search_input = page.locator(search_input_selector).first
        # Wait for the element to be visible before interacting
        await search_input.wait_for(state="visible", timeout=action_timeout) 
        await search_input.fill(keyword, timeout=action_timeout) # Rely on fill's internal checks and timeout
        logger.debug(f"Filled search input with keyword: {keyword}")

        logger.debug(f"Looking for search button with selector: {search_button_selector}")
        # Wait for the button to be attached and visible
        await page.locator(search_button_selector).wait_for(state="attached", timeout=action_timeout)
        search_button = page.locator(search_button_selector).first
        await search_button.wait_for(state="visible", timeout=action_timeout)

        # --- Click search and wait for results --- 
        logger.info("Clicking search button...")
        # Click without waiting for navigation
        await search_button.click(timeout=action_timeout) 
        # Use a simple timeout instead
        await page.wait_for_timeout(5000)
        logger.info("Search button clicked, continuing after timeout")
        
        # Simplified approach to extract image URL from HTML
        try:
            # Get the page HTML directly
            page_html = await page.content()
            logger.info(f"Retrieved page HTML, length: {len(page_html)} characters")
            
            # Simple pattern to find image URLs
            img_pattern = r'src="(/upload/product/simg3/[^"]+)"'
            all_img_matches = re.findall(img_pattern, page_html)
            
            if all_img_matches:
                logger.info(f"Found {len(all_img_matches)} image URLs with pattern '/upload/product/simg3/'")
                
                # Extract parts of the product name to search for
                product_parts = normalized_keyword.split()
                logger.info(f"Looking for product parts: {product_parts}")
                
                # For each image, check if it's associated with our product
                for img_path in all_img_matches:
                    # Get context around this image URL (text before and after)
                    img_pos = page_html.find(img_path)
                    start_pos = max(0, img_pos - 200)
                    end_pos = min(len(page_html), img_pos + 500)
                    context_text = page_html[start_pos:end_pos].lower()
                    
                    # Check if all parts of the product name are in this context
                    all_parts_found = all(part.lower() in context_text for part in product_parts)
                    if all_parts_found:
                        found_image_url = urljoin(haereum_main_url, img_path)
                        logger.info(f"⭐ Found matching image URL: {found_image_url}")
                        
                        # Download the image to the main folder with target information
                        local_path = await download_image_to_main(found_image_url, keyword, config, max_retries=max_download_retries)
                        if local_path:
                            if context: await context.close()
                            return {"url": found_image_url, "local_path": local_path, "source": "haereum"}
                        else:
                            if context: await context.close()
                            return {"url": found_image_url, "local_path": None, "source": "haereum"}
                
                # If we reached here, we found images but none matched our product
                logger.warning(f"Found {len(all_img_matches)} images but none matched the product '{normalized_keyword}'")
            else:
                logger.warning("No image URLs with pattern '/upload/product/simg3/' found in page HTML")
        except Exception as e:
            logger.error(f"Error during HTML extraction: {e}")
        
        # Continue with fallback approach if direct extraction failed
        product_list_selector = selectors.get("product_list_item", 'td[width="160"][bgcolor="ffffff"]')
        
        # Add diagnostic code to capture the full page HTML for troubleshooting
        try:
            page_html = await page.content()
            logger.debug(f"Page HTML length: {len(page_html)} characters")
            # Save a snippet around a key marker to debug structure
            if "/upload/product/simg3/" in page_html:
                logger.info("Found '/upload/product/simg3/' in page HTML - image URLs are present on page")
                img_index = page_html.find("/upload/product/simg3/")
                snippet = page_html[max(0, img_index-100):min(len(page_html), img_index+200)]
                logger.debug(f"HTML snippet around image URL: {snippet}")
            else:
                logger.warning("No '/upload/product/simg3/' pattern found in the entire page HTML")
        except Exception as e:
            logger.warning(f"Could not retrieve page HTML for diagnostics: {e}")
        
        product_items = page.locator(product_list_selector)
        item_count = await product_items.count()
        logger.info(f"Found {item_count} potential product elements on the results page.")

        if item_count == 0:
            logger.warning(f"No product items located for keyword '{keyword}' (count is 0).")
            # Ensure context is closed before returning
            if context: await context.close()
            return None

        # --- Iterate through items to find exact match --- 
        name_selector = selectors.get("product_name_list", 'td[align="center"][style*="line-height:130%"] > a') # Corrected selector based on HTML
        image_selector = selectors.get("product_image_list", 'td[align="center"] > a > img')

        for i in range(item_count):
            item = product_items.nth(i)
            try:
                # Extract Name
                name_element = item.locator(name_selector).first
                raw_extracted_name = ""
                extracted_name = ""
                try:
                     raw_extracted_name = await name_element.text_content(timeout=5000) or ""
                     extracted_name = _normalize_text(raw_extracted_name)
                except PlaywrightError as name_err:
                     logger.debug(f"Could not extract name from item {i} using selector '{name_selector}': {name_err}")
                     continue 

                # Detailed Logging for comparison
                logger.debug(f"Item {i}: Raw Name='{raw_extracted_name}', Normalized Name='{extracted_name}', Keyword='{normalized_keyword}'")

                # Compare normalized name with normalized keyword
                if extracted_name == normalized_keyword:
                    logger.info(f"Exact match found for '{keyword}' at index {i}: '{extracted_name}'")
                    
                    # --- New Image Extraction using Regex on Item HTML ---
                    try:
                        # Get either the specific item HTML or the entire page HTML if needed
                        item_html = await item.inner_html(timeout=3000)
                        
                        # More robust regex that handles variations in the HTML structure
                        # This pattern looks for the exact format seen in the example
                        img_pattern = r'src="(/upload/product/simg3/[^"]+)"'
                        match = re.search(img_pattern, item_html)
                        
                        if match:
                            image_path = match.group(1)
                            # Construct the full URL by joining with base URL
                            found_image_url = urljoin(haereum_main_url, image_path)
                            logger.info(f"Found image URL: {found_image_url}")
                            
                            # Download the image to the main folder with target information
                            local_path = await download_image_to_main(found_image_url, keyword, config, max_retries=max_download_retries)
                            if local_path:
                                if context: await context.close()
                                return {"url": found_image_url, "local_path": local_path, "source": "haereum"}
                            else:
                                if context: await context.close()
                                return {"url": found_image_url, "local_path": None, "source": "haereum"}
                                
                        else:
                            # If no match in the item HTML, try getting the entire page HTML as fallback
                            logger.debug("Could not find image URL in item HTML, attempting fallback with page HTML")
                            page_html = await page.content()
                            # Look for the product name in proximity to image URL
                            pattern = rf'<a href="[^"]+"><img src="(/upload/product/simg3/[^"]+)"[^>]+></a>.*?{re.escape(normalized_keyword)}'
                            match = re.search(pattern, page_html, re.DOTALL | re.IGNORECASE)
                            
                            if match:
                                image_path = match.group(1)
                                found_image_url = urljoin(haereum_main_url, image_path)
                                logger.info(f"Found image URL (from page fallback): {found_image_url}")
                                
                                # Download the image to the main folder with target information
                                local_path = await download_image_to_main(found_image_url, keyword, config, max_retries=max_download_retries)
                                if local_path:
                                    if context: await context.close()
                                    return {"url": found_image_url, "local_path": local_path, "source": "haereum"}
                                else:
                                    if context: await context.close()
                                    return {"url": found_image_url, "local_path": None, "source": "haereum"}
                                    
                            else:
                                logger.warning(f"Could not find image URL pattern in HTML for matched item.")
                    except Exception as e:
                        logger.warning(f"Error extracting image URL: {e}")
                    # --- End New Image Extraction ---

                    break # Break after finding exact match and attempting regex extraction
                else:
                    pass # Reduce noise, only log debug on match or error

            except Exception as e:
                logger.warning(f"Could not process item index {i} to check name/image: {e}", exc_info=False)
                continue

        # --- End of loop --- 
        if not found_image_url:
            logger.warning(f"No exact match found for keyword '{keyword}' among {item_count} items.")

    except PlaywrightError as pe:
         logger.error(f"Playwright error during Haereum scrape for '{keyword}': {pe}")
         found_image_url = None
    except Exception as e:
        logger.error(f"Unexpected error during Haereum scrape for '{keyword}': {e}", exc_info=True)
        found_image_url = None
    finally:
        if context:
             try: await context.close(); logger.debug(f"Closed Playwright context for Haereum keyword '{keyword}'.")
             except Exception as context_close_err: logger.warning(f"Error closing Haereum context: {context_close_err}")

    return None if not found_image_url else {"url": found_image_url, "local_path": None, "source": "haereum"}

async def download_image_to_main(image_url: str, product_name: str, config: configparser.ConfigParser, max_retries: int = 3) -> Optional[str]:
    """Download an image to the main folder with target information.
    
    Args:
        image_url: The URL of the image to download.
        product_name: The name of the product.
        config: ConfigParser object containing configuration settings.
        max_retries: Maximum number of retry attempts for transient errors.
        
    Returns:
        The local path to the downloaded image, or None if download failed.
    """
    if not image_url:
        logger.warning("Empty image URL provided to download_image_to_main")
        return None
        
    # Get the main directory from config 
    try:
        main_dir = config.get('Paths', 'image_main_dir', fallback=None)
        if not main_dir:
            # Use fallback path if config path not found
            main_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'images')
            logger.warning(f"image_main_dir not found in config, using fallback: {main_dir}")
        
        # Ensure the directory exists
        if not os.path.exists(main_dir):
            logger.info(f"Creating image directory: {main_dir}")
            os.makedirs(main_dir, exist_ok=True)
            
        # Check if directory is writable
        if not os.access(main_dir, os.W_OK):
            logger.error(f"Image directory is not writable: {main_dir}")
            # Try to use current directory as fallback
            main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logger.warning(f"Using fallback directory: {main_dir}")
            
    except Exception as e:
        logger.error(f"Error accessing or creating image directory: {e}")
        # Use current directory as fallback
        main_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logger.warning(f"Using fallback directory: {main_dir}")
        os.makedirs(main_dir, exist_ok=True)
    
    # Ensure product_name is a string and properly encoded
    if product_name is None:
        product_name = "unknown_product"
    elif not isinstance(product_name, str):
        product_name = str(product_name)
    
    # Handle potential URL encoding issues (for Korean characters)
    try:
        # Normalize URL if it contains Korean characters
        if any('\uAC00' <= c <= '\uD7A3' for c in image_url):
            # Korean character range check
            parsed = urlparse(image_url)
            encoded_path = parsed.path.encode('utf-8').decode('iso-8859-1')
            image_url = parsed._replace(path=encoded_path).geturl()
            logger.debug(f"URL contains Korean characters, normalized to: {image_url}")
    except Exception as url_err:
        logger.warning(f"URL normalization error (non-critical): {url_err}")
    
    # Create a safe product code from the product name
    # Handle Korean characters by replacing them with transliteration or just use the hash
    try:
        # If product name contains Korean, use only the hash part for safer filenames
        if any('\uAC00' <= c <= '\uD7A3' for c in product_name):
            # For Korean product names, use a hash of the full name 
            product_code = hashlib.md5(product_name.encode('utf-8')).hexdigest()[:16]
            logger.debug(f"Using hash-based product code for Korean name: {product_code}")
        else:
            # For non-Korean names, keep some readable portion plus normalization
            product_code = re.sub(r'[^\w\d-]', '_', product_name)[:30]
    except Exception as code_err:
        # Fallback to simple hash if any encoding/processing issues
        product_code = hashlib.md5(product_name.encode('utf-8', errors='ignore')).hexdigest()[:16]
        logger.warning(f"Error processing product name, using hash: {code_err}")
    
    # Create a hash of URL for uniqueness
    url_hash = hashlib.md5(image_url.encode('utf-8', errors='ignore')).hexdigest()[:8]  # Use shorter hash
    
    # Get file extension from URL
    try:
        parsed_url = urlparse(image_url)
        _, ext = os.path.splitext(parsed_url.path)
        ext = ext.lower() or ".jpg"  # Default to .jpg if no extension
        
        # Check for invalid extensions
        if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            logger.warning(f"Suspicious file extension: {ext}, defaulting to .jpg")
            ext = '.jpg'
            
    except Exception as e:
        logger.error(f"Error parsing URL for file extension: {e}")
        ext = '.jpg'  # Default to .jpg on error
    
    # Include target/source information in the filename
    # Format: source_productcode_hash.ext
    filename = f"haereum_{product_code}_{url_hash}{ext}"
    local_path = os.path.join(main_dir, filename)
    
    # Check if file already exists
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.info(f"Image already exists, skipping download: {local_path}")
        return local_path
    
    # Set up retry logic
    remaining_retries = max_retries
    last_error = None
    retry_delay = 1.0  # Start with 1 second delay, double on each retry
    
    while remaining_retries >= 0:
        try:
            # Create an aiohttp session to download the image
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    headers = {
                        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    
                    async with session.get(image_url, headers=headers) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to download image, HTTP status: {response.status}")
                            if remaining_retries > 0 and 500 <= response.status < 600:
                                # Only retry for server errors (5xx)
                                remaining_retries -= 1
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2  # Exponential backoff
                                logger.info(f"Retrying download due to server error (remaining attempts: {remaining_retries})")
                                continue
                            return None
                        
                        # Check content type
                        content_type = response.headers.get('Content-Type', '')
                        if not content_type.startswith('image/'):
                            logger.warning(f"URL does not return an image (Content-Type: {content_type})")
                            return None
                            
                        # Read the image data
                        image_data = await response.read()
                        
                        # Check image data size
                        if len(image_data) < 100:  # Extremely small file, probably not a valid image
                            logger.warning(f"Image data too small ({len(image_data)} bytes), probably not a valid image")
                            return None
                        
                        # Save the image to the main folder
                        try:
                            async with aiofiles.open(local_path, 'wb') as f:
                                await f.write(image_data)
                                
                            logger.info(f"Downloaded Haereum image to main folder: {local_path}")
                            return local_path
                        except IOError as e:
                            logger.error(f"IO error saving image file: {e}")
                            return None
                except aiohttp.ClientError as e:
                    last_error = e
                    logger.error(f"HTTP client error downloading image: {e}")
                    if remaining_retries > 0:
                        remaining_retries -= 1
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        logger.info(f"Retrying download due to client error (remaining attempts: {remaining_retries})")
                        continue
                    return None
                except asyncio.TimeoutError:
                    last_error = "Timeout"
                    logger.error(f"Timeout downloading image from {image_url}")
                    if remaining_retries > 0:
                        remaining_retries -= 1
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        logger.info(f"Retrying download due to timeout (remaining attempts: {remaining_retries})")
                        continue
                    return None
                    
        except Exception as e:
            last_error = e
            logger.error(f"Unexpected error downloading image from {image_url}: {e}")
            if remaining_retries > 0:
                remaining_retries -= 1
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
                logger.info(f"Retrying download due to unexpected error (remaining attempts: {remaining_retries})")
                continue
            return None
        
        # If we reach here without continuing the retry loop, break out
        break
        
    # If we exhausted all retries
    if last_error:
        logger.error(f"Failed to download image after {max_retries} retries. Last error: {last_error}")
    
    return None

# Example usage (Updated for ConfigParser)
async def _test_main():
    from playwright.async_api import async_playwright
    from utils import load_config # Import config loader
    
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config = load_config(config_path)
    if not config.sections():
        print(f"Test Error: Could not load config from {config_path}")
        return
        
    test_keyword = "777쓰리쎄븐 TS-5500VG 손톱깎이세트" 
    logger.info(f"--- Running Standalone Test for Haereum Gift with keyword: {test_keyword} ---")
    
    async with async_playwright() as p:
        try:
            headless_mode = config.getboolean('Playwright', 'playwright_headless', fallback=False) # Default to False for visual test
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
             headless_mode = False
             
        browser = await p.chromium.launch(headless=headless_mode)
        start_time = time.time()
        try:
            # Pass ConfigParser object
            result = await scrape_haereum_data(browser, test_keyword, config)
        finally:
             await browser.close()
        end_time = time.time()
        logger.info(f"Scraping took {end_time - start_time:.2f} seconds.")

        if result and result.get("url"):
            print("\n--- Scraping Test Results ---")
            print(f"Found Image URL for '{test_keyword}': {result.get('url')}")
            print(f"Local path: {result.get('local_path')}")
            print(f"Source: {result.get('source')}")
            print("---------------------------")
        else:
            print("\n--- Scraping Test Results ---")
            print(f"No exact match image URL found for '{test_keyword}' or an error occurred.")
            print("---------------------------")

if __name__ == "__main__":
    # To run this test: python PythonScript/crawling_haereum_standalone.py
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    logger.info("Running Haereum standalone test...")
    asyncio.run(_test_main()) 