"""
WARNING: DO NOT MODIFY THIS FILE DIRECTLY
This file contains critical scraping logic for Haereum gift website.
Any modifications should be made through the main configuration files.
Direct modifications may break the scraping functionality.
"""

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

# Add semaphore for concurrent task limiting
MAX_CONCURRENT_TASKS = 5
scraping_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

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
    """Find the first product with an exact name match and return its image URL and local path, using Playwright."""
    async with scraping_semaphore:  # Acquire semaphore before starting
        if config is None:
            logger.error("🔴 Configuration object (ConfigParser) is missing for Haereum scrape.")
            return None

        try:
            haereum_main_url = config.get('ScraperSettings', 'haereum_main_url', fallback="https://www.jclgift.com/")
            haereum_image_base_url = config.get('ScraperSettings', 'haereum_image_base_url', fallback="http://i.jclgift.com/")
            user_agent = config.get('ScraperSettings', 'user_agent', fallback="Mozilla/5.0 ...")
            
            default_timeout = config.getint('Playwright', 'playwright_default_timeout_ms', fallback=60000)
            navigation_timeout = config.getint('Playwright', 'playwright_navigation_timeout_ms', fallback=60000)
            action_timeout = config.getint('Playwright', 'playwright_action_timeout_ms', fallback=15000)
            block_resources = config.getboolean('Playwright', 'playwright_block_resources', fallback=True)
            max_download_retries = config.getint('Matching', 'max_retries_downloads', fallback=3)

            # Load selectors from JSON string in config
            selectors_json = config.get('ScraperSettings', 'haereum_selectors_json', fallback='{}')
            try:
                selectors = json.loads(selectors_json)
                if not isinstance(selectors, dict):
                    raise ValueError("Selectors JSON did not parse into a dictionary.")
            except (json.JSONDecodeError, ValueError) as json_err:
                logger.error(f"🔴 Error parsing Haereum selectors JSON from config: {json_err}. Using default selectors.")
                selectors = {
                    "search_input": 'input[name="keyword"]',
                    "search_button": 'input[type="image"][src*="b_search.gif"]',
                    "product_list_item": 'td[width="160"][bgcolor="ffffff"]',
                    "product_name_list": 'td[align="center"][style*="line-height:130%"] > a',
                    "product_image_list": 'td[align="center"] > a > img',
                    "product_list_wrapper": 'form[name="ListForm"]'
                }
            
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
            logger.error(f"🔴 Error reading Haereum/Playwright config: {e}")
            return None

        logger.info(f"🚀 Starting Haereum scrape for keyword: '{keyword}'")
        normalized_keyword = _normalize_text(keyword)

        context = None
        page = None
        try:
            # Check if browser is still valid
            if not browser.is_connected():
                logger.error("🔴 Browser is not connected")
                return None

            context = await browser.new_context(user_agent=user_agent)
            page = await context.new_page()
            page.set_default_timeout(default_timeout)
            page.set_default_navigation_timeout(navigation_timeout)

            if block_resources:
                await setup_page_optimizations(page)

            logger.debug(f"🌐 Navigating to {haereum_main_url}")
            await page.goto(haereum_main_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            logger.debug("⏳ Initial page load wait finished.")

            # --- Search interaction ---
            # Wait for the search input to be present and visible with retry logic
            max_retries = 3
            retry_count = 0
            search_input = None
            
            while retry_count < max_retries:
                try:
                    search_input = page.locator('input[name="keyword"]')
                    await search_input.wait_for(state="visible", timeout=action_timeout)
                    break
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"⚠️ Retry {retry_count}/{max_retries} for search input: {str(e)}")
                    if retry_count < max_retries:
                        await page.reload()
                        await page.wait_for_timeout(5000)
                    else:
                        raise
            
            # Wait for the input to be enabled
            start_time = time.time()
            while time.time() - start_time < action_timeout / 1000:  # Convert ms to seconds
                if await search_input.is_enabled():
                    break
                await page.wait_for_timeout(100)  # Check every 100ms
            
            # Fill the search input
            await search_input.fill(keyword, timeout=action_timeout)
            logger.debug(f"⌨️ Filled search input with keyword: {keyword}")

            # Wait for the search button to be present and visible
            search_button = page.locator('input[type="image"][src*="b_search.gif"]')
            await search_button.wait_for(state="visible", timeout=action_timeout)
            
            # Wait for the button to be enabled
            start_time = time.time()
            while time.time() - start_time < action_timeout / 1000:  # Convert ms to seconds
                if await search_button.is_enabled():
                    break
                await page.wait_for_timeout(100)  # Check every 100ms
            
            # Click the search button and wait for navigation
            await search_button.click(timeout=action_timeout)
            await page.wait_for_timeout(5000)
            logger.info("🔍 Search button clicked, waiting for results")

            # --- Enhanced image URL extraction ---
            try:
                # Wait for the product list to load with multiple possible selectors
                selectors_to_try = [
                    'img[src*="/upload/product/"]',  # More general pattern
                    'img[src*="/upload/product/simg3/"]',  # Original pattern
                    'td[align="center"] img',  # General product image
                    'form[name="ListForm"] img'  # Any image in product list
                ]
                
                # Try each selector until we find images
                product_images = []
                for selector in selectors_to_try:
                    try:
                        await page.wait_for_selector(selector, timeout=action_timeout)
                        images = await page.query_selector_all(selector)
                        if images:
                            product_images = images
                            logger.info(f"📸 Found {len(product_images)} product images using selector: {selector}")
                            break
                    except Exception as e:
                        logger.debug(f"Selector {selector} not found: {str(e)}")
                        continue
                
                if not product_images:
                    logger.warning("⚠️ No product images found on the page with any selector")
                    # Try to get page content for debugging
                    try:
                        content = await page.content()
                        logger.debug(f"Page content: {content[:500]}...")  # Log first 500 chars
                    except Exception as e:
                        logger.error(f"Failed to get page content: {str(e)}")
                    return None
                
                # Get the first product image URL with better error handling
                first_image = product_images[0]
                img_src = await first_image.get_attribute('src')
                
                if not img_src:
                    logger.warning("⚠️ Could not get image source attribute")
                    # Try alternative attributes
                    for attr in ['data-src', 'data-original', 'srcset']:
                        img_src = await first_image.get_attribute(attr)
                        if img_src:
                            logger.info(f"Found image URL in alternative attribute: {attr}")
                            break
                
                if img_src:
                    # Construct full URL if needed
                    if not img_src.startswith(('http://', 'https://')):
                        found_image_url = urljoin(haereum_main_url, img_src)
                    else:
                        found_image_url = img_src
                        
                    logger.info(f"✅ Found image URL: {found_image_url}")
                    
                    # Download the image
                    local_path = await download_image_to_main(found_image_url, keyword, config, max_retries=max_download_retries)
                    if local_path:
                        return {"url": found_image_url, "local_path": local_path, "source": "haereum"}
                    else:
                        return {"url": found_image_url, "local_path": None, "source": "haereum"}
                else:
                    logger.warning("⚠️ Could not get image source from any attribute")
                    return None
                
            except Exception as e:
                logger.error(f"❌ Error during image URL extraction: {e}", exc_info=True)
                return None
                
        except PlaywrightError as pe:
            logger.error(f"❌ Playwright error during Haereum scrape: {pe}")
        except Exception as e:
            logger.error(f"❌ Unexpected error during Haereum scrape: {e}", exc_info=True)
        finally:
            if context:
                try:
                    await context.close()
                except Exception as e:
                    logger.warning(f"⚠️ Error closing context: {e}")

        return None

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
        
    test_keywords = [
        "사랑이 엔젤하트 투포켓 에코백",
        "사랑이 큐피트화살 투포켓 에코백",
        "행복이 스마일플라워 투포켓 에코백",
        "행운이 네잎클로버 투포켓 에코백",
        "캐치티니핑 53 스무디 입체리본 투명 아동우산",
        "아테스토니 뱀부사 소프트 3P 타올 세트"
    ]
    
    logger.info(f"--- Running Parallel Test for Haereum Gift with {len(test_keywords)} keywords ---")
    
    async with async_playwright() as p:
        try:
            headless_mode = config.getboolean('Playwright', 'playwright_headless', fallback=False)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
             headless_mode = False
             
        browser = await p.chromium.launch(headless=headless_mode)
        start_time = time.time()
        
        try:
            # Create tasks for parallel execution
            tasks = []
            for keyword in test_keywords:
                task = asyncio.create_task(scrape_haereum_data(browser, keyword, config))
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process and display results
            print("\n--- Parallel Scraping Test Results ---")
            for keyword, result in zip(test_keywords, results):
                if isinstance(result, Exception):
                    print(f"❌ Error for '{keyword}': {str(result)}")
                elif result and result.get("url"):
                    print(f"✅ Success for '{keyword}':")
                    print(f"  - Image URL: {result.get('url')}")
                    print(f"  - Local path: {result.get('local_path')}")
                    print(f"  - Source: {result.get('source')}")
                else:
                    print(f"❌ No results found for '{keyword}'")
                print("---------------------------")
                
        finally:
            await browser.close()
            
        end_time = time.time()
        logger.info(f"Parallel scraping took {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    # To run this test: python PythonScript/crawling_haereum_standalone.py
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    logger.info("Running Haereum parallel test...")
    asyncio.run(_test_main()) 