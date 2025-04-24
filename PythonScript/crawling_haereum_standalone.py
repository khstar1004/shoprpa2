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
from PIL import Image

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

# Global semaphore for file operations
file_semaphore = asyncio.Semaphore(1)

# Constants moved to config or passed in scrape_haereum_data
# HAEREUM_MAIN_URL = "https://www.jclgift.com/"
# HAEREUM_IMAGE_BASE_URL = "http://i.jclgift.com/" 
# HAEREUM_PAGE_BASE_URL = "https://www.jclgift.com/" 
# USER_AGENT = ...
# SELECTORS = ...
# PATTERNS = ...

# Add browser context timeout settings
BROWSER_CONTEXT_TIMEOUT = 300000  # 5 minutes (reduced from 10)
PAGE_TIMEOUT = 180000  # 3 minutes (reduced from 5)
NAVIGATION_TIMEOUT = 90000  # 1.5 minutes (reduced from 2)
WAIT_TIMEOUT = 20000  # 20 seconds (reduced from 30)

# Add retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
RETRY_BACKOFF_FACTOR = 2  # Exponential backoff factor

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
    # Create a new semaphore for this function call
    max_windows = config.getint('Playwright', 'playwright_max_concurrent_windows', fallback=2)
    scraping_semaphore = asyncio.Semaphore(max_windows)  # Use config value for max concurrent windows
    
    retry_count = 0
    last_error = None
    
    # Check if we should use a default image when all else fails
    use_default_image = config.getboolean('Matching', 'use_default_image_when_not_found', fallback=True)
    default_image_path = config.get('Paths', 'default_image_path', fallback=None)
    
    # Check if we need to reconnect the browser
    need_new_browser = not browser or not browser.is_connected()
    
    while retry_count < MAX_RETRIES:
        try:
            async with scraping_semaphore:  # Acquire semaphore before starting
                if config is None:
                    logger.error("🔴 Configuration object (ConfigParser) is missing for Haereum scrape.")
                    return None

                try:
                    # Check if browser is connected and reconnect if needed
                    if not browser or not browser.is_connected():
                        logger.warning(f"🔶 Browser is not connected for Haereum scrape. Attempting to reconnect.")
                        
                        # If the caller provided a disconnected browser, we'll try to create a new one
                        if need_new_browser:
                            from playwright.async_api import async_playwright
                            playwright = await async_playwright().start()
                            
                            # Get browser launch arguments from config
                            browser_args = []
                            try:
                                browser_args_str = config.get('Playwright', 'playwright_browser_args', fallback='[]')
                                browser_args = json.loads(browser_args_str)
                            except Exception as arg_err:
                                logger.warning(f"Could not parse browser arguments: {arg_err}. Using defaults.")
                                browser_args = ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
                            
                            # Launch a new browser
                            try:
                                headless = config.getboolean('Playwright', 'playwright_headless', fallback=True)
                                browser = await playwright.chromium.launch(
                                    headless=headless,
                                    args=browser_args,
                                    timeout=60000  # 1 minute timeout for browser launch
                                )
                                logger.info("🟢 Successfully launched a new browser instance for Haereum")
                            except Exception as launch_err:
                                logger.error(f"Failed to launch new browser for Haereum: {launch_err}")
                                # Use default image if configured
                                if use_default_image and default_image_path and os.path.exists(default_image_path):
                                    logger.info(f"Using default image after browser launch failure: {default_image_path}")
                                    return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
                                return None
                        else:
                            # Skip this attempt if we couldn't reconnect
                            logger.error(f"🔴 Browser is not connected and cannot be recreated for Haereum scrape.")
                            raise PlaywrightError("Browser disconnected and can't be recreated")

                    haereum_main_url = config.get('ScraperSettings', 'haereum_main_url', fallback="https://www.jclgift.com/")
                    haereum_image_base_url = config.get('ScraperSettings', 'haereum_image_base_url', fallback="http://i.jclgift.com/")
                    user_agent = config.get('Network', 'user_agent', fallback="Mozilla/5.0 ...")
                    
                    # Apply delay before creating a new context if configured
                    context_delay = config.getint('Playwright', 'playwright_new_context_delay_ms', fallback=0)
                    if context_delay > 0:
                        await asyncio.sleep(context_delay / 1000)  # Convert ms to seconds
                    
                    # Create a new context with proper settings
                    context = await browser.new_context(
                        user_agent=user_agent,
                        viewport={'width': 1920, 'height': 1080}
                    )
                    
                    # Create a new page with increased timeouts
                    page = await context.new_page()
                    page.set_default_timeout(config.getint('Playwright', 'playwright_default_timeout_ms', fallback=120000))
                    page.set_default_navigation_timeout(config.getint('Playwright', 'playwright_navigation_timeout_ms', fallback=60000))

                    if config.getboolean('Playwright', 'playwright_block_resources', fallback=True):
                        await setup_page_optimizations(page)

                    logger.debug(f"🌐 Navigating to {haereum_main_url}")
                    # Add retry logic for the initial navigation
                    for nav_attempt in range(3):
                        try:
                            await page.goto(haereum_main_url, wait_until="domcontentloaded", 
                                          timeout=config.getint('ScraperSettings', 'navigation_timeout', fallback=90000))
                            # Short pause after navigation to allow page to stabilize
                            await page.wait_for_timeout(5000)
                            break  # Break out of retry loop if successful
                        except PlaywrightError as nav_err:
                            if nav_attempt < 2:  # Try again if we haven't reached max retries
                                logger.warning(f"Navigation error (attempt {nav_attempt+1}/3): {nav_err}")
                                await asyncio.sleep(2)  # Wait before retry
                            else:
                                raise  # Re-raise on final attempt
                    
                    logger.debug("⏳ Initial page load wait finished.")

                    # --- Search interaction ---
                    # Wait for the search input to be present and visible with retry logic
                    max_retries = 3
                    retry_count = 0
                    search_input = None
                    
                    while retry_count < max_retries:
                        try:
                            search_input = page.locator('input[name="keyword"]')
                            await search_input.wait_for(state="visible", 
                                                      timeout=config.getint('ScraperSettings', 'action_timeout', fallback=30000))
                            break
                        except Exception as e:
                            retry_count += 1
                            logger.warning(f"⚠️ Retry {retry_count}/{max_retries} for search input: {str(e)}")
                            if retry_count < max_retries:
                                await page.reload()
                                await page.wait_for_timeout(5000)
                            else:
                                raise
                    
                    # Wait for the input to be enabled with timeout
                    start_time = time.time()
                    wait_timeout = config.getint('ScraperSettings', 'action_timeout', fallback=30000)
                    while time.time() - start_time < wait_timeout / 1000:  # Convert ms to seconds
                        if await search_input.is_enabled():
                            break
                        await page.wait_for_timeout(100)  # Check every 100ms
                    
                    # Fill the search input with timeout
                    await search_input.fill(keyword, 
                                          timeout=config.getint('ScraperSettings', 'action_timeout', fallback=30000))
                    logger.debug(f"⌨️ Filled search input with keyword: {keyword}")

                    # Wait for the search button to be present and visible
                    search_button = page.locator('input[type="image"][src*="b_search.gif"]')
                    await search_button.wait_for(state="visible", 
                                               timeout=config.getint('ScraperSettings', 'action_timeout', fallback=30000))
                    
                    # Wait for the button to be enabled with timeout
                    start_time = time.time()
                    while time.time() - start_time < wait_timeout / 1000:  # Convert ms to seconds
                        if await search_button.is_enabled():
                            break
                        await page.wait_for_timeout(100)  # Check every 100ms
                    
                    # Click the search button and wait for navigation
                    await search_button.click(timeout=config.getint('ScraperSettings', 'action_timeout', fallback=30000))
                    # Reduced wait time (1 second) before checking for errors or results
                    await page.wait_for_timeout(1000)
                    logger.info("🔍 Search button clicked, checking for server errors or results")
                    
                    # --- Check for specific ADODB server error --- (Added)
                    try:
                        page_content = await page.content()
                        adodb_error_msg = "ADODB.Command 오류 '800a0d5d'"
                        invalid_format_msg = "응용 프로그램이 현재 작업에 대해 잘못된 형식을 가진 값을 사용하고 있습니다."
                        no_results_msg = ["0개의 상품이 검색되었습니다", "검색된 상품이 없습니다", "검색결과가 없습니다"]
                        line_294_error = "/product_w/search_keyword.asp, 줄 294"

                        # Check for server-side errors
                        if (adodb_error_msg in page_content or 
                            invalid_format_msg in page_content or 
                            line_294_error in page_content):
                            logger.warning(f"⚠️ Detected server-side ADODB error ('800a0d5d') or line 294 error for keyword: {keyword}. Skipping.")
                            await context.close()
                            
                            # If server error but we should still return an image
                            if use_default_image and default_image_path and os.path.exists(default_image_path):
                                logger.info(f"Using default image for server error: {default_image_path}")
                                return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
                            return None
                            
                        # Check for definitive "no results found" message
                        product_exists = True
                        for msg in no_results_msg:
                            if msg in page_content:
                                logger.warning(f"⚠️ Definitive 'no results' message found: '{msg}' for keyword: {keyword}")
                                product_exists = False
                                break
                                
                        # If product definitely doesn't exist, return default image or None
                        if not product_exists:
                            await context.close()
                            if use_default_image and default_image_path and os.path.exists(default_image_path):
                                logger.info(f"Using default image for non-existent product: {default_image_path}")
                                return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
                    except PlaywrightError as pe:
                        # Handle potential timeout error when getting content if page is stuck
                        logger.warning(f"⚠️ Timed out or error checking for ADODB error message: {pe}")
                        # Optionally, decide to return None here as well if content check fails
                        # await context.close()
                        # return None
                    except Exception as e:
                        logger.debug(f"Could not check page content for ADODB error: {e}")

                    # --- Existing logic: Check for "no results" --- (Modified indentation)
                    # 검색 결과가 없는 경우를 먼저 확인
                    try:
                        # 검색 결과 없음 메시지의 다양한 패턴 확인
                        no_results_selectors = [
                            'td[align="center"]:has-text("0개의 상품이 검색되었습니다")',
                            'td:has-text("검색된 상품이 없습니다")',
                            'td:has-text("검색결과가 없습니다")',
                            'td[align="center"]:has-text("0")'
                        ]

                        # *** IMPORTANT: MORE RELAXED DETECTION FOR RESULTS ***
                        # First check if ANY product images are present
                        product_image_selectors = [
                            'img[src*="/upload/product/simg3/"]',  # Main product listing image pattern
                            'img[src*="/upload/product/"]',         # Any product image
                            'form[name="ListForm"] td img[src*="/upload/"]'  # Images in product list
                        ]
                        
                        # Check if any product images exist (regardless of "no results" messages)
                        has_product_images = False
                        for selector in product_image_selectors:
                            try:
                                image_element = await page.query_selector(selector)
                                if image_element:
                                    has_product_images = True
                                    logger.info(f"Found product images with selector: {selector}")
                                    break
                            except Exception as e:
                                logger.debug(f"Error checking for product images: {e}")
                                
                        # If we have product images, assume search succeeded regardless of text messages
                        if has_product_images:
                            logger.info(f"Product images found for keyword: {keyword}, proceeding with extraction")
                        else:
                            # Use a shorter timeout for checking no results, as the ADODB error check happened first
                            no_results_found = False
                            for selector in no_results_selectors:
                                try:
                                    # Check if selector exists within a short time
                                    await page.wait_for_selector(selector, state="visible", timeout=3000) # 3 seconds
                                    no_results_found = True
                                    break
                                except PlaywrightError:
                                    continue # Selector not found, try next

                            # Also check for table with products - if present, results exist
                            try:
                                # Look for product table and rows
                                product_table = await page.query_selector('form[name="ListForm"] table')
                                if product_table:
                                    product_rows = await product_table.query_selector_all('tr')
                                    if len(product_rows) > 3:  # Usually there are header rows
                                        logger.info(f"Found product listing table with {len(product_rows)} rows")
                                        no_results_found = False  # Override - we found products
                            except Exception as e:
                                logger.debug(f"Error checking product table: {e}")
                            
                            if no_results_found and not has_product_images:
                                logger.info(f"No search results found for keyword: {keyword}")
                                await context.close()
                                return None

                    except Exception as e:
                        logger.debug(f"Error checking for no results message: {e}")

                    # --- Enhanced image URL extraction ---
                    try:
                        # Wait for the product list to load with multiple possible selectors combined
                        # Use a shorter timeout (e.g., 15 seconds) for finding images
                        IMAGE_SEARCH_TIMEOUT = 15000  # 15 seconds

                        # First: Try to find product image URLs specifically in the format the user mentioned
                        # This is the preferred format: upload/product/simg3/DDAC000xxxxs.jpg
                        haereum_product_pattern = r'src=["\'](?:[^"\']*\/upload\/product\/simg3\/[A-Z]{4}\d+s(?:_\d+)?\.(?:jpg|jpeg|png|gif))["\']'
                        haereum_fallback_pattern = r'src=["\'](?:[^"\']*\/upload\/product\/(?:simg\d?|img\d?)\/[A-Z]{4}\d+(?:s)?(?:_\d+)?\.(?:jpg|jpeg|png|gif))["\']'
                        
                        html_content = await page.content()
                        # Try primary pattern first
                        primary_matches = re.findall(haereum_product_pattern, html_content, re.IGNORECASE)
                        
                        # If found, use these preferred URLs
                        if primary_matches:
                            logger.info(f"Found {len(primary_matches)} product images in preferred format")
                            # Extract just the URL part
                            extracted_urls = []
                            for match in primary_matches:
                                # Extract URL from src="URL" format
                                url = re.sub(r'src=[\"\']([^\"\']*)[\"\']', r'\1', match)
                                extracted_urls.append(url)
                                
                            # Try downloading these URLs
                            for url in extracted_urls[:5]:  # Try up to 5 images
                                full_url = urljoin(haereum_main_url, url)
                                logger.info(f"Trying preferred format URL: {full_url}")
                                local_path = await download_image_to_main(full_url, keyword, config, max_retries=2)
                                if local_path:
                                    logger.info(f"Successfully downloaded preferred format image: {full_url}")
                                    await context.close()
                                    return {"url": full_url, "local_path": local_path, "source": "haereum"}
                            
                            # If we get here, try fallback pattern
                            fallback_matches = re.findall(haereum_fallback_pattern, html_content, re.IGNORECASE)
                            if fallback_matches:
                                logger.info(f"Found {len(fallback_matches)} product images in fallback format")
                                # Extract just the URL part
                                extracted_urls = []
                                for match in fallback_matches:
                                    # Extract URL from src="URL" format
                                    url = re.sub(r'src=[\"\']([^\"\']*)[\"\']', r'\1', match)
                                    extracted_urls.append(url)
                                    
                                # Try downloading these URLs
                                for url in extracted_urls[:5]:  # Try up to 5 images
                                    full_url = urljoin(haereum_main_url, url)
                                    logger.info(f"Trying fallback format URL: {full_url}")
                                    local_path = await download_image_to_main(full_url, keyword, config, max_retries=2)
                                    if local_path:
                                        logger.info(f"Successfully downloaded fallback format image: {full_url}")
                                        await context.close()
                                        return {"url": full_url, "local_path": local_path, "source": "haereum"}
                        
                        # If we get here, continue with original selectors as a last resort
                        combined_selector = ', '.join([
                            'img[src*="/upload/product/simg3/"]',  # Main product listing image pattern
                            'td[align="center"] > a > img[src*="/upload/product/"]',  # Product image in center-aligned cell with link
                            'form[name="ListForm"] td img[src*="/upload/"]',  # Any product image in ListForm
                            'img[src*="/upload/product/"]',  # Any product image as fallback
                            'img[src*=".jpg"], img[src*=".jpeg"], img[src*=".png"], img[src*=".gif"]'  # Explicit extension patterns
                        ])

                        # Add exclusion patterns to avoid non-product images
                        exclude_patterns = [
                            '/images/icon',
                            '/images/button',
                            'btn_',
                            'pixel.gif',
                            'spacer.gif',
                            'no_image',
                            'cart.gif',
                            'wish.gif'
                        ]

                        product_images = []
                        try:
                            # Wait for the first matching image element to appear
                            await page.wait_for_selector(combined_selector, state="visible", timeout=IMAGE_SEARCH_TIMEOUT)
                            
                            # Query all matching images
                            images = await page.query_selector_all(combined_selector)
                            
                            # Filter out non-product images
                            for img in images:
                                src = await img.get_attribute('src')
                                if src and not any(pattern in src for pattern in exclude_patterns):
                                    product_images.append(img)
                            
                            if product_images:
                                logger.info(f"📸 Found {len(product_images)} valid product images using combined selector.")
                                
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
                                    # Try to get a larger version of the image by removing the 's' suffix
                                    # which typically indicates a thumbnail in Haereum
                                    orig_img_src = img_src
                                    
                                    # Check if it's a thumbnail (s.jpg, s.png, etc.)
                                    if 's.' in img_src.lower() or 's(' in img_src.lower():
                                        # Try both with and without the 's' to handle different naming patterns
                                        img_pattern = img_src.lower()
                                        # Try to keep the original for backup
                                        larger_versions = []
                                        
                                        # Try 1: Simply replace s. with . (most common pattern)
                                        larger_versions.append(img_src.replace('s.', '.'))
                                        
                                        # Try 2: Replace 's(' with '(' (for versioned images)
                                        larger_versions.append(img_src.replace('s(', '('))
                                        
                                        # Try 3: Remove 's' before file extension (other pattern)
                                        parts = img_src.rsplit('s.', 1)
                                        if len(parts) == 2:
                                            larger_versions.append(f"{parts[0]}.{parts[1]}")
                                        
                                        # Try these versions and use the first one that works
                                        logger.info(f"Generated {len(larger_versions)} potential larger versions of thumbnail")
                                        
                                        # Keep the original as a fallback if larger versions fail
                                        # We'll test them during download
                                        img_src_variants = [img_src] + larger_versions
                                    else:
                                        img_src_variants = [img_src]
                                    
                                    # Construct full URL if needed - prepare all variants
                                    found_image_urls = []
                                    for variant in img_src_variants:
                                        if not variant.startswith(('http://', 'https://')):
                                            # Make sure image_src starts with / for proper joining
                                            if not variant.startswith('/'):
                                                variant = '/' + variant
                                            full_url = urljoin(haereum_main_url, variant)
                                            found_image_urls.append(full_url)
                                            logger.info(f"✅ Converted relative URL '{variant}' to absolute URL: {full_url}")
                                        else:
                                            found_image_urls.append(variant)
                                            logger.info(f"✅ Using absolute image URL: {variant}")
                                    
                                    # Try downloading each variant until one succeeds
                                    local_path = None
                                    for url in found_image_urls:
                                        logger.info(f"Attempting to download image: {url}")
                                        local_path = await download_image_to_main(url, keyword, config, max_retries=2)
                                        if local_path:
                                            logger.info(f"Successfully downloaded variant: {url}")
                                            break
                                    
                                    # Close context before returning result
                                    await context.close()

                                    # Return the best result we could find
                                    found_url = found_image_urls[0] if found_image_urls else None
                                    if local_path:
                                        return {"url": found_url, "local_path": local_path, "source": "haereum"}
                            else:
                                logger.warning("⚠️ No valid product images found after filtering.")
                                
                            # If we get here, either no product images were found or download failed
                            # Try secondary method - direct HTML search
                            html_content = await page.content()
                            # Improved regex to match more image types and patterns
                            img_pattern = r'<img[^>]+src=["\']([^"\']*upload\/product[^"\']*\.(jpe?g|png|gif|webp))["\']'
                            all_matches = re.findall(img_pattern, html_content)
                            if all_matches:
                                logger.info(f"Found {len(all_matches)} potential images via HTML search")
                                
                                # Process extracted URLs
                                html_extracted_urls = []
                                for match in all_matches:
                                    if isinstance(match, tuple) and len(match) > 0:
                                        img_url = match[0]  # Get full URL (first capture group)
                                    else:
                                        img_url = match
                                        
                                    if not any(p in img_url for p in exclude_patterns):
                                        html_extracted_urls.append(img_url)
                                
                                if html_extracted_urls:
                                    logger.info(f"Found {len(html_extracted_urls)} valid images via HTML search")
                                    # Try downloading the first one
                                    for url in html_extracted_urls[:3]:  # Try top 3 images
                                        full_url = urljoin(haereum_main_url, url)
                                        logger.info(f"Trying HTML extracted URL: {full_url}")
                                        local_path = await download_image_to_main(full_url, keyword, config, max_retries=2)
                                        if local_path:
                                            logger.info(f"Successfully downloaded HTML extracted image: {full_url}")
                                            await context.close()
                                            return {"url": full_url, "local_path": local_path, "source": "haereum"}
                            
                            # If we still couldn't find or download any image, try the direct product code fallback
                            fallback_result = await try_direct_product_code_fallback(page, keyword, config, haereum_main_url)
                            if fallback_result:
                                logger.info(f"✅ Last resort fallback method successfully found image for '{keyword}'")
                                await context.close()
                                return fallback_result
                                    
                            # If all methods failed, return None
                            await context.close()
                            return None

                        except PlaywrightError as pe:
                            # Handle timeout or other selector errors
                            if "Timeout" in str(pe):
                                logger.warning(f"⚠️ No product images found within {IMAGE_SEARCH_TIMEOUT / 1000} seconds using combined selector.")
                                
                                # Try the specialized fallback method that extracts product codes
                                fallback_result = await try_direct_product_code_fallback(page, keyword, config, haereum_main_url)
                                if fallback_result:
                                    logger.info(f"✅ Fallback method successfully found image for '{keyword}'")
                                    await context.close()
                                    return fallback_result
                                
                                # If fallback method also failed, proceed with existing fallback logic
                                try:
                                    # Get all product codes on the page
                                    product_codes = await page.query_selector_all('.pro_code b')
                                    if product_codes:
                                        first_code = await product_codes[0].inner_text()
                                        first_code = first_code.strip()
                                        logger.info(f"Found product code: {first_code}")
                                        
                                        # Try to construct image URL from product code
                                        # Common patterns in Haereum site
                                        possible_image_codes = []
                                        
                                        # Get product names to help generate possible image codes
                                        product_names = await page.query_selector_all('td[align="center"] a.hit_pro')
                                        product_name_text = ""
                                        if product_names:
                                            product_name_text = await product_names[0].inner_text()
                                            product_name_text = product_name_text.strip()
                                            logger.info(f"Found product name: {product_name_text}")
                                        
                                        # Try standard catalog number pattern
                                        if len(first_code) > 3:
                                            # Use the product code directly
                                            possible_image_codes.append(f"{'BBCA'}{first_code.zfill(7)}")
                                            
                                            # Try with different catalog prefixes
                                            for prefix in ["BBCA", "GGBJ", "AAZZ", "CCAA"]:
                                                possible_image_codes.append(f"{prefix}{first_code.zfill(7)}")
                                        
                                        # Try each possible image code
                                        for code in possible_image_codes:
                                            # Priority to the exact format the user mentioned
                                            # Primary format: /upload/product/simg3/DDACxxxxxxs.jpg
                                            for brand_code in ["DDAC", "BBCA", "GGBJ", "AAZZ", "CCAA", "EEBB", "BBCB", "EEAV"]:
                                                # Try both with and without the 's' suffix
                                                for size_suffix in ['s', '']:
                                                    # Try with different version numbers
                                                    for version in ['', '_1', '_2', '_3']:
                                                        # Try different file extensions
                                                        for img_ext in ['.jpg', '.jpeg', '.png', '.gif']:
                                                            test_img_url = f"/upload/product/simg3/{brand_code}{code.zfill(7)}{size_suffix}{version}{img_ext}"
                                                            logger.info(f"Trying exact format URL: {test_img_url}")
                                                            
                                                            # Construct the final URL
                                                            full_url = urljoin(haereum_main_url, test_img_url)
                                                            
                                                            # Download the image and check if it exists
                                                            local_path = await download_image_to_main(full_url, keyword, config, max_retries=2)
                                                            
                                                            if local_path:
                                                                await context.close()
                                                                return {"url": full_url, "local_path": local_path, "source": "haereum"}
                                            
                                            # If exact format didn't work, try original fallback paths
                                            test_img_url = f"/upload/product/simg3/{code}s.jpg"
                                            logger.info(f"Trying fallback image URL: {test_img_url}")
                                            
                                            # Set this as our image source and proceed with the normal flow
                                            img_src = test_img_url
                                            
                                            # Try to get a larger version
                                            larger_img_src = img_src.replace('s.jpg', '.jpg')
                                            logger.info(f"Attempting to use larger version: {larger_img_src}")
                                            
                                            # Construct the final URL
                                            if not larger_img_src.startswith(('http://', 'https://')):
                                                if not larger_img_src.startswith('/'):
                                                    larger_img_src = '/' + larger_img_src
                                                found_image_url = urljoin(haereum_main_url, larger_img_src)
                                            else:
                                                found_image_url = larger_img_src
                                                
                                            logger.info(f"✅ Using fallback image URL: {found_image_url}")
                                            
                                            # Download the image and check if it exists
                                            local_path = await download_image_to_main(found_image_url, keyword, config, max_retries=3)
                                            
                                            if local_path:
                                                await context.close()
                                                return {"url": found_image_url, "local_path": local_path, "source": "haereum"}
                                            
                                            # If we get here, the fallback URL didn't work, try the next one
                                    
                                except Exception as fallback_err:
                                    logger.error(f"Error in fallback image detection: {fallback_err}")
                            else:
                                logger.warning(f"⚠️ Error waiting for image selector: {str(pe)}")
                            # No need to continue if no images found or error occurred
                            await context.close() # Close context before returning
                            return None

                    except Exception as e:
                        logger.error(f"❌ Error during image URL extraction: {e}", exc_info=True)
                        # Ensure context is closed in case of error
                        try:
                            if 'context' in locals() and not context.is_closed():
                                await context.close()
                        except Exception as ce:
                             logger.warning(f"⚠️ Error closing context during exception handling: {ce}")
                        return None
                        
                except PlaywrightError as pe:
                    logger.error(f"❌ Playwright error during Haereum scrape: {pe}")
                    last_error = pe
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        delay = RETRY_DELAY * (RETRY_BACKOFF_FACTOR ** retry_count)
                        logger.info(f"Retrying in {delay} seconds... (Attempt {retry_count + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(delay)
                        continue
                    raise
                except Exception as e:
                    logger.error(f"❌ Unexpected error during Haereum scrape: {e}", exc_info=True)
                    last_error = e
                    retry_count += 1
                    if retry_count < MAX_RETRIES:
                        delay = RETRY_DELAY * (RETRY_BACKOFF_FACTOR ** retry_count)
                        logger.info(f"Retrying in {delay} seconds... (Attempt {retry_count + 1}/{MAX_RETRIES})")
                        await asyncio.sleep(delay)
                        continue
                    raise
                finally:
                    # Ensure proper cleanup
                    try:
                        if 'page' in locals():
                            await page.close()
                        if 'context' in locals():
                            await context.close()
                    except Exception as e:
                        logger.warning(f"⚠️ Error during cleanup: {e}")

        except Exception as e:
            last_error = e
            retry_count += 1
            if retry_count < MAX_RETRIES:
                delay = RETRY_DELAY * (RETRY_BACKOFF_FACTOR ** retry_count)
                logger.info(f"Retrying in {delay} seconds... (Attempt {retry_count + 1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                continue
            logger.error(f"Failed after {MAX_RETRIES} attempts. Last error: {str(last_error)}")
            
            # If all attempts failed but we should return something, use default image
            if use_default_image and default_image_path and os.path.exists(default_image_path):
                logger.info(f"Using default image after all retries failed: {default_image_path}")
                return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
            
            return None

    return None

async def download_image_to_main(image_url: str, product_name: str, config: configparser.ConfigParser, max_retries: int = 3) -> Optional[str]:
    """Download an image to the main folder with target information."""
    if not image_url or not image_url.strip():
        logger.warning("Empty image URL provided to download_image_to_main")
        return None
        
    # Get main folder path from config
    try:
        main_dir = config.get('Paths', 'image_main_dir', fallback=None)
        if not main_dir:
            # Use fallback path
            main_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'images', 'Main')
            logger.warning(f"image_main_dir not specified in config, using fallback: {main_dir}")
            
        # Create Haereum-specific subdirectory
        main_dir = os.path.join(main_dir, 'Haereum')
        
        # Create directory if it doesn't exist
        os.makedirs(main_dir, exist_ok=True)
        
        # Verify directory is writable
        if not os.access(main_dir, os.W_OK):
            logger.error(f"Image directory is not writable: {main_dir}")
            return None
            
        # Check for use_background_removal setting
        use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
    except Exception as e:
        logger.error(f"Error accessing config or creating image directory: {e}")
        return None
    
    # Generate a safe filename
    try:
        # Use product name to generate a safe filename
        safe_name = re.sub(r'[^\w\-_.]', '_', product_name)[:50]  # Limit length
        url_hash = hashlib.md5(image_url.encode()).hexdigest()[:10]
        
        # Get file extension from URL
        parsed_url = urlparse(image_url)
        _, ext = os.path.splitext(parsed_url.path)
        ext = ext.lower() or ".jpg"  # Default to .jpg if no extension
        
        # Check for invalid extensions
        if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            logger.warning(f"Suspicious file extension: {ext}, defaulting to .jpg")
            ext = '.jpg'
            
        # Save original extension for potential conversion
        original_ext = ext
        
        # Force JPG extension for downloaded image (will convert later if needed)
        if ext != '.jpg' and ext != '.jpeg':
            logger.info(f"Will convert {ext} image to JPG format during download")
            ext = '.jpg'
            
        # Create final filename
        filename = f"haereum_{safe_name}_{url_hash}{ext}"
        local_path = os.path.join(main_dir, filename)
        final_image_path = local_path
        
        # Generate unique temporary filename
        temp_path = f"{local_path}.{time.time_ns()}.tmp"
        
        # Check if file already exists
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.info(f"Image already exists: {local_path}")
            
            # Check for existing background-removed version
            if use_bg_removal:
                nobg_path = local_path.replace('.jpg', '_nobg.png', 1)  # Ensure background-removed version is PNG
                if os.path.exists(nobg_path) and os.path.getsize(nobg_path) > 0:
                    logger.debug(f"Using existing background-removed image: {nobg_path}")
                    final_image_path = nobg_path
                else:
                    # Try to remove background if no-bg version doesn't exist
                    try:
                        from image_utils import remove_background
                        if remove_background(local_path, nobg_path):
                            logger.debug(f"Background removed for existing Haereum image: {nobg_path}")
                            final_image_path = nobg_path
                        else:
                            logger.warning(f"Failed to remove background for Haereum image {local_path}. Using original.")
                    except Exception as bg_err:
                        logger.warning(f"Error during background removal: {bg_err}. Using original image.")
            
            return final_image_path
            
    except Exception as e:
        logger.error(f"Error generating filename: {e}")
        return None
    
    # Download the image with concurrency control
    try:
        async with file_semaphore:  # Use global semaphore for file operations
            async with aiohttp.ClientSession() as session:
                # Try different URL variants if the original fails
                url_variants = [image_url]
                
                # If URL has 's.' pattern, try without it for all image types
                if 's.' in image_url.lower():
                    # Try both with and without the 's' suffix for all common extensions
                    url_variants.append(image_url.replace('s.', '.'))
                    
                    # Try different extensions if pattern suggests it's a thumbnail
                    orig_url_no_ext = image_url.rsplit('.', 1)[0].replace('s.', '.').replace('s_', '_')
                    for test_ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                        if not image_url.lower().endswith(test_ext):
                            url_variants.append(f"{orig_url_no_ext}{test_ext}")
                
                # Try all variants
                download_success = False
                
                for url in url_variants:
                    for attempt in range(max_retries):
                        try:
                            # Add timeout and custom headers
                            headers = {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                                'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                                'Referer': 'https://www.jclgift.com/',
                                'Connection': 'keep-alive'
                            }
                            
                            # Try with different URL formats
                            current_url = url
                            if url.startswith('/'):
                                current_url = 'https://www.jclgift.com' + url
                            elif not url.startswith(('http://', 'https://')):
                                current_url = 'https://www.jclgift.com/' + url
                            
                            logger.info(f"Downloading from: {current_url} (attempt {attempt+1}/{max_retries})")
                            
                            async with session.get(current_url, timeout=30, headers=headers, ssl=False) as response:
                                if response.status != 200:
                                    logger.warning(f"HTTP error {response.status} downloading image (attempt {attempt+1}/{max_retries}): {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break  # Try next URL variant
                                    
                                # Check content type
                                content_type = response.headers.get('Content-Type', '')
                                if not content_type.startswith('image/'):
                                    logger.warning(f"Non-image content type: {content_type} for URL: {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break  # Try next URL variant
                                    
                                # Download image data
                                data = await response.read()
                                if len(data) < 100:  # Too small to be a valid image
                                    logger.warning(f"Downloaded image too small: {len(data)} bytes from URL: {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break  # Try next URL variant
                                    
                                # Save to temporary file
                                try:
                                    async with aiofiles.open(temp_path, 'wb') as f:
                                        await f.write(data)
                                    
                                    # Wait for file handle to be fully closed
                                    await asyncio.sleep(0.1)
                                    
                                    # Validate and potentially convert image
                                    try:
                                        with Image.open(temp_path) as img:
                                            img.verify()
                                        
                                        # Re-open for conversion if needed
                                        need_conversion = original_ext.lower() in ['.png', '.gif', '.webp', '.bmp']
                                        if need_conversion:
                                            logger.info(f"Converting {original_ext} image to JPG format")
                                            with Image.open(temp_path) as img:
                                                # For PNG with transparency, fill with white background
                                                if original_ext.lower() == '.png' and img.mode == 'RGBA':
                                                    # Create a white background
                                                    background = Image.new('RGB', img.size, (255, 255, 255))
                                                    # Paste the image on the background, using alpha channel as mask
                                                    background.paste(img, (0, 0), img)
                                                    # Save as JPG
                                                    background.save(temp_path, 'JPEG', quality=95)
                                                    logger.info(f"Converted transparent PNG to JPG with white background")
                                                else:
                                                    # Convert to RGB and save as JPG
                                                    rgb_img = img.convert('RGB')
                                                    rgb_img.save(temp_path, 'JPEG', quality=95)
                                                    logger.info(f"Converted {original_ext} to JPG")
                                        
                                        # Remove existing file if it exists
                                        if os.path.exists(local_path):
                                            try:
                                                os.remove(local_path)
                                                await asyncio.sleep(0.1)  # Wait for file system
                                            except OSError as e:
                                                logger.error(f"Could not remove existing file: {e}")
                                                if os.path.exists(temp_path):
                                                    os.remove(temp_path)
                                                continue  # Try next attempt or URL
                                        
                                        # Move temp file to final location
                                        os.rename(temp_path, local_path)
                                        logger.info(f"Successfully downloaded image: {local_path}")
                                        download_success = True
                                        
                                        # Remove background if requested
                                        if use_bg_removal:
                                            try:
                                                from image_utils import remove_background
                                                nobg_path = local_path.replace('.jpg', '_nobg.png', 1)  # Make background-removed version PNG
                                                if remove_background(local_path, nobg_path):
                                                    logger.info(f"Background removed for Haereum image: {nobg_path}")
                                                    final_image_path = nobg_path
                                                else:
                                                    logger.warning(f"Failed to remove background for Haereum image {local_path}. Using original.")
                                            except Exception as bg_err:
                                                logger.warning(f"Error during background removal: {bg_err}. Using original image.")
                                        
                                        return final_image_path
                                    except Exception as img_err:
                                        logger.error(f"Downloaded file is not a valid image: {img_err} from URL: {current_url}")
                                        if os.path.exists(temp_path):
                                            os.remove(temp_path)
                                        if attempt < max_retries - 1:
                                            await asyncio.sleep(1 * (attempt + 1))
                                            continue
                                        break  # Try next URL variant
                                except Exception as f_err:
                                    logger.error(f"Error saving or validating image: {f_err}")
                                    if os.path.exists(temp_path):
                                        try:
                                            os.remove(temp_path)
                                        except:
                                            pass
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break  # Try next URL variant
                        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                            logger.warning(f"Network error downloading image (attempt {attempt+1}/{max_retries}): {e}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(1 * (attempt + 1))
                                continue
                            break  # Try next URL variant
                    
                    # If download was successful with this URL variant, we're done
                    if download_success:
                        break
                
                # If we got here and download_success is still False, all attempts failed
                if not download_success:
                    logger.error(f"All download attempts failed for image: {image_url}")
                    return None
                    
    except Exception as e:
        logger.error(f"Unexpected error downloading image: {e}")
        return None
        
    # Safety check - this should never be reached but just in case
    if download_success:
        return final_image_path
    return None

async def try_direct_product_code_fallback(page: Page, keyword: str, config: configparser.ConfigParser, haereum_main_url: str) -> Optional[Dict[str, str]]:
    """Try to extract product codes and construct image URLs directly from them."""
    logger.info(f"Trying direct product code fallback for keyword: {keyword}")
    
    # Check if search results exist on the page
    html_content = await page.content()
    no_results_messages = ["0개의 상품이 검색되었습니다", "검색된 상품이 없습니다", "검색결과가 없습니다"]
    
    for msg in no_results_messages:
        if msg in html_content:
            logger.warning(f"No results found message detected: '{msg}' for keyword: {keyword}")
            
            # Check if we should use default image
            use_default_image = config.getboolean('Matching', 'use_default_image_when_not_found', fallback=True)
            default_image_path = config.get('Paths', 'default_image_path', fallback=None)
            
            if use_default_image and default_image_path and os.path.exists(default_image_path):
                logger.info(f"Using default image for confirmed non-existent product: {default_image_path}")
                return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
            return None
    
    try:
        # First try to get product codes from the page using table structure
        product_codes = await page.query_selector_all('.pro_code b')
        
        # If no product codes found, try to search the HTML directly
        if not product_codes or len(product_codes) == 0:
            logger.info("No product codes found in selector, trying HTML extraction")
            
            # Get the page HTML and search for product codes
            html_content = await page.content()
            
            # Look for product code pattern
            code_pattern = r'class="pro_code">.*?<b>(\d+)</b>'
            code_matches = re.findall(code_pattern, html_content, re.DOTALL | re.IGNORECASE)
            
            if code_matches:
                logger.info(f"Found {len(code_matches)} product codes in HTML")
                product_codes_list = code_matches
            else:
                # Try a different approach - look for catalog numbers directly in image URLs
                catalog_pattern = r'\/upload\/product\/\w+\/([A-Z]{4}\d+)s'
                catalog_matches = re.findall(catalog_pattern, html_content, re.IGNORECASE)
                
                if catalog_matches:
                    logger.info(f"Found {len(catalog_matches)} catalog codes directly in image URLs")
                    # Use the catalog codes instead
                    product_codes_list = catalog_matches
                else:
                    logger.warning("No product codes found in HTML")
                    return None
        else:
            product_codes_list = []
            for code_elem in product_codes:
                code_text = await code_elem.inner_text()
                product_codes_list.append(code_text.strip())
            
        logger.info(f"Extracted product codes: {product_codes_list}")
        
        # Get product names if available
        product_names = await page.query_selector_all('td[align="center"] a.hit_pro')
        product_names_list = []
        
        if product_names:
            for name_elem in product_names:
                name_text = await name_elem.inner_text()
                product_names_list.append(name_text.strip())
            logger.info(f"Found product names: {product_names_list}")
            
        # Try to find product images by inspecting HTML directly
        html_content = await page.content()
        
        # Extract all image URLs with product patterns - IMPROVED REGEX to capture more image types
        img_pattern = r'<img[^>]+src=["\']([^"\']*upload\/product[^"\']*\.(jpe?g|png|gif|webp))["\']'
        all_matches = re.findall(img_pattern, html_content)
        logger.info(f"Found {len(all_matches)} potential product images in HTML")
        
        # Filter out non-product images - process tuple results from new regex
        product_imgs = []
        for match in all_matches:
            if isinstance(match, tuple) and len(match) > 0:
                img_url = match[0]  # Get the full URL (first capture group)
            else:
                img_url = match
                
            if not any(p in img_url for p in ['icon', 'button', 'btn_', 'pixel.gif', 'spacer.gif', 'no_image']):
                product_imgs.append(img_url)
                
        logger.info(f"After filtering, {len(product_imgs)} valid product images remain")
        
        if product_imgs:
            # Try each image URL
            for img_url in product_imgs:
                full_url = urljoin(haereum_main_url, img_url)
                logger.info(f"Trying HTML extracted image URL: {full_url}")
                
                # Try different variants of the URL
                url_variants = [full_url]
                if 's.' in full_url:
                    url_variants.append(full_url.replace('s.', '.'))
                
                # Try downloading each variant
                for variant in url_variants:
                    local_path = await download_image_to_main(variant, keyword, config, max_retries=2)
                    if local_path:
                        logger.info(f"Successfully downloaded direct HTML extracted image: {variant}")
                        return {"url": variant, "local_path": local_path, "source": "haereum"}
        
        # If we still don't have an image, try to construct URLs from product codes
        for code in product_codes_list:
            # Try different prefixes used by Haereum
            for prefix in ["BBCA", "GGBJ", "AAZZ", "CCAA", "EEBB", "BBCB", "EEAV"]:
                # Construct the image URL
                padded_code = code.zfill(7)  # Pad to 7 digits
                test_codes = [
                    f"{prefix}{padded_code}",  # Standard format
                    f"{prefix}{code}"          # Unpadded format
                ]
                
                for test_code in test_codes:
                    # Try directory variations
                    for img_dir in ['simg3', 'simg1', 'simg2', 'simg', 'img3', 'img']:
                        # CRITICAL CHANGE: Try file extensions in different order - put GIF last
                        # since it was the only one working, jpg and png first
                        for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                            # Try both small and large versions
                            for size_suffix in ['s', '']:
                                # Try both with and without version numbers
                                for version in ['', '_1', '_2']:
                                    test_url = f"/upload/product/{img_dir}/{test_code}{size_suffix}{version}{ext}"
                                    full_url = urljoin(haereum_main_url, test_url)
                                    logger.info(f"Trying constructed URL: {full_url}")
                                    
                                    local_path = await download_image_to_main(full_url, keyword, config, max_retries=1)
                                    if local_path:
                                        logger.info(f"Successfully downloaded constructed URL: {full_url}")
                                        return {"url": full_url, "local_path": local_path, "source": "haereum"}
        
        logger.warning("All fallback methods failed to find a valid image")
        
        # Use default image if configured
        use_default_image = config.getboolean('Matching', 'use_default_image_when_not_found', fallback=True)
        default_image_path = config.get('Paths', 'default_image_path', fallback=None)
        
        if use_default_image and default_image_path and os.path.exists(default_image_path):
            logger.info(f"Using default image after all fallback methods failed: {default_image_path}")
            return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
        return None
                                
    except Exception as e:
        logger.error(f"Error in direct product code fallback: {e}")
        
        # Use default image even on exception if configured
        use_default_image = config.getboolean('Matching', 'use_default_image_when_not_found', fallback=True)
        default_image_path = config.get('Paths', 'default_image_path', fallback=None)
        
        if use_default_image and default_image_path and os.path.exists(default_image_path):
            logger.info(f"Using default image after fallback exception: {default_image_path}")
            return {"url": "default", "local_path": default_image_path, "source": "haereum_default"}
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
    
    # Set up a default image for testing
    if not config.has_section('Paths'):
        config.add_section('Paths')
    
    # Create a default image path if not already set
    if not config.has_option('Paths', 'default_image_path'):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        default_img_dir = os.path.join(script_dir, '..', 'images', 'defaults')
        os.makedirs(default_img_dir, exist_ok=True)
        
        # Check if a default image exists
        default_img_path = os.path.join(default_img_dir, 'haereum_default.jpg')
        if not os.path.exists(default_img_path):
            # Create a simple default image (black 100x100 square)
            try:
                from PIL import Image
                img = Image.new('RGB', (100, 100), color = 'black')
                img.save(default_img_path)
                logger.info(f"Created default test image at: {default_img_path}")
            except Exception as e:
                logger.error(f"Could not create default image: {e}")
                default_img_path = None
                
        # Set the default image path in config
        if default_img_path:
            config.set('Paths', 'default_image_path', default_img_path)
            logger.info(f"Set default_image_path to: {default_img_path}")
    
    # Enable default image usage
    if not config.has_section('Matching'):
        config.add_section('Matching')
    config.set('Matching', 'use_default_image_when_not_found', 'True')
    
    # Make sure we use the current config.ini settings for browser launching
    if not config.has_section('Playwright'):
        config.add_section('Playwright')
    
    # Set headless mode for testing - use value from config if exists
    if not config.has_option('Playwright', 'playwright_headless'):
        config.set('Playwright', 'playwright_headless', 'true')  # Default to headless for tests
        
    # Set conservative browser limits to avoid resource issues
    if not config.has_option('Playwright', 'playwright_max_concurrent_windows'):
        config.set('Playwright', 'playwright_max_concurrent_windows', '2')  # Limit concurrent windows
        
    # Updated test keywords based on user's request
    test_keywords = [
        "타포린백 중형 35-30-15cm",
        "더카인드 와인오프너",
        "이지링 휴대용",
        "메모패드",
        "텀블러"
    ]
    
    logger.info(f"--- Running Parallel Test for Haereum Gift with {len(test_keywords)} keywords ---")
    
    async with async_playwright() as p:
        browser = None
        try:
            # Get browser arguments from config if available
            browser_args = []
            try:
                browser_args_str = config.get('Playwright', 'playwright_browser_args', fallback='[]')
                import json
                browser_args = json.loads(browser_args_str)
            except Exception:
                browser_args = ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
                
            headless_mode = config.getboolean('Playwright', 'playwright_headless', fallback=True)
            max_windows = config.getint('Playwright', 'playwright_max_concurrent_windows', fallback=2)
            
            logger.info(f"Launching browser (headless: {headless_mode}, max_windows: {max_windows})")
            browser = await p.chromium.launch(
                headless=headless_mode,
                args=browser_args,
                timeout=60000  # 1 minute timeout for browser launch
            )
        except Exception as browser_err:
            logger.error(f"Failed to launch browser: {browser_err}")
            return
             
        start_time = time.time()
        
        try:
            # Create tasks for parallel execution with semaphore and chunking
            scraping_semaphore = asyncio.Semaphore(max_windows)
            
            # Split keywords into smaller batches to avoid overloading the browser
            batch_size = 2  # Process 2 keywords at a time maximum
            results = []
            
            for batch_start in range(0, len(test_keywords), batch_size):
                batch_end = min(batch_start + batch_size, len(test_keywords))
                batch = test_keywords[batch_start:batch_end]
                
                logger.info(f"Processing batch of {len(batch)} keywords ({batch_start+1}-{batch_end} of {len(test_keywords)})")
                
                # Create tasks for this batch
                batch_tasks = []
                for keyword in batch:
                    async def scrape_with_semaphore(kw):
                        async with scraping_semaphore:
                            return (kw, await scrape_haereum_data(browser, kw, config))
                    task = asyncio.create_task(scrape_with_semaphore(keyword))
                    batch_tasks.append(task)
                
                # Wait for this batch to complete
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                results.extend(batch_results)
                
                # Add a small delay between batches to avoid resource spikes
                if batch_end < len(test_keywords):
                    logger.info(f"Batch complete. Waiting before starting next batch...")
                    await asyncio.sleep(3)
            
            # Process and display results
            print("\n--- Parallel Scraping Test Results ---")
            success_count = 0
            error_count = 0
            
            for result in results:
                if isinstance(result, Exception):
                    error_count += 1
                    print(f"❌ Error: {str(result)}")
                elif isinstance(result, tuple) and len(result) == 2:
                    keyword, data = result
                    if isinstance(data, Exception):
                        error_count += 1
                        print(f"❌ Error for '{keyword}': {str(data)}")
                    elif data and data.get("url"):
                        success_count += 1
                        print(f"✅ Success for '{keyword}':")
                        print(f"  - Image URL: {data.get('url')}")
                        print(f"  - Local path: {data.get('local_path')}")
                        print(f"  - Source: {data.get('source')}")
                    else:
                        print(f"❌ No results found for '{keyword}'")
                else:
                    print(f"❌ Unexpected result format: {result}")
                print("---------------------------")
            
            print(f"Summary: {success_count} successes, {error_count} errors out of {len(test_keywords)} keywords")
                
        except Exception as e:
            logger.error(f"Error during test: {e}")
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception as close_err:
                    logger.warning(f"Error closing browser: {close_err}")
            
        end_time = time.time()
        logger.info(f"Parallel scraping took {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    # To run this test: python PythonScript/crawling_haereum_standalone.py
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    logger.info("Running Haereum parallel test...")
    asyncio.run(_test_main()) 