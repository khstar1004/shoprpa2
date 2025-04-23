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
BROWSER_CONTEXT_TIMEOUT = 600000  # 10 minutes
PAGE_TIMEOUT = 300000  # 5 minutes
NAVIGATION_TIMEOUT = 120000  # 2 minutes
WAIT_TIMEOUT = 30000  # 30 seconds

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
    max_windows = config.getint('Playwright', 'playwright_max_concurrent_windows', fallback=3)
    scraping_semaphore = asyncio.Semaphore(max_windows)  # Use config value for max concurrent windows
    
    retry_count = 0
    last_error = None
    
    while retry_count < MAX_RETRIES:
        try:
            async with scraping_semaphore:  # Acquire semaphore before starting
                if config is None:
                    logger.error("🔴 Configuration object (ConfigParser) is missing for Haereum scrape.")
                    return None

                try:
                    haereum_main_url = config.get('ScraperSettings', 'haereum_main_url', fallback="https://www.jclgift.com/")
                    haereum_image_base_url = config.get('ScraperSettings', 'haereum_image_base_url', fallback="http://i.jclgift.com/")
                    user_agent = config.get('ScraperSettings', 'user_agent', fallback="Mozilla/5.0 ...")
                    
                    # Create a new context with proper settings
                    context = await browser.new_context(
                        user_agent=user_agent,
                        viewport={'width': 1920, 'height': 1080}
                    )
                    
                    # Create a new page with increased timeouts
                    page = await context.new_page()
                    page.set_default_timeout(PAGE_TIMEOUT)
                    page.set_default_navigation_timeout(NAVIGATION_TIMEOUT)

                    if config.getboolean('Playwright', 'playwright_block_resources', fallback=True):
                        await setup_page_optimizations(page)

                    logger.debug(f"🌐 Navigating to {haereum_main_url}")
                    await page.goto(haereum_main_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
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
                            await search_input.wait_for(state="visible", timeout=WAIT_TIMEOUT)
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
                    while time.time() - start_time < WAIT_TIMEOUT / 1000:  # Convert ms to seconds
                        if await search_input.is_enabled():
                            break
                        await page.wait_for_timeout(100)  # Check every 100ms
                    
                    # Fill the search input with timeout
                    await search_input.fill(keyword, timeout=WAIT_TIMEOUT)
                    logger.debug(f"⌨️ Filled search input with keyword: {keyword}")

                    # Wait for the search button to be present and visible
                    search_button = page.locator('input[type="image"][src*="b_search.gif"]')
                    await search_button.wait_for(state="visible", timeout=WAIT_TIMEOUT)
                    
                    # Wait for the button to be enabled with timeout
                    start_time = time.time()
                    while time.time() - start_time < WAIT_TIMEOUT / 1000:  # Convert ms to seconds
                        if await search_button.is_enabled():
                            break
                        await page.wait_for_timeout(100)  # Check every 100ms
                    
                    # Click the search button and wait for navigation
                    await search_button.click(timeout=WAIT_TIMEOUT)
                    await page.wait_for_timeout(5000)
                    logger.info("🔍 Search button clicked, waiting for results")

                    # 검색 결과가 없는 경우를 먼저 확인
                    try:
                        # 검색 결과 없음 메시지의 다양한 패턴 확인
                        no_results_selectors = [
                            'td[align="center"]:has-text("0개의 상품이 검색되었습니다")',
                            'td:has-text("검색된 상품이 없습니다")',
                            'td:has-text("검색결과가 없습니다")',
                            'td[align="center"]:has-text("0")'
                        ]
                        
                        for selector in no_results_selectors:
                            no_results = await page.query_selector(selector)
                            if no_results:
                                logger.info(f"No search results found for keyword: {keyword}")
                                return None
                                
                        # 검색 결과 수가 0인지 확인
                        try:
                            results_count_text = await page.evaluate('document.body.innerText')
                            if '0개의 상품' in results_count_text or '검색된 상품이 없습니다' in results_count_text:
                                logger.info(f"No search results found for keyword: {keyword}")
                                return None
                        except Exception as e:
                            logger.debug(f"Error checking results count text: {e}")
                            
                    except Exception as e:
                        logger.debug(f"Error checking for no results message: {e}")

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
                                await page.wait_for_selector(selector, timeout=WAIT_TIMEOUT)
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
                            local_path = await download_image_to_main(found_image_url, keyword, config, max_retries=3)
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
            return None

    return None

async def download_image_to_main(image_url: str, product_name: str, config: configparser.ConfigParser, max_retries: int = 3) -> Optional[str]:
    """Download an image to the main folder with target information."""
    if not image_url:
        logger.warning("Empty image URL provided to download_image_to_main")
        return None
        
    # Get the main directory from config 
    try:
        main_dir = config.get('Paths', 'image_main_dir')
        if not main_dir:
            logger.error("image_main_dir not found in config")
            return None
        
        # Ensure the directory exists
        os.makedirs(main_dir, exist_ok=True)
        if not os.access(main_dir, os.W_OK):
            logger.error(f"No write permission for directory: {main_dir}")
            return None
            
    except Exception as e:
        logger.error(f"Error accessing image directory: {e}")
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
            
        # Create final filename
        filename = f"haereum_{safe_name}_{url_hash}{ext}"
        local_path = os.path.normpath(os.path.join(main_dir, filename))
        
        # Generate unique temporary filename
        temp_path = f"{local_path}.{time.time_ns()}.tmp"
        
        # Check if file already exists
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.info(f"Image already exists, skipping download: {local_path}")
            return local_path
            
    except Exception as e:
        logger.error(f"Error generating filename: {e}")
        return None
    
    # Download the image with concurrency control
    try:
        async with file_semaphore:  # Use global semaphore for file operations
            async with aiohttp.ClientSession() as session:
                for attempt in range(max_retries):
                    try:
                        async with session.get(image_url, timeout=30) as response:
                            if response.status != 200:
                                logger.warning(f"HTTP error {response.status} downloading image (attempt {attempt+1}/{max_retries}): {image_url}")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(1 * (attempt + 1))
                                    continue
                                return None
                                
                            # Check content type
                            content_type = response.headers.get('Content-Type', '')
                            if not content_type.startswith('image/'):
                                logger.warning(f"Non-image content type: {content_type}")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(1 * (attempt + 1))
                                    continue
                                return None
                                
                            # Download image data
                            data = await response.read()
                            if len(data) < 100:  # Too small to be a valid image
                                logger.warning(f"Downloaded image too small: {len(data)} bytes")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(1 * (attempt + 1))
                                    continue
                                return None
                                
                            # Save to temporary file
                            try:
                                async with aiofiles.open(temp_path, 'wb') as f:
                                    await f.write(data)
                                
                                # Wait for file handle to be fully closed
                                await asyncio.sleep(0.1)
                                
                                # Validate image
                                try:
                                    with Image.open(temp_path) as img:
                                        img.verify()
                                    with Image.open(temp_path) as img:
                                        pass  # Just verify it can be opened
                                    
                                    # Remove existing file if it exists
                                    if os.path.exists(local_path):
                                        try:
                                            os.remove(local_path)
                                            await asyncio.sleep(0.1)  # Wait for file system
                                        except OSError as e:
                                            logger.error(f"Could not remove existing file: {e}")
                                            if os.path.exists(temp_path):
                                                os.remove(temp_path)
                                            return None
                                    
                                    # Move temp file to final location
                                    os.rename(temp_path, local_path)
                                    logger.info(f"Successfully downloaded image: {local_path}")
                                    return local_path
                                    
                                except Exception as img_err:
                                    logger.error(f"Invalid image file: {img_err}")
                                    if os.path.exists(temp_path):
                                        os.remove(temp_path)
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    return None
                                    
                            except Exception as file_err:
                                logger.error(f"Error saving file: {file_err}")
                                if os.path.exists(temp_path):
                                    try:
                                        os.remove(temp_path)
                                    except:
                                        pass
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(1 * (attempt + 1))
                                    continue
                                return None
                                
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout downloading image (attempt {attempt+1}/{max_retries}): {image_url}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1 * (attempt + 1))
                            continue
                        return None
                        
                    except Exception as e:
                        logger.error(f"Error downloading image (attempt {attempt+1}/{max_retries}): {e}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1 * (attempt + 1))
                            continue
                        return None
                        
    except Exception as e:
        logger.error(f"Unexpected error downloading image: {e}")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return None
        
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
        "777쓰리쎄븐 TS-6500C 손톱깎이 13P세트",
        "휴대용 360도 회전 각도조절 접이식 핸드폰 거치대",
        "피에르가르뎅 3단 슬림 코지가든 우양산",
        "마루는강쥐 클리어미니케이스",
        "아테스토니 뱀부사 소프트 3P 타올 세트",
        "티드 텔유 Y타입 치실 60개입 연세대학교 치과대학"
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