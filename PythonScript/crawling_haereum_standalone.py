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
import argparse

# Ensure utils can be imported if run directly
# Assuming utils.py is in the same directory or Python path is set correctly
# script_dir = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(script_dir)
# from utils import generate_keyword_variations # Example if needed

# Import helper from Kogift scraper (or move to utils)
from crawling_kogift import should_block_request, setup_page_optimizations 

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
BROWSER_CONTEXT_TIMEOUT = 300000  # 5 minutes
PAGE_TIMEOUT = 240000  # 4 minutes (increased)
NAVIGATION_TIMEOUT = 120000  # 2 minutes (increased)
WAIT_TIMEOUT = 30000  # 30 seconds (increased)

# Add retry settings
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds (increased)
RETRY_BACKOFF_FACTOR = 1.5  # Exponential backoff factor (reduced)

def _normalize_text(text: str) -> str:
    """Normalizes text (remove extra whitespace)."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

# Note: _extract_price is not used in the current logic focused on image URL extraction
# def _extract_price(price_text: str) -> float:
#     ...

# Updated main scraping function to accept browser and ConfigParser
async def scrape_haereum_data(browser: Browser, keyword: str, config: configparser.ConfigParser = None, product_code: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    해오름 기프트 웹사이트에서 상품코드를 검색창에 입력하여 이미지를 찾습니다.
    
    Args:
        browser: Playwright 브라우저 인스턴스
        keyword: 검색 키워드 (product_code가 제공되지 않을 때 사용)
        config: 설정 객체
        product_code: 상품코드 (있으면 우선적으로 사용)
        
    Returns:
        이미지 URL과 로컬 경로를 포함하는 딕셔너리 또는 None
    """
    # Create a new semaphore for this function call
    # Force lower concurrency (1) to prevent connection issues
    max_windows = min(1, config.getint('Playwright', 'playwright_max_concurrent_windows', fallback=1))
    scraping_semaphore = asyncio.Semaphore(max_windows)  # Use config value for max concurrent windows but cap at 1
    
    retry_count = 0
    last_error = None
    
    # Check if we should use a default image when all else fails
    use_default_image = config.getboolean('Matching', 'use_default_image_when_not_found', fallback=True)
    default_image_path = config.get('Paths', 'default_image_path', fallback=None)
    
    # Check if we need to reconnect the browser
    need_new_browser = not browser or not browser.is_connected()

    # 실제 검색에 사용할 키워드 결정 - 상품코드가 있으면 상품코드 사용
    search_term = product_code if product_code else keyword
    
    if not search_term or search_term.strip() == "":
        logger.warning("검색어가 비어있습니다. 상품코드 또는 키워드가 필요합니다.")
        return None
        
    logger.info(f"해오름 사이트 검색 시작 - 검색어: '{search_term}'")
    
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
                                    
                                    # Add SwiftShader flag which fixes WebGL deprecated warnings
                                    if "--enable-unsafe-swiftshader" not in browser_args:
                                        browser_args.append("--enable-unsafe-swiftshader")
                                    
                                    # Add additional stability flags
                                    stability_flags = [
                                        "--disable-background-timer-throttling",
                                        "--disable-backgrounding-occluded-windows",
                                        "--disable-breakpad",
                                        "--disable-component-extensions-with-background-pages",
                                        "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                                        "--disable-ipc-flooding-protection",
                                        "--disable-renderer-backgrounding"
                                    ]
                                    
                                    for flag in stability_flags:
                                        if flag not in browser_args:
                                            browser_args.append(flag)
                                            
                                    browser = await playwright.chromium.launch(
                                        headless=headless,
                                        args=browser_args,
                                        timeout=120000  # 2 minute timeout for browser launch (increased)
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
                    
                    # Create a new context with improved settings
                    context = await browser.new_context(
                        user_agent=user_agent,
                        viewport={'width': 1280, 'height': 800},  # Reduced resolution for stability
                        java_script_enabled=True,  # Ensure JS is enabled
                        bypass_csp=True,  # Bypass Content Security Policy for better compatibility
                        ignore_https_errors=True,  # Ignore HTTPS errors
                    )
                    
                    # Set context timeout
                    context.set_default_timeout(BROWSER_CONTEXT_TIMEOUT)
                    
                    # Create a new page with increased timeouts
                    page = await context.new_page()
                    page.set_default_timeout(PAGE_TIMEOUT)
                    page.set_default_navigation_timeout(NAVIGATION_TIMEOUT)
                    
                    # Enable JavaScript error logging
                    await page.evaluate("""
                        window.addEventListener('error', (event) => {
                            console.error('JavaScript error:', event.message);
                        });
                    """)
                    
                    # Optimize page performance
                    if config.getboolean('Playwright', 'playwright_block_resources', fallback=True):
                        await setup_page_optimizations(page)
                        
                    # Add cleanup handler to ensure page is properly closed
                    page.on("close", lambda: logger.debug("Page closed event triggered"))

                    # ----- 중요 변경 부분: 항상 메인 페이지로 이동하여 검색 -----
                    logger.info(f"🌐 메인 사이트로 이동: {haereum_main_url}")
                    
                    # Improved retry logic for the initial navigation with longer timeouts
                    for nav_attempt in range(4): # Increased retries
                        try:
                            # First try to clear the context if not the first attempt
                            if nav_attempt > 0:
                                try:
                                    # Clear cookies, storage and permissions
                                    await context.clear_cookies()
                                    await context.clear_permissions()
                                    logger.info(f"Context cleared before retry #{nav_attempt+1}")
                                    # Add longer pause between retries
                                    await asyncio.sleep(5)
                                except Exception as clear_err:
                                    logger.warning(f"Error clearing context: {clear_err}")
                            
                            # Use a less strict wait_until policy for more reliable loading
                            try:
                                await page.goto(
                                    haereum_main_url, 
                                    wait_until="domcontentloaded", # Change from "load" to "domcontentloaded"
                                    timeout=config.getint('ScraperSettings', 'navigation_timeout', fallback=120000) 
                                )
                            except PlaywrightError as goto_err:
                                logger.warning(f"Initial navigation error: {goto_err}, trying simpler approach")
                                # Fallback to a simpler navigation approach
                                await page.goto(haereum_main_url, timeout=120000)
                            
                            # Longer pause after navigation to allow page to fully stabilize
                            await page.wait_for_timeout(10000) # Increased from 8000ms
                            
                            # Verify the page loaded correctly by checking for a basic element
                            try:
                                await page.wait_for_selector('body', timeout=10000) # Increased timeout
                                logger.info(f"✅ Page navigation successful on attempt {nav_attempt+1}")
                                break  # Break out of retry loop if successful
                            except Exception as verify_err:
                                logger.warning(f"Page verification failed: {verify_err}")
                                if nav_attempt < 3:
                                    continue
                                else:
                                    raise
                                
                        except PlaywrightError as nav_err:
                            if nav_attempt < 3:  # Try again if we haven't reached max retries
                                logger.warning(f"Navigation error (attempt {nav_attempt+1}/4): {nav_err}")
                                await asyncio.sleep(5 * (nav_attempt + 1))  # Progressively longer waits before retry
                            else:
                                raise  # Re-raise on final attempt
                    
                    logger.info("⏳ 초기 페이지 로드 완료. 검색창 확인 중...")

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
                            logger.warning(f"⚠️ 검색창 찾기 재시도 {retry_count}/{max_retries}: {str(e)}")
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
                    
                    # Fill the search input with the search term (product code or keyword)
                    try:
                        await search_input.fill(search_term, 
                                              timeout=config.getint('ScraperSettings', 'action_timeout', fallback=30000))
                        logger.info(f"⌨️ 검색창에 검색어 입력: '{search_term}'")
                    except Exception as e:
                        logger.error(f"검색어 입력 실패: {e}")
                        raise

                    # Wait for the search button to be present and visible
                    try:
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
                        logger.info("🔍 검색 버튼 클릭 완료, 검색 결과 확인 중...")
                    except Exception as e:
                        logger.error(f"검색 버튼 클릭 실패: {e}")
                        raise
                    
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
                            'img[src*="/upload/product/"]'  # Any product image as fallback
                        ])

                        # Add exclusion patterns to avoid non-product images
                        exclude_patterns = [
                            '/images/icon',
                            '/images/button',
                            '/upload/ad_new/', # Exclude ad images
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
                                    successfully_downloaded_url = None # Variable to store the URL that was successfully downloaded
                                    for variant_url_to_try in found_image_urls: # Iterate through the URLs found from variants
                                        logger.info(f"Attempting to download image: {variant_url_to_try}")
                                        # Call download_image_to_main
                                        current_local_path = await download_image_to_main(variant_url_to_try, keyword, config, max_retries=2)
                                        if current_local_path:
                                            logger.info(f"Successfully downloaded variant: {variant_url_to_try}")
                                            local_path = current_local_path # Store the path of the successfully downloaded image
                                            successfully_downloaded_url = variant_url_to_try # Store the URL that worked
                                            break # Exit loop once a download is successful
                                    
                                    # Close context before returning result
                                    await context.close()

                                    # Return the result if a download was successful using the correct URL
                                    if local_path and successfully_downloaded_url:
                                        return {"url": successfully_downloaded_url, "local_path": local_path, "source": "haereum"}
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
                    # Enhanced cleanup to prevent resource leaks
                    try:
                        if 'page' in locals() and page:
                            try:
                                # Check if page is still connected before trying to close
                                if not page.is_closed():
                                    # First try to remove all listeners to prevent callback errors
                                    try:
                                        page.remove_listener("close", lambda: None)
                                    except:
                                        pass
                                    # Then close with a timeout
                                    try:
                                        await asyncio.wait_for(page.close(run_before_unload=False), timeout=5.0)
                                        logger.debug("Page closed successfully")
                                    except asyncio.TimeoutError:
                                        logger.warning("Page close timed out, continuing with context cleanup")
                                    except Exception as page_err:
                                        logger.warning(f"⚠️ Error closing page: {page_err}")
                            except Exception as e:
                                logger.debug(f"Page already closed or error: {e}")
                        
                        if 'context' in locals() and context:
                            try:
                                # Only attempt to clear context data if browser is still connected
                                if browser and browser.is_connected() and not context.is_closed():
                                    try:
                                        # Try to clear context data first
                                        await context.clear_cookies()
                                    except:
                                        pass
                                    # Then close with a timeout
                                    try:
                                        await asyncio.wait_for(context.close(), timeout=5.0)
                                        logger.debug("Context closed successfully")
                                    except asyncio.TimeoutError:
                                        logger.warning("Context close timed out")
                                    except Exception as ctx_err:
                                        logger.warning(f"⚠️ Error closing context: {ctx_err}")
                            except Exception as e:
                                logger.debug(f"Context already closed or error: {e}")
                        
                        # Force garbage collection to release memory
                        import gc
                        gc.collect()
                    except Exception as e:
                        logger.warning(f"⚠️ Error during enhanced cleanup: {e}")

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

async def download_image_to_main(image_url: str, product_name: str, config: configparser.ConfigParser, product_code: Optional[str] = None, max_retries: int = 3) -> Optional[str]:
    """Downloads an image from a URL, saves it to the 'Main/Haereum' directory with a structured name, and returns the local path."""
    if not product_name or not product_name.strip():
        logger.warning("Product name is required for download_image_to_main")
        return None

    # --- Added Check: Ensure it's a product image URL ---
    if image_url and ('/upload/product/' not in image_url or '/upload/ad_new/' in image_url):
        logger.warning(f"Skipping non-product or ad image URL: {image_url}")
        return None
    # --- End Added Check ---

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
        # 상품명을 해시값으로 변환 (MD5) - 16자로 통일
        name_hash = hashlib.md5(product_name.encode()).hexdigest()[:16]
        
        # 랜덤 해시값 (8자로 통일) - URL 해시 대신 랜덤 사용
        import secrets
        random_hash = secrets.token_hex(4)  # 8자리 랜덤 해시 생성
        
        # Get file extension from URL if available, otherwise default to .jpg
        if image_url:
            parsed_url = urlparse(image_url)
            _, ext = os.path.splitext(parsed_url.path)
            ext = ext.lower() or ".jpg"  # Default to .jpg if no extension
        else:
            ext = ".jpg"  # Default extension when no URL
        
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
            
        # Create final filename - 새로운 형식으로 변경
        filename = f"haereum_{name_hash}_{random_hash}{ext}"
        local_path = os.path.join(main_dir, filename)
        final_image_path = local_path
        
        # Generate unique temporary filename
        temp_path = f"{local_path}.{time.time_ns()}.tmp"
        
        # Check if file already exists
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.info(f"Image already exists: {local_path}")
            
            # Still create background-removed version if needed, but always return original JPG
            if use_bg_removal:
                nobg_path = local_path.replace('.jpg', '_nobg.png', 1)
                if not os.path.exists(nobg_path) or os.path.getsize(nobg_path) <= 0:
                    # Try to remove background if no-bg version doesn't exist
                    try:
                        from image_utils import remove_background
                        if remove_background(local_path, nobg_path):
                            logger.debug(f"Background removed for existing Haereum image: {nobg_path}")
                        else:
                            logger.warning(f"Failed to remove background for Haereum image {local_path}")
                    except Exception as bg_err:
                        logger.warning(f"Error during background removal: {bg_err}")
            
            # Always return the original JPG path
            return local_path
            
    except Exception as e:
        logger.error(f"Error generating filename: {e}")
        return None
    
    # If no URL provided, return the generated path without downloading
    if not image_url or not image_url.strip():
        logger.info(f"No URL provided, returning generated path: {local_path}")
        return local_path
    
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
                            
                            # 최적화: 타임아웃 감소 및 연결 재사용
                            async with session.get(current_url, timeout=15, headers=headers, ssl=False) as response:
                                if response.status != 200:
                                    logger.warning(f"HTTP error {response.status} downloading image (attempt {attempt+1}/{max_retries}): {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(0.5 * (attempt + 1))  # 대기 시간 감소
                                        continue
                                    break  # Try next URL variant
                                    
                                # Check content type
                                content_type = response.headers.get('Content-Type', '')
                                if not content_type.startswith('image/'):
                                    logger.warning(f"Non-image content type: {content_type} for URL: {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(0.5 * (attempt + 1))  # 대기 시간 감소
                                        continue
                                    break  # Try next URL variant
                                    
                                # Download image data
                                data = await response.read()
                                if len(data) < 100:  # Too small to be a valid image
                                    logger.warning(f"Downloaded image too small: {len(data)} bytes from URL: {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(0.5 * (attempt + 1))  # 대기 시간 감소
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
                                        
                                        # Generate background-removed version if requested, but always return original JPG
                                        if use_bg_removal:
                                            try:
                                                from image_utils import remove_background
                                                nobg_path = local_path.replace('.jpg', '_nobg.png', 1)
                                                if remove_background(local_path, nobg_path):
                                                    logger.info(f"Background removed for Haereum image: {nobg_path}")
                                                else:
                                                    logger.warning(f"Failed to remove background for Haereum image {local_path}")
                                            except Exception as bg_err:
                                                logger.warning(f"Error during background removal: {bg_err}")
                                        
                                        # Always return the original JPG path
                                        return local_path
                                    except Exception as img_err:
                                        logger.error(f"Downloaded file is not a valid image: {img_err} from URL: {current_url}")
                                        if os.path.exists(temp_path):
                                            os.remove(temp_path)
                                        if attempt < max_retries - 1:
                                            await asyncio.sleep(0.5 * (attempt + 1))  # 대기 시간 감소
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
                                        await asyncio.sleep(0.5 * (attempt + 1))  # 대기 시간 감소
                                        continue
                                    break  # Try next URL variant
                        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                            logger.warning(f"Network error downloading image (attempt {attempt+1}/{max_retries}): {e}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))  # 대기 시간 감소
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
        return local_path
    return None

async def try_direct_product_code_fallback(page: Page, keyword: str, config: configparser.ConfigParser, haereum_main_url: str) -> Optional[Dict[str, str]]:
    """
    Fallback: If keyword search yields no good image immediately,
    try to extract a product code from the first search result and go to its page.
    """
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
                
            if not any(p in img_url for p in ['icon', 'button', 'btn_', 'pixel.gif', 'spacer.gif', 'no_image', '/upload/ad_new/']):
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

async def setup_page_optimizations(page: Page):
    """Set up page optimizations and resource blocking."""
    try:
        # Store the original route handler to prevent memory leaks
        original_route_handler = None
        
        async def handle_route(route):
            try:
                # Get the request URL
                url = route.request.url
                
                # Skip if page is closed
                if page.is_closed():
                    logger.warning(f"Page is closed, skipping route for: {url}")
                    return
                
                # Block unnecessary resources
                if should_block_request(url):
                    try:
                        await route.abort()
                    except Exception as abort_err:
                        logger.debug(f"Error aborting route: {abort_err}")
                    return
                
                # Continue the request
                try:
                    await route.continue_()
                except Exception as continue_err:
                    logger.warning(f"Error continuing route: {continue_err}")
                    # Try fallback if continue fails
                    try:
                        if not page.is_closed():
                            await route.continue_()
                    except Exception as fallback_err:
                        logger.error(f"Failed to continue request in fallback: {fallback_err}")
            except Exception as e:
                logger.error(f"Error in route handler for {url}: {e}")
                # Don't try to continue if there's an error
                return
        
        # Set up route handler
        await page.route("**/*", handle_route)
        
        # Optimize page settings
        await page.set_viewport_size({"width": 1280, "height": 800})
        await page.set_extra_http_headers({
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        
        # Disable unnecessary features
        await page.evaluate("""
            () => {
                // Disable animations
                document.body.style.setProperty('animation', 'none', 'important');
                document.body.style.setProperty('transition', 'none', 'important');
                
                // Disable unnecessary features
                window.Notification = undefined;
                window.webkitNotifications = undefined;
                window.navigator.vibrate = undefined;
                window.navigator.getBattery = undefined;
                window.navigator.geolocation = undefined;
            }
        """)
        
    except Exception as e:
        logger.error(f"Error setting up page optimizations: {e}")
        # Don't re-raise the exception, just log it

# Example usage (Updated for ConfigParser)
async def _test_main():
    from playwright.async_api import async_playwright
    from utils import load_config # Import config loader
    import sys
    import os.path
    
    # Set up logging first
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    
    # 테스트할 상품 코드 목록
    product_codes = [
        # 사용자 제공 코드
        "442416", "442414", "442413", "442412", "442411", 
        "442409", "442405", "442404", "442403"
    ]
    
    # 헤드리스 모드 설정 (기본값: True)
    headless_mode = True
    
    # 명령줄 인수 처리 (단순화된 방식)
    if len(sys.argv) > 1:
        # 헤드리스 모드 설정 확인
        if '--no-headless' in sys.argv or '--show-browser' in sys.argv:
            headless_mode = False
            logger.info("브라우저 표시 모드로 실행합니다.")
    
    # 설정 파일 로드
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config = load_config(config_path)
    if not config.sections():
        logger.error(f"설정 파일을 로드할 수 없습니다: {config_path}")
        return
    
    # 헤드리스 모드 설정 적용
    if not config.has_section('Playwright'):
        config.add_section('Playwright')
    config.set('Playwright', 'playwright_headless', str(headless_mode).lower())
    
    # 이미지 저장 경로 설정 확인
    if not config.has_section('Paths'):
        config.add_section('Paths')
    
    # 기본 이미지 저장 경로 설정
    if not config.has_option('Paths', 'image_main_dir'):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        default_img_dir = os.path.join(script_dir, '..', 'images', 'Main')
        os.makedirs(default_img_dir, exist_ok=True)
        config.set('Paths', 'image_main_dir', default_img_dir)
        logger.info(f"이미지 저장 경로 설정: {default_img_dir}")
    
    logger.info(f"--- 해오름 기프트 상품코드 검색 테스트 시작 (총 {len(product_codes)}개 코드) ---")
    
    async with async_playwright() as p:
        browser = None
        try:
            # 브라우저 인수 설정
            browser_args = []
            try:
                browser_args_str = config.get('Playwright', 'playwright_browser_args', fallback='[]')
                import json
                browser_args = json.loads(browser_args_str)
            except Exception:
                browser_args = ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
            
            # 브라우저 시작
            logger.info(f"브라우저 시작 중 (헤드리스: {headless_mode})")
            browser = await p.chromium.launch(
                headless=headless_mode,
                args=browser_args,
                timeout=120000  # 2분 타임아웃 (increased)
            )
        except Exception as browser_err:
            logger.error(f"브라우저 시작 실패: {browser_err}")
            return
        
        start_time = time.time()
        
        try:
            # 동시 작업 제한 세마포어 - IP 차단 방지를 위해 1로 제한
            max_windows = 1  # 동시 연결 수를 1로 제한
            scraping_semaphore = asyncio.Semaphore(max_windows)
            
            # 배치 크기 설정 - 안전한 처리 위해 1로 제한
            batch_size = 1  # 배치 크기를 1로 제한
            results = []
            
            # 배치 간 대기 시간 - IP 차단 방지를 위해 충분한 대기 시간 설정
            batch_delay = 5  # 배치 간 5초 대기
            
            # Add a check for browser connection and reconnect if needed
            async def ensure_browser_connected():
                nonlocal browser
                try:
                    if not browser or not browser.is_connected():
                        logger.warning("Browser connection lost, attempting to reconnect...")
                        browser = await p.chromium.launch(
                            headless=headless_mode,
                            args=browser_args,
                            timeout=120000
                        )
                        logger.info("Browser reconnected successfully")
                    return True
                except Exception as e:
                    logger.error(f"Failed to reconnect browser: {e}")
                    return False
            
            # 안전한 처리를 위한 작업 생성 함수
            async def create_scraping_task(code):
                async with scraping_semaphore:
                    # IP 차단 방지를 위한 충분한 대기 시간
                    await asyncio.sleep(2)  # 요청 간 2초 대기
                    # 키워드는 비워두고 상품 코드로만 검색
                    return (code, await scrape_haereum_data(browser, "", config, product_code=code))
            
            # 배치 단위로 처리
            for batch_start in range(0, len(product_codes), batch_size):
                # Ensure browser is connected before starting new batch
                if not await ensure_browser_connected():
                    logger.error("Cannot proceed with batch due to browser connection issues")
                    break
                    
                batch_end = min(batch_start + batch_size, len(product_codes))
                batch = product_codes[batch_start:batch_end]
                
                logger.info(f"배치 처리 중: {len(batch)}개 상품 코드 ({batch_start+1}-{batch_end}/{len(product_codes)})")
                
                # 배치 작업 생성 및 실행
                batch_tasks = [create_scraping_task(code) for code in batch]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                results.extend(batch_results)
                
                # 배치 간 충분한 대기 시간
                if batch_end < len(product_codes):
                    logger.info(f"배치 완료. 다음 배치 시작 전 {batch_delay}초 대기...")
                    await asyncio.sleep(batch_delay)
            
            # 결과 출력
            print("\n" + "="*80)
            print(f"해오름 이미지 스크래퍼 테스트 결과")
            print("="*80)
            
            success_count = 0
            error_count = 0
            not_found_count = 0
            default_count = 0
            
            # 이미지 파일 확인 함수 추가
            def check_image_file(file_path):
                try:
                    if not file_path or not os.path.exists(file_path):
                        return False, "파일 없음", 0, "N/A"
                    
                    file_size = os.path.getsize(file_path)
                    if file_size == 0:
                        return False, "파일 크기 0", 0, "N/A"
                    
                    # 이미지 형식 확인 (선택적)
                    try:
                        from PIL import Image
                        img = Image.open(file_path)
                        img_format = img.format
                        img_size = img.size
                        img.close()
                        return True, "정상", file_size, f"{img_format} {img_size[0]}x{img_size[1]}"
                    except Exception as img_err:
                        return False, f"이미지 검증 실패: {str(img_err)}", file_size, "N/A"
                except Exception as e:
                    return False, f"파일 확인 오류: {str(e)}", 0, "N/A"
            
            # 각 결과 처리
            for result in results:
                if isinstance(result, Exception):
                    error_count += 1
                    print(f"❌ 오류: {str(result)}")
                elif isinstance(result, tuple) and len(result) == 2:
                    code, data = result
                    if isinstance(data, Exception):
                        error_count += 1
                        print(f"❌ 상품코드 '{code}' 오류: {str(data)}")
                    elif data and data.get("url"):
                        # 이미지 파일 상태 확인
                        local_path = data.get('local_path', '')
                        is_valid, status, file_size, img_info = check_image_file(local_path)
                        
                        if data.get('url') == 'default':
                            default_count += 1
                            print(f"⚠️ 상품코드 '{code}': 기본 이미지 사용됨")
                            print(f"   - 로컬 경로: {local_path}")
                            print(f"   - 파일 상태: {status}, 크기: {file_size} 바이트, 정보: {img_info}")
                        elif is_valid:
                            success_count += 1
                            print(f"✅ 상품코드 '{code}' 성공:")
                            print(f"   - 이미지 URL: {data.get('url', 'N/A')}")
                            print(f"   - 로컬 경로: {local_path}")
                            print(f"   - 파일 크기: {file_size} 바이트, 형식: {img_info}")
                        else:
                            error_count += 1
                            print(f"❌ 상품코드 '{code}': 이미지 파일 유효하지 않음")
                            print(f"   - 이미지 URL: {data.get('url', 'N/A')}")
                            print(f"   - 로컬 경로: {local_path}")
                            print(f"   - 파일 상태: {status}")
                    else:
                        not_found_count += 1
                        print(f"❌ 상품코드 '{code}': 이미지를 찾을 수 없음")
                else:
                    error_count += 1
                    print(f"❌ 예상치 못한 결과 형식: {result}")
                print("-" * 40)
            
            # 결과 요약
            print("\n" + "="*80)
            print(f"테스트 요약:")
            print(f"- 성공: {success_count}개")
            print(f"- 기본 이미지 사용: {default_count}개")
            print(f"- 이미지 없음: {not_found_count}개")
            print(f"- 오류: {error_count}개")
            print(f"- 총 테스트: {len(product_codes)}개")
            print("="*80)
                
        except Exception as e:
            logger.error(f"테스트 중 오류 발생: {e}")
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception as close_err:
                    logger.warning(f"브라우저 종료 오류: {close_err}")
            
        end_time = time.time()
        logger.info(f"테스트 완료. 소요 시간: {end_time - start_time:.2f}초")

if __name__ == "__main__":
    # 실행 방법: python PythonScript/crawling_haereum_standalone.py
    # Or with specific product codes: python PythonScript/crawling_haereum_standalone.py --codes=439522,439508
    # Or with a file: python PythonScript/crawling_haereum_standalone.py --file path/to/products.xlsx
    # Or in non-headless mode: python PythonScript/crawling_haereum_standalone.py --no-headless
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    logger.info("해오름 이미지 테스트를 시작합니다...")
    asyncio.run(_test_main()) 