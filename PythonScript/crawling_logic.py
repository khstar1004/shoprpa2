import os
import logging
import hashlib
import time
from urllib.parse import urlparse
import pandas as pd
import asyncio
from playwright.async_api import async_playwright, Playwright, Browser
import configparser
import re
from typing import List

# Recent Changes (2024-05-21):
# 1. Updated crawl_haereum_image_urls to handle dictionary return with URL, local path, source
# 2. Modified _process_single_haoreum_image to use main folder from config.ini
# 3. Images saved with "haereum_" prefix for source identification
# 4. Enhanced source identification throughout the crawling pipeline
# 5. Images stored in image_main_dir from [Paths] section in config.ini

# Import necessary functions from other modules
from utils import download_image
from image_utils import remove_background
from crawl_naver_api import crawl_naver_products
from crawling_kogift import scrape_data as scrape_kogift_data
from crawling_haereum_standalone import scrape_haereum_data as scrape_haereum_image_url

async def crawl_all_sources(product_rows: pd.DataFrame, config: configparser.ConfigParser):
    """Orchestrates crawling Kogift, Naver, and Haereum Image URLs concurrently.
       Manages a shared Playwright browser instance for relevant tasks.
       Accepts a ConfigParser object.
    """
    logging.info("--- Starting Concurrent Crawling (Kogift, Naver API, Haereum URLs) ---")
    start_time = time.time()

    playwright: Playwright = None
    browser: Browser = None
    
    # Check if Playwright is needed (Kogift or Haereum)
    # For simplicity, always launch if product_rows is not empty, 
    # as checking specific scraper needs adds complexity.
    needs_playwright = len(product_rows) > 0  # Changed from not product_rows.empty
    
    try:
        headless_mode = True # Default
        if needs_playwright:
            try:
                headless_mode = config.getboolean('Playwright', 'playwright_headless', fallback=True)
            except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
                logging.warning(f"Error reading playwright_headless from [Playwright] config: {e}. Defaulting to True.")
            
            playwright = await async_playwright().start()
            logging.info(f"Launching shared Playwright browser (Headless: {headless_mode})...")
            browser = await playwright.chromium.launch(headless=headless_mode)
            logging.info("Shared Playwright browser launched.")
        else:
             logging.info("Skipping Playwright browser launch as no tasks require it.")

        # Create tasks for each crawl type, passing the browser if needed
        tasks = []
        kogift_task = None
        haereum_url_task = None
        naver_task = None
        
        # Create a copy of product_rows to pass to each task to avoid race conditions
        product_rows_copy = product_rows.copy()
        
        # Kogift Task (Requires Playwright)
        if needs_playwright:
            kogift_task = asyncio.create_task(
                crawl_kogift_products(product_rows_copy, browser, config), 
                name="kogift_crawl"
            )
            tasks.append(kogift_task)
            logging.info(f"Created Kogift crawl task for {len(product_rows_copy)} products")
        else:
            logging.info("Skipping Kogift crawl task creation (empty input or Playwright disabled).")

        # Haereum Image URLs Task (Requires Playwright)
        if needs_playwright:
            haereum_url_task = asyncio.create_task(
                crawl_haereum_image_urls(product_rows_copy, browser, config), 
                name="haereum_url_crawl"
            )
            tasks.append(haereum_url_task)
            logging.info(f"Created Haereum URL crawl task for {len(product_rows_copy)} products")
        else:
            logging.info("Skipping Haereum URL crawl task creation (empty input or Playwright disabled).")

        # Naver API Task (Async, no browser needed)
        naver_task = asyncio.create_task(
            crawl_naver_products(product_rows_copy, config), 
            name="naver_api_crawl"
        )
        tasks.append(naver_task)
        logging.info(f"Created Naver API crawl task for {len(product_rows_copy)} products")

        # Gather results from all created tasks
        if not tasks:
             logging.warning("No crawl tasks were created. Returning empty results.")
             results = []
        else:
            logging.info(f"Awaiting {len(tasks)} concurrent crawl tasks...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logging.info("All crawl tasks finished.")

    except Exception as e:
         logging.error(f"Error during Playwright setup or task gathering: {e}", exc_info=True)
         if browser and not browser.is_closed():
             await browser.close()
             logging.info("Closed Playwright browser due to error during setup/gather.")
         if playwright:
             await playwright.stop()
         raise
    finally:
        # Ensure Playwright browser and instance are closed cleanly
        logging.info("Entering finally block for Playwright cleanup...")
        # Add a small delay to allow tasks to fully release resources (optional, pragmatic fix)
        await asyncio.sleep(1.0) 
        
        if browser:
            try:
                if browser.is_connected():
                    logging.info("Attempting to close shared Playwright browser...")
                    await browser.close()
                    logging.info("Closed shared Playwright browser.")
                else:
                    logging.warning("Shared Playwright browser was already disconnected before explicit close.")
            except Exception as e:
                # Log error during browser close but proceed to playwright stop
                logging.warning(f"Error closing Playwright browser: {e}")
        else:
             logging.info("No shared browser instance to close.")
            
        if playwright:
            try:
                logging.info("Attempting to stop Playwright instance...")
                await playwright.stop()
                logging.info("Stopped Playwright instance.")
            except Exception as e:
                logging.warning(f"Error stopping playwright instance: {e}")
        else:
            logging.info("No Playwright instance to stop.")

    end_time = time.time()
    logging.info(f"--- Finished Concurrent Crawling orchestration in {end_time - start_time:.2f} seconds ---")

    # Process results based on the order tasks were added
    result_index = 0
    kogift_results = None
    if kogift_task:
        result_data = results[result_index]
        if isinstance(result_data, Exception):
             logging.error(f"Kogift crawl failed: {result_data}", exc_info=result_data)
             kogift_results = {} # Return empty dict on failure
        else:
             kogift_results = result_data
             logging.info(f"Kogift results received: {sum(len(v) for v in kogift_results.values())} total items for {len(kogift_results)} products")
        result_index += 1
    
    haereum_url_map = None
    if haereum_url_task:
        result_data = results[result_index]
        if isinstance(result_data, Exception):
             logging.error(f"Haereum Image URL crawl failed: {result_data}", exc_info=result_data)
             haereum_url_map = {}
        else:
             haereum_url_map = result_data
             # Handle new format with structured dictionary result
             valid_urls = sum(1 for item in haereum_url_map.values() if item and isinstance(item, dict) and item.get("url"))
             logging.info(f"Haereum URL results received: {valid_urls} valid URLs from {len(haereum_url_map)} products")
             
             # Add source info if not already present
             for product_name, item in haereum_url_map.items():
                 if item and isinstance(item, dict) and not item.get("source"):
                     item["source"] = "haereum"
                     
        result_index += 1
        
    naver_results = None
    if naver_task:
        result_data = results[result_index]
        if isinstance(result_data, Exception):
            logging.error(f"Naver API crawl failed: {result_data}", exc_info=result_data)
            naver_results = [] # Return empty list on failure
        else:
            naver_results = result_data
            logging.info(f"Naver API results received: {len(naver_results)} items")
            
            # Add source info to Naver results if needed
            for item in naver_results:
                if isinstance(item, dict) and not item.get("source"):
                    item["source"] = "naver"
                    
        result_index += 1

    # 공급사 정보 검증 및 정리
    if kogift_results:
        for product_name, items in kogift_results.items():
            updated_items = []
            for item in items:
                if isinstance(item, dict):
                    # 공급사 정보가 없으면 추가
                    if 'supplier' not in item:
                        # URL에서 공급사 정보 추출 시도
                        if 'link' in item and item['link']:
                            domain = urlparse(item['link']).netloc
                            if 'koreagift' in domain:
                                item['supplier'] = '고려기프트'
                            elif 'adpanchok' in domain:
                                item['supplier'] = '애드판촉'
                            else:
                                item['supplier'] = domain.split('.')[0] if '.' in domain else 'unknown'
                        else:
                            item['supplier'] = 'unknown'
                    
                    # Add source info if not present
                    if 'source' not in item:
                        item['source'] = 'kogift'
                        
                    updated_items.append(item)
            kogift_results[product_name] = updated_items

    return kogift_results, naver_results, haereum_url_map


async def crawl_kogift_products(product_rows: pd.DataFrame, browser: Browser, config: configparser.ConfigParser):
    """Crawl Kogift data for given product rows asynchronously using a shared browser instance.
       Accepts a ConfigParser object.
    """
    if len(product_rows) == 0:  # Changed from product_rows.empty
        logging.info("🔴 Kogift crawl: Input product_rows is empty. Skipping.")
        return {}

    total_rows = len(product_rows)
    logging.info(f"🔴 Starting Kogift scraping for {total_rows} products using shared browser...")
    start_time = time.time()
    
    try:
        playwright_concurrency = config.getint('Playwright', 'playwright_task_concurrency', fallback=4)
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        logging.warning(f"Error reading playwright_task_concurrency from [Playwright]: {e}. Defaulting to 4.")
        playwright_concurrency = 4
        
    semaphore = asyncio.Semaphore(playwright_concurrency)

    tasks = []
    task_to_product_name_map = {}

    for index, row in product_rows.iterrows():
        original_product_name = row.get('상품명')
        secondary_keyword = None # Currently not using secondary keyword from input file
        if original_product_name:
            task_name = f"kogift_scrape_{index}"
            task = asyncio.create_task(
                _run_single_kogift_scrape(
                    browser, semaphore, original_product_name, secondary_keyword, config), 
                name=task_name
            )
            tasks.append(task)
            task_to_product_name_map[task] = original_product_name
        else:
            logging.warning(f"🔴 Skipping Kogift scrape for row index {index}: Missing product name.")

    scraped_data_map = {}
    processed_scrape_count = 0
    total_scrape_tasks = len(tasks)

    logging.info(f"🔴 Submitting {total_scrape_tasks} Kogift scrape tasks with concurrency limit {playwright_concurrency}.")

    results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True)

    logging.info(f"🔴 Finished processing {len(results_or_exceptions)} Kogift scrape tasks.")

    # Process results from gather
    for i, result_or_exc in enumerate(results_or_exceptions):
        task = tasks[i]
        original_product_name = task_to_product_name_map.get(task)
        
        if not original_product_name:
             task_name_for_log = task.get_name() if hasattr(task, 'get_name') else f"task_{i}"
             logging.error(f"🔴 FATAL: Could not find original product name for completed Kogift task (Name: {task_name_for_log}). Skipping result.")
             continue

        task_name = task.get_name() if hasattr(task, 'get_name') else f"task_{i}"

        if isinstance(result_or_exc, Exception):
            logging.error(f"🔴 Error scraping Kogift for '{original_product_name}' (Task: {task_name}): {result_or_exc}", exc_info=result_or_exc)
            scraped_data_map[original_product_name] = []
        else:
             kogift_result_data = result_or_exc
             if isinstance(kogift_result_data, pd.DataFrame):
                 scraped_data_map[original_product_name] = kogift_result_data.to_dict('records')
             elif isinstance(kogift_result_data, list):
                 scraped_data_map[original_product_name] = kogift_result_data
             else:
                 logging.warning(f"🔴 Unexpected result type from Kogift scrape_data for '{original_product_name}' (Task: {task_name}): {type(kogift_result_data)}. Assuming no results.")
                 scraped_data_map[original_product_name] = []
             logging.debug(f"🔴 Found {len(scraped_data_map.get(original_product_name, []))} Kogift items for '{original_product_name}'")

        processed_scrape_count += 1
        if processed_scrape_count % 50 == 0 or processed_scrape_count == total_scrape_tasks:
            logging.info(f"🔴 Kogift scrape result processing progress: {processed_scrape_count}/{total_scrape_tasks}")


    end_time = time.time()
    logging.info(f"🔴 Finished KoGift crawling orchestration in {end_time - start_time:.2f} seconds.")
    return scraped_data_map

# Helper coroutine to manage concurrency for single Kogift scrape
async def _run_single_kogift_scrape(browser, semaphore, keyword1, keyword2, config):
     async with semaphore:
         logging.debug(f"Acquired semaphore for Kogift: '{keyword1}'")
         try:
             result = await scrape_kogift_data(browser, keyword1, keyword2, config)
             logging.debug(f"Released semaphore for Kogift: '{keyword1}'")
             return result
         except Exception as e:
             logging.error(f"🔴 Error in _run_single_kogift_scrape for '{keyword1}': {e}", exc_info=True)
             raise


async def crawl_haereum_image_urls(product_rows: pd.DataFrame, browser: Browser, config: configparser.ConfigParser):
    """Crawl Haereum Gift image URLs for given product rows asynchronously using a shared browser instance.
       Accepts a ConfigParser object.
    """
    if len(product_rows) == 0:  # Changed from product_rows.empty
        logging.info("🟡 Haereum Image URL crawl: Input product_rows is empty. Skipping.")
        return {}

    total_rows = len(product_rows)
    logging.info(f"🟡 Starting Haereum Gift image URL scraping for {total_rows} products using shared browser...")
    start_time = time.time()

    try:
        playwright_concurrency = config.getint('Playwright', 'playwright_task_concurrency', fallback=4)
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        logging.warning(f"Error reading playwright_task_concurrency from [Playwright]: {e}. Defaulting to 4.")
        playwright_concurrency = 4
        
    semaphore = asyncio.Semaphore(playwright_concurrency)

    tasks = []
    task_to_product_name_map = {}

    for index, row in product_rows.iterrows():
        original_product_name = row.get('상품명')
        if original_product_name:
            original_product_name = str(original_product_name)
            task_name = f"haereum_image_url_scrape_{index}"
            task = asyncio.create_task(
                _run_single_haereum_scrape(browser, semaphore, original_product_name, config),
                name=task_name
            )
            tasks.append(task)
            task_to_product_name_map[task] = original_product_name
        else:
            logging.warning(f"🟡 Skipping Haereum image URL scrape for row index {index}: Missing product name.")

    scraped_image_results = {}
    processed_scrape_count = 0
    total_scrape_tasks = len(tasks)
    
    logging.info(f"🟡 Submitting {total_scrape_tasks} Haereum URL scrape tasks with concurrency limit {playwright_concurrency}.")

    results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True)

    logging.info(f"🟡 Finished processing {len(results_or_exceptions)} Haereum URL scrape tasks.")

    # Process results from gather
    for i, result_or_exc in enumerate(results_or_exceptions):
        task = tasks[i]
        original_product_name = task_to_product_name_map.get(task)

        if not original_product_name:
             task_name_for_log = task.get_name() if hasattr(task, 'get_name') else f"task_{i}"
             logging.error(f"🟡 FATAL: Could not find original product name for completed Haereum task (Name: {task_name_for_log}). Skipping result.")
             continue

        task_name = task.get_name() if hasattr(task, 'get_name') else f"task_{i}"

        if isinstance(result_or_exc, Exception):
            logging.error(f"🟡 Error scraping Haereum image URL for '{original_product_name}' (Task: {task_name}): {result_or_exc}", exc_info=result_or_exc)
            scraped_image_results[original_product_name] = None
        else:
            result = result_or_exc
            # Handle new dictionary return format
            if isinstance(result, dict) and result.get("url") and result.get("url").startswith('http'):
                scraped_image_results[original_product_name] = {
                    "url": result.get("url"),
                    "local_path": result.get("local_path"),
                    "source": result.get("source", "haereum")
                }
                logging.debug(f"🟡 Found Haereum image URL for '{original_product_name}': {result.get('url')}, Local path: {result.get('local_path')}")
            # Handle string return format (backward compatibility)
            elif isinstance(result, str) and result.startswith('http'):
                scraped_image_results[original_product_name] = {
                    "url": result,
                    "local_path": None,
                    "source": "haereum"
                }
                logging.debug(f"🟡 Found Haereum image URL for '{original_product_name}': {result}")
            else:
                scraped_image_results[original_product_name] = None 

        processed_scrape_count += 1
        if processed_scrape_count % 50 == 0 or processed_scrape_count == total_scrape_tasks:
            logging.info(f"🟡 Haereum image URL scrape result processing progress: {processed_scrape_count}/{total_scrape_tasks}")


    end_time = time.time()
    logging.info(f"🟡 Finished Haereum Gift image URL crawling orchestration in {end_time - start_time:.2f} seconds.")
    return scraped_image_results

# Helper coroutine to manage concurrency for single Haereum scrape
async def _run_single_haereum_scrape(browser, semaphore, product_name, config):
    async with semaphore:
        logging.debug(f"Acquired semaphore for Haereum: '{product_name}'")
        try:
            result = await scrape_haereum_image_url(browser, product_name, config)
            logging.debug(f"Released semaphore for Haereum: '{product_name}'")
            return result
        except Exception as e:
             logging.error(f"🟡 Error in _run_single_haereum_scrape for '{product_name}': {e}", exc_info=True)
             raise


def _process_single_haoreum_image(product_code, image_info, config):
    """Downloads and optionally removes background for a single Haereum image."""
    # Handle both old format (string URL) and new format (dictionary with url, local_path, source)
    if isinstance(image_info, dict):
        image_url = image_info.get("url")
        local_path = image_info.get("local_path")
        if local_path and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logging.debug(f"🟡 Using existing downloaded Haereum image: {local_path}")
            
            # Check for existing background-removed version
            try:
                use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
                if use_bg_removal:
                    nobg_path = local_path.replace('.', '_nobg.', 1)
                    if os.path.exists(nobg_path) and os.path.getsize(nobg_path) > 0:
                        logging.debug(f"🟡 Using existing background-removed Haereum image: {nobg_path}")
                        return product_code, nobg_path
                    else:
                        # Try to remove background if no-bg version doesn't exist
                        try:
                            from image_utils import remove_background
                            if remove_background(local_path, nobg_path):
                                logging.debug(f"🟡 Background removed for existing Haereum image: {nobg_path}")
                                return product_code, nobg_path
                        except Exception as bg_err:
                            logging.warning(f"🟡 Error during background removal: {bg_err}. Using original image.")
            except Exception as config_err:
                logging.warning(f"🟡 Error reading background removal config: {config_err}. Using original image.")
                
            return product_code, local_path
    else:
        image_url = image_info
        local_path = None
    
    # Validate image_url
    if not image_url:
        logging.warning(f"🟡 Empty image URL for Haereum product {product_code}")
        return product_code, None
        
    if not isinstance(image_url, str):
        logging.warning(f"🟡 Invalid image URL type ({type(image_url)}) for Haereum product {product_code}")
        return product_code, None
        
    if not image_url.startswith('http'):
        logging.warning(f"🟡 Invalid URL format for Haereum product {product_code}: {image_url}")
        return product_code, None

    # Get main folder path from config
    try:
        main_dir = config.get('Paths', 'image_main_dir', fallback=None)
        if not main_dir:
            # Use fallback path
            main_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'images', 'Main')
            logging.warning(f"🟡 image_main_dir not specified in config, using fallback: {main_dir}")
        
        # Create Haereum-specific subdirectory
        haereum_dir = os.path.join(main_dir, 'Haereum')
        
        # Create directory if it doesn't exist
        os.makedirs(haereum_dir, exist_ok=True)
            
        # Verify directory is writable
        if not os.access(haereum_dir, os.W_OK):
            logging.error(f"🟡 Image directory is not writable: {haereum_dir}")
            return product_code, None
            
        # Check for use_background_removal setting
        use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
    except Exception as e:
        logging.error(f"🟡 Error accessing or creating image directory: {e}")
        return product_code, None
        
    try:
        # Sanitize product code if needed
        if product_code is None:
            sanitized_code = "unknown_product"
        else:
            # Handle Korean characters by using hash instead
            if isinstance(product_code, str) and any('\uAC00' <= c <= '\uD7A3' for c in product_code):
                sanitized_code = hashlib.md5(product_code.encode('utf-8', errors='ignore')).hexdigest()[:16]
                logging.debug(f"🟡 Using hash-based code for Korean product code: {sanitized_code}")
            else:
                # Ensure consistent product code format
                sanitized_code = re.sub(r'[^\w\d-]', '_', str(product_code))[:30]
                # Add padding to ensure consistent length
                sanitized_code = sanitized_code.ljust(30, '_')
        
        # Create a consistent hash of URL for uniqueness
        url_hash = hashlib.md5(image_url.encode('utf-8', errors='ignore')).hexdigest()[:8]
        
        # Determine file extension from URL
        parsed_url = urlparse(image_url)
        file_ext = os.path.splitext(parsed_url.path)[1].lower()
        # Default to .jpg if no extension or invalid extension
        if not file_ext or file_ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            file_ext = '.jpg'
        
        # Include source information in the filename with consistent format
        main_img_filename = f"haereum_{sanitized_code}_{url_hash}{file_ext}"
        main_img_path = os.path.join(haereum_dir, main_img_filename)
        final_image_path = main_img_path

        # Check if image already exists
        if os.path.exists(main_img_path) and os.path.getsize(main_img_path) > 0:
            logging.debug(f"🟡 Using existing Haereum image in main folder: {main_img_path}")
            
            # Check for existing background-removed version
            if use_bg_removal:
                main_img_nobg_path = main_img_path.replace('.', '_nobg.', 1)
                if os.path.exists(main_img_nobg_path) and os.path.getsize(main_img_nobg_path) > 0:
                    final_image_path = main_img_nobg_path
                    logging.debug(f"🟡 Using existing background-removed image: {final_image_path}")
                else:
                    # Try to remove background if no-bg version doesn't exist
                    try:
                        from image_utils import remove_background
                        if remove_background(main_img_path, main_img_nobg_path):
                            final_image_path = main_img_nobg_path
                            logging.debug(f"🟡 Background removed for existing Haereum image: {final_image_path}")
                        else:
                            logging.warning(f"🟡 Failed to remove background for Haereum image {main_img_path}. Using original.")
                    except Exception as bg_err:
                        logging.warning(f"🟡 Error during background removal: {bg_err}. Using original image.")
            
            return product_code, final_image_path
        else:
            # Download the image with custom headers for Korean site compatibility
            logging.info(f"🟡 Downloading Haereum image to: {main_img_path}")
            try:
                # Add custom headers to download request to handle Korean sites
                headers = {
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                # Use utils.download_image function with proper headers
                from utils import download_image
                
                # Pass headers to the download function
                downloaded = download_image(image_url, main_img_path, config=config, headers=headers)
                
                if downloaded:
                    logging.debug(f"🟡 Downloaded Haereum image to main folder: {main_img_path}")
                    
                    # Try background removal if requested
                    if use_bg_removal:
                        main_img_nobg_path = main_img_path.replace('.', '_nobg.', 1)
                        try:
                            from image_utils import remove_background
                            if remove_background(main_img_path, main_img_nobg_path):
                                final_image_path = main_img_nobg_path
                                logging.debug(f"🟡 Background removed for downloaded Haereum image: {final_image_path}")
                            else:
                                logging.warning(f"🟡 Failed to remove background for Haereum image {main_img_path}. Using original.")
                        except Exception as bg_err:
                            logging.warning(f"🟡 Error during background removal: {bg_err}. Using original image.")
                    
                    return product_code, final_image_path
                else:
                    logging.warning(f"🟡 Failed to download Haereum image: {image_url}")
                    return product_code, None
            except Exception as dl_err:
                logging.error(f"🟡 Error downloading Haereum image from {image_url}: {dl_err}")
                return product_code, None
    except Exception as e:
        logging.error(f"🟡 Unexpected error processing Haereum image for product {product_code} URL {image_url}: {e}", exc_info=True)
        return product_code, None 

def get_target_quantities(config: configparser.ConfigParser, product_rows: pd.DataFrame = None) -> List[int]:
    """
    Get target quantities for price checking, prioritizing Excel input.
    
    Args:
        config: ConfigParser object
        product_rows: DataFrame containing product data (optional)
        
    Returns:
        List[int]: List of quantities to check
    """
    quantities = []
    
    # 1. Try to get quantities from Excel input
    if product_rows is not None and '기본수량(1)' in product_rows.columns:
        quantities = product_rows['기본수량(1)'].dropna().unique().tolist()
        quantities = [int(qty) for qty in quantities if str(qty).isdigit()]
        if quantities:
            logging.info(f"Using quantities from Excel input: {quantities}")
            return sorted(quantities)
    
    # 2. Try to get quantities from config
    try:
        target_quantities_str = config.get('ScraperSettings', 'target_quantities', fallback='')
        if target_quantities_str:
            quantities = [int(qty.strip()) for qty in target_quantities_str.split(',') if qty.strip().isdigit()]
            if quantities:
                logging.info(f"Using quantities from config: {quantities}")
                return sorted(quantities)
    except (configparser.Error, ValueError) as e:
        logging.warning(f"Error reading target quantities from config: {e}")
    
    # 3. Use default quantities
    default_quantities = [300, 500, 1000, 2000]
    logging.info(f"Using default quantities: {default_quantities}")
    return default_quantities 