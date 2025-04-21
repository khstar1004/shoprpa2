import os
import logging
import re
import httpx
import pandas as pd
import asyncio
import time
import hashlib
from urllib.parse import urlparse
import json
import configparser
import pprint
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# Import based on how the file is run
try:
    # When imported as module
    from .utils import (
        download_image_async, get_async_httpx_client, generate_keyword_variations, 
        load_config, tokenize_korean, jaccard_similarity
    )
    from .image_utils import remove_background_async
except ImportError:
    # When run directly as script
    from utils import (
        download_image_async, get_async_httpx_client, generate_keyword_variations, 
        load_config, tokenize_korean, jaccard_similarity
    )
    from image_utils import remove_background_async

# Setup logger for this module
logger = logging.getLogger(__name__) # Use module-specific logger

# Note: CONFIG will be passed as config_dict
MIN_RESULTS_THRESHOLD_NAVER = 5 # Minimum desired results for Naver API

async def crawl_naver(original_query: str, client: httpx.AsyncClient, config: configparser.ConfigParser, max_items: int = 50, reference_price: float = 0) -> List[Dict[str, Any]]:
    """
    Search products using Naver Shopping API, trying multiple keyword variations.

    Args:
        original_query: The original search term.
        client: Async HTTPX client for making requests.
        config: ConfigParser object containing configuration.
        max_items: Maximum number of items to return per keyword attempt.
        reference_price: Reference price for filtering (currently only logged).

    Returns:
        List of product dictionaries from the first keyword variation yielding enough results,
        or the results from the last attempted keyword.
    """
    logger.debug(f"Initiating Naver search for query: '{original_query}', max_items: {max_items}, ref_price: {reference_price}")
    try:
        client_id = config.get('API_Keys', 'naver_client_id', fallback='')
        client_secret = config.get('API_Keys', 'naver_client_secret', fallback='')
    except (configparser.NoSectionError, configparser.NoOptionError):
         logger.error("Naver API keys not found in [API_Keys] section of config. Cannot perform search.")
         return []

    if not client_id or not client_secret:
        logger.error("Missing Naver API client ID or secret in config. Cannot perform search.")
        return []

    client_id_display = (client_id[:4] + '...') if client_id else 'Not Set'
    client_secret_display = (client_secret[:4] + '...') if client_secret else 'Not Set'
    logger.info(f"🟢 Naver API Credentials: Client ID starts with '{client_id_display}', Secret starts with '{client_secret_display}'")

    # Get delay between API calls
    api_delay = config.getfloat('ScraperSettings', 'naver_api_delay', fallback=1.0)

    # Get initial similarity threshold from config
    try:
        initial_sim_threshold = config.getfloat('Matching', 'naver_initial_similarity_threshold', fallback=0.1) # Default low threshold
    except (configparser.Error, ValueError):
        logger.warning("Could not read 'naver_initial_similarity_threshold' from [Matching] config. Using default 0.1.")
        initial_sim_threshold = 0.1
    logger.info(f"Using initial Naver result similarity threshold: {initial_sim_threshold}")

    # Tokenize the original query once
    original_query_tokens = tokenize_korean(original_query)

    # Generate keywords to try
    keywords_to_try = generate_keyword_variations(original_query)
    logger.info(f"🟢 Generated Naver keywords for '{original_query}': {keywords_to_try}")

    best_result_list: List[Dict[str, Any]] = [] # Store results from the most successful keyword attempt

    for keyword_idx, query in enumerate(keywords_to_try):
        logger.info(f"🟢 --- Trying Naver keyword variation {keyword_idx+1}/{len(keywords_to_try)}: '{query}' ---")
        current_keyword_results: List[Dict[str, Any]] = []
        processed_api_items = 0

        # API URL and headers
        api_url = "https://openapi.naver.com/v1/search/shop.json"
        headers = {
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret,
            "Accept": "application/json",
        }

        # Define promotional keywords (could be moved outside loop/function)
        promo_keywords = [
            "판촉물", "기프트", "답례품", "기념품", "인쇄", "각인", "제작",
            "홍보", "미스터몽키", "호갱탈출", "고려기프트", "판촉", "기업선물",
            "단체선물", "행사용품", "홍보물", "기업홍보", "로고인쇄", "로고각인",
            "로고제작", "기업답례품", "행사답례품", "기념품제작", "기업기념품",
        ]

        # Search up to 3 pages for the current keyword
        for page in range(1, 4):
            if len(current_keyword_results) >= max_items:
                logger.debug(f"Reached max_items ({max_items}) limit for keyword '{query}', stopping API calls for this keyword.")
                break

            api_display_count = 100 # Max allowed by Naver API
            start_index = (page - 1) * api_display_count + 1
            # Calculate how many more items we need, respecting max_items overall
            effective_display_count = min(api_display_count, max_items - len(current_keyword_results))
            if effective_display_count <= 0:
                 logger.debug(f"Effective display count is zero or less for keyword '{query}', page {page}. Breaking page loop.")
                 break

            params = {"query": query, "display": effective_display_count, "start": start_index, "sort": "sim", "exclude": "used:rental"} # Sort by similarity
            logger.debug(f"🟢 Naver API Request (Keyword: '{query}', Page {page}, Sort: 'sim'): Params={params}")

            # Add delay before API call to avoid hitting rate limits
            if page > 1 or keyword_idx > 0:
                logger.debug(f"🟢 Adding delay of {api_delay:.2f} seconds before Naver API request (Page: {page}, Keyword Attempt: {keyword_idx+1})")
                await asyncio.sleep(api_delay)

            try:
                logger.info(f"🟢 Sending Naver API request for '{query}' (Page {page})")
                start_time = time.monotonic()
                response = await client.get(api_url, headers=headers, params=params)
                response_time = time.monotonic() - start_time
                status_code = response.status_code
                logger.info(f"🟢 Naver API response status: {status_code} (took {response_time:.2f}s)")

                if status_code != 200:
                    error_text = response.text[:200] + "..." if len(response.text) > 200 else response.text
                    logger.error(f"🟢 Naver API error response (Status: {status_code}, Keyword: '{query}', Page: {page}): {error_text}")
                    if status_code == 401: # Unauthorized
                         logger.error("Naver API authentication failed (401). Check credentials.")
                         # Stop trying immediately if credentials are bad
                         return []
                    elif status_code == 429: # Rate limit
                        wait_time = api_delay * 3
                        logger.error(f"🟢 Rate limit exceeded (429). Waiting {wait_time:.2f} seconds before next request.")
                        await asyncio.sleep(wait_time)
                    elif status_code == 404: # Not Found (e.g., invalid API endpoint, unlikely here)
                         logger.error("Naver API endpoint not found (404). Check API URL.")
                    # Continue to next page or keyword for other errors for now
                    continue

                response.raise_for_status() # Raise exception for non-200 after specific handling
                data = response.json()
                total_items_api = data.get('total', 0)
                api_items_on_page = len(data.get('items', []))
                logger.info(f"🟢 Naver API Response (Keyword: '{query}', Page {page}): Found {total_items_api} total items, received {api_items_on_page} on this page.")

                if 'items' not in data or not data.get('items'):
                    logger.warning(f"🟢 Naver API returned no items for '{query}' (Page {page}).")
                    if 'errorMessage' in data:
                        logger.error(f"🟢 Naver API error message: {data.get('errorMessage')}")
                    # Log the full response for debugging if no items found
                    logger.debug(f"🟢 Full Naver API response (no items): {json.dumps(data, ensure_ascii=False)[:500]}")
                    break # No items on this page, stop fetching for this keyword

            except httpx.TimeoutException as timeout_err:
                 wait_time = api_delay * 2
                 logger.error(f"🟢 Timeout during Naver API request (Keyword: '{query}', Page {page}): {timeout_err}. Waiting {wait_time:.2f}s.")
                 await asyncio.sleep(wait_time) # Wait longer on timeout
                 continue # Retry this page/keyword after delay
            except httpx.RequestError as req_err:
                 logger.error(f"🟢 HTTPX Request Error during Naver API request (Keyword: '{query}', Page {page}): {req_err}", exc_info=True)
                 await asyncio.sleep(api_delay) # Basic delay and continue
                 break # Assume persistent issue with this keyword/page
            except json.JSONDecodeError as json_err:
                 logger.error(f"🟢 Error decoding JSON response from Naver API (Keyword: '{query}', Page {page}): {json_err}. Response text: {response.text[:200]}...", exc_info=True)
                 break # Malformed response, stop processing for this keyword
            except Exception as e:
                logger.error(f"🟢 Unexpected error during Naver API request (Keyword: '{query}', Page {page}): {e}", exc_info=True)
                if isinstance(e, RuntimeError) and "client has been closed" in str(e):
                    logger.error(f"🟢 HTTPX client has been closed. Cannot continue with API requests.")
                    return best_result_list # Return whatever we have so far
                await asyncio.sleep(api_delay) # Generic delay for unexpected errors
                break # Stop processing for this keyword on unexpected error

            items_added_this_page = 0
            for item_idx, item in enumerate(data.get('items', [])):
                if len(current_keyword_results) >= max_items:
                    break
                processed_api_items += 1
                try:
                    title = re.sub(r"<.*?>", "", item.get("title", "")).strip()
                    price_str = item.get("lprice", "0")
                    price = int(price_str) if price_str.isdigit() else 0

                    if price <= 0:
                        logger.debug(f"🟢 Skipping item #{item_idx+1} (Keyword: '{query}') due to zero/invalid price: '{title}' (Price String: '{price_str}')")
                        continue

                    # --- Initial Similarity Check ---
                    title_tokens = tokenize_korean(title)
                    similarity = jaccard_similarity(original_query_tokens, title_tokens)
                    if similarity < initial_sim_threshold:
                        logger.debug(f"🟢 Skipping item #{item_idx+1} (Keyword: '{query}') due to low initial similarity ({similarity:.2f} < {initial_sim_threshold}): '{title}'")
                        continue
                    # --- End Initial Similarity Check ---

                    seller = item.get("mallName", "")
                    link = item.get("link", "")
                    image_url = item.get("image", "")
                    mall_product_url = item.get("productUrl", link) # Use link if productUrl missing

                    # Basic check for promotional items
                    is_promotional = any(promo.lower() in title.lower() or promo.lower() in seller.lower() for promo in promo_keywords)
                    if is_promotional:
                        logger.debug(f"🟢 Skipping promotional item #{item_idx+1} (Keyword: '{query}'): '{title}' (Seller: '{seller}')")
                        continue

                    # --- Data Extraction ---
                    product = {
                        'name': title,
                        'price': price,
                        'link': link,
                        'image_url': image_url,
                        'quantity': "1",
                        'mallName': seller,
                        'mallProductUrl': mall_product_url,
                        'initial_similarity': round(similarity, 3) # Store similarity for potential future use/logging
                    }
                    # --- End Data Extraction ---

                    # Optional: Reference price check (only logging for now)
                    if reference_price > 0:
                        price_diff_percent = ((price - reference_price) / reference_price) * 100
                        if 0 < price_diff_percent < 10: # Example: skip if price is less than 10% higher
                            logger.debug(f"🟢 Skipping item #{item_idx+1} (Keyword: '{query}') due to small price difference ({price_diff_percent:.2f}%): '{title}' (Price: {price}, Ref: {reference_price})")
                            # This skip might be too aggressive, consider removing or making configurable
                            # continue # <--- Temporarily disable aggressive skipping based on price diff

                    current_keyword_results.append(product)
                    items_added_this_page += 1
                    logger.debug(f"  -> Added item #{item_idx+1} (Sim: {similarity:.2f}): '{title[:50]}...' (Price: {price}, Seller: '{seller}')")

                except Exception as e:
                    logger.error(f"🟢 Error processing Naver item #{item_idx+1} (Keyword: '{query}'): {e}. Data: {item}", exc_info=True)
                    continue # Skip this item on error

            logger.debug(f"🟢 Processed {items_added_this_page}/{api_items_on_page} items from Naver page {page} for keyword '{query}'. Total results for keyword: {len(current_keyword_results)}/{max_items}")

            # Check if API says there are no more results or we fetched less than requested
            total_api_results = data.get("total", 0)
            current_start = params.get("start", 1)
            items_received_this_page = len(data.get("items", [])) # Use the actual count received
            if current_start + items_received_this_page > total_api_results or items_received_this_page < effective_display_count:
                 logger.debug(f"🟢 Stopping page loop for keyword '{query}': API indicates no more results or page returned fewer items ({items_received_this_page}) than requested ({effective_display_count}). (Start: {current_start}, Total API: {total_api_results})")
                 break # Stop fetching pages for this keyword

        # --- End of page loop for the current keyword ---
        logger.info(f"🟢 Finished API search for keyword '{query}'. Found {len(current_keyword_results)} relevant products.")

        # Update the best result list found so far
        if len(current_keyword_results) > len(best_result_list):
            logger.debug(f"🟢 Updating best Naver result set with {len(current_keyword_results)} items from keyword '{query}'.")
            best_result_list = current_keyword_results
        elif len(current_keyword_results) > 0 and not best_result_list:
             # If the first keyword gave some results, keep them even if subsequent keywords give more later (unless threshold met)
             logger.debug(f"🟢 Keeping first set of {len(current_keyword_results)} results from keyword '{query}' as initial best.")
             best_result_list = current_keyword_results

        # Check if we found enough results with this keyword
        if len(best_result_list) >= MIN_RESULTS_THRESHOLD_NAVER:
            logger.info(f"🟢 Found sufficient results ({len(best_result_list)} >= {MIN_RESULTS_THRESHOLD_NAVER}) with keyword '{query}'. Stopping keyword variations.")
            break # Stop trying other keywords

    # --- End of keyword loop ---
    if not best_result_list:
        logger.warning(f"🟢 No Naver results found for '{original_query}' after trying all keyword variations.")
    elif len(best_result_list) < MIN_RESULTS_THRESHOLD_NAVER:
         logger.warning(f"🟢 Could not find sufficient Naver results ({MIN_RESULTS_THRESHOLD_NAVER} needed) for '{original_query}' after trying {len(keywords_to_try)} variations. Max found: {len(best_result_list)} items.")
    else:
         logger.info(f"🟢 Naver API search finished for '{original_query}'. Final result count: {len(best_result_list)} items.")

    return best_result_list


async def crawl_naver_products(product_rows: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    Crawl product information from Naver Shopping using API asynchronously for multiple product rows,
    including image downloading and optional background removal.

    Args:
        product_rows (pd.DataFrame): DataFrame containing products to search for.
                                     Requires '상품명'. Optional '판매단가(V포함)', '구분'.
        config (configparser.ConfigParser): ConfigParser object containing configuration.

    Returns:
        pd.DataFrame: A DataFrame containing all original columns plus the crawled data columns
                      (like '네이버_상품명', '판매단가(V포함)(3)', '공급사명', etc.).
                      If no results are found for a row, corresponding columns will contain '-'.
    """
    if product_rows is None or len(product_rows) == 0:
        logger.info("🟢 Naver crawl: Input product_rows is empty or None. Skipping.")
        return pd.DataFrame()  # Return empty DataFrame

    total_products = len(product_rows)
    logger.info(f"🟢 --- Starting Naver product crawl for {total_products} products (Async) ---")

    # Get config values using ConfigParser methods
    try:
        image_target_dir = config.get('Paths', 'image_target_dir', fallback=None)
        use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
        # API keys checked within crawl_naver, no need to re-check here
        naver_scrape_limit = config.getint('ScraperSettings', 'naver_scrape_limit', fallback=50)
        max_concurrent_api = config.getint('ScraperSettings', 'naver_max_concurrent_api', fallback=3)
        logger.info(f"🟢 Naver API Configuration: Limit={naver_scrape_limit}, Max Concurrent API={max_concurrent_api}, BG Removal={use_bg_removal}, Image Dir={image_target_dir}")
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        logger.error(f"Error reading required configuration for Naver crawl: {e}. Aborting Naver crawl.", exc_info=True)
        return pd.DataFrame() # Return empty DF on config error

    # Create a semaphore to limit concurrent API requests
    api_semaphore = asyncio.Semaphore(max_concurrent_api)
    logger.debug(f"API Semaphore initialized with limit: {max_concurrent_api}")

    # --- Prepare and run API search tasks concurrently ---
    api_search_tasks = []
    processed_indices = set() # Keep track of indices being processed

    async with get_async_httpx_client(config=config) as client:
        logger.debug("Async HTTPX client created for Naver API calls.")
        # Create tasks for each row
        for idx in product_rows.index:
            if idx in processed_indices:
                 logger.warning(f"Skipping duplicate index {idx} found in input DataFrame.")
                 continue
            processed_indices.add(idx)

            row = product_rows.loc[idx]
            product_name = row.get('상품명', '')
            if not product_name or pd.isna(product_name):
                logger.warning(f"🟢 Skipping row index {idx}: Missing or invalid product name ('{product_name}').")
                continue

            # Calculate reference price safely
            reference_price = 0.0
            ref_price_val = row.get('판매단가(V포함)')
            if pd.notna(ref_price_val):
                try:
                    # Remove commas and convert
                    reference_price = float(str(ref_price_val).replace(',', ''))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse reference price '{ref_price_val}' for index {idx}. Using 0.", exc_info=True)
                    reference_price = 0.0

            logger.debug(f"Creating API search task for index {idx}, product: '{product_name}', ref_price: {reference_price}")
            api_search_tasks.append(
                asyncio.create_task(
                    _run_single_naver_search(idx, row, product_name, row.get('구분', 'A'), reference_price, client, config, naver_scrape_limit, api_semaphore)
                )
            )

        # Gather all API search results
        logger.info(f"Gathering results from {len(api_search_tasks)} API search tasks...")
        start_gather_time = time.monotonic()
        # Use return_exceptions=True to handle potential errors in tasks
        api_results_raw: List[Union[Tuple[int, pd.Series, str, Optional[List[Dict]]], Exception]] = await asyncio.gather(*api_search_tasks, return_exceptions=True)
        gather_time = time.monotonic() - start_gather_time
        logger.info(f"Finished gathering API results in {gather_time:.2f} seconds.")

        # --- Process API Results and Prepare Image Downloads ---
        logger.info("Processing API results and preparing image downloads...")
        image_tasks = []
        image_info_map: Dict[asyncio.Task, Tuple[int, str]] = {} # Map task to (index, target_path)
        processed_results: Dict[int, Dict[str, Any]] = {} # Store final data indexed by original DataFrame index
        successful_api_calls = 0
        failed_api_calls = 0

        for idx_in_results, result_or_exc in enumerate(api_results_raw):
            if isinstance(result_or_exc, Exception):
                 logger.error(f"🟢 API Task {idx_in_results} failed with exception: {result_or_exc}", exc_info=result_or_exc)
                 failed_api_calls += 1
                 # Need to find the original index if possible, maybe pass index to gather?
                 # For now, we can't map this failure back to a specific row easily without more info.
                 continue # Skip processing this failed task

            # Unpack the successful result
            idx, original_row, product_type, naver_data = result_or_exc
            successful_api_calls += 1
            logger.debug(f"Processing result for index {idx}...")

            # Placeholder for the final data for this row
            row_output_data = {
                'original_row': original_row.to_dict(), # Store the original row data
                '네이버_상품명': '-',
                '기본수량(3)': '-',
                '판매단가(V포함)(3)': '-',
                # Price difference columns are removed from here, calculated later
                # '가격차이(3)': '-',
                # '가격차이(3)(%)': '-',
                '공급사명': '-',
                '네이버 쇼핑 링크': '-',
                '공급사 상품링크': '-',
                '네이버 이미지': '-'
            }

            if naver_data: # If API returned results for this product
                # Use the first result from the Naver API data
                first_item = naver_data[0]
                logger.debug(f"  -> Found {len(naver_data)} Naver items for index {idx}. Using first: '{first_item.get('name', 'N/A')[:50]}...'")

                # Populate the output data dictionary
                row_output_data['네이버_상품명'] = first_item.get('name', '-')
                row_output_data['기본수량(3)'] = first_item.get('quantity', '-') # Still default '1' from API
                row_output_data['판매단가(V포함)(3)'] = str(first_item.get('price', '-'))
                row_output_data['공급사명'] = first_item.get('mallName', '-')
                row_output_data['네이버 쇼핑 링크'] = first_item.get('link', '-')
                row_output_data['공급사 상품링크'] = first_item.get('mallProductUrl', '-')

                # Prepare image download task for the first item's image
                image_url = first_item.get('image_url')
                if image_url and image_target_dir:
                    try:
                        # Create a unique filename based on URL hash
                        url_hash = hashlib.md5(image_url.encode()).hexdigest()[:10]
                        # Try to get extension, default to .jpg
                        file_ext = os.path.splitext(urlparse(image_url).path)[1]
                        if not file_ext or len(file_ext) > 5: # Basic sanity check for extension
                            file_ext = '.jpg'
                        target_filename = f"naver_{url_hash}{file_ext}"
                        target_path = os.path.join(image_target_dir, target_filename)

                        logger.debug(f"  -> Preparing image download task for index {idx}: URL='{image_url[:50]}...', Target='{target_path}'")
                        img_task = asyncio.create_task(
                            download_image_async(image_url, target_path, client, config=config)
                        )
                        image_tasks.append(img_task)
                        image_info_map[img_task] = (idx, target_path) # Store original index and target path
                    except Exception as e:
                        logger.error(f"Error preparing image download task for index {idx}, URL '{image_url}': {e}", exc_info=True)
                elif not image_url:
                     logger.debug(f"  -> No image URL found for first item of index {idx}.")
                elif not image_target_dir:
                     logger.warning(f"  -> Image target directory not set. Skipping image download for index {idx}.")

            else: # No results found by API for this product
                logger.debug(f"  -> No Naver items found for index {idx}.")
                # Keep the placeholder '-' values in row_output_data

            # Store the processed data (with or without API results) for this index
            processed_results[idx] = row_output_data

        logger.info(f"Processed {successful_api_calls} successful API calls, {failed_api_calls} failed API calls.")
        logger.info(f"Prepared {len(image_tasks)} image download tasks.")

        # --- Wait for Image Downloads and Optional Background Removal ---
        if image_tasks:
            logger.info(f"Waiting for {len(image_tasks)} image downloads to complete...")
            start_img_time = time.monotonic()
            # Use return_exceptions=True for image downloads as well
            image_results_raw: List[Union[bool, Exception]] = await asyncio.gather(*image_tasks, return_exceptions=True)
            img_time = time.monotonic() - start_img_time
            logger.info(f"Finished image downloads in {img_time:.2f} seconds.")

            successful_downloads = 0
            failed_downloads = 0
            bg_removal_tasks = []
            bg_removal_info_map: Dict[asyncio.Task, Tuple[int, str]] = {} # Map task to (index, bg_removed_path)

            for task, result_or_exc in zip(image_tasks, image_results_raw):
                original_idx, target_path = image_info_map[task] # Get original index and path

                if isinstance(result_or_exc, Exception):
                    logger.error(f"Image download failed for index {original_idx}, target '{target_path}': {result_or_exc}", exc_info=result_or_exc)
                    failed_downloads += 1
                    continue # Skip processing this failed download

                if result_or_exc: # If download succeeded (result is True)
                    successful_downloads += 1
                    logger.debug(f"Image downloaded successfully for index {original_idx} to '{target_path}'")
                    # Update the image path in the results dict immediately
                    if original_idx in processed_results:
                        processed_results[original_idx]['네이버 이미지'] = target_path

                        # Handle background removal if enabled and download succeeded
                        if use_bg_removal:
                            try:
                                # Create path for background removed image (use PNG format)
                                bg_removed_path = target_path.replace('.jpg', '_no_bg.png').replace('.jpeg', '_no_bg.png').replace('.gif', '_no_bg.png')
                                if bg_removed_path == target_path: # Ensure filename changes if extension wasn't common
                                    base, _ = os.path.splitext(target_path)
                                    bg_removed_path = base + '_no_bg.png'

                                logger.debug(f"  -> Preparing background removal task for index {original_idx}: Source='{target_path}', Target='{bg_removed_path}'")
                                bg_task = asyncio.create_task(
                                    remove_background_async(target_path, bg_removed_path)
                                )
                                bg_removal_tasks.append(bg_task)
                                bg_removal_info_map[bg_task] = (original_idx, bg_removed_path)
                            except Exception as e:
                                logger.error(f"Error preparing background removal task for index {original_idx}: {e}", exc_info=True)
                    else:
                         logger.warning(f"Downloaded image for index {original_idx}, but no corresponding entry found in processed_results.")
                else: # Download function returned False (should ideally not happen if it raises exceptions)
                    logger.warning(f"Image download reported failure (returned False) for index {original_idx}, target '{target_path}'.")
                    failed_downloads += 1

            logger.info(f"{successful_downloads} images downloaded successfully, {failed_downloads} failed.")

            # Wait for background removal tasks if any
            if bg_removal_tasks:
                logger.info(f"Waiting for {len(bg_removal_tasks)} background removal tasks...")
                start_bg_time = time.monotonic()
                bg_results_raw = await asyncio.gather(*bg_removal_tasks, return_exceptions=True)
                bg_time = time.monotonic() - start_bg_time
                logger.info(f"Finished background removal tasks in {bg_time:.2f} seconds.")

                successful_bg = 0
                failed_bg = 0
                for task, result_or_exc in zip(bg_removal_tasks, bg_results_raw):
                    original_idx, bg_removed_path = bg_removal_info_map[task]

                    if isinstance(result_or_exc, Exception):
                        logger.error(f"Background removal failed for index {original_idx}, target '{bg_removed_path}': {result_or_exc}", exc_info=result_or_exc)
                        failed_bg += 1
                        continue

                    if result_or_exc: # Background removal succeeded
                        successful_bg += 1
                        logger.debug(f"Background removed successfully for index {original_idx}, saved to '{bg_removed_path}'")
                        # Update the image path in results to the background-removed version
                        if original_idx in processed_results:
                            processed_results[original_idx]['네이버 이미지'] = bg_removed_path
                        else:
                            logger.warning(f"Removed background for index {original_idx}, but no corresponding entry found in processed_results.")
                    else: # Background removal returned False
                        logger.warning(f"Background removal reported failure (returned False) for index {original_idx}, target '{bg_removed_path}'.")
                        failed_bg += 1
                logger.info(f"{successful_bg} backgrounds removed successfully, {failed_bg} failed.")


    # --- Convert Processed Results to DataFrame ---
    logger.info("Converting processed results to DataFrame...")
    final_results_list = []
    # Iterate through the original DataFrame's index to maintain order
    for idx in product_rows.index:
        if idx in processed_results:
            final_results_list.append(processed_results[idx])
        else:
            # This case handles rows that were initially skipped (e.g., missing product name)
            # or rows where the API task failed entirely before reaching processed_results.
            logger.warning(f"No processed result found for original index {idx}. Adding placeholder.")
            final_results_list.append({
                    'original_row': product_rows.loc[idx].to_dict() if idx in product_rows.index else {'상품명': 'Unknown - Skipped Index'},
                    '네이버_상품명': '-',
                    '기본수량(3)': '-',
                    '판매단가(V포함)(3)': '-',
                    # '가격차이(3)': '-', # Removed
                    # '가격차이(3)(%)': '-', # Removed
                    '공급사명': '-',
                    '네이버 쇼핑 링크': '-',
                    '공급사 상품링크': '-',
                    '네이버 이미지': '-'
                })

    if not final_results_list:
         logger.warning("No results were processed. Returning empty DataFrame.")
         return pd.DataFrame()

    # Create the final DataFrame
    final_df = pd.DataFrame(final_results_list)

    # Ensure all required columns exist, adding placeholders if necessary
    # Price diff columns are intentionally excluded here
    required_output_columns = ['original_row', '네이버_상품명', '기본수량(3)', '판매단가(V포함)(3)',
                                '공급사명', '네이버 쇼핑 링크', '공급사 상품링크', '네이버 이미지']
    missing_cols = []
    for col in required_output_columns:
        if col not in final_df.columns:
             missing_cols.append(col)
             final_df[col] = '-' # Add missing column with default value

    if missing_cols:
        logger.warning(f"The following required columns were missing in the final DataFrame and were added: {missing_cols}")

    logger.info(f"Naver crawl finished. Returning DataFrame with {len(final_df)} rows and columns: {final_df.columns.tolist()}")
    # Return only the required columns in the specified order
    return final_df[required_output_columns]


async def _run_single_naver_search(idx: int, row: pd.Series, product_name: str, product_type: str, reference_price: float, client: httpx.AsyncClient, config: configparser.ConfigParser, naver_scrape_limit: int, api_semaphore: asyncio.Semaphore) -> Tuple[int, pd.Series, str, Optional[List[Dict]]]:
    """ Helper coroutine to handle the logic for a single product's Naver API search. """
    task_id = f"Task-{idx}" # Use index for task identification
    logger.debug(f"[{task_id}] Acquiring semaphore for Naver search: '{product_name}'...")
    async with api_semaphore:
        logger.debug(f"[{task_id}] Semaphore acquired. Starting Naver search for '{product_name}'.")
        start_time = time.monotonic()
        try:
            api_results = await crawl_naver(
                original_query=product_name,
                client=client,
                config=config,
                max_items=naver_scrape_limit,
                reference_price=reference_price
            )
            elapsed_time = time.monotonic() - start_time
            logger.debug(f"[{task_id}] Completed Naver search for '{product_name}' in {elapsed_time:.2f}s. Found {len(api_results) if api_results else 0} items.")
            # Return index, original row data, type, and the API results list
            return idx, row, product_type, api_results
        except Exception as e:
            elapsed_time = time.monotonic() - start_time
            logger.error(f"🟢 [{task_id}] Error during Naver search task for '{product_name}' after {elapsed_time:.2f}s: {e}", exc_info=True)
            # Ensure we return the index and row even on failure, but with None for results
            # This allows the main function to know which task failed.
            return idx, row, product_type, None
        finally:
             logger.debug(f"[{task_id}] Releasing semaphore for '{product_name}'.")


# --- Test block Updated for Async ---
async def _test_main():
    # Setup basic logging for the test
    # Keep the level at INFO for production, DEBUG for development testing
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s:%(name)s:%(lineno)d - %(message)s')
    # Silence httpx logs unless they are warnings or errors to reduce noise
    logging.getLogger("httpx").setLevel(logging.WARNING)
    print("--- Running Naver API Test ---")
    logger.info("--- Running Naver API Test (Async) ---")

    # Use the actual load_config function
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Assume config.ini is in the parent directory of PythonScript
    config_path = os.path.join(script_dir, '..', 'config.ini')

    # Import load_config from utils (or execution_setup if preferred)
    try:
        # Load config using the utility function
        config = load_config(config_path)
        print(f"Config loaded from: {config_path}")
        logger.info(f"Config loaded from: {config_path}")
    except Exception as e:
        print(f"ERROR loading config from '{config_path}': {e}")
        logger.error(f"Failed to load config from '{config_path}': {e}", exc_info=True)
        return

    if not config or not config.sections():
        print(f"ERROR: No sections found in config file or config loading failed: {config_path}")
        logger.error(f"Failed to load or parse config file at: {config_path}. Test cannot run.")
        return

    # Check essential keys for the test
    client_id = config.get('API_Keys', 'naver_client_id', fallback=None)
    client_secret = config.get('API_Keys', 'naver_client_secret', fallback=None)

    if not client_id or not client_secret:
        print("ERROR: Naver API credentials missing in config.ini [API_Keys] section!")
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logger.error("!!! Naver API credentials missing in [API_Keys] section of config.ini.")
        logger.error("!!! Test cannot run without valid credentials.")
        logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return

    # Print the API credentials (masked)
    print(f"Using Naver client_id: {client_id[:4]}... (length: {len(client_id)})")
    print(f"Using Naver client_secret: {client_secret[:4]}... (length: {len(client_secret)})")
    logger.info(f"Test will use Naver client_id: {client_id[:4]}...")
    logger.info(f"Test will use Naver client_secret: {client_secret[:4]}...")

    # Verify API keys directly with a simple request
    print("Testing Naver API keys directly...")
    logger.info("Testing Naver API keys directly...")

    # Use a fresh client for initial API test
    async with get_async_httpx_client(config=config) as client:
        try:
            api_url = "https://openapi.naver.com/v1/search/shop.json"
            headers = {
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
                "Accept": "application/json",
            }
            params = {"query": "테스트", "display": 1} # Simple test query

            print(f"Sending test request to Naver API...")
            logger.debug("Sending API key test request...")
            response = await client.get(api_url, headers=headers, params=params)
            status_code = response.status_code
            print(f"Naver API response status: {status_code}")
            logger.info(f"API key test response status: {status_code}")

            if status_code == 200:
                print(f"✅ Naver API key test successful!")
                logger.info(f"✅ Naver API key test successful!")
                try:
                    data = response.json()
                    total_results = data.get('total', 0)
                    print(f"Test search found {total_results} total results for query '테스트'")
                    logger.info(f"Test search found {total_results} total results for query '테스트'")
                except json.JSONDecodeError:
                    logger.error("API key test: Successful status code (200) but failed to decode JSON response.")
                    print("Error decoding JSON response from API key test.")
            else:
                error_text = response.text[:200] + "..." if len(response.text) > 200 else response.text
                print(f"⛔ Naver API key test failed! Status: {status_code}")
                print(f"Error response snippet: {error_text}")
                logger.error(f"⛔ Naver API key test failed! Status: {status_code}")
                logger.error(f"Error response snippet: {error_text}")
                if status_code == 401:
                    print("⛔ API authentication failed (401). Check that your API keys are correct in config.ini.")
                    logger.error("⛔ API authentication failed (401). Check that your API keys are correct in config.ini.")
                elif status_code == 429:
                    print("⛔ API rate limit exceeded during test (429). Wait before making more requests.")
                    logger.error("⛔ API rate limit exceeded during test (429).")
                # Don't exit immediately, allow the main test to run if desired
                # return
        except httpx.RequestError as req_err:
            print(f"⛔ API key test request failed with HTTPX exception: {req_err}")
            logger.error(f"⛔ API key test request failed with HTTPX exception: {req_err}", exc_info=True)
            return # Cannot proceed if basic connection fails
        except Exception as e:
            print(f"⛔ API key test failed with unexpected exception: {e}")
            logger.error(f"⛔ API key test failed with unexpected exception: {e}", exc_info=True)
            return # Cannot proceed

    # Ensure test image directory exists using config or fallback
    default_img_dir = os.path.join(script_dir, '..', 'naver_test_images')
    test_image_dir = config.get('Paths', 'image_target_dir', fallback=default_img_dir)

    # Create the directory if it doesn't exist
    if not os.path.exists(test_image_dir):
        try:
             os.makedirs(test_image_dir)
             logger.info(f"Created test image directory: {test_image_dir}")
        except OSError as e:
             # Log error but continue, image download might fail later
             logger.error(f"Could not create test image directory {test_image_dir}: {e}. Image download might fail.")

    # Test products list (from user example)
    test_products = [
        "사랑이 엔젤하트 투포켓 에코백",
        "사랑이 큐피트화살 투포켓 에코백",
        "행복이 스마일플라워 투포켓 에코백",
        "행운이 네잎클로버 투포켓 에코백",
        "캐치티니핑 53 스무디 입체리본 투명 아동우산",
        "아테스토니 뱀부사 소프트 3P 타올 세트"
    ]

    # Create test DataFrame with reference prices
    test_data = {
        '구분': ['A'] * len(test_products),
        '담당자': ['테스트'] * len(test_products),
        '업체명': ['테스트업체'] * len(test_products), # Added 업체명
        '업체코드': ['T001'] * len(test_products), # Added 업체코드
        'Code': [f'CODE{i+1:03d}' for i in range(len(test_products))], # Added Code
        '중분류카테고리': ['테스트카테고리'] * len(test_products), # Added 카테고리
        '상품명': test_products,
        '기본수량(1)': [1] * len(test_products), # Added 기본수량(1)
        '판매단가(V포함)': [15000, 3000, 500, 300, 15000, 20000], # Example reference prices
        '본사상품링크': ['http://example.com/product{i+1}' for i in range(len(test_products))] # Added 본사상품링크
    }
    test_df = pd.DataFrame(test_data)

    print(f"Testing Naver API with {len(test_df)} products...")
    logger.info(f"Testing Naver API with {len(test_df)} products using DataFrame:")
    logger.info(test_df.to_string()) # Log the test data

    try:
        # Test each product individually first (optional, good for debugging)
        print("Testing individual products via crawl_naver...")
        logger.info("--- Testing individual products via crawl_naver ---")
        individual_results = {}
        async with get_async_httpx_client(config=config) as direct_client:
            for idx, row in test_df.iterrows():
                product = row['상품명']
                ref_price = row['판매단가(V포함)']
                print(f"Testing direct Naver API call for '{product}' (Ref Price: {ref_price})...")
                logger.info(f"Testing direct crawl_naver for '{product}' (Ref Price: {ref_price})")
                direct_results = await crawl_naver(
                    original_query=product,
                    client=direct_client,
                    config=config,
                    max_items=5, # Limit results for individual test
                    reference_price=ref_price
                )
                individual_results[product] = direct_results

                if direct_results:
                    print(f"✅ Direct Naver API call successful! Found {len(direct_results)} results")
                    logger.info(f"Direct call for '{product}' successful. Found {len(direct_results)} results.")
                    for i, item in enumerate(direct_results[:3], 1):  # Show first 3 results
                        print(f"  Result {i}: Name='{item.get('name', 'N/A')[:50]}...', Price=₩{item.get('price', 'N/A')}, Seller='{item.get('seller', 'N/A')}'")
                        logger.debug(f"  Result {i}: {item}")
                else:
                    print(f"⛔ Direct Naver API call returned no results for '{product}'")
                    logger.warning(f"Direct call for '{product}' returned no results.")

                # Add delay between requests to avoid rate limits during testing
                test_api_delay = config.getfloat('ScraperSettings', 'naver_api_delay', fallback=1.0)
                logger.debug(f"Adding test delay of {test_api_delay:.2f}s")
                await asyncio.sleep(test_api_delay)

        # Now test the full crawl_naver_products function
        print("--- Testing full crawl_naver_products function ---")
        logger.info("--- Testing full crawl_naver_products function ---")
        start_full_crawl_time = time.monotonic()
        results_df = await crawl_naver_products(
            product_rows=test_df.copy(), # Pass a copy to avoid modification issues
            config=config
        )
        full_crawl_time = time.monotonic() - start_full_crawl_time
        logger.info(f"crawl_naver_products completed in {full_crawl_time:.2f} seconds.")

        print(f"--- Test Results (crawl_naver_products processed {len(results_df)} rows) ---")
        logger.info(f"--- Test Results (crawl_naver_products processed {len(results_df)} rows) ---")
        logger.info(f"Result DataFrame columns: {results_df.columns.tolist()}")
        # Log the first few rows of the result DataFrame for inspection
        logger.info("Result DataFrame head:")
        logger.info(results_df.head().to_string())


        # Check if the DataFrame is empty or has the expected columns
        if results_df.empty:
            print("ERROR: results_df is empty!")
            logger.error("Test resulted in an empty DataFrame from crawl_naver_products.")
            rows_with_data = 0
        # Check for a key column expected from the processing
        elif '네이버_상품명' not in results_df.columns:
            print("ERROR: '네이버_상품명' column is missing in results_df!")
            logger.error("Test resulted in a DataFrame missing the '네이버_상품명' column.")
            print(f"Available columns: {results_df.columns.tolist()}")
            rows_with_data = 0
        else:
            # Count how many rows have actual Naver data (not just '-')
            # Ensure '네이버_상품명' exists before accessing
            rows_with_data = sum(1 for x in results_df['네이버_상품명'] if x != '-' and pd.notna(x))
            print(f"Results with actual Naver data in '네이버_상품명': {rows_with_data}/{len(results_df)}")
            logger.info(f"Results with actual Naver data in '네이버_상품명': {rows_with_data}/{len(results_df)}")

        # Log example data for each product from the final DataFrame
        for idx, row in results_df.iterrows():
            try:
                # Safely get original product name from the 'original_row' dictionary
                original_row_data = row.get('original_row', {})
                original_name = original_row_data.get('상품명', 'Unknown Original Name') if isinstance(original_row_data, dict) else 'Original Row Data Missing/Invalid'

                # Safely get Naver data, defaulting to '-' if column missing or value is null/NaN
                naver_name = row.get('네이버_상품명', '-')
                naver_price = row.get('판매단가(V포함)(3)', '-') # Use the correct output column name
                naver_seller = row.get('공급사명', '-')          # Use the correct output column name
                naver_image = row.get('네이버 이미지', '-')

                print(f"Processed Row {idx}: Original Product='{original_name}'")
                logger.info(f"Processed Row {idx}: Original Product='{original_name}'")
                if naver_name != '-' and pd.notna(naver_name):
                    print(f"  Naver Match: {naver_name}")
                    print(f"  Price: ₩{naver_price}")
                    print(f"  Seller: {naver_seller}")
                    print(f"  Image Path: {naver_image}")
                    logger.info(f"  -> Match: '{naver_name}' - Price: ₩{naver_price} - Seller: '{naver_seller}' - Image: '{naver_image}'")
                else:
                    print(f"  No Naver results found or populated for this row.")
                    logger.warning(f"  -> No Naver results found or populated for '{original_name}' (Index {idx})")

            except KeyError as ke:
                 logger.error(f"KeyError processing test result row {idx}: Missing key {ke}. Row data: {row.to_dict()}", exc_info=True)
                 print(f"KeyError processing row {idx}: {ke}. Check logs.")
                 continue # Skip to next row on key error
            except Exception as e:
                 logger.error(f"Error processing test result row {idx} ('{original_name}'): {e}", exc_info=True)
                 print(f"Error processing row {idx}: {e}. Check logs.")
                 continue # Skip to next row on other errors

        # Final success/failure assessment based on whether *any* data was found
        if rows_with_data == 0 and not results_df.empty:
            print("⛔ TEST FAILED: No data was returned for any products in the final DataFrame!")
            logger.error("⛔ TEST FAILED: No data was returned for any products in the final DataFrame!")
        elif results_df.empty:
             print("⛔ TEST FAILED: The final DataFrame was empty.")
             logger.error("⛔ TEST FAILED: The final DataFrame was empty.")
        else:
            print(f"✅ TEST COMPLETED: Data was returned for {rows_with_data} products.")
            logger.info(f"✅ TEST COMPLETED: Data was returned for {rows_with_data} products.")

    except Exception as e:
        print(f"An error occurred during the async test run: {e}")
        logger.error(f"An error occurred during the async test run: {e}", exc_info=True)

    logger.info("--- Naver API Test (Async) Finished ---")
    print("--- Naver API Test (Async) Finished ---")

if __name__ == "__main__":
    # Set up basic logging for when run as a script
    # Keep the level at INFO for production, DEBUG for development
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s:%(name)s:%(lineno)d - %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING) # Reduce httpx verbosity
    print("Running Naver API test as main script...")

    # Load config and run the async main test function
    asyncio.run(_test_main())
