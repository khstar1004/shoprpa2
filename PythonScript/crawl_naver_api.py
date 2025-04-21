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

# Import based on how the file is run
try:
    # When imported as module
    from .utils import download_image_async, get_async_httpx_client, generate_keyword_variations, load_config
    from .image_utils import remove_background_async
except ImportError:
    # When run directly as script
    from utils import download_image_async, get_async_httpx_client, generate_keyword_variations, load_config
    from image_utils import remove_background_async

# Note: CONFIG will be passed as config_dict
MIN_RESULTS_THRESHOLD_NAVER = 5 # Minimum desired results for Naver API

async def crawl_naver(original_query, client: httpx.AsyncClient, config: configparser.ConfigParser, max_items=50, reference_price=0):
    """
    Search products using Naver Shopping API, trying multiple keyword variations.

    Args:
        original_query: The original search term.
        client: Async HTTPX client for making requests.
        config: ConfigParser object containing configuration.
        max_items: Maximum number of items to return per keyword attempt.
        reference_price: Reference price for filtering.

    Returns:
        List of product dictionaries from the first keyword variation yielding enough results,
        or the results from the last attempted keyword.
    """
    try:
        client_id = config.get('API_Keys', 'naver_client_id', fallback='')
        client_secret = config.get('API_Keys', 'naver_client_secret', fallback='')
    except (configparser.NoSectionError, configparser.NoOptionError):
         logging.error("Naver API keys not found in [API_Keys] section of config. Cannot perform search.")
         return []
         
    client_id_display = (client_id[:4] + '...') if client_id else 'Not Set'
    client_secret_display = (client_secret[:4] + '...') if client_secret else 'Not Set'
    logging.info(f"🟢 Naver API Credentials: Client ID starts with '{client_id_display}', Secret starts with '{client_secret_display}'")

    # Get delay between API calls
    api_delay = config.getfloat('ScraperSettings', 'naver_api_delay', fallback=1.0)

    # Generate keywords to try
    keywords_to_try = generate_keyword_variations(original_query)
    logging.info(f"🟢 Generated Naver keywords for '{original_query}': {keywords_to_try}")

    best_result_list = [] # Store results from the most successful keyword attempt

    for query in keywords_to_try:
        logging.info(f"🟢 --- Trying Naver keyword variation: '{query}' ---")
        current_keyword_results = []
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
                logging.debug(f"Reached max_items ({max_items}) limit for keyword '{query}', stopping API calls.")
                break

            api_display_count = 100
            start_index = (page - 1) * api_display_count + 1
            effective_display_count = min(api_display_count, max_items - len(current_keyword_results))
            if effective_display_count <= 0:
                 break

            params = {"query": query, "display": effective_display_count, "start": start_index, "sort": "asc", "exclude": "used:rental"}
            logging.debug(f"🟢 Naver API Request (Keyword: '{query}', Page {page}, Sort: 'asc'): Params={params}")

            # Add delay before API call to avoid hitting rate limits
            if page > 1 or query != keywords_to_try[0]:
                logging.debug(f"🟢 Adding delay of {api_delay} seconds before Naver API request")
                await asyncio.sleep(api_delay)

            try:
                logging.info(f"🟢 Sending Naver API request for '{query}'")
                response = await client.get(api_url, headers=headers, params=params)
                status_code = response.status_code
                logging.info(f"🟢 Naver API response status: {status_code}")
                
                if status_code != 200:
                    error_text = response.text[:200] + "..." if len(response.text) > 200 else response.text
                    logging.error(f"🟢 Naver API error response (Status: {status_code}): {error_text}")
                    if status_code == 429:
                        logging.error(f"🟢 Rate limit exceeded (429). Waiting longer before next request.")
                        await asyncio.sleep(api_delay * 3)
                    continue
                
                response.raise_for_status()
                data = response.json()
                total_items = data.get('total', 0)
                api_items_on_page = len(data.get('items', []))
                logging.info(f"🟢 Naver API Response (Keyword: '{query}', Page {page}): Found {total_items} total items, received {api_items_on_page} on this page. Status: {response.status_code}")

                if 'items' not in data or not data.get('items'):
                    logging.warning(f"🟢 Naver API returned no items for '{query}' (Page {page})")
                    if 'errorMessage' in data:
                        logging.error(f"🟢 Naver API error message: {data.get('errorMessage')}")
                    
                    # Log the full response for debugging
                    logging.debug(f"🟢 Full Naver API response: {json.dumps(data, ensure_ascii=False)[:500]}")
                    break

            except Exception as e:
                logging.error(f"🟢 Error during Naver API request (Keyword: '{query}', Page {page}): {e}", exc_info=True)
                
                if isinstance(e, RuntimeError) and "client has been closed" in str(e):
                    logging.error(f"🟢 HTTPX client has been closed. Cannot continue with API requests.")
                    return best_result_list
                    
                # Handle timeouts with backoff
                if "timeout" in str(e).lower():
                    logging.error(f"🟢 Timeout during Naver API request. Waiting before next attempt.")
                    await asyncio.sleep(api_delay * 2)  # Double delay on timeout
                
                # Handle rate limits
                if hasattr(e, 'response') and getattr(e.response, 'status_code', 0) == 429:
                    logging.error(f"🟢 Rate limit exceeded (429). Waiting longer before next request.")
                    await asyncio.sleep(api_delay * 5)  # Increased backoff on rate limit
                
                # For other errors, just delay a bit and continue to next page/keyword
                await asyncio.sleep(api_delay)
                break

            if 'items' not in data or not data['items']:
                logging.debug(f"🟢 No more Naver API results found on page {page} for keyword '{query}'.")
                break

            items_added_this_page = 0
            for item in data['items']:
                if len(current_keyword_results) >= max_items:
                    break
                processed_api_items += 1
                try:
                    title = re.sub(r"<.*?>", "", item.get("title", ""))
                    price_str = item.get("lprice", "0")
                    price = int(price_str) if price_str.isdigit() else 0
                    if price <= 0:
                        logging.debug(f"🟢 Skipping item (Keyword: '{query}') due to zero or invalid price: '{title}' (Price: {price_str})")
                        continue
                    seller = item.get("mallName", "")
                    is_promotional = any(promo.lower() in title.lower() or promo.lower() in seller.lower() for promo in promo_keywords)

                    # --- Enhanced Data Extraction ---
                    product = {
                        'name': title,
                        'price': price,
                        'link': item.get("link", ""),
                        'image_url': item.get("image", ""),
                        'seller': seller,
                        'is_promotional': is_promotional,
                        'product_id': item.get("productId", ""),
                        'category1': item.get("category1", ""),
                        'category2': item.get("category2", ""),
                        'category3': item.get("category3", ""),
                        'category4': item.get("category4", ""),
                        'maker': item.get("maker", ""),
                        'brand': item.get("brand", ""),
                        'hprice': int(item.get("hprice", "0")) if item.get("hprice", "0").isdigit() else 0,
                        'reviewCount': int(item.get("reviewCount", "0")) if item.get("reviewCount", "0").isdigit() else 0,
                        'scoreInfo': item.get("scoreInfo", ""), # Typically a string or could be dict, keep as is
                        'openDate': item.get("openDate", ""), # YYYYMMDD format
                    }
                    # --- End Enhanced Data Extraction ---

                    if reference_price > 0:
                        price_diff_percent = ((price - reference_price) / reference_price) * 100
                        if 0 < price_diff_percent < 10:
                            logging.debug(f"🟢 Skipping item (Keyword: '{query}') due to small price difference ({price_diff_percent:.2f}%): '{title}' (Price: {price}, Ref: {reference_price})")
                            continue
                            
                    current_keyword_results.append(product)
                    items_added_this_page += 1
                except Exception as e:
                    logging.error(f"🟢 Error processing Naver item: {e}. Data: {item}", exc_info=True)
                    continue

            logging.debug(f"🟢 Processed {items_added_this_page}/{api_items_on_page} items from Naver page {page} for keyword '{query}'.")
            if len(current_keyword_results) >= max_items:
                 break
            total_results = data.get("total", 0)
            current_start = params.get("start", 1)
            current_items_received = len(data.get("items", []))
            if current_start + current_items_received > total_results or current_items_received < effective_display_count:
                 logging.debug(f"🟢 Stopping page loop for keyword '{query}': API indicates no more results or page returned fewer items than requested. (Start: {current_start}, Received: {current_items_received}, Total: {total_results})")
                 break # Stop if API indicates no more results

        # --- End of page loop for the current keyword --- 
        logging.info(f"🟢 Finished API search for keyword '{query}'. Found {len(current_keyword_results)} relevant products.")
        
        # Update the best result list found so far
        if len(current_keyword_results) > len(best_result_list):
            best_result_list = current_keyword_results
            logging.debug(f"🟢 Updating best Naver result with {len(best_result_list)} items from keyword '{query}'.")

        # Check if we found enough results with this keyword
        if len(best_result_list) >= MIN_RESULTS_THRESHOLD_NAVER:
            logging.info(f"🟢 Found sufficient results ({len(best_result_list)}) with keyword '{query}'. Stopping keyword variations.")
            break # Stop trying other keywords

    # --- End of keyword loop --- 
    if len(best_result_list) < MIN_RESULTS_THRESHOLD_NAVER:
         logging.warning(f"🟢 Could not find sufficient Naver results ({MIN_RESULTS_THRESHOLD_NAVER} needed) for '{original_query}' after trying variations. Max found: {len(best_result_list)} items.")
    else:
         logging.info(f"🟢 Naver API search finished for '{original_query}'. Final result count: {len(best_result_list)} items.")
    
    # Log final result information
    logging.info(f"🟢 Returning {len(best_result_list)} Naver results for '{original_query}'")
    if not best_result_list:
        logging.warning(f"🟢 No Naver results found for '{original_query}' after trying all keyword variations!")
         
    return best_result_list

async def crawl_naver_products(product_rows: pd.DataFrame, config: configparser.ConfigParser):
    """
    Crawl product information from Naver Shopping using API asynchronously for multiple product rows,
    including image downloading and optional background removal.

    Args:
        product_rows (pd.DataFrame): DataFrame containing products to search for.
                                     Requires '상품명'. Optional '판매단가(V포함)', '구분'.
        config (configparser.ConfigParser): ConfigParser object containing configuration.

    Returns:
        list: A list of dictionaries, each containing 'original_row', 'naver_data', 'product_type'.
              'naver_data' is a list of product dictionaries from the API search,
              including an 'image_path' key if download was successful.
    """
    results_map = {} # Use map for easier updates: {index: result_dict}
    total_products = len(product_rows)
    
    # Get config values using ConfigParser methods
    try:
        image_target_dir = config.get('Paths', 'image_target_dir', fallback=None)
        use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
        naver_client_id = config.get('API_Keys', 'naver_client_id', fallback='')
        naver_client_secret = config.get('API_Keys', 'naver_client_secret', fallback='')
        naver_scrape_limit = config.getint('ScraperSettings', 'naver_scrape_limit', fallback=50)
        # Get maximum concurrent API requests
        max_concurrent_api = config.getint('ScraperSettings', 'naver_max_concurrent_api', fallback=3)
        
        # Log config values
        logging.info(f"🟢 Naver API Configuration: ID={naver_client_id[:4]}..., Secret={naver_client_secret[:4]}..., Limit={naver_scrape_limit}, Max Concurrent={max_concurrent_api}")
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        logging.error(f"Error reading required configuration for Naver crawl: {e}. Aborting Naver crawl.")
        # Populate results with errors for all rows
        for idx, row in product_rows.iterrows():
             results_map[idx] = {'original_row': row, 'naver_data': None, 'product_type': row.get('구분', 'A')}
        return [res for res in results_map.values() if res is not None]

    logging.info(f"🟢 --- Starting Naver product crawl for {total_products} products (Async) ---")
    logging.info(f"🟢 Configuration: Image Dir='{image_target_dir}', Use BG Removal={use_bg_removal}, Limit={naver_scrape_limit}, Max Concurrent API={max_concurrent_api}")

    # Validate essential config before proceeding
    if not naver_client_id or not naver_client_secret:
        logging.error("🟢 Naver API credentials missing in config [API_Keys]. Cannot crawl Naver.")
        for idx, row in product_rows.iterrows():
            results_map[idx] = {'original_row': row, 'naver_data': None, 'product_type': row.get('구분', 'A')}
        return [res for res in results_map.values() if res is not None]

    # Log first few product names we'll be crawling
    sample_products = [row.get('상품명', '[이름 없음]') for idx, row in product_rows.head(min(5, len(product_rows))).iterrows()]
    if len(product_rows) > 5:
        sample_products.append(f"... and {len(product_rows)-5} more")
    logging.info(f"🟢 Products to crawl: {', '.join(sample_products)}")

    can_download_images = bool(image_target_dir)
    if not can_download_images:
        logging.warning("🟢 Image target directory not specified in config [Paths]. Images will not be downloaded.")
    else:
        # Make sure target directory exists
        if not os.path.exists(image_target_dir):
            try:
                os.makedirs(image_target_dir)
                logging.info(f"🟢 Created image target directory: {image_target_dir}")
            except Exception as e:
                logging.error(f"🟢 Failed to create image target directory: {e}")
                can_download_images = False

    # Create a semaphore to limit concurrent API requests
    api_semaphore = asyncio.Semaphore(max_concurrent_api)

    # --- Prepare and run API search tasks concurrently --- 
    api_search_tasks = []
    async with get_async_httpx_client(config=config) as client:
        for idx, row in product_rows.iterrows():
            product_name = row.get('상품명', '')
            product_type = row.get('구분', 'A')
            reference_price = 0
            try:
                ref_price_val = row.get('판매단가(V포함)')
                reference_price = int(float(ref_price_val)) if pd.notna(ref_price_val) else 0
            except (ValueError, TypeError):
                 logging.warning(f"🟢 Could not parse reference price '{ref_price_val}' for product '{product_name}' (Row {idx}). Using 0.")
                 reference_price = 0

            if not product_name:
                logging.warning(f"🟢 Skipping row index {idx}: Missing product name.")
                results_map[idx] = {'original_row': row, 'naver_data': None, 'product_type': product_type}
                continue
            
            api_search_tasks.append(
                asyncio.create_task( 
                    _run_single_naver_search(idx, row, product_name, product_type, reference_price, client, config, naver_scrape_limit, api_semaphore),
                    name=f"naver_search_{idx}" 
                )
            )

        logging.info(f"🟢 Submitted {len(api_search_tasks)} Naver API search tasks.")
        api_results_list = await asyncio.gather(*api_search_tasks, return_exceptions=True)
        logging.info(f"🟢 Finished processing {len(api_results_list)} Naver API search results.")
        
        # Count successful and failed results
        success_count = 0
        error_count = 0
        empty_results_count = 0
        
        # --- Process API results and prepare image download tasks ---
        image_download_tasks = []
        image_info_map = {} # Maps image task future to (original_idx, item_dict, target_path)
        bg_removal_tasks = [] # New list for background removal tasks
        bg_removal_info_map = {} # Map future to (item_dict, target_path, bg_removed_path)

        processed_api_count = 0
        for result_or_exc in api_results_list:
            processed_api_count += 1
            if isinstance(result_or_exc, Exception):
                logging.error(f"🟢 API search task failed with exception: {result_or_exc}")
                error_count += 1
                continue 
            elif result_or_exc is None:
                 logging.error("🟢 Received None from a successful API search task. Skipping.")
                 error_count += 1
                 continue

            idx, original_row, product_type, api_results = result_or_exc
            results_map[idx] = {
                'original_row': original_row,
                'naver_data': api_results, 
                'product_type': product_type
            }
            
            if api_results and len(api_results) > 0:
                success_count += 1
                logging.info(f"🟢 Found {len(api_results)} results for product '{original_row.get('상품명', '')}' (Row {idx})")
            else:
                empty_results_count += 1
                logging.warning(f"🟢 No results found for product '{original_row.get('상품명', '')}' (Row {idx})")

            if api_results and can_download_images:
                for item in api_results:
                    image_url = item.get('image_url')
                    if image_url:
                        try:
                            url_hash = hashlib.md5(image_url.encode()).hexdigest()[:10]
                            file_ext = os.path.splitext(urlparse(image_url).path)[1] or '.jpg'
                            file_ext = ''.join(c for c in file_ext if c.isalnum() or c == '.')[:5]
                            if not file_ext.startswith('.') or len(file_ext) < 2: file_ext = '.jpg'

                            target_filename = f"naver_{url_hash}{file_ext}"
                            target_path = os.path.join(image_target_dir, target_filename)
                            item['image_path_original'] = target_path
                            item['image_path'] = None # Initialize
                            
                            img_task = asyncio.create_task(
                                download_image_async(image_url, target_path, client, config=config),
                                name=f"naver_img_dl_{idx}_{url_hash[:6]}"
                            )
                            image_download_tasks.append(img_task)
                            image_info_map[img_task] = (idx, item, target_path)
                        except Exception as e:
                            logging.error(f"🟢 Error preparing download task for image {image_url} (Row {idx}): {e}")

        # --- Summary of API results ---
        logging.info(f"🟢 API search results summary: Success={success_count}, Empty={empty_results_count}, Errors={error_count}")
        
        # --- Await Image Downloads --- 
        logging.info(f"🟢 Submitted {len(image_download_tasks)} Naver image download tasks.")
        download_results = await asyncio.gather(*image_download_tasks, return_exceptions=True)
        logging.info(f"🟢 Finished processing {len(download_results)} Naver image download results.")

        # --- Process Downloads & Submit Background Removal Tasks --- 
        processed_download_count = 0
        for task, result_or_exc in zip(image_download_tasks, download_results):
            processed_download_count += 1
            original_idx, item_dict, target_path = image_info_map.get(task, (None, None, None))

            if original_idx is None or item_dict is None:
                logging.error("🟢 Could not find mapping info for a completed download task. Skipping.")
                continue

            if isinstance(result_or_exc, Exception):
                logging.error(f"🟢 Download failed for {item_dict.get('image_url', '[URL missing]')} (Row {original_idx}): {result_or_exc}")
                item_dict['image_path'] = None
                item_dict['image_path_original'] = None
            elif result_or_exc is False:
                logging.warning(f"🟢 Download function reported failure for {item_dict.get('image_url', '[URL missing]')} (Row {original_idx}). Saved path: {target_path}")
                item_dict['image_path'] = None
                item_dict['image_path_original'] = target_path # Keep original path even if download failed
            else:
                # Download successful!
                item_dict['image_path_original'] = target_path # Original is always the initially downloaded one
                item_dict['image_path'] = target_path # Default to original if no BG removal

                if use_bg_removal:
                    try:
                        target_path_obj = Path(target_path)
                        bg_removed_filename = f"{target_path_obj.stem}_no_bg{target_path_obj.suffix}"
                        bg_removed_path = target_path_obj.with_name(bg_removed_filename)
                        
                        # Create and add the async background removal task
                        bg_task = asyncio.create_task(
                            remove_background_async(str(target_path_obj), str(bg_removed_path)),
                            name=f"naver_bg_rem_{original_idx}_{target_path_obj.stem[:6]}"
                        )
                        bg_removal_tasks.append(bg_task)
                        bg_removal_info_map[bg_task] = (item_dict, target_path, str(bg_removed_path))
                    except Exception as e:
                        logging.error(f"🟢 Error preparing background removal task for {target_path} (Row {original_idx}): {e}")
                        # Keep original image path if BG removal prep fails
                        item_dict['image_path'] = target_path 

        # --- Await Background Removal Tasks --- 
        if bg_removal_tasks:
            logging.info(f"🟢 Submitted {len(bg_removal_tasks)} Naver background removal tasks.")
            bg_removal_results = await asyncio.gather(*bg_removal_tasks, return_exceptions=True)
            logging.info(f"🟢 Finished processing {len(bg_removal_results)} Naver background removal results.")
            
            # --- Process Background Removal Results --- 
            processed_bg_count = 0
            for task, result_or_exc in zip(bg_removal_tasks, bg_removal_results):
                processed_bg_count += 1
                item_dict, original_path, bg_removed_path = bg_removal_info_map.get(task, (None, None, None))
                
                if item_dict is None:
                    logging.error("🟢 Could not find mapping info for a completed BG removal task. Skipping.")
                    continue
                
                if isinstance(result_or_exc, Exception):
                    logging.error(f"🟢 Background removal failed for {original_path}: {result_or_exc}")
                    item_dict['image_path'] = original_path # Fallback to original
                elif result_or_exc is False:
                    logging.warning(f"🟢 Background removal function reported failure for {original_path}. Using original image.")
                    item_dict['image_path'] = original_path # Fallback to original
                else:
                    # Background removal successful!
                    logging.debug(f"🟢 Background removal successful for {original_path}. Using: {bg_removed_path}")
                    item_dict['image_path'] = bg_removed_path
                    # Optional: Delete original if desired and different from bg_removed?
                    # if original_path != bg_removed_path and Path(original_path).exists():
                    #     try: os.remove(original_path); logging.debug(f"Deleted original: {original_path}")
                    #     except OSError as e: logging.warning(f"Could not delete original {original_path}: {e}")
        else:
            logging.info("🟢 No background removal tasks to process.")
            

    logging.info(f"🟢 --- Finished Naver product crawl. Processed {processed_api_count} API results, {processed_download_count} downloads, {len(bg_removal_tasks)} BG removals. ---")
    
    # Count results with data
    results_with_data = sum(1 for res in results_map.values() if res is not None and res.get('naver_data'))
    logging.info(f"🟢 Final results stats: Total rows={len(results_map)}, With data={results_with_data}, No data={len(results_map)-results_with_data}")
    
    # If no results found, log a clear warning
    if results_with_data == 0:
        logging.error("🟢 !!!! NO NAVER RESULTS FOUND FOR ANY PRODUCTS !!!!")
        logging.error("🟢 Check API keys, rate limits, and network connectivity")
    
    # Return the final results structured correctly
    final_results = [res for res in results_map.values() if res is not None]
    return final_results


async def _run_single_naver_search(idx, row, product_name, product_type, reference_price, client, config, naver_scrape_limit, api_semaphore):
    """ Helper coroutine to handle the logic for a single product's Naver API search. """
    try:
        logging.debug(f"🟢 [API Task {idx}] Starting Naver search for '{product_name}'.")
        async with api_semaphore:
            api_results = await crawl_naver(
                original_query=product_name,
                client=client,
                config=config,
                max_items=naver_scrape_limit,
                reference_price=reference_price
            )
        logging.debug(f"🟢 [API Task {idx}] Completed Naver search for '{product_name}'. Found {len(api_results) if api_results else 0} items.")
        return idx, row, product_type, api_results
    except Exception as e:
        logging.error(f"🟢 [API Task {idx}] Error during Naver search for '{product_name}': {e}", exc_info=True)
        return idx, row, product_type, None

# --- Test block Updated for Async ---
async def _test_main():
    # Setup basic logging for the test
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- Running Naver API Test (Async) ---")
    print("--- Running Naver API Test ---")

    # Use the actual load_config function
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', 'config.ini') # Point to config.ini
    
    # Import load_config from utils (or execution_setup if preferred)
    try:
        config = load_config(config_path)
        print(f"Config loaded from: {config_path}")
    except Exception as e:
        print(f"ERROR loading config: {e}")
        logging.error(f"Failed to load config: {e}", exc_info=True)
        return

    if not config.sections():
        print(f"ERROR: No sections found in config file: {config_path}")
        logging.error(f"Failed to load or parse config file at: {config_path}. Test cannot run.")
        return

    # Check essential keys for the test
    client_id = config.get('API_Keys', 'naver_client_id', fallback=None)
    client_secret = config.get('API_Keys', 'naver_client_secret', fallback=None)
    
    if not client_id or not client_secret:
        print("ERROR: Naver API credentials missing in config.ini!")
        logging.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logging.error("!!! Naver API credentials missing in [API_Keys] section of config.ini.")
        logging.error("!!! Test cannot run without valid credentials.")
        logging.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return 
        
    # Print the API credentials (masked)
    print(f"Using Naver client_id: {client_id[:4]}... (length: {len(client_id)})")
    print(f"Using Naver client_secret: {client_secret[:4]}... (length: {len(client_secret)})")
    logging.info(f"Test will use Naver client_id: {client_id[:4]}... (length: {len(client_id)})")
    logging.info(f"Test will use Naver client_secret: {client_secret[:4]}... (length: {len(client_secret)})")
    
    # Verify API keys directly with a simple request
    print("Testing Naver API keys directly...")
    logging.info("Testing Naver API keys directly...")
    
    # Use a fresh client for initial API test
    async with get_async_httpx_client(config=config) as client:
        try:
            api_url = "https://openapi.naver.com/v1/search/shop.json"
            headers = {
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
                "Accept": "application/json",
            }
            params = {"query": "테스트", "display": 1}
            
            print(f"Sending test request to Naver API...")
            response = await client.get(api_url, headers=headers, params=params)
            status_code = response.status_code
            print(f"Naver API response status: {status_code}")
            
            if status_code == 200:
                print(f"✅ Naver API key test successful!")
                logging.info(f"✅ Naver API key test successful! Status: {status_code}")
                data = response.json()
                total_results = data.get('total', 0) 
                print(f"Test search found {total_results} total results for query '테스트'")
                logging.info(f"Test search found {total_results} total results for query '테스트'")
            else:
                error_text = response.text[:200] + "..." if len(response.text) > 200 else response.text
                print(f"⛔ Naver API key test failed! Status: {status_code}")
                print(f"Error response: {error_text}")
                logging.error(f"⛔ Naver API key test failed! Status: {status_code}")
                logging.error(f"Error response: {error_text}")
                if status_code == 401:
                    print("⛔ API authentication failed (401). Check that your API keys are correct.")
                    logging.error("⛔ API authentication failed (401). Check that your API keys are correct.")
                elif status_code == 429:
                    print("⛔ API rate limit exceeded (429). Wait before making more requests.")
                    logging.error("⛔ API rate limit exceeded (429). Wait before making more requests.")
                return
        except Exception as e:
            print(f"⛔ API key test request failed with exception: {e}")
            logging.error(f"⛔ API key test request failed with exception: {e}")
            return
        
    # Ensure test image directory exists using config
    test_image_dir = config.get('Paths', 'image_target_dir', 
                              fallback=os.path.join(script_dir, '..', 'naver_test_images'))
    if not os.path.exists(test_image_dir):
        try:
             os.makedirs(test_image_dir)
             logging.info(f"Created test image directory: {test_image_dir}")
        except OSError as e:
             logging.error(f"Could not create test image directory {test_image_dir}: {e}. Image download might fail.")

    # Sample DataFrame with real products (nail clipper sets)
    test_data = {
        '구분': ['A', 'A', 'A', 'A'],
        '담당자': ['황진희', '홍락민', '황진희', '황진희'],
        '거래처': ['신신유통', '(주)에스비무역 - 수빈무역', '신신유통', '신신유통'],
        '상품코드': [296, 2553, 296, 296],
        '품목코드': [437766, 435004, 420498, 420497],
        '품목구분': ['손톱깎이세트', '손톱깎이세트', '손톱깎이세트', '손톱깎이세트'],
        '상품명': ['777 쓰리쎄븐 KR-0650C 6P세트', '쓰리쎄븐 777 KR-0650C 손톱깎이 세트', 
                 '777쓰리쎄븐 TS-6500C 손톱깎이 13P세트', '777쓰리쎄븐 TS-5400C 손톱깎이 6P세트'],
        '판매단가(V포함)': [15000, 15000, 25000, 15000]
    }
    test_df = pd.DataFrame(test_data)

    print(f"Testing Naver API with {len(test_df)} nail clipper products...")
    logging.info(f"Testing Naver API with {len(test_df)} nail clipper products...")

    try:
        # Test a single direct API query first
        print("\nTesting direct Naver API call for '777 쓰리쎄븐 KR-0650C 6P세트'...")
        first_product = "777 쓰리쎄븐 KR-0650C 6P세트"
        
        # Create a fresh client for the direct API test
        async with get_async_httpx_client(config=config) as direct_client:
            direct_results = await crawl_naver(
                original_query=first_product,
                client=direct_client,
                config=config,
                max_items=10,
                reference_price=15000
            )
        
        if direct_results:
            print(f"✅ Direct Naver API call successful! Found {len(direct_results)} results")
            print(f"First item: {direct_results[0].get('name')} - ₩{direct_results[0].get('price')}")
        else:
            print("⛔ Direct Naver API call returned no results")
            
        # Now test the full crawl_naver_products function
        print("\nTesting full crawl_naver_products function...")
        results = await crawl_naver_products(
            product_rows=test_df,
            config=config 
        )

        print(f"--- Test Results ({len(results)} rows processed) ---")
        logging.info(f"--- Test Results ({len(results)} rows processed) ---")
        
        # Count how many rows have actual data
        rows_with_data = sum(1 for r in results if r.get('naver_data'))
        print(f"Results with data: {rows_with_data}/{len(results)}")
        logging.info(f"Results with data: {rows_with_data}/{len(results)}")
        
        # Log example data for each product
        for i, result in enumerate(results):
            original_name = result.get('original_row', {}).get('상품명', 'Unknown')
            naver_data = result.get('naver_data', [])
            if naver_data:
                first_item = naver_data[0]
                print(f"Product {i+1}: '{original_name}' - Found {len(naver_data)} results")
                print(f"  First match: {first_item.get('name')} - ₩{first_item.get('price')} - {first_item.get('seller')}")
                logging.info(f"Product {i+1}: '{original_name}' - Found {len(naver_data)} results")
                logging.info(f"  First match: {first_item.get('name')} - ₩{first_item.get('price')} - {first_item.get('seller')}")
            else:
                print(f"Product {i+1}: '{original_name}' - No results found")
                logging.warning(f"Product {i+1}: '{original_name}' - No results found")
        
        if rows_with_data == 0:
            print("⛔ TEST FAILED: No data was returned for any products!")
            logging.error("⛔ TEST FAILED: No data was returned for any products!")
        else:
            print(f"✅ TEST SUCCESSFUL: Data was returned for {rows_with_data} products.")
            logging.info(f"✅ TEST SUCCESSFUL: Data was returned for {rows_with_data} products.")

    except Exception as e:
        print(f"An error occurred during the async test run: {e}")
        logging.error(f"An error occurred during the async test run: {e}", exc_info=True)

    logging.info("--- Naver API Test (Async) Finished ---")
    print("--- Naver API Test (Async) Finished ---")

if __name__ == "__main__":
    # Set up basic logging for when run as a script
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    print("Running Naver API test as main script...")
    
    # Load config within the async main test function
    asyncio.run(_test_main()) 