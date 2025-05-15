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
from typing import List, Dict, Any, Optional, Tuple, Union
import aiohttp
import aiofiles
from PIL import Image
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Error as PlaywrightError
from bs4 import BeautifulSoup
import random
from datetime import datetime
import sys

# Import based on how the file is run
try:
    # When imported as module
    from utils import (
        download_image_async, get_async_httpx_client, generate_keyword_variations, 
        load_config, tokenize_korean, jaccard_similarity
    )
    from image_utils import remove_background_async
    from crawling_UPrice_v2_naver import extract_quantity_prices, get_quantities_from_excel
except ImportError:
    # When run directly as script
    from utils import (
        download_image_async, get_async_httpx_client, generate_keyword_variations, 
        load_config, tokenize_korean, jaccard_similarity
    )
    from image_utils import remove_background_async
    from crawling_UPrice_v2_naver import extract_quantity_prices, get_quantities_from_excel

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
    
    # Define the API URL
    api_url = "https://openapi.naver.com/v1/search/shop.json"  # Example API URL, replace with the correct one if needed

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

    # API 요청 딜레이 설정 수정
    api_delay = config.getfloat('ScraperSettings', 'naver_api_delay', fallback=1.5)
    if api_delay < 2.0:
        api_delay = random.uniform(2.0, 4.0)
    logger.info(f"Using API delay: {api_delay:.2f} seconds")

    # Get initial similarity threshold from config
    try:
        # FIXED: Lower default threshold from 0.6 to 0.4 for better matching
        initial_sim_threshold = config.getfloat('Matching', 'naver_initial_similarity_threshold', fallback=0.4)
    except (configparser.Error, ValueError):
        logger.warning("Could not read 'naver_initial_similarity_threshold' from [Matching] config. Using default 0.4.")
        initial_sim_threshold = 0.4
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

        # API 요청 헤더 개선
        headers = {
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "DNT": "1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        # User-Agent 로테이션
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
        headers['User-Agent'] = random.choice(user_agents)

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

            # Calculate API request parameters
            api_display_count = 100  # Max allowed by Naver API
            start_index = (page - 1) * api_display_count + 1
            # Calculate how many more items we need, respecting max_items overall
            effective_display_count = min(api_display_count, max_items - len(current_keyword_results))
            if effective_display_count <= 0:
                logger.debug(f"Effective display count is zero or less for keyword '{query}', page {page}. Breaking page loop.")
                break

            # Set up request parameters
            params = {
                "query": query,
                "display": effective_display_count,
                "start": start_index,
                "sort": "sim",
                "exclude": "used:rental"
            }
            logger.debug(f"🟢 Naver API Request (Keyword: '{query}', Page {page}, Sort: 'sim'): Params={params}")

            # Add delay before API call to avoid hitting rate limits
            if page > 1 or keyword_idx > 0:
                # Increase delay between pages and keywords
                current_delay = api_delay * (1.2 if page > 1 else 1.0)  # Longer delay between pages
                logger.debug(f"🟢 Adding delay of {current_delay:.2f} seconds before Naver API request (Page: {page}, Keyword Attempt: {keyword_idx+1})")
                await asyncio.sleep(current_delay)

            try:
                # Log headers just before the request
                # Mask secret for safety, though it's fetched locally in this function scope.
                headers_to_log = headers.copy()
                if "X-Naver-Client-Secret" in headers_to_log:
                    headers_to_log["X-Naver-Client-Secret"] = headers_to_log["X-Naver-Client-Secret"][:4] + "..."
                # Use pprint for potentially large headers, limit length if necessary
                log_headers_str = pprint.pformat(headers_to_log, width=120)
                logger.debug(f"🟢 Preparing Naver API request. Headers: {log_headers_str}")

                logger.info(f"🟢 Sending Naver API request for '{query}' (Page {page})")
                start_time = time.monotonic()
                response = await client.get(api_url, headers=headers, params=params)
                response_time = time.monotonic() - start_time
                status_code = response.status_code
                response_text = response.text # Get text immediately for potential logging

                logger.info(f"🟢 Naver API response status: {status_code} (took {response_time:.2f}s)")

                # Enhanced error logging: Check status code first
                if status_code != 200:
                    error_text_snippet = response_text[:200] + "..." if len(response_text) > 200 else response_text
                    logger.error(f"🟢 Naver API error response (Status: {status_code}, Keyword: '{query}', Page: {page}): Snippet: {error_text_snippet}")
                    # Log full text for non-200 errors for detailed debugging
                    logger.debug(f"🟢 Full Naver API error response text (Status {status_code}): {response_text}")

                    if status_code == 401: # Unauthorized
                         logger.error("Naver API authentication failed (401). Check credentials.")
                         # Stop trying immediately if credentials are bad
                         return [] # Return empty list, signalling fatal auth error
                    elif status_code == 429: # Rate limit
                        wait_time = api_delay * 3
                        logger.error(f"🟢 Rate limit exceeded (429). Waiting {wait_time:.2f} seconds before next request.")
                        await asyncio.sleep(wait_time)
                    elif status_code == 404: # Not Found (e.g., invalid API endpoint, unlikely here)
                         logger.error("Naver API endpoint not found (404). Check API URL.")
                    # Continue to next page or keyword for other errors for now
                    continue

                # --- Status code IS 200 ---
                # Now try to parse JSON and check for embedded errors
                try:
                    data = response.json()
                except json.JSONDecodeError as json_err:
                    # Handle cases where status is 200 but body is not valid JSON
                    logger.error(f"🟢 Failed to decode JSON from Naver API (Status 200, Keyword: '{query}', Page: {page}): {json_err}")
                    logger.error(f"🟢 Full Naver API response text (Status 200, JSON decode failed): {response_text}")
                    # Check if the raw text contains the specific auth error message
                    if "Not Exist Client ID" in response_text:
                         logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                         logger.error("!!! Detected 'Not Exist Client ID' error in response body despite Status 200!")
                         logger.error("!!! This indicates an API inconsistency or header issue. Check credentials/headers.")
                         logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                         # Consider stopping if auth error appears even with 200 status
                         # return [] # Option: Treat as fatal auth error
                    break # Stop processing pages for this keyword due to bad response

                # --- JSON decoded successfully (Status 200) ---
                total_items_api = data.get('total', 0)
                api_items_on_page = len(data.get('items', []))
                logger.info(f"🟢 Naver API Response (Keyword: '{query}', Page {page}): Found {total_items_api} total items, received {api_items_on_page} on this page.")

                # Check for 'errorMessage' key within the successfully decoded JSON
                if 'errorMessage' in data:
                    api_error_message = data.get('errorMessage')
                    logger.error(f"🟢 Naver API error message found in JSON (Status 200): {api_error_message}")
                    if "Not Exist Client ID" in str(api_error_message):
                         logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                         logger.error("!!! Detected 'Not Exist Client ID' error in JSON response body despite Status 200!")
                         logger.error("!!! Check API Key/Secret and API application settings in Naver Developer portal.")
                         logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                         # Consider stopping if auth error appears even with 200 status
                         # return [] # Option: Treat as fatal auth error
                    # If there's an error message, even if items exist, treat page as problematic
                    break # Stop fetching pages for this keyword

                if 'items' not in data or not data.get('items'):
                    # Logged 'errorMessage' above if present. Now handle case of no items AND no error message.
                    logger.warning(f"🟢 Naver API returned Status 200 but no items for '{query}' (Page {page}).")
                    # Log the full response for debugging if no items found
                    logger.debug(f"🟢 Full Naver API response (Status 200, no items/no error msg): {json.dumps(data, ensure_ascii=False)[:500]}")
                    break # No items on this page, stop fetching for this keyword

            except httpx.TimeoutException as timeout_err:
                 wait_time = api_delay * 3  # Increase wait time on timeout
                 logger.error(f"🟢 Timeout during Naver API request (Keyword: '{query}', Page {page}): {timeout_err}. Waiting {wait_time:.2f}s.")
                 await asyncio.sleep(wait_time) # Wait longer on timeout
                 continue # Retry this page/keyword after delay
            except httpx.RequestError as req_err:
                 logger.error(f"🟢 HTTPX Request Error during Naver API request (Keyword: '{query}', Page {page}): {req_err}", exc_info=True)
                 # Log response text if available
                 try:
                     if response and response.text:
                         logger.error(f"🟢 Response text during HTTPX Request Error: {response.text[:500]}...")
                         # Check for rate limit error in response
                         if "rate limit" in response.text.lower() or "429" in response.text:
                             wait_time = api_delay * 4  # Even longer wait on rate limit
                             logger.error(f"🟢 Detected rate limit error. Waiting {wait_time:.2f}s before retry.")
                             await asyncio.sleep(wait_time)
                             continue  # Retry after longer wait
                 except NameError: pass # response might not be defined
                 await asyncio.sleep(api_delay) # Basic delay and continue
                 break # Assume persistent issue with this keyword/page
            # except json.JSONDecodeError handled above for status 200 case
            except Exception as e:
                logger.error(f"🟢 Unexpected error processing Naver API response (Keyword: '{query}', Page {page}): {e}", exc_info=True)
                # Log response text if available
                try:
                     if response and response.text:
                         logger.error(f"🟢 Response text during unexpected error: {response.text[:500]}...")
                except NameError: pass # response might not be defined

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

                    # --- Enhanced Similarity Check ---
                    title_tokens = tokenize_korean(title)
                    similarity = jaccard_similarity(original_query_tokens, title_tokens)
                    
                    # FIXED: Improved similarity calculation with more weight on exact matches
                    # Enhanced weighting based on token length and exact matches
                    weight = 1.0
                    common_tokens = set(original_query_tokens) & set(title_tokens)
                    
                    # Add weight for longer common tokens (more significant matches)
                    for token in common_tokens:
                        if len(token) >= 4:  # 4+ character tokens get more weight
                            weight += 0.15
                        elif len(token) >= 3: # 3 character tokens get some weight
                            weight += 0.1
                            
                    # Check for exact word matches (higher confidence)
                    original_words = ' '.join(original_query_tokens).split()
                    title_words = ' '.join(title_tokens).split()
                    exact_word_matches = set(original_words) & set(title_words)
                    
                    # Add weight for exact word matches
                    weight += len(exact_word_matches) * 0.2
                    
                    # Apply weight to similarity
                    weighted_similarity = similarity * weight
                    
                    # FIXED: Log more useful information about similarity calculation
                    if weighted_similarity >= initial_sim_threshold * 0.8:  # Log near-matches too
                        logger.debug(f"🟢 Item #{item_idx+1} similarity: Jaccard={similarity:.2f}, Weight={weight:.2f}, " +
                                   f"Final={weighted_similarity:.2f}, Threshold={initial_sim_threshold:.2f}, " +
                                   f"Common={len(common_tokens)}, ExactWords={len(exact_word_matches)}")
                    
                    # FIXED: Use stricter threshold
                    if weighted_similarity < initial_sim_threshold:
                        logger.debug(f"🟢 Skipping item #{item_idx+1} (Keyword: '{query}') due to low weighted similarity ({weighted_similarity:.2f} < {initial_sim_threshold}): '{title}'")
                        continue
                    # --- End Enhanced Similarity Check ---

                    seller = item.get("mallName", "")
                    link = item.get("link", "")
                    image_url = item.get("image", "")
                    mall_product_url = item.get("productUrl", link) # Use link if productUrl missing

                    # 공급사 분류 (Kogift 방식 참고)
                    supplier_type = "일반"
                    
                    # 주요 공급사 확인
                    if "네이버" in seller or "스마트스토어" in seller:
                        supplier_type = "네이버"
                    elif "쿠팡" in seller:
                        supplier_type = "쿠팡"
                    elif "11번가" in seller:
                        supplier_type = "11번가"
                    elif "G마켓" in seller or "지마켓" in seller:
                        supplier_type = "G마켓"
                    elif "옥션" in seller:
                        supplier_type = "옥션"
                    elif "인터파크" in seller:
                        supplier_type = "인터파크"
                    elif "위메프" in seller:
                        supplier_type = "위메프"
                    elif "티몬" in seller:
                        supplier_type = "티몬"
                    
                    # Enhanced check for promotional items - better filtering
                    is_promotional = False
                    for promo in promo_keywords:
                        if promo.lower() in title.lower() or promo.lower() in seller.lower():
                            is_promotional = True
                            logger.debug(f"🟢 Detected promotional keyword '{promo}' in '{title}' or '{seller}'")
                            break
                            
                    if is_promotional:
                        logger.debug(f"🟢 Skipping promotional item #{item_idx+1} (Keyword: '{query}'): '{title}' (Seller: '{seller}')")
                        continue

                    # --- Data Extraction ---
                    product = {
                        'name': title,
                        'price': price,
                        'link': link,
                        'image_url': image_url,  # Make sure this is explicitly set
                        'quantity': "1",
                        'mallName': seller,
                        'mallProductUrl': mall_product_url,
                        'initial_similarity': round(weighted_similarity, 3),  # Store weighted similarity
                        'supplier': supplier_type,  # 공급사 유형 추가
                        'source': 'naver',  # 출처 명시
                        'seller_name': seller,  # Add seller name for easier access
                        'seller_link': mall_product_url  # Add seller link for easier access
                    }
                    # --- End Data Extraction ---

                    # Optional: Reference price check (only logging for now)
                    if reference_price > 0:
                        price_diff_percent = ((price - reference_price) / reference_price) * 100
                        logger.debug(f"🟢 Price difference for '{title[:30]}...': {price_diff_percent:.2f}% (Item: {price}, Ref: {reference_price})")

                    current_keyword_results.append(product)
                    items_added_this_page += 1
                    logger.debug(f"  -> Added item #{item_idx+1} (Sim: {weighted_similarity:.2f}): '{title[:50]}...' (Price: {price}, Seller: '{seller}')")

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

    # FIXED: Sort results by similarity score before returning
    if best_result_list:
        best_result_list.sort(key=lambda x: x.get('initial_similarity', 0), reverse=True)
        logger.info(f"Sorted {len(best_result_list)} Naver results by similarity score (highest first)")
        
        # Log the top match for debugging
        if best_result_list:
            top_match = best_result_list[0]
            logger.info(f"Top Naver match for '{original_query}': '{top_match.get('name', '')}' with similarity {top_match.get('initial_similarity', 0):.3f}")

    return best_result_list


async def download_naver_image(url: str, save_dir: str, product_name: str, config: configparser.ConfigParser) -> Optional[str]:
    """
    Download a single Naver image to the specified directory with enhanced processing.

    Args:
        url (str): The image URL to download.
        save_dir (str): The directory to save the image in.
        product_name (str): The product name for generating the filename.
        config (configparser.ConfigParser): ConfigParser object containing configuration.

    Returns:
        Optional[str]: The local path to the downloaded image, or None if download failed.
    """
    if not url or not save_dir:
        logger.warning("Empty URL or save directory provided to download_naver_image.")
        return None

    try:
        # Ensure URL is properly encoded and valid
        if not (url.startswith('http://') or url.startswith('https://')):
            logger.warning(f"Invalid URL format: {url}")
            return None
            
        # Handle URL encoding
        if '%' not in url and ' ' in url:
            url = url.replace(' ', '%20')

        # Create all necessary directories
        try:
            # Create RPA base directory
            rpa_base = os.path.join('C:', 'RPA')
            os.makedirs(rpa_base, exist_ok=True)
            
            # Create Output directory
            output_dir = os.path.join(rpa_base, 'Output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create Image directory
            image_dir = os.path.join(rpa_base, 'Image')
            os.makedirs(image_dir, exist_ok=True)
            
            # Create Main directory
            main_dir = os.path.join(image_dir, 'Main')
            os.makedirs(main_dir, exist_ok=True)
            
            # Create save directory
            os.makedirs(save_dir, exist_ok=True)
            
            # Log directory creation
            logger.info(f"Created/verified directories: {rpa_base}, {output_dir}, {image_dir}, {main_dir}, {save_dir}")
        except Exception as e:
            logger.error(f"Failed to create directories: {e}")
            return None
        
        # Always add Naver subdirectory unless it already exists in the path
        if not save_dir.endswith('Naver'):
            # Normalize path separators and handle Korean characters
            save_dir_normalized = os.path.normpath(save_dir)
            
            if 'Naver' not in save_dir_normalized.split(os.sep):
                # Create the Naver subdirectory
                naver_dir = os.path.join(save_dir, 'Naver')
                os.makedirs(naver_dir, exist_ok=True)
                save_dir = naver_dir
                logger.debug(f"Using Naver subdirectory: {save_dir}")
        
        # Sanitize product name more carefully - Kogift 방식과 유사하게 처리
        if product_name is None:
            sanitized_name = "unknown_product"
        else:
            # 한글 문자가 포함된 경우 해시 기반 이름 사용 (깨짐 방지)
            if any('\uAC00' <= c <= '\uD7A3' for c in product_name):
                # 한글이 포함된 상품명은 해시로 처리
                sanitized_name = hashlib.md5(product_name.encode('utf-8', errors='ignore')).hexdigest()[:16]
                logger.debug(f"Using hash-based name for Korean product name: {sanitized_name}")
            else:
                # 영문/숫자로만 구성된 상품명은 적절히 정리
                sanitized_name = re.sub(r'[^\w\d-]', '_', product_name)[:30]
                # 일관된 길이를 위해 패딩 추가
                sanitized_name = sanitized_name.ljust(30, '_')
        
        # URL의 고유 해시 생성 (파일명 중복 방지)
        url_hash = hashlib.md5(url.encode('utf-8', errors='ignore')).hexdigest()[:8]
        
        # URL에서 파일 확장자 추출
        parsed_url = urlparse(url)
        file_ext = os.path.splitext(parsed_url.path)[1].lower()
        # 확장자가 없거나 유효하지 않은 경우 기본값 사용
        if not file_ext or file_ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            file_ext = '.jpg'
        
        # 일관된 형식의 파일명 생성 (출처 정보 포함)
        filename = f"naver_{sanitized_name}_{url_hash}{file_ext}"
        local_path = os.path.join(save_dir, filename)
        final_image_path = local_path
        
        # 이미 파일이 존재하는 경우 중복 다운로드 방지
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.debug(f"Image already exists: {local_path}")
            
            # Create consistent image data structure
            image_data = {
                'url': url,
                'local_path': os.path.abspath(local_path),
                'source': 'naver',
                'product_name': product_name
            }
            
            # 배경 제거 버전이 이미 있는지 확인
            try:
                use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
                if use_bg_removal:
                    bg_removed_path = local_path.replace('.', '_nobg.', 1)
                    if os.path.exists(bg_removed_path) and os.path.getsize(bg_removed_path) > 0:
                        final_image_path = bg_removed_path
                        image_data['local_path'] = os.path.abspath(bg_removed_path)
                        logger.debug(f"Using existing background-removed image: {final_image_path}")
                    else:
                        # 배경 제거 버전이 없으면 생성 시도
                        try:
                            from image_utils import remove_background
                            if remove_background(local_path, bg_removed_path):
                                final_image_path = bg_removed_path
                                image_data['local_path'] = os.path.abspath(bg_removed_path)
                                logger.debug(f"Background removed for existing Naver image: {final_image_path}")
                            else:
                                logger.warning(f"Failed to remove background for Naver image {local_path}. Using original.")
                        except Exception as bg_err:
                            logger.warning(f"Error during background removal: {bg_err}. Using original image.")
            except Exception as config_err:
                logger.warning(f"Error reading background removal config: {config_err}. Using original image.")
            
            # Make sure the path exists and absolute (fixing a common issue)
            final_image_path = os.path.abspath(final_image_path)
            logger.debug(f"Using absolute path for existing image: {final_image_path}")
            
            return final_image_path

        # 네트워크 요청 헤더 설정 (한국 사이트 호환성 위한 사용자 에이전트 등 추가)
        headers = {
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # 재시도 로직으로 다운로드
        max_retries = config.getint('Network', 'max_retries', fallback=3)
        for attempt in range(max_retries):
            try:
                # 이미지 다운로드
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30), headers=headers) as response:
                        if response.status != 200:
                            logger.error(f"Failed to download image: {url}, status: {response.status}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(1)  # 재시도 전 대기
                                continue
                            return None
                        
                        # 임시 파일에 저장
                        temp_path = f"{local_path}.{time.time_ns()}.tmp"
                        try:
                            async with aiofiles.open(temp_path, 'wb') as f:
                                await f.write(await response.read())
                            
                            # 이미지 검증
                            with Image.open(temp_path) as img:
                                img.verify()
                            with Image.open(temp_path) as img:
                                if img.mode in ('RGBA', 'LA'):
                                    img = img.convert('RGB')
                                    img.save(temp_path, 'JPEG', quality=85)
                            
                            # 임시 파일을 최종 위치로 이동
                            if os.path.exists(local_path):
                                os.remove(local_path)
                            os.rename(temp_path, local_path)
                            logger.info(f"Successfully downloaded image: {url} -> {local_path}")
                            
                            # Create base image data
                            image_data = {
                                'url': url,
                                'local_path': os.path.abspath(local_path),
                                'original_path': os.path.abspath(local_path),
                                'source': 'naver',
                                'product_name': product_name
                            }
                            
                            # 필요시 배경 제거 시도
                            try:
                                use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
                                if use_bg_removal:
                                    from image_utils import remove_background
                                    bg_removed_path = local_path.replace('.', '_nobg.', 1)
                                    if remove_background(local_path, bg_removed_path):
                                        final_image_path = bg_removed_path
                                        image_data['local_path'] = os.path.abspath(bg_removed_path)
                                        logger.debug(f"Background removed for downloaded Naver image: {final_image_path}")
                                    else:
                                        logger.warning(f"Failed to remove background for Naver image {local_path}. Using original.")
                            except Exception as bg_err:
                                logger.warning(f"Error during background removal: {bg_err}. Using original image.")
                                
                            # Convert to absolute path and update the data
                            final_image_path = os.path.abspath(final_image_path)
                            image_data['local_path'] = final_image_path
                            
                            return final_image_path
                        except Exception as e:
                            logger.error(f"Error processing image {url}: {e}")
                            if os.path.exists(temp_path):
                                try:
                                    os.remove(temp_path)
                                except:
                                    pass
                            if attempt < max_retries - 1:
                                await asyncio.sleep(1)  # 재시도 전 대기
                                continue
                            return None
            except aiohttp.ClientError as e:
                logger.error(f"Network error downloading image {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)  # 재시도 전 대기
                    continue
                return None
                
    except Exception as e:
        logger.error(f"Error downloading image {url}: {e}")
        return None

async def extract_quantity_price_from_naver_link(page: Page, product_url: str) -> Dict[str, Any]:
    """
    Extracts quantity-based pricing information from a Naver product page.
    Returns all available quantity-price combinations from the table.
    
    Args:
        page: Playwright Page object
        product_url: URL of the product page
        
    Returns:
        Dictionary containing pricing information and whether it's a promotional site
    """
    result = {
        "is_promotional_site": False,
        "has_quantity_pricing": False,
        "quantity_prices": {},
        "vat_included": False,
        "supplier_name": "",
        "supplier_url": "",
        "price_table": None,
        "raw_price_table": None,  # Store the raw price table data
        "has_captcha": False
    }
    
    try:
        # Set random viewport size
        viewport_sizes = [
            {"width": 1366, "height": 768},
            {"width": 1920, "height": 1080},
            {"width": 1440, "height": 900},
            {"width": 1536, "height": 864}
        ]
        await page.set_viewport_size(random.choice(viewport_sizes))
        
        # Set random user agent and headers
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/92.0.902.84 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0'
        ]
        
        await page.set_extra_http_headers({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
        
        # Add anti-detection JavaScript
        await page.evaluate('''() => {
            // Hide automation flags
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
            
            // Add random mouse movements
            const randomMove = () => {
                const event = new MouseEvent('mousemove', {
                    bubbles: true,
                    cancelable: true,
                    clientX: Math.floor(Math.random() * window.innerWidth),
                    clientY: Math.floor(Math.random() * window.innerHeight)
                });
                document.dispatchEvent(event);
            };
            setInterval(randomMove, Math.random() * 2000 + 1000);
        }''')
        
        # Navigate to the product page with random delay
        logger.info(f"Navigating to Naver product page: {product_url}")
        await asyncio.sleep(random.uniform(1, 3))
        await page.goto(product_url, wait_until='networkidle', timeout=30000)
        
        # Check for captcha
        captcha_selectors = [
            'form#captcha_form', 
            'img[alt*="captcha"]',
            'div.captcha_wrap',
            'input[name="captchaBotKey"]',
            'div[class*="captcha"]',
            'iframe[src*="captcha"]',
            'div[class*="bot-check"]',
            'div[class*="security-check"]'
        ]
        
        for selector in captcha_selectors:
            if await page.locator(selector).count() > 0:
                logger.info(f"CAPTCHA detected on page: {product_url}")
                result["has_captcha"] = True
                return result
        
        # Get supplier name with enhanced selectors
        supplier_selectors = [
            'div.basicInfo_mall_title__3IDPK a',
            'a.seller_name',
            'span.mall_txt',
            'div.shop_info a.txt',
            'div[class*="mall_title"] a',
            'div[class*="seller"] a',
            'a[class*="mall-name"]'
        ]
        
        for selector in supplier_selectors:
            if await page.locator(selector).count() > 0:
                result["supplier_name"] = await page.locator(selector).text_content()
                result["supplier_name"] = result["supplier_name"].strip()
                
                # Get supplier URL
                supplier_url = await page.locator(selector).get_attribute('href') or ""
                if supplier_url and not supplier_url.startswith('http'):
                    supplier_url = f"https://shopping.naver.com{supplier_url}"
                result["supplier_url"] = supplier_url
                break
        
        # Enhanced lowest price button handling for Naver sellers
        if "네이버" in result["supplier_name"]:
            lowest_price_selectors = [
                '//div[contains(@class, "lowestPrice_btn_box")]/div[contains(@class, "buyButton_compare_wrap")]/a[text()="최저가 사러가기"]',
                '//a[contains(text(), "최저가 사러가기")]',
                '//a[contains(text(), "최저가")]',
                '//a[contains(@class, "lowest_price")]',
                '//button[contains(text(), "최저가")]',
                '//div[contains(@class, "lowest")]/a',
                '//div[contains(@class, "price_compare")]/a',
                '//a[contains(@class, "price_compare")]',
                '//div[contains(@class, "compare")]/a[contains(@class, "link")]',
                '//a[contains(@href, "search/gate")]'
            ]
            
            max_retries = 3
            retry_count = 0
            button_found = False
            
            while retry_count < max_retries and not button_found:
                for selector in lowest_price_selectors:
                    try:
                        # Wait for selector with timeout
                        await page.wait_for_selector(selector, timeout=5000)
                        element = page.locator(selector).first
                        
                        if await element.is_visible():
                            logger.info(f"Found lowest price button with selector: {selector}")
                            
                            # Get button position and add slight random offset
                            box = await element.bounding_box()
                            if box:
                                x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
                                y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)
                                
                                # Move mouse naturally to button
                                await page.mouse.move(x, y, steps=random.randint(5, 10))
                                await asyncio.sleep(random.uniform(0.1, 0.3))
                            
                            # Try to get href first
                            href = await element.get_attribute('href')
                            if href:
                                logger.info(f"Navigating to lowest price URL: {href}")
                                await page.goto(href, wait_until='networkidle', timeout=30000)
                            else:
                                logger.info("Clicking lowest price button")
                                await element.click()
                                await page.wait_for_load_state('networkidle', timeout=30000)
                            
                            # Add random delay after click
                            await asyncio.sleep(random.uniform(2, 4))
                            
                            button_found = True
                            break
                    except Exception as e:
                        logger.warning(f"Error with lowest price selector {selector} (attempt {retry_count + 1}): {e}")
                        continue
                
                if not button_found:
                    retry_count += 1
                    if retry_count < max_retries:
                        # Add increasing delay between retries
                        await asyncio.sleep(random.uniform(2, 4) * retry_count)
                        # Reload page before retry
                        await page.reload(wait_until='networkidle', timeout=30000)
            
            if not button_found:
                logger.warning("Could not find lowest price button after all attempts")
        
        # Visit the seller's site to check for quantity-based pricing
        visit_store_selector = 'a.btn_link__dHQPb, a.go_mall, a.link_btn[href*="mall"]'
        if await page.locator(visit_store_selector).count() > 0:
            seller_site_url = await page.locator(visit_store_selector).get_attribute('href') or ""
            logger.info(f"Found seller's site URL: {seller_site_url}")
            
            if seller_site_url:
                try:
                    # Create a new context for visiting the seller's site
                    browser = page.context.browser
                    seller_context = await browser.new_context(
                        user_agent=random.choice(user_agents),
                        viewport=random.choice(viewport_sizes)
                    )
                    seller_page = await seller_context.new_page()
                    
                    # Add anti-detection measures to the new context
                    await seller_page.evaluate('''() => {
                        Object.defineProperty(navigator, 'webdriver', { get: () => false });
                        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                        Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
                    }''')
                    
                    # Navigate to the seller's site with random delay
                    await asyncio.sleep(random.uniform(1, 3))
                    await seller_page.goto(seller_site_url, wait_until='domcontentloaded', timeout=30000)
                    
                    # Check for quantity-based pricing tables
                    # Look for table with keywords like "수량" and "단가"
                    quantity_table_selectors = [
                        'table:has(th:has-text("수량")):has(th:has-text("단가"))',
                        'table:has(th:has-text("수량")):has(th:has-text("가격"))',
                        'table.quantity_price__table',
                        'div.price-box table',
                        'div.quantity_discount table',
                        'div.quantity_pricing table',
                        'table.price_by_quantity'
                    ]
                    
                    for selector in quantity_table_selectors:
                        if await seller_page.locator(selector).count() > 0:
                            logger.info(f"Found quantity-based pricing table with selector: {selector}")
                            result["has_quantity_pricing"] = True
                            result["is_promotional_site"] = True
                            
                            # Extract table HTML
                            table_html = await seller_page.locator(selector).evaluate('el => el.outerHTML')
                            
                            # Use pandas to extract the table data
                            try:
                                tables = pd.read_html(table_html)
                                if tables and len(tables) > 0:
                                    df = tables[0]
                                    
                                    # Find quantity and price columns
                                    qty_col = None
                                    price_col = None
                                    
                                    for i, col in enumerate(df.columns):
                                        col_str = str(col).lower()
                                        if any(k in col_str for k in ['수량', 'qty', 'quantity']):
                                            qty_col = i
                                        elif any(k in col_str for k in ['단가', '가격', 'price']):
                                            price_col = i
                                    
                                    if qty_col is not None and price_col is not None:
                                        # Extract price table
                                        price_table = []
                                        for _, row in df.iterrows():
                                            try:
                                                qty = row.iloc[qty_col]
                                                price = row.iloc[price_col]
                                                
                                                # Clean data
                                                if isinstance(qty, str):
                                                    qty = ''.join(filter(str.isdigit, qty.replace(',', '')))
                                                    qty = int(qty) if qty else 0
                                                
                                                if isinstance(price, str):
                                                    price = ''.join(filter(str.isdigit, price.replace(',', '')))
                                                    price = int(price) if price else 0
                                                
                                                if qty and price:
                                                    price_table.append({"quantity": int(qty), "price": int(price)})
                                            except Exception as e:
                                                logger.warning(f"Error extracting row from price table: {e}")
                                                continue
                                        
                                        if price_table:
                                            # Sort price table by quantity
                                            price_table.sort(key=lambda x: x["quantity"])
                                            result["price_table"] = price_table
                                            
                                            # Store raw table data
                                            result["raw_price_table"] = {
                                                "quantities": [item["quantity"] for item in price_table],
                                                "prices": [item["price"] for item in price_table]
                                            }
                                            
                                            # Check for VAT info on the page
                                            vat_text_selectors = [
                                                'div:has-text("부가세")',
                                                'div:has-text("VAT")',
                                                'p:has-text("부가세")',
                                                'p:has-text("VAT")'
                                            ]
                                            
                                            for vat_selector in vat_text_selectors:
                                                if await seller_page.locator(vat_selector).count() > 0:
                                                    vat_text = await seller_page.locator(vat_selector).text_content()
                                                    if '별도' in vat_text or '미포함' in vat_text:
                                                        result["vat_included"] = False
                                                        logger.info(f"VAT not included based on text: {vat_text}")
                                                        break
                                                    elif '포함' in vat_text:
                                                        result["vat_included"] = True
                                                        logger.info(f"VAT included based on text: {vat_text}")
                                                        break
                                            
                                            # Store all quantity prices
                                            for item in price_table:
                                                qty = item["quantity"]
                                                price = item["price"]
                                                price_with_vat = price if result["vat_included"] else round(price * 1.1)
                                                result["quantity_prices"][qty] = {
                                                    "price": price,
                                                    "price_with_vat": price_with_vat,
                                                    "exact_match": True
                                                }
                                            
                                            logger.info(f"Extracted {len(result['quantity_prices'])} quantity-price pairs")
                                            break
                            except Exception as e:
                                logger.warning(f"Error parsing quantity price table: {e}")
                    
                    # Close the seller page context
                    await seller_context.close()
                    
                except Exception as e:
                    logger.error(f"Error visiting seller site: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting quantity pricing from {product_url}: {e}")
        return result

async def crawl_naver_products(product_rows: pd.DataFrame, config: configparser.ConfigParser, browser=None) -> list:
    """
    Crawl product information from Naver Shopping using API asynchronously for multiple product rows,
    including image downloading, optional background removal, and quantity-based pricing.
    """
    if product_rows is None or len(product_rows) == 0:
        logger.info("🟢 Naver crawl: Input product_rows is empty or None. Skipping.")
        return []  # Return empty list

    total_products = len(product_rows)
    logger.info(f"🟢 --- Starting Naver product crawl for {total_products} products (Async) ---")

    # Get config values
    try:
        base_image_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
        naver_image_dir = os.path.join(base_image_dir, 'Naver')
        os.makedirs(naver_image_dir, exist_ok=True)
        
        use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
        naver_scrape_limit = config.getint('ScraperSettings', 'naver_scrape_limit', fallback=50)
        max_concurrent_api = config.getint('ScraperSettings', 'naver_max_concurrent_api', fallback=3)
        
        target_quantities_str = config.get('ScraperSettings', 'target_quantities', fallback='300,500,1000,2000')
        target_quantities = [int(qty.strip()) for qty in target_quantities_str.split(',') if qty.strip().isdigit()]
        if not target_quantities:
            target_quantities = [300, 500, 1000, 2000]
            
        visit_seller_sites = config.getboolean('ScraperSettings', 'naver_visit_seller_sites', fallback=True)
        
        logger.info(f"🟢 Naver API Configuration: Limit={naver_scrape_limit}, Max Concurrent API={max_concurrent_api}, "
                    f"BG Removal={use_bg_removal}, Image Dir={naver_image_dir}, "
                    f"Target Quantities={target_quantities}, Visit Seller Sites={visit_seller_sites}")
    except Exception as e:
        logger.error(f"Error reading config: {e}")
        return []

    # Create semaphore for concurrent API requests
    api_semaphore = asyncio.Semaphore(max_concurrent_api)
    
    # Initialize Playwright browser if we're visiting seller sites and no browser was provided
    playwright = None
    if visit_seller_sites and not browser:
        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(headless=False)
            logger.info("Created new browser instance for seller site visits")
        except Exception as e:
            logger.error(f"Failed to create browser instance: {e}")
            browser = None
            visit_seller_sites = False

    try:
        # Create HTTP client
        async with get_async_httpx_client(config=config) as client:
            # Process each row concurrently
            tasks = []
            for idx, row in product_rows.iterrows():
                task = _process_single_naver_row(
                    idx=idx,
                    row=row,
                    config=config,
                    client=client,
                    api_semaphore=api_semaphore,
                    naver_scrape_limit=naver_scrape_limit,
                    naver_image_dir=naver_image_dir,
                    browser=browser,
                    target_quantities=target_quantities,
                    visit_seller_sites=visit_seller_sites
                )
                tasks.append(task)

            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            valid_results = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error processing product: {result}")
                elif result is not None:
                    valid_results.append(result)

            return valid_results

    except Exception as e:
        logger.error(f"Error in crawl_naver_products: {e}")
        return []
    finally:
        # Clean up browser if we created it
        if playwright:
            try:
                await browser.close()
                await playwright.stop()
                logger.info("Closed browser and playwright instance")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")

async def _process_single_naver_row(idx, row, config, client, api_semaphore, naver_scrape_limit, naver_image_dir, browser=None, target_quantities=None, visit_seller_sites=False):
    """Processes a single product row for Naver API search and image download."""
    product_name = row.get('상품명', '')
    if not product_name or pd.isna(product_name):
        logger.debug(f"Skipping row {idx} due to missing product name.")
        return None

    # Get reference price
    reference_price = 0.0
    if '판매단가(V포함)' in row and pd.notna(row['판매단가(V포함)']):
        try:
            reference_price = float(str(row['판매단가(V포함)']).replace(',', ''))
        except:
            pass

    # Search Naver API
    async with api_semaphore:
        naver_data = await crawl_naver(
            original_query=product_name,
            client=client,
            config=config,
            max_items=naver_scrape_limit,
            reference_price=reference_price
        )

    if not naver_data:
        logger.warning(f"🟢 No Naver results found for '{product_name}' after trying all keyword variations.")
        return None

    # Get the threshold from config or use a default
    try:
        min_similarity = config.getfloat('Matching', 'naver_minimum_similarity', fallback=0.15)
    except:
        min_similarity = 0.15
    
    # Check the first item's similarity
    first_item = naver_data[0]
    similarity = first_item.get('initial_similarity', 0)
    
    if similarity < min_similarity:
        logger.warning(f"🟢 Skipping Naver result for '{product_name}' due to low similarity score: {similarity:.3f} < {min_similarity:.3f}")
        return None
    
    # Return the first Naver result with the original product name
    result_data = {
        'original_product_name': product_name,
        'name': first_item.get('name'),
        'price': first_item.get('price'),
        'seller_name': first_item.get('mallName'),
        'link': first_item.get('link'),
        'seller_link': first_item.get('mallProductUrl'),
        'source': 'naver',
        'initial_similarity': similarity,
        'is_naver_site': False,
        'has_quantity_pricing': False,  # Default values for when we skip crawling
        'quantity_prices': {},
        'vat_included': False,
        'has_captcha': False
    }
    
    # 네이버 사이트인지 체크
    product_url = result_data['link']
    seller_name = result_data['seller_name']
    
    # 공급사명이 '네이버'인 경우 특별 처리
    is_naver_seller = seller_name == "네이버" or "네이버" in seller_name
    is_naver_domain = product_url and ("naver.com" in product_url or "shopping.naver.com" in product_url)
    
    result_data['is_naver_site'] = is_naver_domain
    result_data['is_naver_seller'] = is_naver_seller

    # 판촉물 사이트 감지 키워드 확장
    promo_keywords = [
        '온오프마켓', '답례품', '기프트', '판촉', '기념품', '인쇄', '각인', '제작', 
        '미스터몽키', '홍보', '호갱탈출', '다조아', '기업판촉', '단체선물', '사은품',
        '홍보물', '판촉물', '기업기념품', '단체주문', '대량구매', '대량주문', '맞춤제작',
        '로고인쇄', '로고각인', '주문제작', '제품홍보', '기업홍보', '단체구매'
    ]

    # 판촉물 사이트 감지 로직 강화 - 외부 사이트만 체크
    is_promotional = False
    matching_keywords = []

    # 상품명, 판매자명, 링크 URL에서 키워드 검사
    for keyword in promo_keywords:
        if keyword in product_name.lower():
            is_promotional = True
            matching_keywords.append(f"상품명: {keyword}")
        if keyword in result_data['seller_name'].lower():
            is_promotional = True
            matching_keywords.append(f"판매자: {keyword}")
        if result_data['link'] and keyword in result_data['link'].lower():
            is_promotional = True
            matching_keywords.append(f"링크: {keyword}")

    if matching_keywords:
        logger.info(f"판촉물 사이트 감지 - 매칭된 키워드: {', '.join(matching_keywords)}")
    
    result_data['is_promotional_site'] = is_promotional

    # Visit seller site to check for quantity-based pricing only if we have a browser and visit_seller_sites is True
    # AND it's a promotional item
    if visit_seller_sites and browser and first_item.get('link') and is_promotional:
        page = None
        try:
            page = await browser.new_page()
            await page.set_viewport_size({"width": 1366, "height": 768})
            
            # First check for CAPTCHA
            await page.goto(first_item.get('link'), wait_until='networkidle', timeout=30000)
            
            # Check for CAPTCHA using direct detection
            has_captcha = False
            captcha_selectors = [
                'form#captcha_form', 
                'img[alt*="captcha"]',
                'div.captcha_wrap',
                'input[name="captchaBotKey"]',
                'div[class*="captcha"]',
                'iframe[src*="captcha"]',
                'div[class*="bot-check"]',
                'div[class*="security-check"]'
            ]
            for selector in captcha_selectors:
                if await page.locator(selector).count() > 0:
                    logger.info(f"CAPTCHA detected on page: {first_item.get('link')} for product '{product_name}'")
                    has_captcha = True
                    break
            
            if has_captcha:
                logger.info(f"CAPTCHA detected for '{product_name}'. Skipping further crawling and using API data only.")
                result_data['has_captcha'] = True
                # Don't attempt any further crawling, just use the API data
            else:
                # No CAPTCHA, proceed with normal crawling
                if is_naver_seller:
                    logger.info(f"Detected Naver seller for '{product_name}'. Navigating to product page and clicking 최저가 button.")
                    try:
                        # 최저가 버튼 찾기 및 클릭
                        lowest_price_selectors = [
                            '//div[contains(@class, "lowestPrice_btn_box")]/div[contains(@class, "buyButton_compare_wrap")]/a[text()="최저가 사러가기"]',
                            '//a[contains(text(), "최저가 사러가기")]',
                            '//a[contains(text(), "최저가")]',
                            '//a[contains(@class, "lowest_price")]',
                            '//button[contains(text(), "최저가")]'
                        ]
                        
                        button_found = False
                        for selector in lowest_price_selectors:
                            try:
                                if await page.locator(selector).count() > 0:
                                    logger.info(f"Found lowest price button with selector: {selector}")
                                    href = await page.locator(selector).get_attribute('href')
                                    if href:
                                        logger.info(f"Navigating to lowest price URL: {href}")
                                        await page.goto(href, wait_until='networkidle', timeout=30000)
                                    else:
                                        logger.info("Clicking lowest price button")
                                        await page.locator(selector).click()
                                        await page.wait_for_load_state('networkidle', timeout=30000)
                                    
                                    button_found = True
                                    current_url = page.url
                                    logger.info(f"After clicking lowest price button, now at URL: {current_url}")
                                    result_data['link'] = current_url
                                    break
                            except Exception as e:
                                logger.warning(f"Error with lowest price selector {selector}: {e}")
                                continue
                        
                        if not button_found:
                            logger.warning(f"Could not find lowest price button for Naver seller item '{product_name}'")
                    
                    except Exception as e:
                        logger.error(f"Error navigating to product page or clicking lowest price button: {e}")
                
                # Check for quantity pricing only if no CAPTCHA was detected
                try:
                    logger.info(f"Calling extract_quantity_prices for '{product_name}' with link: {page.url}")
                    print(f"Checking quantity prices for '{product_name}'. Please observe the browser window...")
                    
                    quantity_pricing = await extract_quantity_prices(page, page.url, target_quantities)
                    await asyncio.sleep(5)
                    print(f"Completed quantity price check for '{product_name}'")

                    # Update result data with quantity pricing information
                    result_data.update({
                        'has_quantity_pricing': quantity_pricing.get('has_quantity_pricing', False),
                        'quantity_prices': quantity_pricing.get('quantity_prices', {}),
                        'vat_included': quantity_pricing.get('vat_included', False),
                        'is_naver_site': quantity_pricing.get('is_naver_site', False),
                        'has_captcha': quantity_pricing.get('has_captcha', False)
                    })

                    # Update promotional site status based on quantity pricing
                    if quantity_pricing.get('has_quantity_pricing'):
                        result_data['is_promotional_site'] = True
                        logger.info("수량별 가격표 발견으로 판촉물 사이트로 판단")
                except Exception as e:
                    logger.error(f"Error extracting quantity prices for '{product_name}': {e}")
                
        except Exception as e:
            logger.error(f"Error visiting seller site for '{product_name}': {e}")
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    logger.error(f"Error closing page: {e}")
    elif visit_seller_sites and browser and first_item.get('link') and not is_promotional:
        logger.info(f"판촉물이 아니므로 판매자 사이트 방문 건너뜀: {product_name}")

    # Process image if available
    image_url = first_item.get('image_url')
    if image_url:
        result_data['image_url'] = image_url
        local_path = await download_naver_image(image_url, naver_image_dir, product_name, config)
        if local_path:
            # Ensure absolute path
            abs_local_path = os.path.abspath(local_path)
            result_data['image_path'] = abs_local_path
            
            # Create consistent image data structure
            image_data = {
                'url': image_url,
                'local_path': abs_local_path,
                'original_path': abs_local_path,
                'source': 'naver',
                'product_name': product_name,
                'similarity': similarity,
                'type': 'naver'  # Explicitly mark as Naver image
            }
            
            # Add image data to result
            result_data['image_data'] = image_data
            result_data['naver_image_data'] = image_data  # Duplicate for correct column mapping
            
            # Create a dedicated naver_image entry with the correct structure for '네이버 이미지' column
            naver_image_entry = {
                'url': image_url,
                'local_path': abs_local_path,
                'source': 'naver',
                'score': similarity  # Include similarity score for filtering
            }
            result_data['네이버 이미지'] = naver_image_entry
    
    return result_data


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
    """Test Naver API and price crawling functionality"""
    # Setup basic logging for the test
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s:%(name)s:%(lineno)d - %(message)s')
    logging.getLogger("httpx").setLevel(logging.WARNING)
    print("--- Running Naver API Test with Promotional Items ---")
    logger.info("--- Running Naver API Test with Promotional Items (Async) ---")

    # Load config
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, '..', 'config.ini')

    try:
        config = load_config(config_path)
        print(f"Config loaded from: {config_path}")
    except Exception as e:
        print(f"ERROR loading config: {e}")
        return

    # Test products including promotional items
    test_data = {
        '구분': ['A', 'A', 'A'],  # Adjusted for three products
        '담당자': ['테스트', '테스트', '테스트'],
        '업체명': ['테스트업체', '테스트업체', '테스트업체'],
        '업체코드': ['T001', 'T001', 'T001'],
        'Code': ['CODE001', 'CODE002', 'CODE003'],
        '중분류카테고리': ['테스트카테고리', '테스트카테고리', '테스트카테고리'],
        '상품명': [
            '멀티 아쿠아 쿨토시',  # New test product 1
            '원형 미니거울 3TMM007',  # New test product 2
            '메쉬가방 대형 비치백 망사가방 비치가방 43X39X20'  # New test product 3
        ],
        '기본수량(1)': [300, 500, 1000],  # Adjusted quantities for testing
        '판매단가(V포함)': [15000, 20000, 25000],
        '본사상품링크': ['', '', '']
    }
    test_df = pd.DataFrame(test_data)
    
    print("\n=== Testing with promotional items ===")
    print("Test products:")
    for idx, name in enumerate(test_df['상품명'], 1):
        print(f"{idx}. {name}")

    try:
        # Initialize Playwright
        async with async_playwright() as playwright:
            try:
                # Switch to non-headless mode to observe the crawling process
                browser = await playwright.chromium.launch(headless=False)
                print(f"Browser launched successfully: {browser}")
                
                print("\n--- Testing crawl_naver_products with promotional items ---")
                result_list = await crawl_naver_products(test_df, config, browser=browser)
                
                # Print results
                for idx, result in enumerate(result_list, 1):
                    print(f"\nProduct {idx}: {result.get('original_product_name', 'Unknown')}")
                    print("-" * 50)
                    
                    # Basic info
                    print(f"Matched name: {result.get('name', 'No match')}")
                    print(f"Price: {result.get('price', 'N/A'):,}원")
                    print(f"Seller: {result.get('seller_name', 'Unknown')}")
                    print(f"Similarity score: {result.get('initial_similarity', 0):.3f}")
                    
                    # Links
                    print(f"네이버 쇼핑 링크: {result.get('link', 'No link')}")
                    print(f"공급사 상품링크: {result.get('seller_link', 'No link')}")
                    
                    # Promotional site check
                    is_promo = result.get('is_promotional_site', False)
                    has_qty_pricing = result.get('has_quantity_pricing', False)
                    print(f"Is promotional site: {'Yes' if is_promo else 'No'}")
                    print(f"Has quantity pricing: {'Yes' if has_qty_pricing else 'No'}")
                    
                    # Add error information if any
                    if result.get('error'):
                        print(f"Error: {result.get('error')}")
                    
                    # Quantity prices if available
                    qty_prices = result.get('quantity_prices', {})
                    if qty_prices:
                        print("\nQuantity-based prices:")
                        print("-" * 50)
                        print("| {:^8} | {:^12} | {:^12} | {:^20} |".format(
                            "수량", "단가", "VAT포함", "비고"))
                        print("-" * 50)
                        
                        # Ensure sorted display of quantities (smallest to largest)
                        sorted_quantities = sorted(qty_prices.keys())
                        
                        for qty in sorted_quantities:
                            price_info = qty_prices[qty]
                            
                            # Ensure numeric types for proper formatting
                            try:
                                qty_num = int(qty)
                                price = int(price_info.get('price', 0))
                                price_vat = int(price_info.get('price_with_vat', 0))
                                note = "정확한 수량" if price_info.get('exact_match', False) else "근사치"
                                
                                print("| {:>8,d} | {:>12,} | {:>12,} | {:<20} |".format(
                                    qty_num, price, price_vat, note))
                            except (ValueError, TypeError) as e:
                                print(f"Error formatting quantity {qty}: {e}")
                        print("-" * 50)
                    
                    # Image information
                    if 'image_data' in result:
                        img_data = result['image_data']
                        print("\nImage information:")
                        print(f"URL: {img_data.get('url', 'No URL')}")
                        local_path = img_data.get('local_path', 'No local path')
                        print(f"Local path: {local_path}")
                        
                        if local_path and os.path.exists(local_path):
                            size = os.path.getsize(local_path)
                            print(f"Image file size: {size:,} bytes")
                        else:
                            print("Warning: Image file not found locally")
            except Exception as browser_error:
                print(f"\n⛔ BROWSER ERROR: {browser_error}")
                logger.error(f"Browser error: {browser_error}", exc_info=True)
            finally:
                if browser:
                    await browser.close()

            # Summary
            print("\n=== Test Summary ===")
            total_results = len(result_list)
            promo_sites = sum(1 for r in result_list if r.get('is_promotional_site', False))
            with_qty_pricing = sum(1 for r in result_list if r.get('has_quantity_pricing', False))
            with_images = sum(1 for r in result_list if 'image_data' in r)
            
            print(f"Total products processed: {total_results}")
            print(f"Promotional sites detected: {promo_sites}")
            print(f"Products with quantity pricing: {with_qty_pricing}")
            print(f"Products with images: {with_images}")
            
            if total_results == 0:
                print("\n⛔ TEST FAILED: No results returned")
            else:
                print("\n✅ TEST COMPLETED SUCCESSFULLY")
    
    except Exception as e:
        print(f"\n⛔ TEST ERROR: {e}")
        logger.error(f"Test error: {e}", exc_info=True)
    
    print("\n--- Naver API Test Finished ---")

def generate_output_filename(base_name: str, timestamp: str = None) -> str:
    """
    Generate a properly formatted output filename.
    
    Args:
        base_name: Base name for the file
        timestamp: Optional timestamp string
        
    Returns:
        Properly formatted filename
    """
    # Clean the base name
    base_name = base_name.strip()
    
    # Generate timestamp if not provided
    if not timestamp:
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
    
    # Create filename with proper path handling
    filename = f"{base_name}-{timestamp}.xlsx"
    
    # Normalize the path to handle Korean characters
    output_dir = os.path.normpath(os.path.join('C:', 'RPA', 'Output'))
    full_path = os.path.normpath(os.path.join(output_dir, filename))
    
    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    return full_path

def save_results_to_excel(df: pd.DataFrame, base_name: str = "승인관리", timestamp: str = None) -> str:
    """
    Save results to Excel file with proper path handling.
    
    Args:
        df: DataFrame to save
        base_name: Base name for the output file
        timestamp: Optional timestamp string
        
    Returns:
        Path to the saved file
    """
    try:
        # Generate proper output path
        output_path = generate_output_filename(base_name, timestamp)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save with encoding specification
        df.to_excel(output_path, index=False, engine='openpyxl')
        logger.info(f"Successfully saved results to: {output_path}")
        
        return output_path
    except Exception as e:
        logger.error(f"Error saving results to Excel: {e}")
        raise

async def process_matching(df: pd.DataFrame, config: configparser.ConfigParser, browser=None) -> Tuple[pd.DataFrame, str]:
    """Process product matching with proper output handling."""
    try:
        # ... existing processing code ...
        
        # Generate timestamp once for consistent naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save results with proper path handling
        if len(df) <= 10:
            base_name = f"성은({len(df)}개)-승인관리"
        else:
            base_name = f"승인관리"
        
        output_path = save_results_to_excel(df, base_name, timestamp)
        
        return df, output_path
    except Exception as e:
        logger.error(f"Error in process_matching: {e}")
        raise

async def main(input_file: str = None, output_file: str = None, config_file: str = None):
    """
    Main entry point for the Naver crawler.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output Excel file (optional)
        config_file: Path to config file (optional)
    """
    try:
        # Set up logging
        setup_logging()
        
        # Load configuration
        config = load_config(config_file)
        
        # Read input file
        df = pd.read_excel(input_file) if input_file else create_test_dataframe()
        
        # Process matching
        result_df, output_path = await process_matching(df, config)
        
        logger.info(f"Processing completed successfully. Output saved to: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

def setup_logging():
    """Configure logging with proper encoding for Korean characters."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('naver_crawler.log', encoding='utf-8')
        ]
    )

def load_config(config_file: str = None) -> configparser.ConfigParser:
    """Load configuration with proper encoding."""
    config = configparser.ConfigParser()
    
    if config_file and os.path.exists(config_file):
        config.read(config_file, encoding='utf-8')
    
    return config

def create_test_dataframe() -> pd.DataFrame:
    """Creates a sample DataFrame for testing purposes."""
    logger.info("Creating a sample DataFrame for testing as no input file was provided.")
    test_data = {
        '구분': ['A', 'A', 'A'],
        '담당자': ['테스트', '테스트', '테스트'],
        '업체명': ['테스트업체', '테스트업체', '테스트업체'],
        '업체코드': ['T001', 'T001', 'T001'],
        'Code': ['CODE001', 'CODE002', 'CODE003'],
        '중분류카테고리': ['테스트카테고리', '테스트카테고리', '테스트카테고리'],
        '상품명': [
            '멀티 아쿠아 쿨토시',
            '원형 미니거울 3TMM007',
            '메쉬가방 대형 비치백 망사가방 비치가방 43X39X20'
        ],
        '기본수량(1)': [300, 500, 1000],
        '판매단가(V포함)': [15000, 20000, 25000],
        '본사상품링크': ['', '', '']
    }
    return pd.DataFrame(test_data)

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    print("Running Naver API test with promotional items...")
    asyncio.run(_test_main())
