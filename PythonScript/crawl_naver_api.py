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
from playwright.async_api import async_playwright

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

        # Ensure save directory exists (standardize paths)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir, exist_ok=True)
        
        # Always add Naver subdirectory unless it already exists in the path
        if not save_dir.endswith('Naver'):
            # Normalize path separators
            save_dir_normalized = save_dir.replace('/', os.sep).replace('\\', os.sep)
            
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
            
            # 배경 제거 버전이 이미 있는지 확인
            try:
                use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
                if use_bg_removal:
                    bg_removed_path = local_path.replace('.', '_nobg.', 1)
                    if os.path.exists(bg_removed_path) and os.path.getsize(bg_removed_path) > 0:
                        final_image_path = bg_removed_path
                        logger.debug(f"Using existing background-removed image: {final_image_path}")
                    else:
                        # 배경 제거 버전이 없으면 생성 시도
                        try:
                            from image_utils import remove_background
                            if remove_background(local_path, bg_removed_path):
                                final_image_path = bg_removed_path
                                logger.debug(f"Background removed for existing Naver image: {final_image_path}")
                            else:
                                logger.warning(f"Failed to remove background for Naver image {local_path}. Using original.")
                        except Exception as bg_err:
                            logger.warning(f"Error during background removal: {bg_err}. Using original image.")
            except Exception as config_err:
                logger.warning(f"Error reading background removal config: {config_err}. Using original image.")
            
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
                            
                            # 필요시 배경 제거 시도
                            try:
                                use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
                                if use_bg_removal:
                                    from image_utils import remove_background
                                    bg_removed_path = local_path.replace('.', '_nobg.', 1)
                                    if remove_background(local_path, bg_removed_path):
                                        final_image_path = bg_removed_path
                                        logger.debug(f"Background removed for downloaded Naver image: {final_image_path}")
                                    else:
                                        logger.warning(f"Failed to remove background for Naver image {local_path}. Using original.")
                            except Exception as bg_err:
                                logger.warning(f"Error during background removal: {bg_err}. Using original image.")
                                
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

async def crawl_naver_products(product_rows: pd.DataFrame, config: configparser.ConfigParser) -> list:
    """
    Crawl product information from Naver Shopping using API asynchronously for multiple product rows,
    including image downloading and optional background removal.

    Args:
        product_rows (pd.DataFrame): DataFrame containing products to search for.
                                     Requires '상품명'. Optional '판매단가(V포함)', '구분'.
        config (configparser.ConfigParser): ConfigParser object containing configuration.

    Returns:
        list: A list of dictionaries containing crawled Naver data with original product names
    """
    if product_rows is None or len(product_rows) == 0:
        logger.info("🟢 Naver crawl: Input product_rows is empty or None. Skipping.")
        return []  # Return empty list

    total_products = len(product_rows)
    logger.info(f"🟢 --- Starting Naver product crawl for {total_products} products (Async) ---")

    # Get config values
    try:
        base_image_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
        # Use image_main_dir for Naver images to match the pattern used by Kogift and Haereum
        naver_image_dir = os.path.join(base_image_dir, 'Naver')
        os.makedirs(naver_image_dir, exist_ok=True)
        
        use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
        naver_scrape_limit = config.getint('ScraperSettings', 'naver_scrape_limit', fallback=50)
        max_concurrent_api = config.getint('ScraperSettings', 'naver_max_concurrent_api', fallback=3)
        logger.info(f"🟢 Naver API Configuration: Limit={naver_scrape_limit}, Max Concurrent API={max_concurrent_api}, BG Removal={use_bg_removal}, Image Dir={naver_image_dir}")
    except Exception as e:
        logger.error(f"Error reading config: {e}")
        return []

    # Create semaphore for concurrent API requests
    api_semaphore = asyncio.Semaphore(max_concurrent_api)

    # Create tasks for concurrent processing
    tasks = []
    async with get_async_httpx_client(config=config) as client:
        for idx, row in product_rows.iterrows():
            tasks.append(
                _process_single_naver_row(
                    idx, row, config, client, api_semaphore, 
                    naver_scrape_limit, naver_image_dir
                )
            )
        
        # Run tasks concurrently and collect results
        processed_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions and None results
    results = []
    for res in processed_results:
        if isinstance(res, Exception):
            logger.error(f"Error processing Naver row: {res}")
        elif res is not None:
            results.append(res)

    logger.info(f"🟢 Naver crawl finished. Processed {len(results)} valid results out of {total_products} rows.")
    
    # FIXED: Add validation for results to ensure image paths are properly set
    validated_results = []
    for result in results:
        try:
            # Skip invalid results
            if not isinstance(result, dict) or 'original_product_name' not in result:
                logger.warning(f"Skipping invalid Naver result: {result}")
                continue
            
            # Ensure image_data exists and is properly formatted
            if 'image_data' in result and isinstance(result['image_data'], dict):
                # Make sure image_data has required fields
                image_data = result['image_data']
                if 'local_path' not in image_data or not image_data['local_path']:
                    # Check if we have image_url but no local_path
                    if 'image_url' in result and result['image_url']:
                        # Try to download image again
                        try:
                            local_path = await download_naver_image(
                                result['image_url'], naver_image_dir, 
                                result['original_product_name'], config
                            )
                            if local_path:
                                image_data['local_path'] = local_path
                                logger.info(f"Fixed missing local_path for {result['original_product_name']}")
                        except Exception as e:
                            logger.error(f"Failed to download image during validation: {e}")
                
                # Ensure URL is present
                if 'url' not in image_data and 'image_url' in result:
                    image_data['url'] = result['image_url']
                
                # Ensure source is present
                if 'source' not in image_data:
                    image_data['source'] = 'naver'
                
                # Update result
                result['image_data'] = image_data
            
            validated_results.append(result)
        except Exception as e:
            logger.error(f"Error validating Naver result: {e}")
    
    logger.info(f"Validation complete. {len(validated_results)} valid results (removed {len(results) - len(validated_results)} invalid)")
    return validated_results

# Helper function to process a single row for crawl_naver_products
async def _process_single_naver_row(idx, row, config, client, api_semaphore, naver_scrape_limit, naver_image_dir):
    """Processes a single product row for Naver API search and image download."""
    product_name = row.get('상품명', '')
    if not product_name or pd.isna(product_name):
        logger.debug(f"Skipping row {idx} due to missing product name.")
        return None # Skip this row

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
        return None  # No Naver data found

    # FIXED: Add additional similarity check before returning result
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
        'source': 'naver',  # 공급사 정보 명시 (Kogift 방식을 따라)
        'initial_similarity': similarity  # Keep track of similarity score
    }

    # Process image if available
    image_url = first_item.get('image_url')
    if image_url:
        # FIXED: Ensure we clearly store the original image URL
        result_data['image_url'] = image_url
        
        # Download the image
        local_path = await download_naver_image(image_url, naver_image_dir, product_name, config) 
        if local_path:
            # Kogift처럼 image_path 대신 더 명확한 구조화된 이미지 정보 제공
            result_data['image_path'] = local_path
            # 이미지 데이터를 excel_utils.py에서 사용할 수 있는 형식으로 제공
            result_data['image_data'] = {
                'url': image_url,
                'local_path': local_path,
                'original_path': local_path,
                'source': 'naver',
                'image_url': image_url,  # FIXED: Explicitly add image_url to the dictionary
                'product_name': product_name,  # FIXED: Add product name for better traceability
                'similarity': similarity  # FIXED: Add similarity score to image data
            }
    
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
                    
                    # Check if we have items and validate image URLs
                    items = data.get('items', [])
                    if items:
                        print(f"✅ Found {len(items)} items in API response")
                        logger.info(f"✅ Found {len(items)} items in API response")
                        
                        # Verify first item has image URL
                        first_item = items[0]
                        image_url = first_item.get('image')
                        if image_url:
                            print(f"✅ First item has image URL: {image_url}")
                            logger.info(f"✅ First item has image URL: {image_url}")
                            
                            # Test image URL accessibility
                            try:
                                img_response = await client.get(image_url, timeout=10.0)
                                if img_response.status_code == 200:
                                    content_type = img_response.headers.get('content-type', '')
                                    content_length = img_response.headers.get('content-length', '0')
                                    
                                    if 'image' in content_type.lower():
                                        print(f"✅ Image URL is valid! Content-Type: {content_type}, Size: {content_length} bytes")
                                        logger.info(f"✅ Image URL is valid! Content-Type: {content_type}, Size: {content_length} bytes")
                                    else:
                                        print(f"⚠️ URL returns non-image content: {content_type}")
                                        logger.warning(f"⚠️ URL returns non-image content: {content_type}")
                                else:
                                    print(f"⚠️ Image URL returned status code {img_response.status_code}")
                                    logger.warning(f"⚠️ Image URL returned status code {img_response.status_code}")
                            except Exception as img_err:
                                print(f"⚠️ Failed to validate image URL: {img_err}")
                                logger.warning(f"⚠️ Failed to validate image URL: {img_err}")
                        else:
                            print("⚠️ First item has no image URL!")
                            logger.warning("⚠️ First item has no image URL!")
                    else:
                        print("⚠️ No items found in test API response")
                        logger.warning("⚠️ No items found in test API response")
                        
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
    # Use target_dir for Naver test images
    default_img_dir = os.path.join(script_dir, '..', 'naver_test_images')
    test_image_base_dir = config.get('Paths', 'image_target_dir', fallback=default_img_dir)
    test_image_dir = os.path.join(test_image_base_dir, 'Naver') # Specify Naver subdirectory

    # Create the directory if it doesn't exist
    if not os.path.exists(test_image_dir):
        try:
             os.makedirs(test_image_dir)
             logger.info(f"Created test image directory: {test_image_dir}")
        except OSError as e:
             # Log error but continue, image download might fail later
             logger.error(f"Could not create test image directory {test_image_dir}: {e}. Image download might fail.")

    # Test products list (Common Test Data)
    common_test_products = [
        "777쓰리쎄븐 TS-6500C 손톱깎이 13P세트",
        "휴대용 360도 회전 각도조절 접이식 핸드폰 거치대",
        "피에르가르뎅 3단 슬림 코지가든 우양산",
        "마루는강쥐 클리어미니케이스",
        "아테스토니 뱀부사 소프트 3P 타올 세트",
        "티드 텔유 Y타입 치실 60개입 연세대학교 치과대학"
    ]
    
    # Create test DataFrame with reference prices (Using common test data)
    test_data = {
        '구분': ['A'] * len(common_test_products),
        '담당자': ['테스트'] * len(common_test_products),
        '업체명': ['테스트업체'] * len(common_test_products),
        '업체코드': ['T001'] * len(common_test_products),
        'Code': [f'CODE{i+1:03d}' for i in range(len(common_test_products))],
        '중분류카테고리': ['테스트카테고리'] * len(common_test_products),
        '상품명': common_test_products,
        '기본수량(1)': [1] * len(common_test_products),
        '판매단가(V포함)': [10000, 15000, 25000, 12000, 5000, 8000], # Example reference prices
        '본사상품링크': [f'http://example.com/product{i+1}' for i in range(len(common_test_products))]
    }
    test_df = pd.DataFrame(test_data)
    
    print(f"Testing Naver API with {len(test_df)} products...")
    logger.info(f"Testing Naver API with {len(test_df)} products using DataFrame:")
    logger.info(test_df.to_string()) # Log the test data
    
    try:
        # Now test the full crawl_naver_products function
        print("--- Testing full crawl_naver_products function ---")
        logger.info("--- Testing full crawl_naver_products function ---")
        start_full_crawl_time = time.monotonic()
        
        # FIXED: crawl_naver_products now returns a list, not a DataFrame
        result_list = await crawl_naver_products(
            product_rows=test_df.copy(), # Pass a copy to avoid modification issues
            config=config
        )
        full_crawl_time = time.monotonic() - start_full_crawl_time
        logger.info(f"crawl_naver_products completed in {full_crawl_time:.2f} seconds.")
    
        print(f"--- Test Results (crawl_naver_products processed {len(result_list)} rows) ---")
        logger.info(f"--- Test Results (crawl_naver_products processed {len(result_list)} items) ---")
        
        # FIXED: Handle the list output format instead of expecting a DataFrame
        if isinstance(result_list, list):
            logger.info(f"Result list contains {len(result_list)} items")
            
            # Log sample data for each result
            for idx, result in enumerate(result_list[:3]): # Show first 3 results only
                if isinstance(result, dict):
                    logger.info(f"Result {idx+1} keys: {list(result.keys())}")
                    product_name = result.get('original_product_name', 'Unknown')
                    matched_name = result.get('name', 'No match')
                    price = result.get('price', 'N/A')
                    
                    logger.info(f"Product: '{product_name}' -> Matched: '{matched_name}' (Price: {price})")
                    
                    # Enhanced image validation
                    if 'image_data' in result and isinstance(result['image_data'], dict):
                        img_path = result['image_data'].get('local_path', 'No local path')
                        img_url = result['image_data'].get('url', 'No URL')
                        logger.info(f"  Image path: {img_path}")
                        logger.info(f"  Image URL: {img_url}")
                        
                        # Validate downloaded image exists
                        if os.path.exists(img_path):
                            try:
                                img_size = os.path.getsize(img_path)
                                print(f"✅ Downloaded image exists: {img_path} ({img_size} bytes)")
                                logger.info(f"✅ Downloaded image exists: {img_path} ({img_size} bytes)")
                                
                                # Validate image can be opened with PIL
                                try:
                                    with Image.open(img_path) as img:
                                        width, height = img.size
                                        print(f"✅ Image is valid: {width}x{height} pixels, format: {img.format}")
                                        logger.info(f"✅ Image is valid: {width}x{height} pixels, format: {img.format}")
                                except Exception as img_err:
                                    print(f"⚠️ Downloaded image cannot be opened: {img_err}")
                                    logger.warning(f"⚠️ Downloaded image cannot be opened: {img_err}")
                            except Exception as os_err:
                                print(f"⚠️ Error checking image file: {os_err}")
                                logger.warning(f"⚠️ Error checking image file: {os_err}")
                        else:
                            print(f"⚠️ Downloaded image file not found: {img_path}")
                            logger.warning(f"⚠️ Downloaded image file not found: {img_path}")
                    else:
                        print(f"⚠️ No image data for product: {product_name}")
                        logger.warning(f"⚠️ No image data for product: {product_name}")
                else:
                    logger.info(f"Result {idx+1} is not a dictionary: {type(result)}")
            
            # Additional validation: Check original image URLs are accessible
            print("\n--- Validating original image URLs ---")
            logger.info("--- Validating original image URLs ---")
            
            async with aiohttp.ClientSession() as session:
                image_validation_tasks = []
                
                for idx, result in enumerate(result_list[:3]):  # Test first 3 for speed
                    if isinstance(result, dict) and 'image_url' in result and result['image_url']:
                        image_url = result['image_url']
                        product_name = result.get('original_product_name', f'Product {idx+1}')
                        
                        async def validate_image_url(url, product):
                            try:
                                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                    status = response.status
                                    content_type = response.headers.get('content-type', '')
                                    
                                    if status == 200 and 'image' in content_type.lower():
                                        content_length = response.headers.get('content-length', 'unknown')
                                        print(f"✅ Image URL valid for '{product}': {url} ({content_type}, {content_length} bytes)")
                                        logger.info(f"✅ Image URL valid for '{product}': {url} ({content_type}, {content_length} bytes)")
                                        return True
                                    else:
                                        print(f"⚠️ Image URL issue for '{product}': Status {status}, Content-Type: {content_type}")
                                        logger.warning(f"⚠️ Image URL issue for '{product}': Status {status}, Content-Type: {content_type}")
                                        return False
                            except Exception as e:
                                print(f"⚠️ Error validating image URL for '{product}': {e}")
                                logger.warning(f"⚠️ Error validating image URL for '{product}': {e}")
                                return False
                        
                        image_validation_tasks.append(validate_image_url(image_url, product_name))
                
                # Execute image validation tasks
                if image_validation_tasks:
                    image_validation_results = await asyncio.gather(*image_validation_tasks, return_exceptions=True)
                    valid_urls = sum(1 for res in image_validation_results if res is True)
                    print(f"Image URL validation: {valid_urls}/{len(image_validation_tasks)} URLs are valid")
                    logger.info(f"Image URL validation: {valid_urls}/{len(image_validation_tasks)} URLs are valid")
                else:
                    print("No image URLs to validate")
                    logger.warning("No image URLs to validate")
        else:
            logger.error(f"Unexpected result type: {type(result_list)}")
    
        # Count the actual results with image data
        items_with_images = sum(1 for r in result_list if isinstance(r, dict) and 'image_data' in r)
        logger.info(f"Results with image data: {items_with_images}/{len(result_list)}")
    
        # Final success/failure assessment based on whether *any* data was found
        if len(result_list) == 0:
            print("⛔ TEST FAILED: No data was returned by the crawler!")
            logger.error("⛔ TEST FAILED: No data was returned by the crawler!")
        elif items_with_images == 0:
            print(f"⚠️ TEST PARTIAL SUCCESS: {len(result_list)} results but no images!")
            logger.warning(f"⚠️ TEST PARTIAL SUCCESS: {len(result_list)} results but no images!")
        else:
            print(f"✅ TEST COMPLETED: Data was returned for {len(result_list)} products ({items_with_images} with images).")
            logger.info(f"✅ TEST COMPLETED: Data was returned for {len(result_list)} products ({items_with_images} with images).")
    
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
