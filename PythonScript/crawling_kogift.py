import asyncio
import pandas as pd
from playwright.async_api import Browser, Page, Error as PlaywrightError
import random
import logging
from urllib.parse import urlparse, urljoin
import re
from typing import Optional, List, Dict, Any, Tuple
from utils import generate_keyword_variations
import configparser # Import configparser
import os
import time
import httpx
import aiohttp
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)
import requests
from concurrent.futures import ThreadPoolExecutor
from image_downloader import predownload_kogift_images, verify_image_url
import argparse
from pathlib import Path
from PIL import Image
import shutil
import hashlib
import json

# --- 해오름 기프트 입력 데이터에서 수량 추출 함수 ---

# 고려기프트 이미지 경로 중요 정보:
# /ez/ 경로가 이미지 URL에 반드시 포함되어야 합니다.
# 잘못된 형식: https://koreagift.com/upload/mall/shop_1736386408518966_0.jpg
# 올바른 형식: https://koreagift.com/ez/upload/mall/shop_1736386408518966_0.jpg
# 위의 /ez/ 경로가 없으면 이미지 로드가 실패하므로 모든 이미지 URL 처리 시 확인해야 합니다.

# 로거 설정 (basicConfig는 메인에서 한 번만 호출하는 것이 좋음)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Get logger instance

class SkipItemException(Exception):
    """Custom exception to signal that an item should be skipped during scraping."""
    pass

def get_kogift_urls(config: configparser.ConfigParser) -> List[str]:
    """
    Get list of Kogift URLs from config file.
    
    Args:
        config: ConfigParser object containing configuration
        
    Returns:
        List[str]: List of Kogift URLs to scrape
    """
    urls = []
    try:
        if config.has_section('ScraperSettings'):
            # Get URLs from config
            if config.has_option('ScraperSettings', 'kogift_urls'):
                urls_str = config.get('ScraperSettings', 'kogift_urls')
                urls = [url.strip() for url in urls_str.split(',') if url.strip()]
            
            # If no URLs in config, use defaults
            if not urls:
                urls = [
                    'https://koreagift.com/ez/index.php',
                    'https://adpanchok.co.kr/ez/index.php'
                ]
    except Exception as e:
        logger.error(f"Error getting Kogift URLs from config: {e}")
        # Fallback to default URLs
        urls = [
            'https://koreagift.com/ez/index.php',
            'https://adpanchok.co.kr/ez/index.php'
        ]
    
    return urls

def get_quantities_from_excel(config: configparser.ConfigParser) -> Optional[List[int]]:
    """
    Get quantities from input Excel file specified in config.
    
    Args:
        config: ConfigParser object containing configuration
        
    Returns:
        Optional[List[int]]: List of quantities if found, None if not found or error
    """
    try:
        if not config.has_section('Input'):
            logger.info("No 'Input' section found in config")
            return None
            
        input_filename = config.get('Input', 'input_file')
        logger.info(f"DETAILED_CONFIG_READ (kogift): [Input] input_file (filename) retrieved as: '{input_filename}'")

        if not input_filename:
            logger.info("No input_file specified in config")
            return None

        resolved_path = input_filename # Default in case it's already an absolute path

        if not os.path.isabs(input_filename):
            input_dir = None
            if config.has_option('Paths', 'input_dir'):
                input_dir = config.get('Paths', 'input_dir')
                logger.info(f"Input directory from [Paths]input_dir: '{input_dir}'")
            elif config.has_option('Input', 'input_dir'): # Common alternative section
                input_dir = config.get('Input', 'input_dir')
                logger.info(f"Input directory from [Input]input_dir: '{input_dir}'")
            
            if input_dir:
                resolved_path = os.path.join(input_dir, input_filename)
            else:
                logger.error(
                    f"Input file '{input_filename}' is relative, but 'input_dir' was not found "
                    f"in config under [Paths] or [Input] sections. Cannot resolve full path."
                )
                return None
            
        if not os.path.exists(resolved_path):
            logger.warning(f"Input Excel file not found at the resolved path: {resolved_path}")
            return None
            
        logger.info(f"Reading quantities from Excel file: {resolved_path}")
        df = pd.read_excel(resolved_path)
        df.columns = df.columns.str.strip() # Strip spaces from column names
        
        if '기본수량(1)' not in df.columns:
            logger.warning(f"Column '기본수량(1)' not found in Excel file after stripping column names. Available columns: {df.columns.tolist()}")
            return None
            
        quantities = df['기본수량(1)'].dropna().unique().tolist()
        quantities = [int(qty) for qty in quantities if str(qty).isdigit()]
        
        if quantities:
            logger.info(f"Found {len(quantities)} unique quantities in Excel: {quantities}")
            return sorted(quantities)
        else:
            logger.warning("No valid quantities found in Excel file")
            return None
            
    except Exception as e:
        logger.error(f"Error reading quantities from Excel: {e}")
        return None

def get_max_items_per_variation(config: configparser.ConfigParser) -> int:
    """
    Get maximum number of items to scrape per keyword variation from config.
    
    Args:
        config: ConfigParser object containing configuration
        
    Returns:
        int: Maximum number of items to scrape per variation
    """
    try:
        if config.has_section('ScraperSettings'):
            return config.getint('ScraperSettings', 'kogift_max_items', fallback=100)
    except Exception as e:
        logger.error(f"Error getting max items per variation from config: {e}")
    
    return 100  # Default value from config.ini

# Constants removed, now loaded from config
# KOGIFT_URLS = [...]
# USER_AGENT = "..."
# MIN_RESULTS_THRESHOLD = 5

# Add browser context timeout settings
BROWSER_CONTEXT_TIMEOUT = 300000  # 5 minutes
PAGE_TIMEOUT = 120000  # 2 minutes
NAVIGATION_TIMEOUT = 60000  # 1 minute

# --- Helper function to download images ---
async def download_image(url: str, save_dir: str, product_name: Optional[str] = None, config: configparser.ConfigParser = None, max_retries: int = 3) -> Optional[str]:
    """
    Download a single Kogift image to the specified directory with enhanced processing.
    Uses the same approach as Naver and Haereum for consistency.
    
    Args:
        url (str): The image URL to download.
        save_dir (str): The directory to save the image in.
        product_name (str): The product name for generating the filename.
        config (configparser.ConfigParser): ConfigParser object containing configuration.
        max_retries (int): Maximum number of retry attempts.
        
    Returns:
        Optional[str]: The local path to the downloaded image, or None if download failed.
    """
    if not url or not save_dir:
        logger.warning("Empty URL or save directory provided to download_image.")
        return None
        
    if not product_name:
        logger.error("❌ 상품명이 제공되지 않았습니다. 이미지 다운로드를 건너뜁니다. URL: {url}")
        return None

    try:
        # Ensure URL is properly encoded and valid
        if not (url.startswith('http://') or url.startswith('https://')):
            logger.warning(f"Invalid URL format: {url}")
            return None

        # Create save directory if it doesn't exist
        try:
            os.makedirs(save_dir, exist_ok=True)
            
            # Verify directory is writable
            if not os.access(save_dir, os.W_OK):
                logger.error(f"Image directory is not writable: {save_dir}")
                return None
                
            # Check for use_background_removal setting
            if config:
                use_bg_removal = config.getboolean('Matching', 'use_background_removal', fallback=True)
            else:
                use_bg_removal = False
        except Exception as e:
            logger.error(f"Error accessing config or creating image directory: {e}")
            return None
        
        # Generate filename using the same method as naver/haereum
        try:
            # 상품명 해시값 생성 (MD5) - 16자로 통일
            try:
                from utils import generate_product_name_hash
                name_hash = generate_product_name_hash(product_name)
            except ImportError:
                logger.warning("Could not import generate_product_name_hash, using fallback method")
                # 상품명 정규화 (공백 제거, 소문자 변환)
                normalized_name = ''.join(product_name.split()).lower()
                name_hash = hashlib.md5(normalized_name.encode('utf-8')).hexdigest()[:16]
            
            # 두 번째 해시값도 상품명 기반으로 생성 (일관성을 위해)
            normalized_name = ''.join(product_name.split()).lower()
            second_hash = hashlib.md5(normalized_name.encode('utf-8')).hexdigest()[16:24]
            
            # URL에서 파일 확장자 추출
            parsed_url = urlparse(url)
            file_ext = os.path.splitext(parsed_url.path)[1].lower()
            # 확장자가 없거나 유효하지 않은 경우 기본값 사용
            if not file_ext or file_ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                file_ext = '.jpg'
            
            # 새로운 형식으로 파일명 생성 (사이트이름_상품명해시_고유식별자)
            filename = f"kogift_{name_hash}_{second_hash}{file_ext}"
            local_path = os.path.join(save_dir, filename)
            final_image_path = local_path
            
        except Exception as e:
            logger.error(f"Error generating filename: {e}")
            return None
        
        # 이미 파일이 존재하는 경우 중복 다운로드 방지
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.debug(f"Image already exists: {local_path}")
            
            # 배경 제거 버전이 이미 있는지 확인
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
                            logger.debug(f"Background removed for existing Kogift image: {final_image_path}")
                        else:
                            logger.warning(f"Failed to remove background for Kogift image {local_path}. Using original.")
                    except Exception as bg_err:
                        logger.warning(f"Error during background removal: {bg_err}. Using original image.")
            
            return os.path.abspath(final_image_path)

        # Download the image using aiohttp (same as naver/haereum)
        import aiohttp
        import asyncio
        
        # Generate unique temporary filename
        temp_path = f"{local_path}.{time.time_ns()}.tmp"
        
        try:
            async with aiohttp.ClientSession() as session:
                # Try different URL variants if the original fails
                url_variants = [url]
                
                # Download with retries
                download_success = False
                
                for current_url in url_variants:
                    for attempt in range(max_retries):
                        try:
                            # Add headers for Kogift
                            headers = {
                                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                                'Referer': 'https://koreagift.com/',
                                'Connection': 'keep-alive',
                                'Cache-Control': 'max-age=0'
                            }
                            
                            logger.debug(f"Downloading image: attempt {attempt+1}/{max_retries}: {current_url}")
                            
                            async with session.get(current_url, timeout=30, headers=headers) as response:
                                if response.status != 200:
                                    logger.warning(f"HTTP error {response.status} downloading image (attempt {attempt+1}/{max_retries}): {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break
                                    
                                # Check content type
                                content_type = response.headers.get('Content-Type', '')
                                if not content_type.startswith('image/'):
                                    logger.warning(f"Non-image content type: {content_type} for URL: {current_url}")
                                    # For kogift, proceed anyway as they might return incorrect content-type
                                    
                                # Download image data
                                data = await response.read()
                                if len(data) < 100:  # Too small to be a valid image
                                    logger.warning(f"Downloaded image too small: {len(data)} bytes from URL: {current_url}")
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break
                                    
                                # Save to temporary file
                                with open(temp_path, 'wb') as f:
                                    f.write(data)
                                
                                # Validate image
                                try:
                                    from PIL import Image
                                    with Image.open(temp_path) as img:
                                        img.verify()
                                        # Re-open to check dimensions
                                        img = Image.open(temp_path)
                                        if img.width < 10 or img.height < 10:
                                            logger.warning(f"Image dimensions too small: {img.width}x{img.height}")
                                            if attempt < max_retries - 1:
                                                os.remove(temp_path)
                                                await asyncio.sleep(1 * (attempt + 1))
                                                continue
                                            break
                                        
                                    # Move temp file to final location
                                    if os.path.exists(local_path):
                                        os.remove(local_path)
                                    os.rename(temp_path, local_path)
                                    
                                    logger.debug(f"✅ 이미지 다운로드 성공: {local_path}")
                                    download_success = True
                                    break  # Success!
                                    
                                except Exception as img_err:
                                    logger.warning(f"Invalid image file: {img_err}")
                                    if os.path.exists(temp_path):
                                        os.remove(temp_path)
                                    if attempt < max_retries - 1:
                                        await asyncio.sleep(1 * (attempt + 1))
                                        continue
                                    break
                                    
                        except Exception as e:
                            logger.warning(f"Error downloading image (attempt {attempt+1}/{max_retries}): {e}")
                            if os.path.exists(temp_path):
                                try:
                                    os.remove(temp_path)
                                except:
                                    pass
                            if attempt < max_retries - 1:
                                await asyncio.sleep(1 * (attempt + 1))
                                continue
                            break
                    
                    if download_success:
                        break  # Don't try other URL variants if successful
                
                if not download_success:
                    logger.error(f"❌ 모든 시도 실패: {url}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ 예상치 못한 오류 발생: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return None
        
        # Background removal if enabled
        if use_bg_removal and os.path.exists(local_path):
            try:
                bg_removed_path = local_path.replace('.', '_nobg.', 1)
                if bg_removed_path.endswith('_nobg.jpg'):
                    bg_removed_path = bg_removed_path.replace('_nobg.jpg', '_nobg.png')
                
                from image_utils import remove_background
                if remove_background(local_path, bg_removed_path):
                    final_image_path = bg_removed_path
                    logger.debug(f"Background removed successfully: {final_image_path}")
                else:
                    logger.warning(f"Failed to remove background for Kogift image {local_path}. Using original.")
                    final_image_path = local_path
            except Exception as bg_err:
                logger.warning(f"Error during background removal: {bg_err}. Using original image.")
                final_image_path = local_path
        else:
            final_image_path = local_path
            
        return os.path.abspath(final_image_path)
        
    except Exception as e:
        logger.error(f"❌ 전체 다운로드 프로세스 실패: {e}")
        return None

async def download_images_batch(img_urls, save_dir='downloaded_images', product_name=None, config=None, max_workers=10):
    """
    Download multiple images in parallel using asyncio.
    
    Args:
        img_urls: List of image URLs to download
        save_dir: Directory to save the images
        product_name: Product name for generating filename
        config: Configuration object
        max_workers: Maximum number of concurrent downloads
        
    Returns:
        dict: Mapping of URL to local file path for successful downloads
    """
    results = {}
    
    logger.info(f"Downloading {len(img_urls)} images to {save_dir}")
    
    # Create semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(max_workers)
    
    async def download_with_semaphore(url):
        async with semaphore:
            return await download_image(url, save_dir, product_name, config)
    
    # Create tasks for all downloads
    tasks = []
    for url in img_urls:
        if url:
            tasks.append(download_with_semaphore(url))
    
    # Execute all downloads concurrently
    if tasks:
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Map results back to URLs
        for i, result in enumerate(results_list):
            if i < len(img_urls) and img_urls[i]:
                if isinstance(result, str):  # Successful download returns path
                    results[img_urls[i]] = result
                elif isinstance(result, Exception):
                    logger.error(f"Error downloading image {img_urls[i]}: {result}")
                # None results (failed downloads) are simply not added to results
    
    success_count = len(results)
    logger.info(f"Downloaded {success_count}/{len(img_urls)} images successfully")
    
    return results

# --- Helper function to block unnecessary resources --- 
def should_block_request(url: str) -> bool:
    """Determines if a network request should be blocked."""
    # Block images (except product images if needed - needs more specific pattern), 
    # stylesheets, fonts, and common tracking/ad domains.
    # Adjust the patterns based on actual site structure and needs.
    # Example: Allow product images from koreagift main server
    if ("koreagift.com" in url or "adpanchok.co.kr" in url) and any(ext in url for ext in [".jpg", ".png", ".jpeg"]):
        # Add more specific logic if needed, e.g., checking path like '/goods/'
        return False # Don't block potential product images from main domains

    blocked_domains = ["google-analytics.com", "googletagmanager.com", "facebook.net", "adservice.google.com", "googlesyndication.com", "doubleclick.net"]
    parsed_url = urlparse(url)
    # Block specific resource types based on typical file extensions
    if any(parsed_url.path.lower().endswith(ext) for ext in [".css", ".woff", ".woff2", ".ttf", ".eot", ".gif", ".svg", ".webp"]):
        # Consider allowing specific crucial CSS if site breaks
        return True # Block styles/fonts/non-product images by default
    # Block based on domain
    if any(domain in parsed_url.netloc for domain in blocked_domains):
        logger.debug(f"Blocking request to tracking/ad domain: {url}")
        return True
    return False

async def setup_page_optimizations(page: Page):
    """Applies optimizations like request blocking to a Playwright page."""
    async def handle_route(route):
        """Handle route requests with optimized resource blocking."""
        try:
            if should_block_request(route.request.url):
                try:
                    await route.abort()
                except Exception:
                    # If abort fails, try to continue as fallback
                    try:
                        await route.continue_()
                    except Exception:
                        # If both abort and continue fail, log and move on
                        pass
                return

            # For non-blocked requests, try to continue
            try:
                await route.continue_()
            except Exception as e:
                if "Target page, context or browser has been closed" in str(e):
                    # This is an expected error during cleanup, no need to log as warning
                    logging.debug(f"Route continue failed due to page/context closure: {e}")
                elif "object has been collected" in str(e):
                    # Also an expected cleanup error
                    logging.debug(f"Route continue failed due to object collection: {e}")
                else:
                    # Log unexpected errors as warnings
                    logging.warning(f"Error continuing route: {e}")
        except Exception as e:
            # Log any unexpected errors in the main handler
            logging.error(f"Unexpected error in route handler: {e}")

    try:
        await page.route("**/*", handle_route)
        logger.debug("Applied network request blocking rules.")
    except Exception as e:
        logger.warning(f"Failed to set up page optimizations: {e}")

# --- 상세 페이지에서 수량-단가 테이블 추출 함수 추가 ---
async def extract_price_table(page, product_url, timeout=30000):
    """
    상품 상세 페이지에서 수량-단가 테이블을 추출합니다.
    모든 가용한 수량-가격 정보를 가져옵니다.
    
    Args:
        page: Playwright Page 객체
        product_url: 상품 상세 페이지 URL
        timeout: 타임아웃(ms)
        
    Returns:
        DataFrame: 수량-단가 정보가 담긴 DataFrame 또는 None
    """
    try:
        await page.goto(product_url, wait_until='domcontentloaded', timeout=timeout)
        await page.wait_for_timeout(500) # Allow some time for dynamic content

        # 고려기프트 사이트의 특정 테이블 구조 우선 처리 (quantity_price__table)
        kogift_specific_table_selector = "div.product_table table.quantity_price__table"
        if await page.locator(kogift_specific_table_selector).count() > 0:
            table_element = page.locator(kogift_specific_table_selector).first
            rows = await table_element.locator("tbody > tr").all()

            quantities = []
            prices = []
            
            qty_row_found = False
            price_row_found = False

            # Identify quantity and price rows based on their first cell's content
            for row_index, row in enumerate(rows):
                try:
                    first_cell_text_content = await row.locator("td").first.text_content(timeout=1000)
                    first_cell_text = first_cell_text_content.strip() if first_cell_text_content else ""

                    if "수량" in first_cell_text and not qty_row_found:
                        qty_cells = await row.locator("td").all()
                        for i, cell in enumerate(qty_cells):
                            if i > 0: # Skip header cell
                                qty_text = await cell.text_content(timeout=1000)
                                qty_clean = ''.join(filter(str.isdigit, qty_text.replace(',', '')))
                                if qty_clean:
                                    quantities.append(int(qty_clean))
                        qty_row_found = True
                    elif "단가" in first_cell_text and not price_row_found:
                        price_cells = await row.locator("td").all()
                        for i, cell in enumerate(price_cells):
                            if i > 0: # Skip header cell
                                price_text = await cell.text_content(timeout=1000)
                                price_clean = ''.join(filter(str.isdigit, price_text.replace(',', '')))
                                if price_clean:
                                    prices.append(int(price_clean))
                        price_row_found = True
                    
                    # If both rows are found, no need to iterate further
                    if qty_row_found and price_row_found:
                        break
                except PlaywrightError as e:
                    logger.debug(f"Error processing a row in specific Kogift table: {e}")
                    continue # continue to next row
            
            # Fallback for the older structure if specific text like "수량", "단가" is not in the first td
            if not (qty_row_found and price_row_found) and len(rows) >= 3:
                try:
                    qty_row_locator = table_element.locator("tr.color1") # As seen in example
                    price_row_locator = table_element.locator("tr.color2") # As seen in example

                    if await qty_row_locator.count() > 0 and await price_row_locator.count() > 0:
                        quantities = [] # Reset if we are trying this new logic
                        prices = [] # Reset

                        qty_cells = await qty_row_locator.locator("td").all()
                        for i, cell in enumerate(qty_cells):
                            if i > 0:
                                qty_text = await cell.text_content(timeout=1000)
                                qty_clean = ''.join(filter(str.isdigit, qty_text.replace(',', '')))
                                if qty_clean:
                                    quantities.append(int(qty_clean))
                        
                        price_cells = await price_row_locator.locator("td").all()
                        for i, cell in enumerate(price_cells):
                            if i > 0:
                                price_text = await cell.text_content(timeout=1000)
                                price_clean = ''.join(filter(str.isdigit, price_text.replace(',', '')))
                                if price_clean:
                                    prices.append(int(price_clean))
                        qty_row_found = bool(quantities) # Mark as found if we got data
                        price_row_found = bool(prices)
                except PlaywrightError as e:
                     logger.debug(f"Error in fallback logic for specific Kogift table (color1/color2 classes): {e}")

            if quantities and prices and len(quantities) == len(prices):
                result_df = pd.DataFrame({'수량': quantities, '단가': prices})
                vat_info_text = ""
                try:
                    vat_wrapper_locator = page.locator("div.quantity_price__wrapper")
                    if await vat_wrapper_locator.count() > 0:
                        vat_div_locator = vat_wrapper_locator.locator("div:has-text('부가세별도')")
                        if await vat_div_locator.count() > 0:
                             vat_info_text = await vat_div_locator.first.text_content(timeout=1000) or ""
                        else: 
                            all_divs_in_wrapper = await vat_wrapper_locator.locator("div").all_text_contents()
                            for text_content in all_divs_in_wrapper:
                                if "부가세별도" in text_content or "부가세 별도" in text_content:
                                    vat_info_text = text_content
                                    break
                except PlaywrightError as e:
                    logger.debug(f"Error fetching VAT info for specific Kogift table: {e}")
                
                has_vat_excluded = "부가세별도" in vat_info_text or "부가세 별도" in vat_info_text
                if has_vat_excluded:
                    result_df.attrs['vat_excluded'] = True
                
                result_df = result_df.sort_values('수량').reset_index(drop=True)
                logger.info(f"Successfully extracted price table (Kogift specific structure) for {product_url}: {len(result_df)} tiers")
                return result_df
            elif quantities or prices: 
                 logger.warning(f"Kogift specific table found, but quantity ({len(quantities)}) and price ({len(prices)}) count mismatch for {product_url}")


        # General table selectors (fallback)
        table_selectors = [
            "div.product_table table:not(.quantity_price__table)",
            "table.detail_table",
            "div.detail_price table",
            "div.goods_detail table",
            "table#option_price_table", 
            "table.shop_table", 
            "table[summary='수량별가격']"
        ]
        
        for selector_idx, selector in enumerate(table_selectors):
            if await page.locator(selector).count() > 0:
                logger.info(f"Attempting to parse table with general selector #{selector_idx+1}: '{selector}' for {product_url}")
                try:
                    tables_found = await page.locator(selector).all()
                    for table_index, table_element in enumerate(tables_found):
                        table_html = await table_element.inner_html(timeout=2000)
                        soup = BeautifulSoup("<table>" + table_html + "</table>", 'html.parser')
                        parsed_tables = pd.read_html(str(soup), flavor='bs4')

                        if not parsed_tables:
                            logger.debug(f"No tables parsed by pandas from HTML of selector '{selector}', table index {table_index}")
                            continue
                        
                        table_df = parsed_tables[0]
                        
                        if len(table_df.columns) < 2:
                            logger.debug(f"Table from selector '{selector}', index {table_index} has < 2 columns. Skipping.")
                            continue

                        table_df = table_df.applymap(lambda x: str(x).strip() if pd.notna(x) else '')

                        col_names_original = table_df.columns.tolist()
                        col_names_lower = [str(col).lower() for col in col_names_original]
                        
                        qty_keywords = ['수량', 'qty', 'q\'ty', 'quantity', '구매수량', '주문수량', '개수', '수 량']
                        price_keywords = ['단가', '가격', '금액', 'price', 'unit price', '판매가', '할인가', '원가']
                        
                        qty_col_idx = -1
                        price_col_idx = -1
                        
                        for i, col_name in enumerate(col_names_lower):
                            if any(keyword in col_name for keyword in qty_keywords) and qty_col_idx == -1:
                                qty_col_idx = i
                            if any(keyword in col_name for keyword in price_keywords) and price_col_idx == -1:
                                price_col_idx = i
                        
                        header_row_offset = 0
                        if (qty_col_idx == -1 or price_col_idx == -1) and not table_df.empty:
                            first_data_row_values = [str(val).lower() for val in table_df.iloc[0].values]
                            potential_qty_col_from_row = -1
                            potential_price_col_from_row = -1

                            for i, cell_value in enumerate(first_data_row_values):
                                if any(keyword in cell_value for keyword in qty_keywords) and potential_qty_col_from_row == -1 :
                                    potential_qty_col_from_row = i
                                if any(keyword in cell_value for keyword in price_keywords) and potential_price_col_from_row == -1:
                                    potential_price_col_from_row = i
                            
                            if potential_qty_col_from_row != -1 and potential_price_col_from_row != -1:
                                qty_col_idx = potential_qty_col_from_row
                                price_col_idx = potential_price_col_from_row
                                table_df = table_df.iloc[1:].reset_index(drop=True) 
                                header_row_offset = 1
                                logger.info(f"Header keywords found in first data row for selector '{selector}'. Offset applied.")
                            elif qty_col_idx != -1 and price_col_idx == -1 and potential_price_col_from_row != -1: 
                                price_col_idx = potential_price_col_from_row
                                table_df = table_df.iloc[1:].reset_index(drop=True)
                                header_row_offset = 1
                            elif price_col_idx != -1 and qty_col_idx == -1 and potential_qty_col_from_row != -1: 
                                qty_col_idx = potential_qty_col_from_row
                                table_df = table_df.iloc[1:].reset_index(drop=True)
                                header_row_offset = 1

                        if qty_col_idx == -1 and len(col_names_original) >= 1: qty_col_idx = 0
                        if price_col_idx == -1 and len(col_names_original) >= 2: price_col_idx = 1
                        
                        if qty_col_idx == -1 or price_col_idx == -1 or qty_col_idx == price_col_idx:
                            logger.debug(f"Could not reliably identify quantity and price columns for selector '{selector}', table index {table_index}. Skipping.")
                            continue

                        try:
                            result_df = table_df.iloc[:, [qty_col_idx, price_col_idx]].copy()
                            result_df.columns = ['수량', '단가']
                        except IndexError:
                            logger.warning(f"IndexError when selecting columns for selector '{selector}'. qty_idx={qty_col_idx}, price_idx={price_col_idx}, table_shape={table_df.shape}")
                            continue
                            
                        result_df['수량_원본'] = result_df['수량'] 
                        result_df['단가_원본'] = result_df['단가'] 

                        result_df['수량'] = result_df['수량'].astype(str).apply(
                            lambda x: ''.join(filter(str.isdigit, str(x).split('~')[0].split('-')[0].replace(',', '')))
                        )
                        result_df['단가'] = result_df['단가'].astype(str).apply(
                            lambda x: ''.join(filter(str.isdigit, str(x).replace(',', '')))
                        )
                        
                        result_df = result_df[result_df['수량'].apply(lambda x: x.isdigit() and x != '')]
                        result_df = result_df[result_df['단가'].apply(lambda x: x.isdigit() and x != '')]
                        
                        if result_df.empty:
                            logger.debug(f"Table from selector '{selector}', index {table_index} is empty after cleaning. Skipping.")
                            continue

                        result_df['수량'] = result_df['수량'].astype(int)
                        result_df['단가'] = result_df['단가'].astype(int)
                        
                        result_df = result_df[result_df['수량'] > 0]
                        result_df = result_df[result_df['단가'] > 0] 

                        if result_df.empty:
                            logger.debug(f"Table from selector '{selector}', index {table_index} is empty after filtering zero qty/price. Skipping.")
                            continue

                        result_df = result_df.sort_values('수량').reset_index(drop=True)
                        
                        page_text_near_table = ""
                        try:
                            page_text_near_table = await table_element.evaluate("element => element.parentElement.innerText")
                            if not page_text_near_table: 
                                 page_text_near_table = await table_element.evaluate("element => element.parentElement.parentElement.innerText")
                        except PlaywrightError as e:
                            logger.debug(f"Could not get parent/grandparent innerText for VAT check: {e}")

                        has_vat_excluded_general = False
                        if page_text_near_table:
                            page_text_lower = page_text_near_table.lower()
                            if "부가세별도" in page_text_lower or "부가세 별도" in page_text_lower or "vat 별도" in page_text_lower or "vat 미포함" in page_text_lower:
                                has_vat_excluded_general = True
                        
                        if has_vat_excluded_general:
                            result_df.attrs['vat_excluded'] = True
                            logger.info(f"VAT excluded noted for general table selector '{selector}' based on surrounding text.")

                        logger.info(f"Successfully extracted price table (General selector '{selector}', table index {table_index}) for {product_url}: {len(result_df)} tiers")
                        if len(result_df) > 0 : return result_df 
                except pd.errors.EmptyDataError:
                    logger.debug(f"Pandas EmptyDataError for selector '{selector}'. No table found in HTML.")
                except ValueError as ve: 
                    if "No tables found" in str(ve):
                        logger.debug(f"pd.read_html found no tables for selector '{selector}'.")
                    else:
                        logger.warning(f"ValueError parsing table with selector '{selector}' for {product_url}: {ve}")
                except Exception as table_error:
                    logger.warning(f"Error parsing table with selector '{selector}' for {product_url}: {table_error}")
                    # No continue here, let the outer loop handle the next selector
            # No valid table found with this selector, try the next one in table_selectors
        
        logger.warning(f"No valid quantity-price table found on page: {product_url} after trying all selectors.")
        return None
        
    except PlaywrightError as pe:
        logger.error(f"Playwright error during price table extraction for {product_url}: {pe}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during price table extraction for {product_url}: {e}")
        return None

# --- 이미지 URL 처리 전용 함수 추가 ---
def normalize_kogift_image_url(img_url: str, base_url: str = "https://www.kogift.com") -> Tuple[str, bool]:
    """
    고려기프트 및 애드판촉 이미지 URL을 표준화하고 유효성을 검사합니다.
    '/ez/' 경로를 필요한 경우 추가합니다.

    Args:
        img_url: 원본 이미지 URL 또는 경로
        base_url: 기본 도메인 URL

    Returns:
        Tuple[str, bool]: 정규화된 이미지 URL과 유효성 여부
    """
    if not img_url:
        return "", False

    # data:image URI인 경우 (인라인 이미지)
    if img_url.startswith('data:image/'):
        logger.warning(f"Data URI 이미지 발견 (사용 불가)")
        return "", False

    # 대상 도메인 리스트
    target_domains = ['koreagift.com', 'adpanchok.co.kr']

    # 이미 완전한 URL인 경우
    if img_url.startswith(('http://', 'https://')):
        parsed_url = urlparse(img_url)
        domain = parsed_url.netloc
        path = parsed_url.path

        # 대상 도메인인지 확인
        is_target_domain = any(td in domain for td in target_domains)

        if is_target_domain:
            # 이미 /ez/가 있는 경우 그대로 사용
            if '/ez/' in path:
                return img_url, True
            # /upload/로 시작하는 경로에 /ez/ 추가
            elif path.startswith('/upload/'):
                new_path = '/ez' + path
                return f"{parsed_url.scheme}://{domain}{new_path}", True
            # 루트 경로 등 /ez/가 필요 없는 경우 (예: /main/img.jpg)
            elif not path.startswith('/upload/'):
                 # /ez/ 가 없고, /upload/ 도 아니면 그대로 사용
                 return img_url, True
            # 그 외 대상 도메인의 경로는 일단 유효하다고 간주
            else:
                 return img_url, True
        else:
            # 대상 도메인이 아니면, 유효한 URL 형식인지 확인 후 반환
            if domain and path: # 기본적인 유효성 검사
                return img_url, True
            else:
                return img_url, False # 유효하지 않은 형식

    # '//' 시작하는 프로토콜-상대 URL 처리
    if img_url.startswith('//'):
        # // 다음이 도메인이어야 함
        temp_url = f"https:{img_url}"
        parsed_temp = urlparse(temp_url)
        if parsed_temp.netloc:
            # 재귀 호출로 /ez/ 처리 위임
            return normalize_kogift_image_url(temp_url, base_url)
        else:
            return "", False # // 다음에 도메인이 없는 잘못된 형식

    # './' 시작하는 상대 경로 처리
    if img_url.startswith('./'):
        img_url = img_url[2:]  # './' 제거

    # 절대 경로 ('/'로 시작)
    if img_url.startswith('/'):
        # 대상 도메인이고 /upload/로 시작하면 /ez/ 추가
        is_target_domain = any(td in base_url for td in target_domains)
        if is_target_domain and img_url.startswith('/upload/'):
            img_url = '/ez' + img_url
        # 그 외 절대 경로는 그대로 사용
    # 상대 경로 (파일명 또는 하위 경로)
    else:
        # 대상 도메인이고 'upload/'로 시작하면 /ez/ 추가
        is_target_domain = any(td in base_url for td in target_domains)
        if is_target_domain and img_url.startswith('upload/'):
            img_url = '/ez/' + img_url
        # 그 외 상대 경로는 앞에 '/' 추가
        else:
            img_url = '/' + img_url

    # 최종 URL 생성 (urljoin 사용)
    final_url = urljoin(base_url, img_url)

    # 중복 경로 확인 및 수정 ('/ez/ez/' -> '/ez/')
    if '/ez/ez/' in final_url:
        final_url = final_url.replace('/ez/ez/', '/ez/')

    # 최종 URL 유효성 검사 (간단히)
    parsed_final = urlparse(final_url)
    if parsed_final.scheme and parsed_final.netloc:
        return final_url, True
    else:
        logger.warning(f"최종 URL 생성 실패: base='{base_url}', img='{img_url}', final='{final_url}'")
        return final_url, False # 생성 실패

# --- Function to select price when base quantity is not available ---
def select_highest_price_if_no_base_quantity(quantity_price_tiers, base_quantity_val):
    """    
    Selects a price based on quantity_price_tiers.
    If base_quantity_val is provided and is a valid tier, its price is returned.
    Otherwise (base_quantity_val is None or not a valid tier), the price for the
    smallest available quantity tier (highest unit price) is returned.

    Args:
        quantity_price_tiers (dict): Expected format {int_qty: {'price': X, ...}}
        base_quantity_val (int, optional): The base quantity to look for.

    Returns:
        tuple: (selected_quantity, selected_price_or_None)
    """
    # 1. If base_quantity_val is provided
    if base_quantity_val is not None:
        price_for_base_qty = None
        if quantity_price_tiers and base_quantity_val in quantity_price_tiers:
            price_info = quantity_price_tiers.get(base_quantity_val)
            if price_info:
                price_for_base_qty = price_info.get('price')
        return base_quantity_val, price_for_base_qty
    
    # 2. If base_quantity_val is None: Find the price for the smallest quantity tier
    if not quantity_price_tiers:
        return None, None # No tiers, no price
    
    try:
        # Assuming keys in quantity_price_tiers are integers as constructed
        min_quantity_tier = min(quantity_price_tiers.keys()) 
    except (TypeError, ValueError):
        logger.warning("select_highest_price_if_no_base_quantity: Could not determine min quantity tier from keys.")
        return None, None

    price_info_for_min_tier = quantity_price_tiers.get(min_quantity_tier)
    highest_price = None
    if price_info_for_min_tier:
        highest_price = price_info_for_min_tier.get('price')
    
    return min_quantity_tier, highest_price

async def verify_kogift_images(product_list: List[Dict], sample_percent: int = 10) -> List[Dict]:
    """고려기프트 상품 목록의 이미지 URL을 검증하고 표준화한 후, 이미지를 다운로드합니다."""
    if not product_list:
        return []
    
    # 설정에서 검증 여부 확인
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    
    verify_enabled = config.getboolean('Matching', 'verify_image_urls', fallback=True)
    download_enabled = config.getboolean('Matching', 'download_images', fallback=True)
    
    # 이미지 저장 경로 설정 (Main 디렉토리로 변경)
    base_image_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main') # Changed from image_target_dir
    images_dir = os.path.join(base_image_dir, 'kogift')  # kogift 하위 디렉토리 사용
    os.makedirs(images_dir, exist_ok=True)
    
    logger.info(f"고려기프트 상품 {len(product_list)}개의 이미지 처리 시작 (저장 경로: {images_dir})")
    
    # 검증 및 정규화 처리
    if verify_enabled:
        verified_count = 0
        valid_count = 0
        
        for product in product_list:
            img_url = product.get('image')
            if not img_url:
                continue
                
            verified_count += 1
            
            # 이미지 URL 검증 및 정규화
            try:
                normalized_url, is_valid = normalize_kogift_image_url(img_url)
                if is_valid:
                    product['image'] = normalized_url
                    product['image_url'] = normalized_url
                    valid_count += 1
                else:
                    logger.warning(f"무효한 이미지 URL: {img_url}")
                    product['image'] = None
                    product['image_url'] = None
            except Exception as e:
                logger.error(f"이미지 URL 정규화 오류: {img_url}, 오류: {e}")
                product['image'] = None
                product['image_url'] = None
        
        logger.info(f"이미지 URL 검증 완료: {verified_count}개 검증, {valid_count}개 유효")
    
    # 이미지 다운로드 처리
    if download_enabled:
        # 각 상품별로 개별 이미지 다운로드 (상품명별로 처리)
        download_success_count = 0
        skipped_count = 0
        total_images = 0
        
        for product in product_list:
            img_url = product.get('image')
            if not img_url:
                continue
                
            total_images += 1
            
            # 이미 즉시 다운로드로 처리된 이미지인지 확인
            if product.get('local_image_path') and os.path.exists(product.get('local_image_path')):
                logger.debug(f"이미 다운로드된 이미지 건너뜀: {product.get('local_image_path')}")
                skipped_count += 1
                continue
            
            # 엑셀의 원본 상품명 사용 (해시값 통일을 위해)
            product_name = product.get('original_excel_product_name')
            
            if not product_name:
                # Fallback: 웹사이트에서 스크래핑한 상품명 사용
                product_name = product.get('name') or product.get('product_name') or product.get('title')
                if product_name:
                    logger.warning(f"엑셀 원본 상품명이 없어 웹사이트 상품명 사용: {product_name}")
                else:
                    logger.warning(f"상품명을 찾을 수 없음, URL: {img_url}")
                    continue
            
            # 개별 이미지 다운로드 (상품명 전달)
            try:
                local_path = await download_image(img_url, images_dir, product_name, config)
                if local_path:
                    product['local_image_path'] = local_path
                    download_success_count += 1
                    logger.debug(f"이미지 다운로드 성공: {product_name} -> {local_path}")
                else:
                    logger.warning(f"이미지 다운로드 실패: {product_name}, URL: {img_url}")
            except Exception as e:
                logger.error(f"이미지 다운로드 오류: {product_name}, URL: {img_url}, 오류: {e}")
        
        logger.info(f"이미지 다운로드 완료: {download_success_count}/{total_images} 성공, {skipped_count}개 건너뜀 (이미 다운로드됨)")
    
    return product_list

# --- 상세 페이지에서 수량 설정하고 가격 가져오는 함수 추가 ---
async def get_price_for_specific_quantity(page, product_url, target_quantity, timeout=30000):
    """
    상품 상세 페이지에서 특정 수량을 입력하고 업데이트된 가격을 가져옵니다.
    기본수량 미만 경고 메시지도 감지합니다.
    
    Args:
        page: Playwright Page 객체
        product_url: 상품 상세 페이지 URL
        target_quantity: 설정할 수량 (int)
        timeout: 타임아웃(ms)
        
    Returns:
        dict: 수량, 단가(부가세 포함/미포함), 성공 여부, 최소 수량 안내
    """
    result = {
        "quantity": target_quantity,
        "price": 0,
        "price_with_vat": 0,
        "success": False,
        "min_quantity_error": False,
        "min_quantity": None,
        "price_inquiry_needed": False # New flag
    }
    
    try:
        # Navigate to the product page
        await page.goto(product_url, wait_until='domcontentloaded', timeout=timeout)
        
        # Wait for a short period for any initial scripts to run
        await page.wait_for_timeout(1000)

        # Check for "price inquiry needed" text
        inquiry_text_indicator = "본 제품은 가격 문의가 필요한 상품입니다."
        try:
            # Check if the specific inquiry text is visible on the page
            # This locator targets any element containing the exact text.
            inquiry_locator = page.locator(f'text="{inquiry_text_indicator}"')
            if await inquiry_locator.count() > 0 and await inquiry_locator.first.is_visible(timeout=2000):
                logger.info(f"가격 문의 필요 상품 감지 (텍스트 '{inquiry_text_indicator}' 찾음): {product_url}")
                result["price_inquiry_needed"] = True
                result["success"] = False
                return result
        except PlaywrightError as e:
            logger.debug(f"Error checking for price inquiry text: {e}")
            # Continue if there's an error, it might not be an inquiry item.

        # Try different selectors for the quantity input field
        input_selectors = [
            'input#buynum',            # Standard buynum
            'input[name="buynum"]',    # By name
            'input.buynum',            # By class
            'input[id^="buy"]',        # Starts with buy
            'input[id*="num"]',        # Contains num
            'input.btn_count'          # Common class for counter inputs
        ]
        
        # Try to find quantity input field with different selectors
        buynum_input = None
        for selector in input_selectors:
            if await page.locator(selector).count() > 0:
                buynum_input = page.locator(selector).first
                logger.info(f"Found quantity input with selector: {selector}")
                break
        
        if not buynum_input or await buynum_input.count() == 0:
            logger.warning(f"수량 입력 필드를 찾을 수 없습니다: {product_url}")
            return result
            
        # Get current quantity value to check if we need to change it
        current_value_text = await buynum_input.input_value()
        try:
            current_value = int(current_value_text)
        except ValueError:
            current_value = 0
            
        # Only change if quantity is different
        if current_value != target_quantity:
            # Focus on the input and clear existing value
            await buynum_input.click()
            await buynum_input.press("Control+a")
            await buynum_input.press("Delete")
            
            # Input the new quantity
            await buynum_input.fill(str(target_quantity))
            
            # Different ways to trigger price update
            # 1. Press Enter
            await buynum_input.press("Enter")
            
            # 2. Click outside the input
            try:
                await page.click('body', position={'x': 10, 'y': 10})
            except:
                pass
                
            # 3. Look for and click update buttons
            update_button_selectors = [
                'button.update_price',
                'button.btn_update',
                'a.update_price',
                'a.btn_update',
                'button:has-text("계산")',
                'button:has-text("적용")',
                'button:has-text("변경")'
            ]
            
            for btn_selector in update_button_selectors:
                try:
                    if await page.locator(btn_selector).count() > 0:
                        await page.locator(btn_selector).first.click()
                        logger.info(f"Clicked update button: {btn_selector}")
                        break
                except:
                    continue
            
            # Wait for price to update
            await page.wait_for_timeout(2000)  # Increased wait time
        
        # 기본수량 미만 경고 메시지 확인
        min_quantity_error_selectors = [
            'div.alert:has-text("기본수량 미만")',
            'div.notice:has-text("기본수량 미만")',
            'div.quantity_error:has-text("기본수량")',
            'div.alert:has-text("최소 주문")',
            'span.alert:has-text("최소 주문")',
            'div.notice:has-text("최소 주문")',
            'p.alert:has-text("기본수량")'
        ]
        
        # 경고 메시지 확인
        for error_selector in min_quantity_error_selectors:
            if await page.locator(error_selector).count() > 0:
                error_text = await page.locator(error_selector).text_content()
                logger.info(f"최소 수량 경고 메시지 발견: {error_text}")
                result["min_quantity_error"] = True
                
                # 최소 수량 값 추출 시도
                try:
                    # 경고 메시지에서 숫자 추출 (예: "기본수량은 100개 이상입니다")
                    min_qty_match = re.search(r'(\d+)(?:개|EA|ea|pcs)', error_text)
                    if min_qty_match:
                        result["min_quantity"] = int(min_qty_match.group(1))
                        logger.info(f"최소 주문 수량: {result['min_quantity']}개")
                except Exception as ex:
                    logger.warning(f"최소 수량 추출 실패: {ex}")
                break
        
        # 최소 수량 확인을 위한 인풋 필드의 min 속성 확인
        if not result["min_quantity"] and buynum_input:
            try:
                min_attr = await buynum_input.get_attribute('min')
                if min_attr and min_attr.isdigit():
                    result["min_quantity"] = int(min_attr)
                    logger.info(f"입력 필드의 최소 수량: {result['min_quantity']}개")
            except Exception:
                pass
        
        # Price selectors to try
        price_selectors = [
            'span#main_price',         # Standard kogift price
            'span.main_price',         # By class
            'strong.price',            # Common price element
            'div.price_wrap .price',   # Price in price wrapper
            'div.price_value',         # Price value
            'span.font_price',         # Price with special font
            '*[id*="price"]:not(input):not(select):not(button)', # Contains price but not form elements
            'div.total_price'          # Total price
        ]
        
        # Try different price selectors
        price_element = None
        for selector in price_selectors:
            if await page.locator(selector).count() > 0:
                price_element = page.locator(selector).first
                logger.info(f"Found price element with selector: {selector}")
                break
                
        if not price_element or await price_element.count() == 0:
            logger.warning(f"가격 요소를 찾을 수 없습니다: {product_url}")
            
            # Last resort - try to get any visible price text
            try:
                page_content = await page.content()
                # Look for price patterns in text
                price_matches = re.findall(r'[\d,]+원', page_content)
                if price_matches:
                    price_text = price_matches[0]
                    logger.info(f"Found price using regex pattern: {price_text}")
                else:
                    return result
            except:
                return result
        else:
            # Get price text from element
            price_text = await price_element.text_content()
        
        # Clean and extract price value
        # Remove non-digit characters except commas, then remove commas
        price_clean = ''.join(filter(lambda c: c.isdigit() or c == ',', price_text)).replace(',', '')
        if not price_clean:
            logger.warning(f"유효한 가격을 찾을 수 없습니다: {product_url} (텍스트: {price_text})")
            return result
            
        # Convert to integer
        try:
            price = int(price_clean)
        except ValueError:
            logger.warning(f"가격을 정수로 변환할 수 없습니다: {price_clean}")
            return result
        
        # Calculate price with VAT (10%)
        price_with_vat = round(price * 1.1)

        # Also check if price is per-unit
        per_unit_selectors = [
            'span:has-text("단가")',
            'span:has-text("개당")',
            'span:has-text("EA당")',
            'span:has-text("단위가격")'
        ]
        
        is_per_unit = False
        for selector in per_unit_selectors:
            if await page.locator(selector).count() > 0:
                is_per_unit = True
                break
                
        result["price"] = price
        result["price_with_vat"] = price_with_vat
        result["is_per_unit"] = is_per_unit
        result["success"] = True

        return result
        
    except Exception as e:
        logger.error(f"수량 설정 및 가격 조회 중 오류 발생: {e}")
        return result

# --- Main scraping function에 상세 페이지 크롤링 로직 추가 --- 
async def scrape_data(browser: Browser, original_keyword1: str, original_keyword2: Optional[str] = None, config: configparser.ConfigParser = None, fetch_price_tables: bool = True, custom_quantities: List[int] = None):
    """Scrape data from Kogift website."""
    
    # Initialize variables
    results = []
    raw_kogift_urls = get_kogift_urls(config)
    unique_kogift_urls = list(dict.fromkeys(raw_kogift_urls))
    if len(raw_kogift_urls) != len(unique_kogift_urls):
        logger.info(f"Removed {len(raw_kogift_urls) - len(unique_kogift_urls)} duplicate Kogift base URLs. Using: {unique_kogift_urls}")
    else:
        logger.info(f"Using Kogift base URLs: {unique_kogift_urls}")

    # 엑셀의 원본 상품명 저장 (이미지 다운로드용)
    original_excel_product_name = original_keyword1
    logger.info(f"엑셀 원본 상품명 저장: '{original_excel_product_name}'")

    max_items_per_variation = get_max_items_per_variation(config)
    
    # Generate keyword variations
    raw_keyword_variations = generate_keyword_variations(original_keyword1, original_keyword2)
    unique_keyword_variations = list(dict.fromkeys(raw_keyword_variations))
    if len(raw_keyword_variations) != len(unique_keyword_variations):
        logger.info(f"Removed {len(raw_keyword_variations) - len(unique_keyword_variations)} duplicate keyword variations.")
    
    logger.info(f"Generated {len(unique_keyword_variations)} unique keyword variations for search: {unique_keyword_variations}")
    logger.info(f"Will scrape up to {max_items_per_variation} items per keyword variation")
    
    # Check if we need to recreate the browser
    need_new_browser = not browser or not browser.is_connected()
    
    # Get quantities to check - use input quantities or fallback to defaults
    if custom_quantities is None or len(custom_quantities) == 0:
        logger.info("No custom quantities provided, attempting to read from Excel...")
        # Try to get quantities from input Excel
        excel_quantities = get_quantities_from_excel(config) if config else None
        
        if excel_quantities:
            custom_quantities = excel_quantities
            logger.info(f"Successfully loaded quantities from Excel: {custom_quantities}")
        else:
            # If still no quantities, use defaults
            custom_quantities = [300, 800, 1100, 2000]
            logger.info(f"Using default quantities (no Excel quantities found): {custom_quantities}")
    else:
        logger.info(f"Using provided custom quantities: {custom_quantities}")
    
    logger.info(f"Will check prices for quantities: {custom_quantities}")
    
    # Continue with the rest of the original function implementation
    if config is None:
        logger.error("Configuration object is required")
        return pd.DataFrame()

    # Get image directory from config
    try:
        images_dir = config.get('Paths', 'image_main_dir')
        if not images_dir or not os.path.exists(images_dir):
            logger.error("Invalid image_main_dir in config")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error getting image directory from config: {e}")
        return pd.DataFrame()
    
    all_results = []
    seen_product_urls = set()  # Track product URLs to avoid duplicates
    
    # Try each URL in sequence
    for base_url in unique_kogift_urls:
        context = None
        page = None
        try:
            # --- Add check for browser connection and reconnect if needed --- 
            if not browser or not browser.is_connected():
                logger.warning(f"🔶 Browser is not connected before processing URL: {base_url}. Attempting to reconnect.")
                
                # If the caller provided a disconnected browser, we'll try to create a new one
                if need_new_browser:
                    from playwright.async_api import async_playwright
                    p = await async_playwright().start()
                    
                    # Get browser launch arguments from config
                    browser_args = []
                    try:
                        browser_args_str = config.get('Playwright', 'playwright_browser_args', fallback='[]')
                        import json
                        browser_args = json.loads(browser_args_str)
                    except Exception as arg_err:
                        logger.warning(f"Could not parse browser arguments: {arg_err}. Using defaults.")
                        browser_args = ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
                    
                    # Launch a new browser
                    try:
                        headless = config.getboolean('Playwright', 'playwright_headless', fallback=True)
                        browser = await p.chromium.launch(
                            headless=headless,
                            args=browser_args,
                            timeout=60000  # 1 minute timeout for browser launch
                        )
                        logger.info("🟢 Successfully launched a new browser instance")
                    except Exception as launch_err:
                        logger.error(f"Failed to launch new browser: {launch_err}")
                        return pd.DataFrame()
                else:
                    # Skip this URL if we couldn't reconnect
                    logger.error(f"🔴 Browser is not connected and cannot be recreated for {base_url}. Skipping this URL.")
                    continue
            # --- End check ---
            
            # Create a new context for each URL
            logger.debug(f"Attempting to create new browser context for {base_url}")
            
            # Apply delay before creating a new context if configured
            context_delay = config.getint('Playwright', 'playwright_new_context_delay_ms', fallback=0)
            if context_delay > 0:
                await asyncio.sleep(context_delay / 1000)  # Convert ms to seconds
                
            context = await browser.new_context(
                user_agent=config.get('Network', 'user_agent', fallback='Mozilla/5.0 ...'),
                viewport={'width': 1920, 'height': 1080},
            )
            logger.debug(f"Successfully created context for {base_url}")
            
            # Create a new page
            page = await context.new_page()
            page.set_default_timeout(config.getint('Playwright', 'playwright_default_timeout_ms', fallback=120000))
            page.set_default_navigation_timeout(config.getint('Playwright', 'playwright_navigation_timeout_ms', fallback=60000))

            # Setup resource blocking if enabled
            if config.getboolean('Playwright', 'playwright_block_resources', fallback=True):
                await setup_page_optimizations(page)
            
            # Search with each keyword variation for this URL
            for keyword_index, keyword in enumerate(unique_keyword_variations):
                try:
                    logger.info(f"Attempting to search with variation {keyword_index+1}/{len(unique_keyword_variations)}: '{keyword}' on {base_url}")
                    
                    # Construct search URL
                    search_url = f"{base_url.strip()}/goods/goods_search.php"

                    try:
                        # Navigate to the search page with increased timeout and retry logic
                        for nav_attempt in range(3):  # Add retry logic
                            try:
                                await page.goto(search_url, wait_until="domcontentloaded", 
                                               timeout=config.getint('ScraperSettings', 'navigation_timeout', fallback=90000))
                                # Short pause after navigation to allow page to stabilize
                                await page.wait_for_timeout(3000)
                                break  # Break out of retry loop if successful
                            except PlaywrightError as nav_err:
                                if nav_attempt < 2:  # Try again if we haven't reached max retries
                                    logger.warning(f"Navigation error (attempt {nav_attempt+1}/3): {nav_err}")
                                    await asyncio.sleep(2)  # Wait before retry
                                else:
                                    raise  # Re-raise on final attempt

                        # --- Perform Search --- 
                        search_input_locator = page.locator('input#main_keyword[name="keyword"]')
                        search_button_locator = page.locator('img#search_submit')
                        
                        await search_input_locator.wait_for(state="visible", 
                                                           timeout=config.getint('ScraperSettings', 'action_timeout', fallback=15000))
                        
                        # Clear any default value in the search input
                        await search_input_locator.click()
                        await search_input_locator.press("Control+a")
                        await search_input_locator.press("Delete")
                        
                        # Fill the search input with the current keyword variation
                        await search_input_locator.fill(keyword)
                        await search_button_locator.wait_for(state="visible", 
                                                           timeout=config.getint('ScraperSettings', 'action_timeout', fallback=15000))
                        
                        logger.debug(f"🔍 Clicking search for variation '{keyword}'...")
                        await search_button_locator.click()
                        logger.info(f"🔍 Search submitted for: '{keyword}' on {base_url}")

                        # --- Wait for results OR "no results" message --- 
                        results_container_selector = 'div.product_lists'
                        no_results_selector = 'div.not_result span.icon_dot2:has-text("검색 결과가 없습니다")'
                        combined_selector = f"{results_container_selector}, {no_results_selector}"
                        
                        logger.debug(f"⏳ Waiting for search results or 'no results' message...")
                        try:
                            found_element = await page.wait_for_selector(
                                combined_selector, 
                                state='visible', 
                                timeout=config.getint('ScraperSettings', 'action_timeout', fallback=15000)
                            )
                            
                            # Check if the 'no results' text is visible
                            no_results_element = page.locator(no_results_selector)
                            if await no_results_element.is_visible():
                                no_results_text = await no_results_element.text_content(timeout=1000) or "[No text found]"
                                logger.info(f"⚠️ 'No results' message found for keyword '{keyword}' on {base_url}. Text: {no_results_text.strip()}")
                                continue # Skip to the next keyword variation
                            else:
                                logger.debug("✅ Results container found. Proceeding to scrape.")
                                
                        except PlaywrightError as wait_error:
                            logger.warning(f"⚠️ Timeout or error waiting for results/no_results for keyword '{keyword}' on {base_url}: {wait_error}")
                            continue # Skip to the next keyword variation

                        # --- Scrape Results Pages --- 
                        page_number = 1
                        processed_items = 0
                        product_item_selector = 'div.product'
                        data = []

                        # Limit pages to scrape from config
                        max_pages = config.getint('ScraperSettings', 'kogift_max_pages', fallback=5)

                        while processed_items < max_items_per_variation and page_number <= max_pages:
                            try:
                                logger.info(f"📄 Scraping page {page_number} (Keyword: '{keyword}', URL: {base_url})... Items processed: {processed_items}")
                                
                                # Wait for at least one product item to be potentially visible
                                await page.locator(product_item_selector).first.wait_for(state="attached", 
                                                 timeout=config.getint('ScraperSettings', 'action_timeout', fallback=15000))
                                
                                # Short pause to ensure page is fully loaded
                                await page.wait_for_timeout(1000)
                                
                                rows = page.locator(product_item_selector)
                                count = await rows.count()
                                logger.debug(f"📊 Found {count} product elements on page {page_number}.")

                                if count == 0 and page_number > 1:
                                    logger.info(f"⚠️ No product elements found on page {page_number}. Stopping pagination.")
                                    break
                                elif count == 0 and page_number == 1:
                                    logger.info(f"⚠️ No product elements found on first page (page {page_number}). Stopping scrape for this keyword.")
                                    break

                                items_on_page = []
                                for i in range(count):
                                    if processed_items >= max_items_per_variation:
                                        break
                                    try:
                                        row = rows.nth(i)
                                        
                                        # Check for "품절" (Sold Out) before processing
                                        item_data = {} # Initialize item_data here to access 'name' in logs
                                        try:
                                            item_text = await row.text_content(timeout=2000)
                                            # Try to get name early for logging 품절
                                            try:
                                                name_locator_temp = row.locator('div.name > a')
                                                item_data['name'] = await name_locator_temp.text_content(timeout=1000) or "Unknown Product"
                                            except:
                                                item_data['name'] = "Unknown Product"

                                            if item_text and "품절" in item_text:
                                                logger.info(f"Skipping item '{item_data['name']}' from list page as it is '품절'.")
                                                continue # Skip this item
                                        except Exception as sold_out_check_err:
                                            logger.warning(f"Could not check for '품절' on item {i} ('{item_data['name']}'): {sold_out_check_err}")
                                            
                                        # item_data = {} # Already initialized
                                        
                                        # Extract data using locators with short timeouts
                                        try:
                                            img_locator = row.locator('div.pic > a > img')
                                            img_src = await img_locator.get_attribute('src', timeout=5000)
                                        except Exception as e:
                                            logger.debug(f"Error getting image source: {e}")
                                            img_src = None
                                        
                                        try:
                                            link_locator = row.locator('div.pic > a')
                                            a_href = await link_locator.get_attribute('href', timeout=5000)
                                        except Exception as e:
                                            logger.debug(f"Error getting link: {e}")
                                            a_href = None
                                        
                                        # Name is already in item_data if successfully extracted for 품절 log
                                        if not item_data.get('name') or item_data.get('name') == "Unknown Product":
                                            try:
                                                name_locator = row.locator('div.name > a')
                                                name_text = await name_locator.text_content(timeout=5000)
                                                item_data['name'] = name_text.strip() if name_text else "Unknown Product"
                                            except Exception as e:
                                                logger.debug(f"Error getting name: {e}")
                                                item_data['name'] = "Unknown Product"
                                        
                                        try:
                                            price_locator = row.locator('div.price')
                                            price_text = await price_locator.text_content(timeout=5000)
                                        except Exception as e:
                                            logger.debug(f"Error getting price: {e}")
                                            price_text = None

                                        # Skip item if we couldn't get essential data
                                        if not a_href or not item_data.get('name') or item_data.get('name') == "Unknown Product":
                                            logger.debug(f"Skipping item due to missing essential data (link or name)")
                                            continue

                                        # Process extracted data
                                        base_domain_url = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
                                        
                                        # 이미지 URL 정규화
                                        final_img_url, valid_img_url = normalize_kogift_image_url(img_src, base_domain_url) if img_src else ("", False)
                                        if not valid_img_url:
                                            logger.warning(f"⚠️ Invalid or unnormalizable image URL skipped: {img_src}")
                                        
                                        # 상품 URL 처리
                                        if a_href:
                                            if a_href.startswith('http'):
                                                final_href_url = a_href
                                            elif a_href.startswith('./'):
                                                processed_href = '/' + a_href[2:]
                                                final_href_url = urljoin(base_domain_url, processed_href)
                                            elif a_href.startswith('/'):
                                                final_href_url = urljoin(base_domain_url, a_href)
                                            else:
                                                final_href_url = urljoin(base_domain_url, '/' + a_href)
                                        else:
                                            final_href_url = ""

                                        # Check for duplicates
                                        if final_href_url and final_href_url in seen_product_urls:
                                            logger.debug(f"Skipping duplicate product URL: {final_href_url}")
                                            continue

                                        if final_href_url:
                                            seen_product_urls.add(final_href_url)

                                        # 도메인에서 공급사 정보 추출
                                        supplier = urlparse(base_url).netloc.split('.')[0]
                                        if supplier == 'koreagift':
                                            supplier = '고려기프트'
                                        elif supplier == 'adpanchok':
                                            supplier = '애드판촉'
                                        
                                        # ===== 네이버/해오름처럼 즉시 이미지 다운로드 =====
                                        # 이미지 저장 디렉토리 설정
                                        try:
                                            base_image_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
                                            kogift_image_dir = os.path.join(base_image_dir, 'kogift')
                                            os.makedirs(kogift_image_dir, exist_ok=True)
                                        except Exception as img_dir_err:
                                            logger.warning(f"Error setting up image directory: {img_dir_err}")
                                            kogift_image_dir = "downloaded_images"  # Fallback
                                            os.makedirs(kogift_image_dir, exist_ok=True)
                                        
                                        # 즉시 이미지 다운로드 (네이버/해오름과 동일한 방식)
                                        local_image_path = None
                                        if valid_img_url and final_img_url:
                                            try:
                                                logger.info(f"Immediately downloading image for product: {item_data.get('name', 'Unknown')}")
                                                local_image_path = await download_image(
                                                    final_img_url, 
                                                    kogift_image_dir, 
                                                    original_excel_product_name,  # 엑셀 원본 상품명 사용 (해시 통일을 위해)
                                                    config
                                                )
                                                if local_image_path:
                                                    logger.info(f"Successfully downloaded Kogift image: {local_image_path}")
                                                    item_data['local_image_path'] = local_image_path
                                                else:
                                                    logger.warning(f"Failed to download Kogift image: {final_img_url}")
                                            except Exception as img_download_err:
                                                logger.error(f"Error downloading Kogift image {final_img_url}: {img_download_err}")
                                        
                                        # 유효한 이미지 URL만 저장
                                        if valid_img_url:
                                            item_data['image_path'] = final_img_url
                                            item_data['image_url'] = final_img_url
                                            item_data['src'] = final_img_url
                                            item_data['image'] = final_img_url  # verify_kogift_images와 호환성을 위해
                                        else:
                                            item_data['image_path'] = None
                                            item_data['image_url'] = None
                                            item_data['src'] = None
                                            item_data['image'] = None
                                        
                                        item_data['href'] = final_href_url
                                        item_data['link'] = final_href_url
                                        # item_data['name'] is already set
                                        item_data['supplier'] = supplier
                                        item_data['search_keyword'] = keyword
                                        # 엑셀의 원본 상품명 저장 (이미지 다운로드용)
                                        item_data['original_excel_product_name'] = original_excel_product_name
                                        
                                        # 가격 정보 처리 (목록 페이지에 표시된 기본 가격 - 특정 수량에 대한 가격이 아님)
                                        price_cleaned = re.sub(r'[^\d.]', '', price_text) if price_text else ""
                                        try:
                                            price_value = float(price_cleaned) if price_cleaned else 0.0
                                        except ValueError:
                                            price_value = 0.0
                                        
                                        item_data['list_price'] = price_value
                                        item_data['list_price_with_vat'] = round(price_value * 1.1)
                                        
                                        # 상품 상세 페이지에서 수량별 가격 정보 가져오기
                                        quantity_prices = {}
                                        price_detail_context = None
                                        price_detail_page = None
                                        
                                        try:
                                            price_detail_context = await browser.new_context(
                                                user_agent=config.get('Network', 'user_agent', fallback='Mozilla/5.0 ...'),
                                                viewport={'width': 1920, 'height': 1080},
                                            )
                                            price_detail_page = await price_detail_context.new_page()
                                            
                                            logger.info(f"Fetching prices for {len(custom_quantities)} quantities for product: {item_data['name']}")
                                            
                                            price_table = None
                                            if fetch_price_tables:
                                                price_table = await extract_price_table(price_detail_page, final_href_url, timeout=20000)
                                            
                                            if price_table is not None and not price_table.empty:
                                                actual_tiers_dict = {}
                                                for _, tier_row in price_table.iterrows():
                                                    try:
                                                        tier_qty = int(tier_row['수량'])
                                                        tier_price = int(tier_row['단가'])
                                                        actual_tiers_dict[tier_qty] = {
                                                            'price': tier_price,
                                                            'price_with_vat': round(tier_price * 1.1) 
                                                        }
                                                    except ValueError:
                                                        product_name_for_log = item_data.get('name', 'Unknown Product')
                                                        logger.warning(f"Skipping invalid tier row in price_table for '{product_name_for_log}': {str(tier_row)}")
                                                item_data['product_actual_price_tiers'] = actual_tiers_dict

                                                logger.info(f"Using price table for {item_data['name']}, table has {len(price_table)} rows")
                                                min_table_quantity = price_table['수량'].min()
                                                logger.info(f"테이블 최소 수량: {min_table_quantity}개")
                                                
                                                for qty in custom_quantities:
                                                    if qty < min_table_quantity:
                                                        logger.info(f"주문 수량({qty})이 최소 수량({min_table_quantity})보다 작습니다. 최소 수량의 가격을 적용합니다.")
                                                        min_qty_row = price_table[price_table['수량'] == min_table_quantity]
                                                        if not min_qty_row.empty:
                                                            min_qty_price = min_qty_row['단가'].values[0]
                                                            quantity_prices[qty] = {
                                                                'price': min_qty_price,
                                                                'price_with_vat': round(min_qty_price * 1.1),
                                                                'exact_match': False,
                                                                'actual_quantity': min_table_quantity,
                                                                'note': f"최소 주문 수량({min_table_quantity}) 가격 적용"
                                                            }
                                                            continue
                                                    
                                                    exact_match = price_table[price_table['수량'] == qty]
                                                    if not exact_match.empty:
                                                        exact_price = exact_match['단가'].values[0]
                                                        quantity_prices[qty] = {
                                                            'price': exact_price,
                                                            'price_with_vat': round(exact_price * 1.1),
                                                            'exact_match': True
                                                        }
                                                        logger.info(f"수량 {qty}개 정확히 일치: {exact_price}원")
                                                        continue
                                                    
                                                    lower_rows = price_table[price_table['수량'] <= qty]
                                                    if not lower_rows.empty:
                                                        max_lower_qty = lower_rows['수량'].max()
                                                        max_lower_row = price_table[price_table['수량'] == max_lower_qty]
                                                        max_lower_price = max_lower_row['단가'].values[0]
                                                        
                                                        quantity_prices[qty] = {
                                                            'price': max_lower_price,
                                                            'price_with_vat': round(max_lower_price * 1.1),
                                                            'exact_match': False,
                                                            'actual_quantity': max_lower_qty,
                                                            'note': f"구간 가격({max_lower_qty}개 이상) 적용"
                                                        }
                                                        logger.info(f"수량 {qty}개는 {max_lower_qty}개 구간 가격 적용: {max_lower_price}원")
                                                        continue
                                                    
                                                    max_table_quantity = price_table['수량'].max()
                                                    max_qty_row = price_table[price_table['수량'] == max_table_quantity]
                                                    if not max_qty_row.empty:
                                                        max_qty_price = max_qty_row['단가'].values[0]
                                                        quantity_prices[qty] = {
                                                            'price': max_qty_price,
                                                            'price_with_vat': round(max_qty_price * 1.1),
                                                            'exact_match': False,
                                                            'actual_quantity': max_table_quantity,
                                                            'note': f"최대 구간({max_table_quantity}개) 가격 적용"
                                                        }
                                                        logger.info(f"수량 {qty}개는 최대 구간 {max_table_quantity}개 가격 적용: {max_qty_price}원")
                                            else:
                                                min_quantity_info = None
                                                for qty_idx, qty in enumerate(custom_quantities):
                                                    price_result = await get_price_for_specific_quantity(price_detail_page, final_href_url, qty, timeout=20000)
                                                    
                                                    if price_result.get("price_inquiry_needed", False):
                                                        logger.info(f"상품 '{item_data.get('name', 'Unknown Product')}'은(는) 가격 문의 필요 항목입니다. 다음 상품으로 건너뜁니다.")
                                                        raise SkipItemException() # Signal to skip this item

                                                    if price_result['min_quantity_error'] and price_result['min_quantity']:
                                                        logger.info(f"수량 {qty}개는 최소 주문 수량({price_result['min_quantity']})보다 작습니다.")
                                                        min_qty_val = price_result['min_quantity']
                                                        min_price_result = await get_price_for_specific_quantity(price_detail_page, final_href_url, min_qty_val, timeout=20000)
                                                        
                                                        if min_price_result['success']:
                                                            min_quantity_info = {
                                                                'min_quantity': min_qty_val,
                                                                'price': min_price_result['price'],
                                                                'price_with_vat': min_price_result['price_with_vat']
                                                            }
                                                            quantity_prices[qty] = {
                                                                'price': min_price_result['price'],
                                                                'price_with_vat': min_price_result['price_with_vat'],
                                                                'exact_match': False,
                                                                'actual_quantity': min_qty_val,
                                                                'note': f"최소 주문 수량({min_qty_val}) 가격 적용"
                                                            }
                                                            logger.info(f"수량 {qty}개에 최소 수량({min_qty_val})의 가격 {min_price_result['price']}원 적용")
                                                        else:
                                                            logger.warning(f"최소 주문 수량({min_qty_val})에 대한 가격 조회 실패")
                                                    elif price_result['success']:
                                                        quantity_prices[qty] = {
                                                            'price': price_result['price'],
                                                            'price_with_vat': price_result['price_with_vat'],
                                                            'exact_match': True
                                                        }
                                                        logger.info(f"수량 {qty}개 가격 조회 성공: {price_result['price']}원")
                                                    else:
                                                        logger.warning(f"수량 {qty}개에 대한 가격 조회 실패: {item_data['name']}")
                                            
                                            item_data['quantity_prices'] = quantity_prices
                                            default_price_qty_to_check = None
                                            actual_selected_qty, selected_price = select_highest_price_if_no_base_quantity(
                                                item_data.get('product_actual_price_tiers'), 
                                                default_price_qty_to_check
                                            )

                                            if selected_price is not None:
                                                item_data['price'] = selected_price
                                                item_data['price_with_vat'] = round(selected_price * 1.1)
                                            else:
                                                item_data['price'] = price_value
                                                item_data['price_with_vat'] = round(price_value * 1.1)
                                            
                                            items_on_page.append(item_data)
                                            processed_items += 1
                                        
                                        except SkipItemException:
                                            # This item is flagged for skipping (e.g. price inquiry)
                                            # Log already happened in get_price_for_specific_quantity or here
                                            # The 'finally' block below will handle cleanup.
                                            # Then the outer loop's 'continue' (or natural end of this iteration) will skip.
                                            pass # Allow finally to run, then continue to next item
                                        finally:
                                            if price_detail_page and not price_detail_page.is_closed():
                                                await price_detail_page.close()
                                            if price_detail_context:
                                                await price_detail_context.close()
                                        
                                    except Exception as item_error:
                                        logger.warning(f"⚠️ Error processing item {i} on page {page_number}: {item_error}")
                                        # Ensure cleanup if error happened mid-processing this item
                                        if 'price_detail_page' in locals() and price_detail_page and not price_detail_page.is_closed():
                                            try: await price_detail_page.close()
                                            except: pass
                                        if 'price_detail_context' in locals() and price_detail_context:
                                            try: await price_detail_context.close()
                                            except: pass
                                        continue # Skip to next item from the product list page
                                
                                data.extend(items_on_page)
                                logger.debug(f"📊 Scraped {len(items_on_page)} items from page {page_number}. Total processed: {processed_items}")

                                if processed_items >= max_items_per_variation:
                                    logger.info(f"✅ Reached scrape limit ({max_items_per_variation}) for keyword '{keyword}'.")
                                    break

                                # --- Pagination --- 
                                next_page_locator_str = f'div.custom_paging > div[onclick*="getPageGo1({page_number + 1})"]'
                                next_page_locator = page.locator(next_page_locator_str)
                                
                                try:
                                    if await next_page_locator.is_visible(timeout=5000):
                                        logger.debug(f"📄 Clicking next page ({page_number + 1})")
                                        await next_page_locator.click(timeout=5000)
                                        await page.wait_for_load_state('domcontentloaded', 
                                                                     timeout=config.getint('ScraperSettings', 'navigation_timeout', fallback=90000))
                                        # Extra delay after pagination to ensure page stability
                                        await page.wait_for_timeout(2000)
                                        page_number += 1
                                    else:
                                        logger.info("⚠️ Next page element not found or not visible. Ending pagination.")
                                        break
                                except Exception as pagination_error:
                                    logger.warning(f"⚠️ Error during pagination: {pagination_error}")
                                    break
                                    
                            except Exception as page_error:
                                logger.error(f"⚠️ Error processing page {page_number}: {page_error}")
                                break
                        
                        # Add scraped data to results if we found anything
                        if data:
                            logger.info(f"✅ Successfully scraped {len(data)} items for keyword '{keyword}' from {base_url}")
                            df = pd.DataFrame(data)
                            all_results.append(df)
                        else:
                            logger.warning(f"⚠️ No data could be scraped for keyword '{keyword}' from {base_url}")

                    except Exception as search_error:
                        logger.error(f"⚠️ Error during search for keyword '{keyword}': {search_error}")
                        continue
                        
                except Exception as keyword_error:
                    logger.error(f"⚠️ Error processing keyword '{keyword}': {keyword_error}")
                    continue

        except Exception as url_error:
            logger.error(f"⚠️ Error processing URL {base_url}: {url_error}")
        finally:
            # Clean up resources
            if page:
                try:
                    await page.close()
                except Exception as page_close_error:
                    logger.warning(f"⚠️ Error closing page: {page_close_error}")
            if context:
                try:
                    await context.close()
                except Exception as context_close_error:
                    logger.warning(f"⚠️ Error closing context: {context_close_error}")

    # Combine all results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        if 'href' in final_df.columns:
            final_df = final_df.drop_duplicates(subset=['href'], keep='first')
        logger.info(f"Total unique results from all keyword variations: {len(final_df)}")
        return final_df
    else:
        logger.warning("No results found from any keyword variation or Kogift URL")
        return pd.DataFrame()

# Simple function to test direct image download
def test_kogift_scraper():
    """Test Kogift scraper functionality"""
    import sys
    import os
    import logging
    import requests
    import random
    import time
    import pandas as pd
    from datetime import datetime
    from playwright.async_api import async_playwright
    from utils import load_config

    # --- command-line args ---
    parser = argparse.ArgumentParser(description='Test Kogift scraper functionality')
    parser.add_argument('--test-type', choices=['all','images','products','quantities','test2'], default='all',
                        help='Which test to run')
    parser.add_argument('--max-items', type=int, default=5,
                        help='Max items per keyword')
    parser.add_argument('--quantity', type=int, action='append',
                        help='Quantities to test')
    parser.add_argument('--input-notepad', type=str,
                        help='Path to tab-delimited input file (test2)')
    parser.add_argument('--search-terms', nargs='+', 
                        default=["모스니에 제로웨이스트 대나무칫솔", "하모니 심플칫솔세트 805", "CLIO 크리오 알파솔루션 휴대용 양치세트"],
                        help='Search terms to use for testing. Can include multiple terms separated by spaces.')
    parser.add_argument('--max-variations', type=int, default=2,
                        help='Maximum number of keyword variations to generate (default: 2)')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')
    
    args = parser.parse_args()
    if not args.quantity:
        args.quantity = [30, 50, 100, 300]

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('kogift_test')

    # Load config
    config = load_config(os.path.join(os.path.dirname(__file__), '..','config.ini'))
    # adjust max items
    if config.has_section('ScraperSettings'):
        config.set('ScraperSettings','kogift_max_items', str(args.max_items))
    
    # --- Helper tests inside test_kogift_scraper() ---
    # 1) Image download test (synchronous)
    def test_image_download():
        """이미지 다운로드 테스트 및 정규화 확인"""
        print(f"\n{'=' * 70}")
        print(f"🖼️ 고려기프트 이미지 다운로드 테스트")
        print(f"{'=' * 70}")
        
        # 동기적으로 async 함수를 호출하기 위한 래퍼
        def sync_download_image(url, save_dir, product_name):
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(download_image(url, save_dir, product_name, config))
            finally:
                loop.close()
        
        # 테스트용 이미지 URL들
        test_urls = [
            "https://koreagift.com/upload/mall/shop_1735274388623093_0.jpg",
            "https://koreagift.com/upload/mall/shop_1735274388623093_1.jpg",
            "https://koreagift.com/ez/upload/mall/shop_1735274388623093_0.jpg",
            "https://adpanchok.co.kr/upload/mall/shop_1735267708606983_0.jpg",
            "https://adpanchok.co.kr/ez/upload/mall/shop_1735267708606983_0.jpg"
        ]
        
        # 다운로드 디렉토리 생성
        save_dir = "test_kogift_images"
        os.makedirs(save_dir, exist_ok=True)
        
        # 테스트 결과 변수
        total_urls = len(test_urls)
        normalized_count = 0
        successful_downloads = 0
        failed_downloads = 0
        
        print(f"총 {total_urls}개 URL 테스트 시작...")
        print()
        
        for i, url in enumerate(test_urls):
            print(f"[{i+1}/{total_urls}] 테스트 URL: {url}")
            
            # URL 정규화
            normalized_url, is_valid = normalize_kogift_image_url(url)
            print(f"정규화 결과: {normalized_url}")
            print(f"유효성: {'✅ 유효' if is_valid else '❌ 무효'}")
            
            if is_valid:
                normalized_count += 1
                norm_url = normalized_url
            else:
                print(f"⚠️ 무효한 URL이므로 다운로드를 건너뜁니다.")
                failed_downloads += 1
                print()
                continue
                
            # 이미지 다운로드
            print(f"이미지 다운로드 시도 중...")
            test_filename = f"test_{i+1}_{hashlib.md5(url.encode()).hexdigest()[:6]}.jpg"
            path = sync_download_image(norm_url, save_dir, test_filename)
            
            if path:
                successful_downloads += 1
                file_size = os.path.getsize(path) if os.path.exists(path) else 0
                print(f"✅ 다운로드 성공: {os.path.basename(path)} ({file_size/1024:.1f} KB)")
                
                # 이미지 유효성 검사 (PIL 사용)
                try:
                    with Image.open(path) as img:
                        width, height = img.size
                        print(f"   이미지 크기: {width}x{height} 픽셀")
                        print(f"   이미지 형식: {img.format}")
                except Exception as img_err:
                    print(f"⚠️ 이미지 검증 오류: {img_err}")
            else:
                failed_downloads += 1
                print(f"❌ 다운로드 실패")
        
        # 테스트 결과 요약
        print(f"\n{'=' * 70}")
        print(f"📊 이미지 테스트 결과 요약")
        print(f"{'=' * 70}")
        print(f"총 테스트 URL: {len(test_urls)}개")
        print(f"정규화된 URL: {normalized_count}개")
        print(f"다운로드 성공: {successful_downloads}개")
        print(f"다운로드 실패: {failed_downloads}개")
        print(f"다운로드 성공률: {successful_downloads/len(test_urls)*100:.1f}%")
        
        # 실제 다운로드된 모든 파일 표시
        if os.path.exists(save_dir):
            downloaded_files = [f for f in os.listdir(save_dir) if os.path.isfile(os.path.join(save_dir, f))]
            if downloaded_files:
                print(f"\n📁 다운로드된 파일 목록:")
                for i, file in enumerate(downloaded_files[:10]):  # 최대 10개만 표시
                    file_path = os.path.join(save_dir, file)
                    file_size = os.path.getsize(file_path)
                    print(f"   {i+1}. {file} ({file_size/1024:.1f} KB)")
                
                if len(downloaded_files) > 10:
                    print(f"   ... 외 {len(downloaded_files) - 10}개 파일")
                    
        print(f"{'=' * 70}")

    # 2) Product info test (requires browser)
    async def test_product_info(browser):
        logger.info("=== TESTING PRODUCT INFORMATION RETRIEVAL ===")
        
        # 동기적으로 async 함수를 호출하기 위한 래퍼
        def sync_download_image(url, save_dir, product_name):
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(download_image(url, save_dir, product_name, config))
            finally:
                loop.close()
        
        # Use specified search terms
        test_keywords = args.search_terms
        