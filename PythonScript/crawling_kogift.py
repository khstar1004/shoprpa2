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
def download_image(url: str, save_dir: str, file_name: Optional[str] = None) -> Optional[str]:
    """Download an image from a URL and save it to the specified disk directory."""
    if not url or not save_dir:
        logger.warning("URL or save directory not provided to download_image")
        return None
    
    # Normalize URL
    if not url.startswith(('http://', 'https://')):
        if url.startswith('//'):
            url = f"https:{url}"
        else:
            url = f"https://{url}"
    
    # Extract filename from URL if not provided
    if not file_name:
        # URL의 해시값을 사용하여 고유한 파일명 생성
        url_hash = hashlib.md5(url.encode()).hexdigest()
        original_ext = os.path.splitext(urlparse(url).path)[1].lower()
        if not original_ext or original_ext not in ['.jpg', '.jpeg', '.png']:
            original_ext = '.jpg'
        file_name = f"kogift_{url_hash}{original_ext}"
    
    # Create save directory if it doesn't exist
    try:
        os.makedirs(save_dir, exist_ok=True)
    except PermissionError:
        logger.error(f"Permission denied when creating directory: {save_dir}")
        return None
    
    # Generate file path and check if it exists
    file_path = os.path.join(save_dir, file_name)
    if os.path.exists(file_path):
        # Check if file size is > 0 to consider it valid
        if os.path.getsize(file_path) > 0:
            logger.debug(f"Image already exists: {file_path}")
            return file_path
    
    # Create a unique temporary file path
    temp_file_path = os.path.join(save_dir, f"{os.path.splitext(file_name)[0]}_{random.randint(1000, 9999)}.tmp")
    
    # Try to download with retries and exponential backoff
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Download image
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            }
            response = requests.get(url, headers=headers, timeout=15, stream=True)
            
            # Check response
            if not response.ok:
                logger.warning(f"Failed to download image from {url} (attempt {attempt+1}): HTTP {response.status_code}")
                # Exponential backoff for retry delay
                delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                logger.debug(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
                continue
            
            # Validate content type
            content_type = response.headers.get('Content-Type', '').lower()
            if 'image' not in content_type and 'octet-stream' not in content_type:
                # Some sites may return valid images without proper content type
                if len(response.content) < 1000 and 'text/html' in content_type:
                    logger.warning(f"Not an image (HTML page received): {url}")
                    delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                    time.sleep(delay)
                    continue
            
            # Write to temp file
            try:
                with open(temp_file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                    f.flush()
                
                # Small delay after writing to ensure file handles are closed
                time.sleep(0.2)  # Increased delay to ensure file handles are released
            except (PermissionError, OSError) as e:
                logger.warning(f"Error writing to temp file {temp_file_path}: {e}")
                # Try a different temp filename
                temp_file_path = os.path.join(save_dir, f"{os.path.splitext(file_name)[0]}_{int(time.time())}_{random.randint(1000, 9999)}.tmp")
                delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                time.sleep(delay)
                continue
            
            # Validate image
            try:
                # Check file size
                if os.path.getsize(temp_file_path) < 100:
                    logger.warning(f"Downloaded file too small: {url}")
                    try:
                        os.remove(temp_file_path)
                    except:
                        pass
                    delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                    time.sleep(delay)
                    continue
                
                # Validate image with PIL
                try:
                    with Image.open(temp_file_path) as img:
                        img.verify()  # Verify it's a valid image
                    
                    # Open again to check dimensions (verify closes the file)
                    with Image.open(temp_file_path) as img:
                        width, height = img.size
                        if width < 10 or height < 10:
                            logger.warning(f"Image too small ({width}x{height}): {url}")
                            try:
                                os.remove(temp_file_path)
                            except:
                                pass
                            delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                            time.sleep(delay)
                            continue
                except Exception as img_err:
                    logger.warning(f"Invalid image data: {img_err}")
                    try:
                        os.remove(temp_file_path)
                    except:
                        pass
                    delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                    time.sleep(delay)
                    continue
                
                # Try to move the temporary file to the final location
                move_success = False
                for move_attempt in range(3):
                    try:
                        # If file exists, try to remove it first
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                # Add additional delay after file removal
                                time.sleep(0.5)
                            except (OSError, PermissionError) as e:
                                if "WinError 32" in str(e):  # File being used by another process
                                    logger.warning(f"File in use (WinError 32): {file_path}")
                                    # Create alternative filename with timestamp and random number
                                    file_path = os.path.join(save_dir, f"{os.path.splitext(file_name)[0]}_{int(time.time())}_{random.randint(1000, 9999)}{os.path.splitext(file_name)[1]}")
                                    # Skip the remove operation and try with new filename
                                    time.sleep(0.5)
                                else:
                                    logger.warning(f"Cannot remove existing file {file_path}: {e}")
                                    # Create alternative filename
                                    file_path = os.path.join(save_dir, f"{os.path.splitext(file_name)[0]}_{int(time.time())}_{random.randint(1000, 9999)}{os.path.splitext(file_name)[1]}")
                        
                        # Try to rename (fastest method)
                        os.rename(temp_file_path, file_path)
                        move_success = True
                        break
                    except (OSError, PermissionError) as e:
                        err_msg = str(e)
                        logger.warning(f"OS error renaming file (attempt {move_attempt+1}): {err_msg}")
                        
                        # Handle "file in use" errors (Windows Error 32)
                        if "WinError 32" in err_msg:
                            logger.info(f"File in use (WinError 32), waiting before retry...")
                            # Longer delay for file access issues
                            time.sleep(1.5 + random.uniform(0, 1.0))
                            # Create alternative filename with more randomness
                            file_path = os.path.join(save_dir, f"{os.path.splitext(file_name)[0]}_{int(time.time())}_{random.randint(10000, 99999)}{os.path.splitext(file_name)[1]}")
                        else:
                            time.sleep(0.8 + random.uniform(0, 0.7))
                        
                        # On the second attempt, try with shutil.move
                        if move_attempt == 1:
                            try:
                                # Use copy2 + remove instead of move to reduce file locking issues
                                shutil.copy2(temp_file_path, file_path)
                                time.sleep(0.5)  # Wait before deleting source
                                try:
                                    os.remove(temp_file_path)
                                except:
                                    pass  # Ignore if temp file can't be deleted
                                move_success = True
                                break
                            except Exception as e2:
                                logger.warning(f"Shutil move error: {e2}")
                                time.sleep(0.8 + random.uniform(0, 0.5))
                
                # If move failed, try copy + delete approach
                if not move_success:
                    try:
                        # Try another unique filename for last attempt
                        file_path = os.path.join(save_dir, f"{os.path.splitext(file_name)[0]}_{int(time.time())}_{random.randint(100000, 999999)}{os.path.splitext(file_name)[1]}")
                        shutil.copy2(temp_file_path, file_path)
                        time.sleep(0.5)  # Wait before trying to delete the source
                        try:
                            os.remove(temp_file_path)
                        except:
                            pass  # Ignore failure to delete temp file
                        move_success = True
                    except Exception as e:
                        logger.error(f"OS error saving image to {temp_file_path} or {file_path} (attempt {attempt+1}): {e}")
                        # Last resort - use temp file as actual file
                        if os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0:
                            logger.warning(f"Using temp file as final file: {temp_file_path}")
                            file_path = temp_file_path
                            move_success = True
                
                if move_success:
                    logger.info(f"Downloaded image: {url} -> {file_path}")
                    return file_path
                
            except Exception as e:
                logger.warning(f"Invalid image data from {url}: {e}")
                if os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                    except:
                        pass
                delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
                time.sleep(delay)
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error downloading {url} (attempt {attempt+1}): {e}")
            delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error downloading {url} (attempt {attempt+1}): {e}")
            delay = 1.0 * (2 ** attempt) + random.uniform(0, 0.5)
            time.sleep(delay)
    
    logger.error(f"Failed to download image after {max_attempts} attempts: {url}")
    return None

def download_images_batch(img_urls, save_dir='downloaded_images', max_workers=10):
    """
    Download multiple images in parallel using a thread pool.
    
    Args:
        img_urls: List of image URLs to download
        save_dir: Directory to save the images
        max_workers: Maximum number of concurrent downloads
        
    Returns:
        dict: Mapping of URL to local file path for successful downloads
    """
    results = {}
    
    logger.info(f"Downloading {len(img_urls)} images to {save_dir}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(download_image, url, save_dir): url 
            for url in img_urls if url
        }
        
        for future in future_to_url:
            url = future_to_url[future]
            try:
                path = future.result()
                if path:
                    results[url] = path
            except Exception as e:
                logger.error(f"Error downloading image {url}: {e}")
    
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
    try:
        await page.route("**/*", lambda route:
            route.abort() if should_block_request(route.request.url) else route.continue_()
        )
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
        
        # 고려기프트 사이트의 다양한 테이블 선택자
        table_selectors = [
            "table.quantity_price__table",  # 고려기프트 수량-단가 테이블
            "div.product_table table",      # 고려기프트 상품 테이블
            "table.detail_table",           # 일반적인 상세 테이블
            "div.detail_price table",       # 가격 정보 테이블
            "div.goods_detail table"        # 상품 상세 테이블
        ]
        
        # 고려기프트 특유의 테이블 구조 처리
        kogift_selector = "table.quantity_price__table"
        if await page.locator(kogift_selector).count() > 0:
            # 수량 행과 가격 행이 각각 별도 행에 있는 특별한 구조 처리
            qty_cells = await page.locator(f"{kogift_selector} tr:first-child td").all()
            price_cells = await page.locator(f"{kogift_selector} tr:nth-child(2) td").all()
            
            # 첫 번째 열은 헤더이므로 제외 (수량, 단가 라는 텍스트가 있음)
            quantities = []
            prices = []
            
            # 수량 행 추출
            for i, cell in enumerate(qty_cells):
                if i > 0:  # 첫 번째 열(헤더) 건너뛰기
                    qty_text = await cell.text_content()
                    # 수량에서 쉼표 제거하고 숫자만 추출
                    qty_clean = ''.join(filter(str.isdigit, qty_text.replace(',', '')))
                    if qty_clean:
                        quantities.append(int(qty_clean))
            
            # 가격 행 추출
            for i, cell in enumerate(price_cells):
                if i > 0:  # 첫 번째 열(헤더) 건너뛰기
                    price_text = await cell.text_content()
                    # 가격에서 쉼표 제거하고 숫자만 추출
                    price_clean = ''.join(filter(str.isdigit, price_text.replace(',', '')))
                    if price_clean:
                        prices.append(int(price_clean))
            
            # 유효한 데이터가 있는지 확인
            if quantities and prices and len(quantities) == len(prices):
                # DataFrame 생성
                result_df = pd.DataFrame({
                    '수량': quantities,
                    '단가': prices
                })
                
                # 부가세 정보 확인
                vat_info = await page.locator("div.quantity_price__wrapper div:last-child").text_content()
                has_vat = "부가세별도" in vat_info or "부가세 별도" in vat_info
                
                # 부가세 별도라면 메타데이터로 추가
                if has_vat:
                    result_df.attrs['vat_excluded'] = True
                
                # 수량 기준으로 정렬
                result_df = result_df.sort_values('수량')
                return result_df
        
        # 다른 선택자 시도
        for selector in table_selectors:
            # 이미 처리한 선택자 건너뛰기
            if selector == kogift_selector:
                continue
                
            if await page.locator(selector).count() > 0:
                try:
                    # 테이블 HTML 가져오기
                    table_html = await page.locator(selector).first.inner_html()
                    
                    # 테이블을 pandas DataFrame으로 파싱
                    tables = pd.read_html("<table>" + table_html + "</table>")
                    if not tables:
                        continue
                    
                    table_df = tables[0]
                    
                    # 테이블이 수량-단가 정보인지 확인
                    if len(table_df.columns) >= 2:
                        # 컬럼명에 '수량', '가격', '단가' 등의 키워드가 있는지 확인
                        col_names = [str(col).lower() for col in table_df.columns]
                        qty_keywords = ['수량', 'qty', '개수', '갯수']
                        price_keywords = ['가격', '단가', '금액', 'price']
                        
                        qty_col = None
                        price_col = None
                        
                        # 수량 컬럼 찾기
                        for i, col in enumerate(col_names):
                            if any(keyword in col for keyword in qty_keywords):
                                qty_col = i
                                break
                        
                        # 가격 컬럼 찾기
                        for i, col in enumerate(col_names):
                            if any(keyword in col for keyword in price_keywords):
                                price_col = i
                                break
                        
                        # 컬럼명에서 찾지 못했다면 첫 번째 행에서 찾기
                        if qty_col is None and price_col is None and not table_df.empty:
                            first_row = table_df.iloc[0]
                            for i, value in enumerate(first_row):
                                value_str = str(value).lower()
                                if any(keyword in value_str for keyword in qty_keywords):
                                    qty_col = i
                                if any(keyword in value_str for keyword in price_keywords):
                                    price_col = i
                            
                            # 첫 번째 행이 헤더인 경우 제거
                            if qty_col is not None or price_col is not None:
                                table_df = table_df.iloc[1:]
                        
                        # 그래도 못 찾았다면 첫 번째와 두 번째 컬럼 사용
                        if qty_col is None and price_col is None:
                            qty_col = 0
                            price_col = 1
                        
                        if qty_col is not None and price_col is not None:
                            # 수량-가격 테이블 확인됨
                            result_df = table_df.copy()
                            
                            # 컬럼명 재지정
                            new_cols = result_df.columns.tolist()
                            new_cols[qty_col] = '수량'
                            new_cols[price_col] = '단가'
                            result_df.columns = new_cols
                            
                            # 필요한 컬럼만 선택
                            result_df = result_df[['수량', '단가']]
                            
                            # 데이터 정제
                            result_df['수량'] = result_df['수량'].astype(str).apply(
                                lambda x: ''.join(filter(str.isdigit, str(x).replace(',', '')))
                            )
                            result_df['단가'] = result_df['단가'].astype(str).apply(
                                lambda x: ''.join(filter(str.isdigit, str(x).replace(',', '')))
                            )
                            
                            # 숫자로 변환 가능한 행만 유지
                            result_df = result_df[result_df['수량'].apply(lambda x: x.isdigit())]
                            result_df = result_df[result_df['단가'].apply(lambda x: x.isdigit())]
                            
                            # 데이터 타입 변환
                            result_df['수량'] = result_df['수량'].astype(int)
                            result_df['단가'] = result_df['단가'].astype(int)
                            
                            # 수량 기준 정렬
                            result_df = result_df.sort_values('수량')
                            
                            if not result_df.empty:
                                return result_df
                except Exception as table_error:
                    continue
        
        return None
        
    except Exception as e:
        logger.error(f"수량-가격 테이블 추출 중 오류 발생: {e}")
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
    
    # 이미지 URL 표준화
    for product in product_list:
        # 'image' 또는 'image_url' 키에서 이미지 URL 찾기
        img_url = product.get('image') or product.get('image_url') or product.get('src')
        if img_url:
            product['original_image'] = img_url  # 원본 URL 백업
            
            # URL 표준화
            normalized_url, is_valid = normalize_kogift_image_url(img_url)
            
            if normalized_url:
                # 표준화된 URL 저장
                product['image'] = normalized_url
                product['image_url'] = normalized_url  # 호환성 유지
                product['src'] = normalized_url  # 호환성 유지
            else:
                # 유효하지 않은 URL은 빈 문자열로 표시
                product['image'] = ""
                product['image_url'] = ""
                product['src'] = ""
    
    # 이미지 다운로드 처리
    if download_enabled:
        # 유효한 이미지 URL만 수집
        valid_urls = []
        url_to_product_map = {}
        
        for product in product_list:
            img_url = product.get('image')
            if img_url:
                valid_urls.append(img_url)
                url_to_product_map[img_url] = product
        
        logger.info(f"총 {len(valid_urls)}개 이미지 다운로드 시작")
        
        # 이미지 일괄 다운로드
        downloaded_images = download_images_batch(valid_urls, save_dir=images_dir)
        
        # 다운로드된 이미지 경로를 제품 데이터에 추가
        for url, local_path in downloaded_images.items():
            if url in url_to_product_map:
                url_to_product_map[url]['local_image_path'] = local_path
        
        logger.info(f"이미지 다운로드 완료: {len(downloaded_images)}/{len(valid_urls)} 성공")
    
    # 샘플링 비율에 따라 URL 검증 (기존 코드는 주석 처리)
    if verify_enabled and sample_percent > 0 and not download_enabled:
        # 이미지가 있는 상품만 선택
        products_with_images = [p for p in product_list if p.get('image')]
        if not products_with_images:
            return product_list
            
        # 검증할 상품 샘플링
        sample_size = max(1, int(len(products_with_images) * sample_percent / 100))
        sample_products = random.sample(products_with_images, min(sample_size, len(products_with_images)))
        
        logger.info(f"{sample_percent}% 샘플링으로 {len(sample_products)}개 이미지 URL 검증 시작")
        
        # 검증 결과 카운팅
        verified_count = 0
        failed_count = 0
        
        # 비동기 세션 생성
        async with aiohttp.ClientSession() as session:
            for product in sample_products:
                img_url = product['image']
                if not img_url:
                    continue
                
                # 이미지 URL 실제 접근 검증
                url, is_valid, reason = await verify_image_url(session, img_url)
                
                if is_valid:
                    verified_count += 1
                else:
                    failed_count += 1
                    # koreagift.com 실패 URL 처리
                    if 'koreagift.com' in img_url and is_valid == False:
                        # URL을 고쳐도 실패할 가능성이 높으므로 처리하지 않음
                        pass
        
        logger.info(f"이미지 URL 검증 결과: 성공 {verified_count}, 실패 {failed_count}")
    
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
        "min_quantity": None
    }
    
    try:
        # Navigate to the product page
        await page.goto(product_url, wait_until='domcontentloaded', timeout=timeout)
        
        # Wait for a short period for any initial scripts to run
        await page.wait_for_timeout(1000)
        
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
async def scrape_data(browser: Browser, original_keyword1: str, original_keyword2: Optional[str] = None, config: configparser.ConfigParser = None, fetch_price_tables: bool = False, custom_quantities: List[int] = None):
    """Scrape data from Kogift website."""
    
    # Initialize variables
    results = []
    kogift_urls = get_kogift_urls(config)
    max_items_per_variation = get_max_items_per_variation(config)
    
    # Generate keyword variations
    keyword_variations = generate_keyword_variations(original_keyword1, original_keyword2)
    logger.info(f"Generated {len(keyword_variations)} keyword variations for search: {keyword_variations}")
    logger.info(f"Will scrape up to {max_items_per_variation} items per keyword variation")
    
    # Check if we need to recreate the browser
    need_new_browser = not browser or not browser.is_connected()
    
    # Get quantities to check - use input quantities or fallback to defaults
    if custom_quantities is None or len(custom_quantities) == 0:
        # Try to get quantities from input Excel if config is provided
        if config and config.has_section('Input'):
            try:
                input_file = config.get('Input', 'input_file')
                df = pd.read_excel(input_file)
                if '기본수량(1)' in df.columns:
                    custom_quantities = df['기본수량(1)'].dropna().unique().tolist()
                    custom_quantities = [int(qty) for qty in custom_quantities if str(qty).isdigit()]
                    logger.info(f"Using quantities from input Excel: {custom_quantities}")
            except Exception as e:
                logger.warning(f"Could not get quantities from input Excel: {e}")
                
        # If still no quantities, use defaults
        if not custom_quantities:
            custom_quantities = [300, 800, 1100, 2000]
            logger.info("Using default quantities")
    
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
    for base_url in kogift_urls:
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
            for keyword_index, keyword in enumerate(keyword_variations):
                try:
                    logger.info(f"Attempting to search with variation {keyword_index+1}/{len(keyword_variations)}: '{keyword}' on {base_url}")
                    
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
                                        try:
                                            item_text = await row.text_content(timeout=2000)
                                            if item_text and "품절" in item_text:
                                                logger.info(f"Skipping item {i} as it appears to be sold out (품절).")
                                                continue # Skip this item
                                        except Exception as sold_out_check_err:
                                            logger.warning(f"Could not check for '품절' on item {i}: {sold_out_check_err}")
                                            # Optionally continue processing or skip, depending on desired behavior
                                            # continue 
                                            
                                        item_data = {}
                                        
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
                                        
                                        try:
                                            name_locator = row.locator('div.name > a')
                                            name = await name_locator.text_content(timeout=5000)
                                        except Exception as e:
                                            logger.debug(f"Error getting name: {e}")
                                            name = None
                                        
                                        try:
                                            price_locator = row.locator('div.price')
                                            price_text = await price_locator.text_content(timeout=5000)
                                        except Exception as e:
                                            logger.debug(f"Error getting price: {e}")
                                            price_text = None

                                        # Skip item if we couldn't get essential data
                                        if not a_href or not name:
                                            logger.debug(f"Skipping item due to missing essential data")
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
                                        
                                        # 유효한 이미지 URL만 저장
                                        if valid_img_url:
                                            item_data['image_path'] = final_img_url
                                            item_data['image_url'] = final_img_url
                                            item_data['src'] = final_img_url
                                        else:
                                            item_data['image_path'] = None
                                            item_data['image_url'] = None
                                            item_data['src'] = None
                                        
                                        item_data['href'] = final_href_url
                                        item_data['link'] = final_href_url
                                        item_data['name'] = name.strip() if name else ""
                                        item_data['supplier'] = supplier
                                        item_data['search_keyword'] = keyword
                                        
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
                                        price_detail_context = await browser.new_context(
                                            user_agent=config.get('Network', 'user_agent', fallback='Mozilla/5.0 ...'),
                                            viewport={'width': 1920, 'height': 1080},
                                        )
                                        price_detail_page = await price_detail_context.new_page()
                                        
                                        # 각 수량에 대한 가격 정보 가져오기
                                        logger.info(f"Fetching prices for {len(custom_quantities)} quantities for product: {item_data['name']}")
                                        
                                        # 먼저 수량-가격 테이블 가져오기 시도
                                        price_table = None
                                        if fetch_price_tables:
                                            price_table = await extract_price_table(price_detail_page, final_href_url, timeout=20000)
                                        
                                        # 테이블이 있으면 테이블에서 가격 정보 추출
                                        if price_table is not None and not price_table.empty:
                                            logger.info(f"Using price table for {item_data['name']}, table has {len(price_table)} rows")
                                            
                                            # 테이블에서 최소 수량 확인
                                            min_table_quantity = price_table['수량'].min()
                                            logger.info(f"테이블 최소 수량: {min_table_quantity}개")
                                            
                                            for qty in custom_quantities:
                                                # 주문 수량이 테이블의 최소 수량보다 작은 경우
                                                if qty < min_table_quantity:
                                                    logger.info(f"주문 수량({qty})이 최소 수량({min_table_quantity})보다 작습니다. 최소 수량의 가격을 적용합니다.")
                                                    # 테이블의 최소 수량에 해당하는 가격 정보 사용
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
                                                
                                                # 주문 수량이 테이블 범위 내에 있는 경우 적절한 가격 선택
                                                # 먼저 정확히 일치하는지 확인
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
                                                
                                                # 정확히 일치하지 않는 경우, 범위에 맞는 가격 찾기
                                                # 예: 100개=5천원, 200개=4천원 일 때 120개는 5천원을 적용
                                                lower_rows = price_table[price_table['수량'] <= qty]
                                                if not lower_rows.empty:
                                                    # 주문 수량보다 작거나 같은 최대 수량 찾기
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
                                                
                                                # 테이블의 모든 수량보다 큰 경우, 가장 큰 수량의 가격 적용
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
                                        
                                        # 테이블이 없으면 직접 수량 변경하여 가격 가져오기
                                        else:
                                            # 최소 수량 정보를 저장하기 위한 변수
                                            min_quantity_info = None
                                            
                                            for qty in custom_quantities:
                                                # 이미 최소 수량 오류가 있는 경우, 최소 수량 이하는 처리 안함
                                                if min_quantity_info and qty < min_quantity_info['min_quantity']:
                                                    # 최소 수량에 해당하는 가격 정보 사용
                                                    quantity_prices[qty] = {
                                                        'price': min_quantity_info['price'],
                                                        'price_with_vat': min_quantity_info['price_with_vat'],
                                                        'exact_match': False,
                                                        'actual_quantity': min_quantity_info['min_quantity'],
                                                        'note': f"최소 주문 수량({min_quantity_info['min_quantity']}) 가격 적용"
                                                    }
                                                    logger.info(f"수량 {qty}개는 최소 구간 {min_quantity_info['min_quantity']}개 가격 적용: {min_quantity_info['price']}원")
                                                    continue
                                                
                                                # 특정 수량에 대한 가격 조회
                                                price_result = await get_price_for_specific_quantity(price_detail_page, final_href_url, qty, timeout=20000)
                                                
                                                # 최소 수량 오류가 발생한 경우
                                                if price_result['min_quantity_error'] and price_result['min_quantity']:
                                                    logger.info(f"수량 {qty}개는 최소 주문 수량({price_result['min_quantity']})보다 작습니다.")
                                                    
                                                    # 최소 수량에 대한 가격 조회
                                                    min_qty = price_result['min_quantity']
                                                    min_price_result = await get_price_for_specific_quantity(price_detail_page, final_href_url, min_qty, timeout=20000)
                                                    
                                                    if min_price_result['success']:
                                                        # 최소 수량 정보 저장
                                                        min_quantity_info = {
                                                            'min_quantity': min_qty,
                                                            'price': min_price_result['price'],
                                                            'price_with_vat': min_price_result['price_with_vat']
                                                        }
                                                        
                                                        # 현재 수량에 대한 가격 정보 설정
                                                        quantity_prices[qty] = {
                                                            'price': min_price_result['price'],
                                                            'price_with_vat': min_price_result['price_with_vat'],
                                                            'exact_match': False,
                                                            'actual_quantity': min_qty,
                                                            'note': f"최소 주문 수량({min_qty}) 가격 적용"
                                                        }
                                                        logger.info(f"수량 {qty}개에 최소 수량({min_qty})의 가격 {min_price_result['price']}원 적용")
                                                    else:
                                                        logger.warning(f"최소 주문 수량({min_qty})에 대한 가격 조회 실패")
                                                # 정상적으로 가격을 가져온 경우
                                                elif price_result['success']:
                                                    quantity_prices[qty] = {
                                                        'price': price_result['price'],
                                                        'price_with_vat': price_result['price_with_vat'],
                                                        'exact_match': True
                                                    }
                                                    logger.info(f"수량 {qty}개 가격 조회 성공: {price_result['price']}원")
                                                else:
                                                    logger.warning(f"수량 {qty}개에 대한 가격 조회 실패: {item_data['name']}")
                                        
                                        # 수량별 가격 정보 저장
                                        item_data['quantity_prices'] = quantity_prices
                                        
                                        # 기본 가격 정보 설정 (가장 작은 수량의 가격 또는 목록 페이지 가격)
                                        if quantity_prices:
                                            min_qty = min(quantity_prices.keys())
                                            item_data['price'] = quantity_prices[min_qty]['price']
                                            item_data['price_with_vat'] = quantity_prices[min_qty]['price_with_vat']
                                        else:
                                            # 수량별 가격을 가져오지 못한 경우 목록 페이지 가격 사용
                                            item_data['price'] = price_value
                                            item_data['price_with_vat'] = round(price_value * 1.1)
                                        
                                        # 리소스 정리
                                        await price_detail_page.close()
                                        await price_detail_context.close()
                                        
                                        items_on_page.append(item_data)
                                        processed_items += 1
                                        
                                    except Exception as item_error:
                                        logger.warning(f"⚠️ Error processing item {i} on page {page_number}: {item_error}")
                                        continue
                                
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
        logger.info("=== TESTING IMAGE DOWNLOAD FUNCTIONALITY ===")
        
        test_urls = [
            "https://koreagift.com/ez/upload/mall/shop_1707873892937710_0.jpg",  # 올바른 형식 (ez 포함)
            "https://koreagift.com/upload/mall/shop_1736386408518966_0.jpg",     # 잘못된 형식 (ez 미포함)
            "https://adpanchok.co.kr/upload/mall/shop_1234567890_0.jpg",         # 애드판촉 이미지
            "https://koreagift.com/ez/upload/no_image.jpg"                       # 존재하지 않는 이미지
        ]
        
        print(f"\n{'=' * 70}")
        print(f"🔍 이미지 URL 정규화 및 다운로드 테스트 (총 {len(test_urls)}개 URL)")
        print(f"{'=' * 70}")
        
        # 테스트 디렉토리 설정
        save_dir = os.path.join(config.get('Paths','image_target_dir',fallback='downloaded_images'), 'kogift_test_images')
        os.makedirs(save_dir, exist_ok=True)
        print(f"📁 테스트 이미지 저장 경로: {save_dir}")
        
        # 결과 요약을 위한 카운터
        successful_downloads = 0
        failed_downloads = 0
        normalized_count = 0
        
        # 각 URL에 대한 테스트 수행
        for i, url in enumerate(test_urls):
            print(f"\n[테스트 {i+1}/{len(test_urls)}]")
            print(f"원본 URL: {url}")
            
            # URL 정규화
            norm_url, valid = normalize_kogift_image_url(url)
            
            if norm_url != url:
                normalized_count += 1
                print(f"정규화 URL: {norm_url} (변경됨)")
            else:
                print(f"정규화 URL: {norm_url} (변경 없음)")
                
            print(f"URL 유효성: {'✅ 유효함' if valid else '❌ 유효하지 않음'}")
            
            if not valid:
                failed_downloads += 1
                print(f"⚠️ 유효하지 않은 URL - 다운로드 건너뜀")
                continue
                
            # 이미지 다운로드
            print(f"이미지 다운로드 시도 중...")
            test_filename = f"test_{i+1}_{hashlib.md5(url.encode()).hexdigest()[:6]}.jpg"
            path = download_image(norm_url, save_dir, test_filename)
            
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
        
        # Use specified search terms
        test_keywords = args.search_terms
        
        # 이미지 저장 디렉토리 생성 (테스트용)
        test_image_dir = os.path.join(config.get('Paths', 'image_target_dir', fallback='downloaded_images'), 'kogift_test')
        os.makedirs(test_image_dir, exist_ok=True)
        
        for keyword in test_keywords:
            logger.info(f"\n--- Searching for '{keyword}' ---")
            try:
                # Pass the custom quantities to scrape_data
                df = await scrape_data(browser, keyword, config=config, 
                                 custom_quantities=args.quantity, 
                                 fetch_price_tables=True)  # 테이블 데이터도 가져오기
                
                if df.empty:
                    print(f"No products found for '{keyword}'")
                    continue
                    
                print(f"Found {len(df)} products for '{keyword}'")
                
                # Display image URLs and prices for each product
                for idx, row in df.iterrows():
                    print(f"\n{'=' * 70}")
                    print(f"Product {idx+1}: {row.get('name', 'Unknown Name')}")
                    print(f"  URL: {row.get('href', 'N/A')}")
                    print(f"  Supplier: {row.get('supplier', 'Unknown')}")
                    
                    # 이미지 정보 출력 및 테스트 다운로드
                    img_url = row.get('image_url')
                    if img_url:
                        norm_url, valid = normalize_kogift_image_url(img_url)
                        print(f"  Image URL: {img_url}")
                        print(f"  Normalized URL: {norm_url}")
                        print(f"  Image URL valid: {'Yes' if valid else 'No'}")
                        
                        # 이미지 다운로드 테스트
                        if valid:
                            print("  Testing image download...")
                            product_name_hash = hashlib.md5(row.get('name', '').encode()).hexdigest()[:8]
                            img_filename = f"test_{idx}_{product_name_hash}.jpg"
                            
                            download_path = download_image(norm_url, test_image_dir, img_filename)
                            if download_path:
                                img_size = os.path.getsize(download_path) if os.path.exists(download_path) else 0
                                print(f"  ✅ Image downloaded: {os.path.basename(download_path)} ({img_size/1024:.1f} KB)")
                            else:
                                print(f"  ❌ Failed to download image")
                    else:
                        print(f"  ❌ No image URL available")
                    
                    print(f"\n  Price Information:")
                    print(f"  Basic Price (excl. VAT): {row.get('price', 'N/A')} KRW")
                    print(f"  Basic Price (incl. VAT): {row.get('price_with_vat', 'N/A')} KRW")
                    
                    # 수량별 가격 정보 상세 분석 및 표시
                    if 'quantity_prices' in row and row['quantity_prices']:
                        print("\n  Quantity-based prices:")
                        print("  " + "-" * 68)
                        print("  | {:^8} | {:^12} | {:^12} | {:^28} |".format("수량", "단가(VAT제외)", "단가(VAT포함)", "비고"))
                        print("  " + "-" * 68)
                        
                        # 수량 순서대로 정렬하여 표시
                        sorted_quantities = sorted(row['quantity_prices'].keys())
                        
                        for qty in sorted_quantities:
                            price_info = row['quantity_prices'][qty]
                            price = price_info['price']
                            price_with_vat = price_info['price_with_vat']
                            
                            # 비고 정보 구성
                            if price_info.get('exact_match', False):
                                note = "정확한 수량 일치"
                            elif 'note' in price_info:
                                note = price_info['note']
                            elif 'actual_quantity' in price_info:
                                note = f"근사값 (실제 수량: {price_info['actual_quantity']}개)"
                            else:
                                note = "-"
                                
                            print("  | {:>8,d} | {:>12,d} | {:>12,d} | {:<28} |".format(
                                qty, price, price_with_vat, note))
                        
                        print("  " + "-" * 68)
                        
                        # 수량별 가격 변화 추이 분석
                        if len(sorted_quantities) > 1:
                            min_qty = min(sorted_quantities)
                            max_qty = max(sorted_quantities)
                            min_price = row['quantity_prices'][min_qty]['price']
                            max_price = row['quantity_prices'][max_qty]['price']
                            
                            if min_price > max_price:
                                price_trend = f"수량이 증가할수록 단가 감소 ({min_price}원 → {max_price}원), 할인율: {(1 - max_price/min_price)*100:.1f}%"
                            elif min_price < max_price:
                                price_trend = f"수량이 증가할수록 단가 증가 ({min_price}원 → {max_price}원), 상승률: {(max_price/min_price - 1)*100:.1f}%"
                            else:
                                price_trend = "수량에 관계없이 단가 일정"
                                
                            print(f"\n  가격 추이 분석: {price_trend}")
                    else:
                        print("\n  ❌ No quantity-based price information available")
                    
                    # 수량과 가격 조합이 적절한지 검증
                    if 'price' in row and 'quantity_prices' in row and row['quantity_prices']:
                        min_qty_price = min([info['price'] for info in row['quantity_prices'].values()])
                        base_price = row.get('price', 0)
                        
                        if abs(min_qty_price - base_price) > base_price * 0.1:  # 10% 이상 차이
                            print(f"\n  ⚠️ Warning: Base price ({base_price}원) differs significantly from minimum quantity price ({min_qty_price}원)")
                    
                    print(f"{'=' * 70}")
                    
                    # Limit display to first 3 products per keyword to avoid too much output
                    if idx >= 2:
                        print(f"... and {len(df) - 3} more products")
                        break
            except Exception as e:
                print(f"Error searching for '{keyword}': {e}")
                logger.error(f"Error during test_product_info for keyword '{keyword}': {e}", exc_info=True)
                print(f"Skipping to next keyword...")
                continue

    # 3) Custom quantities pricing test (requires browser)
    async def test_custom_quantities(browser):
        logger.info("=== TESTING CUSTOM QUANTITIES FUNCTIONALITY ===")
        
        # Use the first search term for quantity testing
        keyword = args.search_terms[0]
        logger.info(f"Testing quantities for '{keyword}'...")
        
        try:
            # Check if browser is connected
            if not browser or not browser.is_connected():
                logger.warning("Browser is not connected. Attempting to create a new browser...")
                from playwright.async_api import async_playwright
                p = await async_playwright().start()
                browser = await p.chromium.launch(
                    headless=config.getboolean('Playwright', 'playwright_headless', fallback=True),
                    args=json.loads(config.get('Playwright', 'playwright_browser_args', fallback='["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]')),
                    timeout=60000
                )
                logger.info("Successfully created new browser instance")

            # Create a new context for price testing
            context = await browser.new_context(
                user_agent=config.get('Network', 'user_agent', fallback='Mozilla/5.0 ...'),
                viewport={'width': 1920, 'height': 1080},
            )
            page = await context.new_page()
            
            # Test direct product search first to get product URL
            print(f"\n{'=' * 70}")
            print(f"검색어: '{keyword}'에 대한 상품 검색 중...")
            df = await scrape_data(browser, keyword, config=config, custom_quantities=args.quantity)
            
            if df.empty:
                print(f"❌ 검색어 '{keyword}'에 대한 상품을 찾을 수 없습니다.")
                await context.close()
                return
                
            print(f"✅ {len(df)}개 상품을 찾았습니다.")
            
            # 테스트할 상품 선택 (최대 2개)
            test_products = min(2, len(df))
            for product_idx in range(test_products):
                # Get product info
                product = df.iloc[product_idx]
                product_url = product.get('href', None)
                product_name = product.get('name', 'Unknown Product')
                
                if not product_url:
                    print(f"❌ 상품 URL을 찾을 수 없습니다.")
                    continue
                    
                print(f"\n{'=' * 70}")
                print(f"👉 상품 테스트 #{product_idx+1}: {product_name}")
                print(f"   URL: {product_url}")
                
                # 1. 직접 수량 입력 방식 테스트
                print(f"\n[1] 직접 수량 입력 방식 테스트")
                print(f"{'-' * 50}")
                
                # 수량별 결과 저장
                qty_results = []
                
                for qty in sorted(args.quantity):
                    try:
                        result = await get_price_for_specific_quantity(page, product_url, qty, timeout=20000)
                        qty_results.append({
                            'quantity': qty,
                            'success': result['success'],
                            'price': result.get('price', 0),
                            'price_with_vat': result.get('price_with_vat', 0),
                            'min_quantity_error': result.get('min_quantity_error', False),
                            'min_quantity': result.get('min_quantity', None)
                        })
                        
                        if result['success']:
                            print(f"✅ 수량 {qty:,d}개: {result['price']:,d}원 (VAT포함: {result['price_with_vat']:,d}원)")
                        else:
                            if result.get('min_quantity_error'):
                                print(f"⚠️ 수량 {qty:,d}개: 최소 주문 수량은 {result['min_quantity']:,d}개 입니다.")
                            else:
                                print(f"❌ 수량 {qty:,d}개: 가격 조회 실패")
                            
                    except Exception as e:
                        logger.error(f"Error getting price for quantity {qty}: {e}")
                        print(f"❌ 수량 {qty:,d}개: 오류 발생 - {str(e)}")
                        
                # 2. 가격 테이블 테스트
                print(f"\n[2] 가격 테이블 테스트")
                print(f"{'-' * 50}")
                
                try:
                    price_table = await extract_price_table(page, product_url)
                    
                    if price_table is not None and not price_table.empty:
                        print("✅ 가격 테이블 발견!")
                        print("\n📊 가격 테이블 내용:")
                        print("-" * 50)
                        print("| {:^8} | {:^12} | {:^12} | {:^15} |".format(
                            "수량", "단가(VAT제외)", "단가(VAT포함)", "비고"))
                        print("-" * 50)
                        
                        for _, row in price_table.iterrows():
                            qty = row['수량']
                            price = row['단가']
                            price_with_vat = round(price * 1.1)
                            note = row.get('비고', '')
                            
                            print("| {:>8,d} | {:>12,d} | {:>12,d} | {:<15} |".format(
                                qty, price, price_with_vat, note))
                        
                        print("-" * 50)
                        
                        # 가격 추이 분석
                        if len(price_table) > 1:
                            min_price = price_table['단가'].min()
                            max_price = price_table['단가'].max()
                            price_diff = max_price - min_price
                            if price_diff > 0:
                                discount_rate = (price_diff / max_price) * 100
                                print(f"\n가격 추이 분석: 수량이 증가할수록 단가 감소 ({max_price:,d}원 → {min_price:,d}원), 할인율: {discount_rate:.1f}%")
                    else:
                        print(f"❌ 가격 테이블을 찾을 수 없습니다.")
                        
                        # 직접 입력 방식 결과만 요약 표시
                        if qty_results:
                            print(f"\n📊 직접 수량 입력 결과 요약:")
                            print("-" * 50)
                            print("| {:^8} | {:^12} | {:^12} | {:^15} |".format(
                                "수량", "단가(VAT제외)", "단가(VAT포함)", "비고"))
                            print("-" * 50)
                            
                            for result in qty_results:
                                note = ""
                                if result['min_quantity_error']:
                                    note = f"최소수량({result['min_quantity']})"
                                elif not result['success']:
                                    note = "조회실패"
                                    
                                print("| {:>8,d} | {:>12,d} | {:>12,d} | {:<15} |".format(
                                    result['quantity'], result['price'], result['price_with_vat'], note))
                            
                            print("-" * 50)
                        
                except Exception as e:
                    logger.error(f"Error extracting price table: {e}")
                    print(f"❌ 가격 테이블 추출 중 오류 발생: {str(e)}")
            
            await page.close()
            await context.close()
            
        except Exception as e:
            logger.error(f"Error in test_custom_quantities: {e}")
            print(f"❌ 수량별 가격 테스트 중 오류 발생: {str(e)}")
            # Try to clean up resources even if there was an error
            try:
                if 'page' in locals():
                    await page.close()
                if 'context' in locals():
                    await context.close()
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {cleanup_error}")

    # 4) Standard test dispatcher
    async def run_standard_tests():
        print(f"\n{'=' * 70}")
        print(f"🧪 고려기프트 크롤링 테스트 시작")
        print(f"{'=' * 70}")
        print(f"테스트 유형: {args.test_type}")
        print(f"검색어: {args.search_terms}")
        print(f"테스트 수량: {args.quantity}")
        
        # 테스트 시작 시간 기록
        start_time = time.time()
        
        async with async_playwright() as p:
            # Use headless mode from args
            headless = True
            if hasattr(args, 'headless'):
                headless = args.headless
            else:
                headless = config.getboolean('Playwright','playwright_headless',fallback=True)
                
            logger.info(f"브라우저 실행 중 (headless: {headless})")
            
            browser_args = []
            try:
                browser_args_str = config.get('Playwright', 'playwright_browser_args', fallback='[]')
                import json
                browser_args = json.loads(browser_args_str)
            except Exception as arg_err:
                logger.warning(f"브라우저 인수 파싱 오류, 기본값 사용: {arg_err}")
                browser_args = ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
            
            # 브라우저 실행
            browser = await p.chromium.launch(
                headless=headless,
                args=browser_args,
                timeout=60000  # 1분 타임아웃
            )
            
            print(f"\n{'=' * 70}")
            print(f"🔍 테스트 실행 순서")
            print(f"{'=' * 70}")
            
            tests_to_run = []
            if args.test_type in ['all', 'images']:
                tests_to_run.append("1. 이미지 URL 정규화 및 다운로드 테스트")
            if args.test_type in ['all', 'products']:
                tests_to_run.append("2. 상품 검색 및 정보 조회 테스트")
            if args.test_type in ['all', 'quantities']:
                tests_to_run.append("3. 수량별 가격 조회 및 가격 테이블 테스트")
                
            for i, test in enumerate(tests_to_run):
                print(f"  {test}")
            
            # 테스트 실행
            test_results = {}
            
            if args.test_type in ['all', 'images']:
                print(f"\n{'=' * 70}")
                print(f"🖼️ 이미지 URL 정규화 및 다운로드 테스트 시작")
                print(f"{'=' * 70}")
                
                img_test_start = time.time()
                test_image_download()
                img_test_time = time.time() - img_test_start
                test_results['images'] = {'time': img_test_time, 'status': 'completed'}
            
            if args.test_type in ['all', 'products']:
                print(f"\n{'=' * 70}")
                print(f"📝 상품 검색 및 정보 조회 테스트 시작")
                print(f"{'=' * 70}")
                
                prod_test_start = time.time()
                await test_product_info(browser)
                prod_test_time = time.time() - prod_test_start
                test_results['products'] = {'time': prod_test_time, 'status': 'completed'}
            
            if args.test_type in ['all', 'quantities']:
                print(f"\n{'=' * 70}")
                print(f"📊 수량별 가격 조회 및 가격 테이블 테스트 시작")
                print(f"{'=' * 70}")
                
                qty_test_start = time.time()
                await test_custom_quantities(browser)
                qty_test_time = time.time() - qty_test_start
                test_results['quantities'] = {'time': qty_test_time, 'status': 'completed'}
                
            # 브라우저 종료
            logger.info("브라우저 종료 중...")
            await browser.close()
            
            # 테스트 결과 요약
            total_time = time.time() - start_time
            
            print(f"\n{'=' * 70}")
            print(f"📋 테스트 결과 요약")
            print(f"{'=' * 70}")
            print(f"총 테스트 실행 시간: {total_time:.2f}초")
            
            if test_results:
                print(f"\n세부 테스트 실행 시간:")
                for test_name, result in test_results.items():
                    test_desc = {
                        'images': '이미지 URL 및 다운로드 테스트',
                        'products': '상품 검색 및 정보 조회 테스트',
                        'quantities': '수량별 가격 조회 테스트'
                    }.get(test_name, test_name)
                    
                    print(f"  - {test_desc}: {result['time']:.2f}초")
            
            print(f"\n✅ 테스트 완료")
            print(f"{'=' * 70}")

    # dispatch
    print(f"Test mode: {args.test_type}")
    print(f"Search terms: {args.search_terms}")
    print(f"Test quantities: {args.quantity}")
    if args.test_type == 'test2':
        # TODO: Implement run_test2 or remove this branch if not needed
        # import asyncio; asyncio.run(run_test2())
        logger.warning("Test type 'test2' selected but run_test2 is not implemented.")
    else:
        import asyncio; asyncio.run(run_standard_tests())

if __name__ == "__main__":
    # If this file is run directly, run the test
    if os.path.basename(__file__) == "crawling_kogift.py":
        # Setup basic logging FOR THE TEST ONLY
        # In production, logging is set up by initialize_environment
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger.info("Running Kogift scraper test...")
        
        # Run the comprehensive test function
        test_kogift_scraper()

# --- 해오름 기프트 입력 데이터에서 수량 추출 함수 ---
def extract_quantities_from_input(input_data: str) -> List[int]:
    """
    탭으로 구분된 입력 데이터에서 수량 컬럼을 찾아 유니크 수량 리스트를 반환합니다.
    """
    quantities = []
    if not input_data:
        return quantities
    lines = input_data.strip().split('\n')
    if len(lines) < 2:
        return quantities
    headers = lines[0].split('\t')
    qty_idx = next((i for i, h in enumerate(headers) if '수량' in h), None)
    if qty_idx is None:
        return quantities
    for row in lines[1:]:
        cols = row.split('\t')
        if len(cols) > qty_idx:
            raw = ''.join(filter(str.isdigit, cols[qty_idx]))
            if raw:
                quantities.append(int(raw))
    return sorted(set(quantities))

# --- 해오름 기프트 입력 데이터에서 상품명/수량/단가 추출 함수 ---
def extract_products_from_input(input_data: str) -> List[Dict[str, Any]]:
    """
    입력 데이터에서 상품명, 수량, 단가 컬럼을 파싱하여 딕셔너리 리스트로 반환합니다.
    """
    products = []
    if not input_data:
        return products
    lines = input_data.strip().split('\n')
    if len(lines) < 2:
        return products
    headers = lines[0].split('\t')
    idx_name = next((i for i,h in enumerate(headers) if '상품명' in h), None)
    idx_qty  = next((i for i,h in enumerate(headers) if '수량' in h), None)
    idx_prc  = next((i for i,h in enumerate(headers) if '단가' in h or '가격' in h), None)
    if idx_name is None:
        return products
    for row in lines[1:]:
        cols = row.split('\t')
        if len(cols) <= idx_name:
            continue
        item = {'name': cols[idx_name].strip()}
        if idx_qty is not None and len(cols)>idx_qty:
            raw_q=''.join(filter(str.isdigit,cols[idx_qty])); item['quantity']=int(raw_q) if raw_q else None
        if idx_prc is not None and len(cols)>idx_prc:
            raw_p=''.join(filter(str.isdigit,cols[idx_prc])); item['price']=int(raw_p) if raw_p else None
        products.append(item)
    return products

