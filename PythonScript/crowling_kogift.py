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

# 고려기프트 이미지 경로 중요 정보:
# /ez/ 경로가 이미지 URL에 반드시 포함되어야 합니다.
# 잘못된 형식: https://koreagift.com/upload/mall/shop_1736386408518966_0.jpg
# 올바른 형식: https://koreagift.com/ez/upload/mall/shop_1736386408518966_0.jpg
# 위의 /ez/ 경로가 없으면 이미지 로드가 실패하므로 모든 이미지 URL 처리 시 확인해야 합니다.

# 로거 설정 (basicConfig는 메인에서 한 번만 호출하는 것이 좋음)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Get logger instance

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
                        
                        # 컬럼명에서 찾지 못했다면 첫 번째, 두 번째 컬럼으로 가정
                        if qty_col is None and price_col is None and len(table_df.columns) >= 2:
                            # 첫 번째 행에 수량, 단가 등의 키워드가 있는지 확인
                            if not table_df.empty:
                                first_row = table_df.iloc[0]
                                for i, value in enumerate(first_row):
                                    value_str = str(value).lower()
                                    if any(keyword in value_str for keyword in qty_keywords):
                                        qty_col = i
                                    if any(keyword in value_str for keyword in price_keywords):
                                        price_col = i
                            
                            # 그래도 못 찾았다면 첫 번째와 두 번째 컬럼 사용
                            if qty_col is None and price_col is None:
                                qty_col = 0
                                price_col = 1
                        
                        if qty_col is not None and price_col is not None:
                            # 수량-가격 테이블 확인됨
                            # 컬럼 이름 변경
                            result_df = table_df.copy()
                            new_cols = result_df.columns.tolist()
                            
                            # 첫 번째 행이 헤더인 경우 처리
                            if any(keyword in str(result_df.iloc[0, qty_col]).lower() for keyword in qty_keywords) and \
                               any(keyword in str(result_df.iloc[0, price_col]).lower() for keyword in price_keywords):
                                # 첫 번째 행을 제외하고 처리
                                result_df = result_df.iloc[1:].copy()
                            
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
                    # 테이블 파싱 실패 시 다음 선택자로 진행
                    continue
        
        # 셀렉트 박스에서 단가 정보 찾기
        option_selector = "select[name='chadung_list'] option"
        if await page.locator(option_selector).count() > 0:
            options = await page.locator(option_selector).all()
            
            quantities = []
            prices = []
            
            for option in options:
                value = await option.get_attribute('value')
                text = await option.text_content()
                
                # 상품 선택 안내 옵션 스킵
                if not value or "선택해 주세요" in text or "----------" in text:
                    continue
                
                # 단가 정보가 있는 옵션 처리
                if "단가::" in value:
                    parts = value.split('|^|')
                    if len(parts) >= 3:
                        qty_part = parts[0].replace('단가::', '')
                        price_part = parts[1]
                        
                        # 수량과 가격 추출
                        if qty_part.isdigit() and price_part.isdigit():
                            quantities.append(int(qty_part))
                            prices.append(int(price_part))
            
            # 유효한 데이터가 있는지 확인
            if quantities and prices:
                # DataFrame 생성
                result_df = pd.DataFrame({
                    '수량': quantities,
                    '단가': prices
                })
                
                # 수량 기준으로 정렬
                result_df = result_df.sort_values('수량')
                return result_df
        
        # 테이블을 찾지 못함
        return None
        
    except Exception as e:
        # 오류 발생 시 None 반환
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
    
    Args:
        page: Playwright Page 객체
        product_url: 상품 상세 페이지 URL
        target_quantity: 설정할 수량 (int)
        timeout: 타임아웃(ms)
        
    Returns:
        dict: 수량, 단가(부가세 포함/미포함), 성공 여부
    """
    result = {
        "quantity": target_quantity,
        "price": 0,
        "price_with_vat": 0,
        "success": False
    }
    
    try:
        await page.goto(product_url, wait_until='domcontentloaded', timeout=timeout)
        
        # 수량 입력 필드 확인
        buynum_input = page.locator('input#buynum')
        if await buynum_input.count() == 0:
            logger.warning(f"수량 입력 필드를 찾을 수 없습니다: {product_url}")
            return result
            
        # 현재 값 지우고 타겟 수량 입력
        await buynum_input.click()
        await buynum_input.press("Control+a")
        await buynum_input.press("Delete")
        await buynum_input.fill(str(target_quantity))
        
        # 'change' 이벤트 트리거를 위해 다른 곳 클릭 또는 Enter 키 입력
        await buynum_input.press("Enter")
        
        # 가격이 업데이트될 때까지 짧게 대기
        await page.wait_for_timeout(1000)
        
        # 업데이트된 가격 가져오기
        price_element = page.locator('span#main_price')
        if await price_element.count() == 0:
            logger.warning(f"가격 요소를 찾을 수 없습니다: {product_url}")
            return result
            
        price_text = await price_element.text_content()
        
        # 숫자만 추출
        price_clean = ''.join(filter(str.isdigit, price_text.replace(',', '')))
        if not price_clean:
            logger.warning(f"유효한 가격을 찾을 수 없습니다: {product_url}")
            return result
            
        price = int(price_clean)
        
        # 항상 부가세 10%를 추가하여 계산
        price_with_vat = round(price * 1.1)

        result["price"] = price
        result["price_with_vat"] = price_with_vat
        result["success"] = True

        return result
        
    except Exception as e:
        logger.error(f"수량 설정 및 가격 조회 중 오류 발생: {e}")
        return result

# --- Main scraping function에 상세 페이지 크롤링 로직 추가 --- 
async def scrape_data(browser: Browser, original_keyword1: str, original_keyword2: Optional[str] = None, config: configparser.ConfigParser = None, fetch_price_tables: bool = False, custom_quantities: List[int] = None):
    """Scrape data from Kogift website."""
    if config is None:
        logger.error("Configuration object is required")
        return pd.DataFrame()

    # Get URLs from config
    try:
        kogift_urls = config.get('ScraperSettings', 'kogift_urls', fallback='https://koreagift.com/ez/index.php,https://adpanchok.co.kr/ez/index.php').split(',')
        if not kogift_urls:
            logger.error("No valid Kogift URLs found in config")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error getting Kogift URLs from config: {e}")
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

    # Generate keyword variations using the imported utility function
    keyword_variations = generate_keyword_variations(original_keyword1, max_variations=3)
    logger.info(f"Generated {len(keyword_variations)} keyword variations for search: {keyword_variations}")
    
    all_results = []
    seen_product_urls = set()  # Track product URLs to avoid duplicates

    # Get maximum items to scrape per keyword
    max_items_per_keyword = config.getint('ScraperSettings', 'kogift_max_items', fallback=10)
    # Reduce max items per variation to avoid excessive scraping
    max_items_per_variation = max(5, max_items_per_keyword // len(keyword_variations)) if keyword_variations else max_items_per_keyword
    logger.info(f"Will scrape up to {max_items_per_variation} items per keyword variation")
    
    # Try each URL in sequence
    for base_url in kogift_urls:
        # --- Add check for browser connection --- 
        if not browser or not browser.is_connected():
            logger.error(f"🔴 Browser is not connected before processing URL: {base_url}. Skipping this URL.")
            continue
        # --- End check ---
        
        context = None # Initialize context to None
        try:
            # Create a new context for each URL
            logger.debug(f"Attempting to create new browser context for {base_url}")
            context = await browser.new_context(
                user_agent=config.get('Network', 'user_agent', fallback='Mozilla/5.0 ...'),
                viewport={'width': 1920, 'height': 1080},
                # Add other context options if needed
            )
            logger.debug(f"Successfully created context for {base_url}")
            
            # Create a new page
            page = await context.new_page()
            page.set_default_timeout(PAGE_TIMEOUT)
            page.set_default_navigation_timeout(NAVIGATION_TIMEOUT)

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
                        # Navigate to the search page
                        await page.goto(search_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                        await page.wait_for_timeout(2000)  # Short wait after initial load

                        # --- Perform Search --- 
                        search_input_locator = page.locator('input#main_keyword[name="keyword"]') # More specific selector
                        search_button_locator = page.locator('img#search_submit')
                        
                        await search_input_locator.wait_for(state="visible", timeout=PAGE_TIMEOUT)
                        
                        # Clear any default value in the search input
                        await search_input_locator.click()
                        await search_input_locator.press("Control+a")
                        await search_input_locator.press("Delete")
                        
                        # Fill the search input with the current keyword variation
                        await search_input_locator.fill(keyword)
                        await search_button_locator.wait_for(state="visible", timeout=PAGE_TIMEOUT)
                        
                        logger.debug(f"🔍 Clicking search for variation '{keyword}'...")
                        await search_button_locator.click()
                        logger.info(f"🔍 Search submitted for: '{keyword}' on {base_url}")

                        # --- Wait for results OR "no results" message --- 
                        results_container_selector = 'div.product_lists' # Selector for the container holding results
                        # Refined selector for "no results" message
                        no_results_selector = 'div.not_result span.icon_dot2:has-text("검색 결과가 없습니다")' 
                        combined_selector = f"{results_container_selector}, {no_results_selector}"
                        
                        logger.debug(f"⏳ Waiting for search results or 'no results' message (timeout: {NAVIGATION_TIMEOUT}ms)...")
                        try:
                            found_element = await page.wait_for_selector(
                                combined_selector, 
                                state='visible', 
                                timeout=NAVIGATION_TIMEOUT
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

                        # --- Check product count (Optional Re-search) --- 
                        productCont = 0
                        try:
                            product_count_element = page.locator('div.list_info span').first # Simpler selector
                            productContText = await product_count_element.text_content(timeout=5000) 
                            productContDigits = re.findall(r'\d+', productContText.replace(',', ''))
                            if productContDigits:
                                productCont = int("".join(productContDigits))
                            logger.info(f"📊 Reported product count for '{keyword}': {productCont}")
                        except (PlaywrightError, Exception) as e:
                            logger.warning(f"⚠️ Could not find/parse product count: {e}")

                        # Re-search logic (only if initial search had results)
                        if original_keyword2 and original_keyword2.strip() != "" and productCont >= 100:
                            logger.info(f"🔍 Count >= 100. Performing re-search with: '{original_keyword2}'")
                            try:
                                re_search_input = page.locator('input#re_keyword')
                                re_search_button = page.locator('button[onclick^="re_search"]')
                                await re_search_input.fill(original_keyword2)
                                
                                logger.debug("🔍 Clicking re-search...")
                                await re_search_button.click()
                                
                                # Wait again after re-search, checking for no results again
                                logger.debug(f"⏳ Waiting after re-search (timeout: {NAVIGATION_TIMEOUT}ms)...")
                                try:
                                    await page.wait_for_selector(
                                        combined_selector, 
                                        state='visible', 
                                        timeout=NAVIGATION_TIMEOUT
                                    )
                                    if await page.locator(no_results_selector).is_visible():
                                         logger.info(f"⚠️ 'No results' found after re-searching with '{original_keyword2}'.")
                                         # Continue with current URL, using initial search results
                                    else:
                                         logger.info(f"✅ Re-search completed for: '{original_keyword2}'. Proceeding with scraping new results.")
                                except PlaywrightError as re_wait_error:
                                    logger.warning(f"⚠️ Timeout/error waiting for results after re-search with '{original_keyword2}': {re_wait_error}")
                                    # Continue with current URL, using initial search results
                                    
                            except (PlaywrightError, Exception) as e:
                                logger.warning(f"⚠️ Failed during re-search attempt: {e}")
                                # Continue with initial results if re-search fails here

                        # --- Scrape Results Pages --- 
                        page_number = 1
                        processed_items = 0
                        product_item_selector = 'div.product' # Selector for individual product blocks
                        data = []

                        while processed_items < max_items_per_variation and page_number <= 10:
                            logger.info(f"📄 Scraping page {page_number} (Keyword: '{keyword}', URL: {base_url})... Items processed: {processed_items}")
                            try:
                                 # Wait for at least one product item to be potentially visible
                                 await page.locator(product_item_selector).first.wait_for(state="attached", timeout=PAGE_TIMEOUT)
                            except PlaywrightError:
                                 logger.warning(f"⚠️ Product items selector ('{product_item_selector}') not found/attached on page {page_number}. Stopping scrape for this keyword.")
                                 break
                             
                            rows = page.locator(product_item_selector)
                            count = await rows.count()
                            logger.debug(f"📊 Found {count} product elements on page {page_number}.")

                            if count == 0 and page_number > 1: # Allow page 1 to have 0 if count check failed earlier
                                 logger.info(f"⚠️ No product elements found on page {page_number}. Stopping pagination.")
                                 break
                            elif count == 0 and page_number == 1:
                                 logger.info(f"⚠️ No product elements found on first page (page {page_number}). Stopping scrape for this keyword.")
                                 break

                            items_on_page = []
                            for i in range(count):
                                if processed_items >= max_items_per_variation:
                                    break
                                row = rows.nth(i)
                                item_data = {}
                                try:
                                    # Extract data using locators with short timeouts
                                    img_locator = row.locator('div.pic > a > img')
                                    img_src = await img_locator.get_attribute('src', timeout=PAGE_TIMEOUT)
                                    
                                    link_locator = row.locator('div.pic > a')
                                    a_href = await link_locator.get_attribute('href', timeout=PAGE_TIMEOUT)
                                    
                                    name_locator = row.locator('div.name > a')
                                    name = await name_locator.text_content(timeout=PAGE_TIMEOUT)
                                    
                                    price_locator = row.locator('div.price')
                                    price_text = await price_locator.text_content(timeout=PAGE_TIMEOUT)

                                    # Process extracted data
                                    base_domain_url = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
                                    
                                    # 디버깅: 원본 URL 및 변환 과정 로깅
                                    logger.debug(f"🔗 Raw image src: {img_src}")
                                    logger.debug(f"🔗 Raw product href: {a_href}")
                                    logger.debug(f"🌐 Base domain URL: {base_domain_url}")
                                    
                                    # 이미지 URL 정규화 (normalize_kogift_image_url 함수 사용)
                                    final_img_url, valid_img_url = normalize_kogift_image_url(img_src, base_domain_url)
                                    if not valid_img_url:
                                        logger.warning(f"⚠️ Invalid or unnormalizable image URL skipped: {img_src}")
                                    
                                    # 상품 URL 처리
                                    if a_href:
                                        if a_href.startswith('http'):
                                            # 이미 완전한 URL
                                            final_href_url = a_href
                                        elif a_href.startswith('./'):
                                            # 상대 경로
                                            processed_href = '/' + a_href[2:]  # './' 제거
                                            final_href_url = urljoin(base_domain_url, processed_href)
                                        elif a_href.startswith('/'):
                                            # 절대 경로
                                            final_href_url = urljoin(base_domain_url, a_href)
                                        else:
                                            # 기타 상대 경로
                                            final_href_url = urljoin(base_domain_url, '/' + a_href)
                                    else:
                                        final_href_url = ""

                                    # Check if we already processed this product URL to avoid duplicates
                                    if final_href_url and final_href_url in seen_product_urls:
                                        logger.debug(f"Skipping duplicate product URL: {final_href_url}")
                                        continue

                                    # Add to seen URLs only if it's valid
                                    if final_href_url:
                                        seen_product_urls.add(final_href_url)

                                    # 도메인에서 공급사 정보 추출
                                    supplier = urlparse(base_url).netloc.split('.')[0]
                                    if supplier == 'koreagift':
                                        supplier = '고려기프트'
                                    elif supplier == 'adpanchok':
                                        supplier = '애드판촉'
                                    
                                    # 가격 정보 처리
                                    price_cleaned = re.sub(r'[^\d.]', '', price_text) if price_text else ""
                                    try:
                                        price_value = float(price_cleaned) if price_cleaned else 0.0
                                    except ValueError:
                                        price_value = 0.0
                                    
                                    # 유효한 이미지 URL만 저장
                                    if valid_img_url:
                                        item_data['image_path'] = final_img_url
                                        item_data['image_url'] = final_img_url
                                        item_data['src'] = final_img_url
                                    else:
                                        # 경고는 normalize_kogift_image_url 내부 또는 위에서 이미 발생
                                        item_data['image_path'] = None
                                        item_data['image_url'] = None
                                        item_data['src'] = None
                                    
                                    item_data['href'] = final_href_url
                                    item_data['link'] = final_href_url  # 매칭 로직 호환성
                                    item_data['name'] = name.strip() if name else ""
                                    item_data['price'] = price_value
                                    item_data['supplier'] = supplier  # 공급사 정보 추가
                                    # Add which keyword found this item
                                    item_data['search_keyword'] = keyword
                                    
                                    logger.debug(f"📦 Extracted item: {item_data}")

                                    items_on_page.append(item_data)
                                    processed_items += 1
                                except (PlaywrightError, Exception) as e:
                                    logger.warning(f"⚠️ Could not extract data for item index {i} on page {page_number}: {e}")
                                    continue # Skip this item
                            
                            data.extend(items_on_page)
                            logger.debug(f"📊 Scraped {len(items_on_page)} items from page {page_number}. Total processed: {processed_items}")

                            if processed_items >= max_items_per_variation:
                                logger.info(f"✅ Reached scrape limit ({max_items_per_variation}) for keyword '{keyword}'.")
                                break

                            # --- Pagination --- 
                            next_page_locator_str = f'div.custom_paging > div[onclick*="getPageGo1({page_number + 1})"]' # CSS selector
                            next_page_locator = page.locator(next_page_locator_str)
                            
                            try:
                                 if await next_page_locator.is_visible(timeout=5000):
                                     logger.debug(f"📄 Clicking next page ({page_number + 1})")
                                     # Click and wait for navigation/load state
                                     await next_page_locator.click(timeout=PAGE_TIMEOUT)
                                     # Wait for content to likely reload after click
                                     await page.wait_for_load_state('domcontentloaded', timeout=NAVIGATION_TIMEOUT) 
                                     page_number += 1
                                 else:
                                     logger.info("⚠️ Next page element not found or not visible. Ending pagination.")
                                     break 
                            except (PlaywrightError, Exception) as e:
                                 logger.warning(f"⚠️ Failed to click or load next page ({page_number + 1}): {e}")
                                 break
                        
                        # Add scraped data to results if we found anything
                        if data:
                            logger.info(f"✅ Successfully scraped {len(data)} items for keyword '{keyword}' from {base_url}")
                            df = pd.DataFrame(data)
                            all_results.append(df)
                        else:
                            logger.warning(f"⚠️ No data could be scraped for keyword '{keyword}' from {base_url}")

                    except PlaywrightError as pe:
                        logger.warning(f"Failed to search with keyword '{keyword}' on {base_url}: {pe}")
                        continue  # Try next keyword
                    except Exception as e:
                        logger.warning(f"Error scraping with keyword '{keyword}' from {base_url}: {e}")
                        continue  # Try next keyword

                except Exception as e:
                    logger.error(f"Error during Kogift scraping with keyword '{keyword}' from {base_url}: {e}")
                    continue  # Try next keyword

            # Close context and page cleanly after processing this URL
            if page:
                try:
                    await page.close()
                except Exception as page_close_err:
                    logger.warning(f"Error closing page for {base_url}: {page_close_err}")
            if context:
                try:
                    await context.close()
                except Exception as context_close_err:
                    logger.warning(f"Error closing context for {base_url}: {context_close_err}")
        
        except PlaywrightError as pe:
            # Catch TargetClosedError specifically if it occurs despite the check
            if "Target page, context or browser has been closed" in str(pe):
                 logger.error(f"🔴 TargetClosedError caught while creating/using context for {base_url}. Browser likely closed prematurely. Skipping.")
            else:
                 logger.error(f"🔴 Playwright Error processing URL {base_url}: {pe}")
            # Ensure context is closed if it was partially created
            if context:
                try: await context.close()
                except: pass
            continue # Skip to the next URL
        except Exception as url_proc_err:
            logger.error(f"🔴 Unexpected Error processing URL {base_url}: {url_proc_err}", exc_info=True)
            # Ensure context is closed if it was partially created
            if context:
                try: await context.close()
                except: pass
            continue # Skip to the next URL

    # --- 검색 및 기본 상품 정보 수집 후 ---
    
    # Combine all results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        # If we have duplicates based on URL (from different keyword variations), keep only the first occurrence
        if 'href' in final_df.columns:
            final_df = final_df.drop_duplicates(subset=['href'], keep='first')
        
        # --- 상세 페이지에서 수량별 가격 정보 가져오기 ---
        if custom_quantities and len(final_df) > 0:
            logger.info(f"지정된 수량 {custom_quantities}에 대한 가격 정보 수집 시작")
            
            # 새로운 컨텍스트 생성
            try:
                browser_context = await browser.new_context(
                    user_agent=config.get('Network', 'user_agent', fallback='Mozilla/5.0 ...'),
                    viewport={'width': 1920, 'height': 1080}
                )
                detail_page = await browser_context.new_page()
                detail_page.set_default_timeout(PAGE_TIMEOUT)
                
                # 결과를 저장할 새로운 컬럼 생성
                for qty in custom_quantities:
                    final_df[f'price_{qty}'] = None
                    final_df[f'price_{qty}_with_vat'] = None
                
                # 각 상품별로 상세 페이지 방문하여 수량별 가격 확인
                for idx, row in final_df.iterrows():
                    product_url = row['href']
                    if not product_url:
                        continue
                    
                    logger.info(f"상품 '{row['name'][:30]}...' 수량별 가격 정보 수집 중")
                    
                    for qty in custom_quantities:
                        try:
                            price_result = await get_price_for_specific_quantity(
                                detail_page, product_url, qty
                            )
                            
                            if price_result['success']:
                                # 결과 저장
                                final_df.at[idx, f'price_{qty}'] = price_result['price']
                                final_df.at[idx, f'price_{qty}_with_vat'] = price_result['price_with_vat']
                                logger.info(f"수량 {qty}개: {price_result['price']:,}원 (부가세포함: {price_result['price_with_vat']:,}원)")
                            else:
                                logger.warning(f"수량 {qty}개에 대한 가격 정보 수집 실패")
                            
                        except Exception as e:
                            logger.error(f"수량 {qty}개 가격 조회 중 오류: {e}")
                    
                    # 브라우저 부하 방지를 위한 짧은 대기
                    await detail_page.wait_for_timeout(1000)
                
                # 브라우저 컨텍스트 정리
                await detail_page.close()
                await browser_context.close()
                
                logger.info("수량별 가격 정보 수집 완료")
            except Exception as e:
                logger.error(f"수량별 가격 정보 수집 중 오류 발생: {e}")
                # Context cleanup in case of error
                try:
                    if 'detail_page' in locals() and detail_page: 
                        await detail_page.close()
                    if 'browser_context' in locals() and browser_context:
                        await browser_context.close()
                except:
                    pass
        
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
    
    args = parser.parse_args()
    if not args.quantity:
        args.quantity = [30,50,100]

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
            "https://koreagift.com/ez/upload/mall/shop_1707873892937710_0.jpg",
            "https://koreagift.com/upload/mall/shop_1736386408518966_0.jpg",
            "https://adpanchok.co.kr/upload/mall/shop_1234567890_0.jpg"
        ]
        print(f"Testing direct image download for {len(test_urls)} URLs")
        save_dir = os.path.join(config.get('Paths','image_target_dir',fallback='downloaded_images'), 'kogift_test_images')
        os.makedirs(save_dir, exist_ok=True)
        for url in test_urls:
            norm_url, valid = normalize_kogift_image_url(url)
            if not valid:
                logger.warning(f"Invalid URL skipped: {url}")
                continue
            print(f"Downloading {norm_url}")
            path = download_image(norm_url, save_dir)
            print(f" -> {path}")

    # 2) Product info test (requires browser)
    async def test_product_info(browser):
        logger.info("=== TESTING PRODUCT INFORMATION RETRIEVAL ===")
        # Use test keywords instead of quantities for product search
        test_keywords = ["볼펜", "수첩", "달력"]  # Default test keywords
        if args.quantity:  # If quantities provided, use first few as test keywords
            test_keywords = [str(qty) for qty in args.quantity[:3]]
        
        for keyword in test_keywords:
            logger.info(f"Searching for '{keyword}'...")
            df = await scrape_data(browser, keyword, config=config)
            print(f"Keyword '{keyword}': found {len(df)} items")

    # 3) Custom quantities pricing test (requires browser)
    async def test_custom_quantities(browser):
        logger.info("=== TESTING CUSTOM QUANTITIES FUNCTIONALITY ===")
        for qty in args.quantity:
            logger.info(f"Testing quantity {qty}...")
            # Use a generic search term that's likely to find products
            df = await scrape_data(browser, "판촉물", config=config, custom_quantities=[qty])
            col = f'price_{qty}_with_vat'
            if not df.empty and col in df.columns:
                print(f"Qty {qty}: {df.iloc[0][col]}")
            else:
                print(f"Qty {qty}: no data")

    # 4) Standard test dispatcher
    async def run_standard_tests():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=config.getboolean('Playwright','playwright_headless',fallback=False))
            if args.test_type in ['all','images']:
                test_image_download()
            if args.test_type in ['all','products']:
                await test_product_info(browser)
            if args.test_type in ['all','quantities']:
                await test_custom_quantities(browser)
            await browser.close()

    # dispatch
    print(f"Test mode: {args.test_type}")
    if args.test_type == 'test2':
        # TODO: Implement run_test2 or remove this branch if not needed
        # import asyncio; asyncio.run(run_test2())
        logger.warning("Test type 'test2' selected but run_test2 is not implemented.")
    else:
        import asyncio; asyncio.run(run_standard_tests())

if __name__ == "__main__":
    # If this file is run directly, run the test
    if os.path.basename(__file__) == "crowling_kogift.py":
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