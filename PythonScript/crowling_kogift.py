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

# Add semaphore for concurrent task limiting
MAX_CONCURRENT_TASKS = 5
scraping_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

# --- Helper function to download images ---
def download_image(img_url, save_dir='downloaded_images', filename=None):
    """
    Download an image from URL and save it locally.
    
    Args:
        img_url: URL of the image to download
        save_dir: Directory to save the image
        filename: Optional filename; if None, will be derived from URL
        
    Returns:
        str: Path to the saved image or None if download failed
    """
    if not img_url:
        return None
        
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)
    
    try:
        # Extract filename from URL if not provided
        if not filename:
            parsed_url = urlparse(img_url)
            filename = os.path.basename(parsed_url.path)
            
            # Ensure filename is valid
            if not filename or filename == '':
                # Generate random filename if URL doesn't have a valid one
                filename = f"image_{int(time.time())}_{random.randint(1000, 9999)}.jpg"
                
            # Ensure kogift images always have jpg extension
            is_kogift = "kogift" in img_url.lower() or "koreagift" in img_url.lower() or "adpanchok" in img_url.lower()
            if is_kogift or not filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                # Get base name without extension
                base_name = os.path.splitext(filename)[0]
                # Force jpg extension for kogift images
                filename = f"{base_name}.jpg"
        
        # Full path to save the image
        save_path = os.path.join(save_dir, filename)
        
        # Check if file already exists
        if os.path.exists(save_path):
            logger.debug(f"Image already exists at {save_path}")
            return save_path
            
        # Download image
        response = requests.get(img_url, stream=True, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Check if response contains image data - more permissive check
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"Content-Type for {img_url}: {content_type}")
        
        # We're being more permissive with content types since Kogift sometimes returns 
        # incorrect content types for images
        if content_type and 'text/html' in content_type and len(response.content) < 1000:
            logger.warning(f"URL likely returns HTML error page instead of image: {img_url}")
            return None
        
        # Save image
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Image downloaded successfully: {save_path}")
        return save_path
        
    except Exception as e:
        logger.error(f"Failed to download image from {img_url}: {e}")
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
    고려기프트 이미지 URL을 표준화하고 유효성을 검사합니다.
    
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
    
    # 이미 완전한 URL인 경우
    if img_url.startswith(('http://', 'https://')):
        parsed_url = urlparse(img_url)
        domain = parsed_url.netloc
        path = parsed_url.path
        
        # koreagift.com 도메인인 경우 항상 /ez/ 경로가 있는지 확인
        if 'koreagift.com' in domain:
            # 이미 /ez/가 있는 경우 그대로 사용
            if '/ez/' in path:
                return img_url, True
            # /upload/로 시작하는 경로에 /ez/ 추가
            elif path.startswith('/upload/'):
                new_path = '/ez' + path
                return f"{parsed_url.scheme}://{domain}{new_path}", True
            # 그 외 경로는 그대로 사용
            else:
                return img_url, True
        
        # 유효한 도메인 확인
        kogift_domains = ['kogift.com', 'www.kogift.com', 'img.kogift.com', 'adpanchok.co.kr', 'www.adpanchok.co.kr']
        if any(kogift_domain in domain for kogift_domain in kogift_domains):
            return img_url, True
        else:
            # 다른 도메인이면 기존 URL 그대로 반환하되 유효하지 않음 표시
            return img_url, False
    
    # '//' 시작하는 프로토콜-상대 URL 처리
    if img_url.startswith('//'):
        return f"https:{img_url}", True
    
    # './웹 경로' 상대 경로 처리
    if img_url.startswith('./'):
        img_url = img_url[2:]  # './' 제거
    
    # 절대 경로('/upload/'로 시작)인 경우
    if img_url.startswith('/upload/'):
        # koreagift.com 도메인에 대해서는 항상 /ez/ 경로 추가
        if 'koreagift.com' in base_url:
            img_url = '/ez' + img_url
    # 기타 절대 경로
    elif img_url.startswith('/'):
        # 그대로 사용
        pass
    # 상대 경로(파일명 또는 하위 경로)
    else:
        # 경로가 'upload/'로 시작하면 앞에 '/'를 추가
        if img_url.startswith('upload/'):
            # koreagift.com 도메인에 대해서는 항상 /ez/ 경로 추가
            if 'koreagift.com' in base_url:
                img_url = '/ez/' + img_url
            else:
                img_url = '/' + img_url
        # 기타 경로는 그대로 /를 붙여서 사용
        else:
            img_url = '/' + img_url
    
    # 최종 URL 생성
    final_url = urljoin(base_url, img_url)
    
    # 중복 경로 확인 및 수정
    if '/ez/ez/' in final_url:
        final_url = final_url.replace('/ez/ez/', '/ez/')
    
    return final_url, True

async def verify_kogift_images(product_list: List[Dict], sample_percent: int = 10) -> List[Dict]:
    """
    고려기프트 상품 목록의 이미지 URL을 검증하고 표준화한 후, 이미지를 다운로드합니다.
    
    Args:
        product_list: 상품 목록 (각 항목은 'image' 또는 'image_url' 키를 포함해야 함)
        sample_percent: 전체 URL 중 실제로 검증할 비율 (%)
        
    Returns:
        List[Dict]: 이미지 URL이 표준화되고 로컬 이미지 경로가 추가된 상품 목록
    """
    if not product_list:
        return []
    
    # 설정에서 검증 여부 확인
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    
    verify_enabled = config.getboolean('Matching', 'verify_image_urls', fallback=True)
    download_enabled = config.getboolean('Matching', 'download_images', fallback=True)
    images_dir = config.get('Matching', 'images_dir', fallback='downloaded_images')
    
    logger.info(f"고려기프트 상품 {len(product_list)}개의 이미지 처리 시작")
    
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

# --- Main scraping function에 상세 페이지 크롤링 로직 추가 --- 
async def scrape_data(browser: Browser, original_keyword1: str, original_keyword2: Optional[str] = None, config: configparser.ConfigParser = None, fetch_price_tables: bool = False):
    """Scrape product data from Koreagift using a shared Browser instance.
    
    Args:
        browser: An active Playwright Browser instance.
        original_keyword1: The primary keyword to search for.
        original_keyword2: An optional secondary keyword for re-search if results >= 100.
        config: ConfigParser object containing configuration settings.
        fetch_price_tables: 상품 상세 페이지에서 수량-단가 정보도 함께 가져올지 여부

    Returns:
        A pandas DataFrame containing the best found results, or an empty DataFrame.
    """
    async with scraping_semaphore:  # Acquire semaphore before starting
        if config is None:
            logger.error("🔴 Configuration object (ConfigParser) is missing for Kogift scrape.")
            return pd.DataFrame() # Return empty dataframe on critical config error
        
        # Get settings from config with defaults using ConfigParser methods
        try:
            kogift_urls_str = config.get('ScraperSettings', 'kogift_urls', 
                                       fallback='https://koreagift.com/ez/index.php,https://adpanchok.co.kr/ez/index.php')
            kogift_urls = [url.strip() for url in kogift_urls_str.split(',') if url.strip()]
            if not kogift_urls:
                 logger.error("🔴 Kogift URLs are missing or invalid in [ScraperSettings] config.")
                 return pd.DataFrame()
            
            user_agent = config.get('ScraperSettings', 'user_agent', 
                                  fallback='Mozilla/5.0 ...') # Use actual default from utils/DEFAULT_CONFIG if desired
            min_results_threshold = config.getint('ScraperSettings', 'kogift_min_results_threshold', fallback=5)
            max_items_to_scrape = config.getint('ScraperSettings', 'kogift_max_items', fallback=200)
            max_pages_to_scrape = config.getint('ScraperSettings', 'kogift_max_pages', fallback=10)
            
            default_timeout = config.getint('Playwright', 'playwright_default_timeout_ms', fallback=120000)  # 2분
            navigation_timeout = config.getint('Playwright', 'playwright_navigation_timeout_ms', fallback=120000)  # 2분
            action_timeout = config.getint('Playwright', 'playwright_action_timeout_ms', fallback=30000)  # 30초
            # Add a shorter timeout specifically for waiting for search results/no results
            search_results_wait_timeout = config.getint('Playwright', 'playwright_search_results_timeout_ms', fallback=60000)  # 1분
            block_resources = config.getboolean('Playwright', 'playwright_block_resources', fallback=True)
            
            # Image download settings
            download_images = config.getboolean('Matching', 'download_images', fallback=True)
            images_dir = config.get('Matching', 'images_dir', fallback='downloaded_images')
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
            logger.error(f"🔴 Error reading Kogift/Playwright config: {e}. Using hardcoded defaults where possible.")
            # Set critical defaults again or decide to return empty
            kogift_urls = ["https://koreagift.com/ez/index.php", "https://adpanchok.co.kr/ez/index.php"]
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
            min_results_threshold = 5
            max_items_to_scrape = 200
            max_pages_to_scrape = 10
            default_timeout = 120000
            navigation_timeout = 120000
            action_timeout = 30000
            search_results_wait_timeout = 60000
            block_resources = True
            download_images = True
            images_dir = 'downloaded_images'

        keywords_to_try = generate_keyword_variations(original_keyword1)
        best_result_df = pd.DataFrame() 

        logger.info(f"🔍 Generated keywords for '{original_keyword1}': {keywords_to_try}")

        # Shared browser context for this scrape attempt
        context = None
        page = None
        try:
            context = await browser.new_context(user_agent=user_agent)
            page = await context.new_page()
            page.set_default_timeout(default_timeout)
            page.set_default_navigation_timeout(navigation_timeout)
            
            if block_resources:
                await setup_page_optimizations(page)

            for keyword in keywords_to_try:
                logger.info(f"🔍 Trying keyword variation: '{keyword}' --- ({keywords_to_try.index(keyword) + 1}/{len(keywords_to_try)}) ---")
                current_keyword_best_df = pd.DataFrame()
                keyword_found_sufficient = False

                for base_url in kogift_urls:
                    logger.info(f"🌐 Attempting scrape: URL='{base_url}', Keyword='{keyword}'")
                    data = []
                    page_instance = page
                    
                    try:
                        # Navigate to the base URL
                        await page_instance.goto(base_url, wait_until='domcontentloaded')
                        logger.debug(f"🌐 Navigated to {base_url}")

                        # --- Perform Search --- 
                        search_input_locator = page_instance.locator('input#main_keyword[name="keyword"]') # More specific selector
                        search_button_locator = page_instance.locator('img#search_submit')
                        
                        await search_input_locator.wait_for(state="visible", timeout=action_timeout)
                        await search_input_locator.fill(keyword)
                        await search_button_locator.wait_for(state="visible", timeout=action_timeout)
                        
                        results_container_selector = 'div.product_lists' # Selector for the container holding results
                        # Refined selector for "no results" message based on provided HTML
                        no_results_selector = 'div.not_result span.icon_dot2:has-text("검색 결과가 없습니다")' 
                        combined_selector = f"{results_container_selector}, {no_results_selector}"
                        
                        logger.debug("🔍 Clicking search...")
                        await search_button_locator.click()
                        logger.info(f"🔍 Search submitted for: '{keyword}' on {base_url}")

                        # --- Wait for results OR "no results" message --- 
                        logger.debug(f"⏳ Waiting for search results or 'no results' message (timeout: {search_results_wait_timeout}ms)...")
                        try:
                            found_element = await page_instance.wait_for_selector(
                                combined_selector, 
                                state='visible', 
                                timeout=search_results_wait_timeout
                            )
                            
                            # Check if the 'no results' text is visible
                            no_results_element = page_instance.locator(no_results_selector)
                            if await no_results_element.is_visible():
                                no_results_text = await no_results_element.first.text_content(timeout=1000) or "[No text found]"
                                logger.info(f"⚠️ 'No results' message found for keyword '{keyword}' on {base_url}. Text: {no_results_text.strip()}")
                                continue # Skip to the next URL/keyword
                            else:
                                logger.debug("✅ Results container found. Proceeding to scrape.")
                                # Results container is visible, fall through to scraping logic
                                pass 
                                
                        except PlaywrightError as wait_error:
                            logger.warning(f"⚠️ Timeout or error waiting for results/no_results for keyword '{keyword}' on {base_url}: {wait_error}")
                            continue # Skip to the next URL/keyword

                        # --- Check product count (Optional Re-search) --- 
                        # This section remains largely the same, but runs ONLY if results were found
                        productCont = 0
                        try:
                            product_count_element = page_instance.locator('div.list_info span').first # Simpler selector
                            productContText = await product_count_element.text_content(timeout=5000) 
                            productContDigits = re.findall(r'\d+', productContText.replace(',', ''))
                            if productContDigits:
                                productCont = int("".join(productContDigits))
                            logger.info(f"📊 Reported product count: {productCont}")
                        except (PlaywrightError, Exception) as e:
                            logger.warning(f"⚠️ Could not find/parse product count: {e}")

                        # Re-search logic (only if initial search had results)
                        if original_keyword2 and original_keyword2.strip() != "" and productCont >= 100:
                            logger.info(f"🔍 Initial count >= 100. Performing re-search with: '{original_keyword2}'")
                            try:
                                re_search_input = page_instance.locator('input#re_keyword')
                                re_search_button = page_instance.locator('button[onclick^="re_search"]')
                                await re_search_input.fill(original_keyword2)
                                
                                logger.debug("🔍 Clicking re-search...")
                                await re_search_button.click()
                                
                                # Wait again after re-search, checking for no results again
                                logger.debug(f"⏳ Waiting after re-search (timeout: {search_results_wait_timeout}ms)...")
                                try:
                                    await page_instance.wait_for_selector(
                                        combined_selector, 
                                        state='visible', 
                                        timeout=search_results_wait_timeout
                                    )
                                    if await page_instance.locator(no_results_selector).is_visible():
                                         logger.info(f"⚠️ 'No results' found after re-searching with '{original_keyword2}'.")
                                         # Decide whether to break or continue based on re-search logic
                                         # For now, let's assume re-search failure means stop for this URL
                                         continue # Skip to next URL
                                    else:
                                         logger.info(f"✅ Re-search completed for: '{original_keyword2}'. Proceeding with scraping new results.")
                                         # Reset page number and counts for scraping re-search results
                                         page_number = 1
                                         processed_items = 0
                                         data = [] # Clear previous data if re-search successful
                                except PlaywrightError as re_wait_error:
                                    logger.warning(f"⚠️ Timeout/error waiting for results after re-search with '{original_keyword2}': {re_wait_error}")
                                    continue # Skip to next URL
                                    
                            except (PlaywrightError, Exception) as e:
                                logger.warning(f"⚠️ Failed during re-search attempt: {e}")
                                # Continue with initial results if re-search fails here.

                        # --- Scrape Results Pages --- 
                        page_number = 1
                        processed_items = 0
                        product_item_selector = 'div.product' # Selector for individual product blocks

                        while processed_items < max_items_to_scrape and page_number <= max_pages_to_scrape:
                            logger.info(f"📄 Scraping page {page_number} (Keyword: '{keyword}', URL: {base_url})... Items processed: {processed_items}")
                            try:
                                 # Wait for at least one product item to be potentially visible
                                 await page_instance.locator(product_item_selector).first.wait_for(state="attached", timeout=action_timeout)
                            except PlaywrightError:
                                 logger.warning(f"⚠️ Product items selector ('{product_item_selector}') not found/attached on page {page_number}. Stopping scrape for this URL/Keyword.")
                                 break
                                 
                            rows = page_instance.locator(product_item_selector)
                            count = await rows.count()
                            logger.debug(f"📊 Found {count} product elements on page {page_number}.")

                            if count == 0 and page_number > 1: # Allow page 1 to have 0 if count check failed earlier
                                 logger.info(f"⚠️ No product elements found on page {page_number}. Stopping pagination.")
                                 break
                            elif count == 0 and page_number == 1:
                                 logger.info(f"⚠️ No product elements found on first page (page {page_number}). Stopping scrape for this URL/Keyword.")
                                 break

                            items_on_page = []
                            for i in range(count):
                                if processed_items >= max_items_to_scrape:
                                    break
                                row = rows.nth(i)
                                item_data = {}
                                try:
                                    # Extract data using locators with short timeouts
                                    img_locator = row.locator('div.pic > a > img')
                                    img_src = await img_locator.get_attribute('src', timeout=action_timeout)
                                    
                                    link_locator = row.locator('div.pic > a')
                                    a_href = await link_locator.get_attribute('href', timeout=action_timeout)
                                    
                                    name_locator = row.locator('div.name > a')
                                    name = await name_locator.text_content(timeout=action_timeout)
                                    
                                    price_locator = row.locator('div.price')
                                    price_text = await price_locator.text_content(timeout=action_timeout)

                                    # Process extracted data
                                    base_domain_url = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}"
                                    
                                    # 디버깅: 원본 URL 및 변환 과정 로깅
                                    logger.debug(f"🔗 Raw image src: {img_src}")
                                    logger.debug(f"🔗 Raw product href: {a_href}")
                                    logger.debug(f"🌐 Base domain URL: {base_domain_url}")
                                    
                                    # 이미지 URL 처리
                                    if img_src:
                                        # 이미지 소스 처리
                                        if img_src.startswith('http'):
                                            # 이미 완전한 URL인 경우
                                            processed_img_src = img_src
                                        elif img_src.startswith('./'):
                                            # './로 시작하는 상대 경로를 /ez/로 변환 (koreagift.com)
                                            if 'koreagift.com' in base_domain_url:
                                                processed_img_src = '/ez/' + img_src[2:]  # './' 제거하고 /ez/ 추가
                                            else:
                                                processed_img_src = '/' + img_src[2:]  # './' 제거
                                        elif img_src.startswith('/upload/'):
                                            # /upload/로 시작하는 경로에 /ez/ 추가 (koreagift.com)
                                            if 'koreagift.com' in base_domain_url:
                                                processed_img_src = '/ez' + img_src
                                            else:
                                                processed_img_src = img_src
                                        elif img_src.startswith('/'):
                                            # 다른 절대 경로는 그대로 사용
                                            processed_img_src = img_src
                                        else:
                                            # 상대 경로는 적절히 처리
                                            if 'koreagift.com' in base_domain_url and img_src.startswith('upload/'):
                                                processed_img_src = f"/ez/{img_src}"
                                            else:
                                                processed_img_src = f"/{img_src}"
                                        
                                        # /ez/ez/ 중복 수정
                                        if '/ez/ez/' in processed_img_src:
                                            processed_img_src = processed_img_src.replace('/ez/ez/', '/ez/')
                                            
                                        # 최종 URL 생성
                                        final_img_url = urljoin(base_domain_url, processed_img_src)
                                        
                                        # 이미지 URL 검증 - 기본 구조만 확인
                                        valid_img_url = False
                                        if final_img_url and final_img_url.startswith('http'):
                                            url_parts = urlparse(final_img_url)
                                            if url_parts.netloc and url_parts.path:
                                                valid_img_url = True
                                    else:
                                        final_img_url = ""
                                        valid_img_url = False
                                    
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

                                    # 도메인에서 공급사 정보 추출
                                    supplier = urlparse(base_url).netloc.split('.')[0]
                                    if supplier == 'koreagift':
                                        supplier = '고려기프트'
                                    elif supplier == 'adpanchok':
                                        supplier = '애드판촉'
                                    
                                    # 유효한 이미지 URL만 저장
                                    if valid_img_url:
                                        item_data['image_path'] = final_img_url
                                    else:
                                        item_data['image_path'] = None
                                        logger.warning(f"⚠️ 유효하지 않은 이미지 URL 무시: {final_img_url}")
                                    
                                    item_data['src'] = final_img_url  # 이전 호환성 유지
                                    item_data['href'] = final_href_url
                                    item_data['link'] = final_href_url  # 매칭 로직 호환성
                                    item_data['name'] = name.strip() if name else ""
                                    price_cleaned = re.sub(r'[^\d.]', '', price_text) if price_text else ""
                                    item_data['price'] = float(price_cleaned) if price_cleaned else 0.0
                                    item_data['supplier'] = supplier  # 공급사 정보 추가
                                    
                                    logger.debug(f"📦 Extracted item: {item_data}")

                                    items_on_page.append(item_data)
                                    processed_items += 1
                                except (PlaywrightError, Exception) as e:
                                    logger.warning(f"⚠️ Could not extract data for item index {i} on page {page_number}: {e}")
                                    continue # Skip this item
                            
                            data.extend(items_on_page)
                            logger.debug(f"📊 Scraped {len(items_on_page)} items from page {page_number}. Total processed: {processed_items}")

                            if processed_items >= max_items_to_scrape:
                                logger.info(f"✅ Reached scrape limit ({max_items_to_scrape}) for keyword '{keyword}'.")
                                break

                            # --- Pagination --- 
                            next_page_locator_str = f'div.custom_paging > div[onclick*="getPageGo1({page_number + 1})"]' # CSS selector
                            next_page_locator = page_instance.locator(next_page_locator_str)
                            
                            try:
                                 if await next_page_locator.is_visible(timeout=5000):
                                     logger.debug(f"📄 Clicking next page ({page_number + 1})")
                                     # Click and wait for navigation/load state
                                     await next_page_locator.click(timeout=action_timeout)
                                     # Wait for content to likely reload after click
                                     await page_instance.wait_for_load_state('domcontentloaded', timeout=navigation_timeout) 
                                     page_number += 1
                                 else:
                                     logger.info("⚠️ Next page element not found or not visible. Ending pagination.")
                                     break 
                            except (PlaywrightError, Exception) as e:
                                 logger.warning(f"⚠️ Failed to click or load next page ({page_number + 1}): {e}")
                                 break 

                    except PlaywrightError as pe:
                        logger.error(f"❌ Playwright error during setup/search for URL '{base_url}', Keyword '{keyword}': {pe}")
                    except Exception as e:
                        logger.error(f"❌ Unexpected error during scrape setup/search for URL '{base_url}', Keyword '{keyword}': {e}", exc_info=True)
                    # Loop continues to next URL or keyword if error occurred before scraping loop

                    # --- End of single URL/Keyword attempt --- 
                    logger.info(f"✅ Scraping attempt finished for URL='{base_url}', Keyword='{keyword}'. Found {len(data)} items.")
                    current_attempt_df = pd.DataFrame(data)

                    # Keep track of the best result for the current keyword across URLs
                    if len(current_attempt_df) > len(current_keyword_best_df):
                        current_keyword_best_df = current_attempt_df
                        logger.debug(f"📊 Updating best result for keyword '{keyword}' with {len(current_keyword_best_df)} items from {base_url}.")
                    
                    # If this URL attempt yielded enough results, use it and maybe stop checking other URLs for this keyword
                    if len(current_attempt_df) >= min_results_threshold:
                         logger.info(f"✅ Found sufficient results ({len(current_attempt_df)}) with keyword '{keyword}' from {base_url}. Using this result.")
                         # current_keyword_best_df = current_attempt_df # Already assigned if it's the best
                         # keyword_found_sufficient = True # Optional: break inner URL loop if one URL is enough
                         # break # Uncomment to stop checking other URLs for this keyword once one works well

                # --- End of URL loop for the current keyword --- 
                # Update overall best result if current keyword's best is better
                if len(current_keyword_best_df) > len(best_result_df):
                    best_result_df = current_keyword_best_df
                    logger.debug(f"📊 Updating overall best result with {len(best_result_df)} items from keyword '{keyword}'.")

                # Check if the best result found *for this keyword* is sufficient to stop trying other keywords
                if len(current_keyword_best_df) >= min_results_threshold:
                    logger.info(f"✅ Found sufficient results ({len(current_keyword_best_df)}) with keyword '{keyword}'. Stopping keyword variations.")
                    # Instead of returning early, break the keyword loop to ensure cleanup runs
                    break # Stop trying further keywords

        except Exception as e:
            logger.error(f"❌ Major error during Kogift scrape execution for '{original_keyword1}': {e}", exc_info=True)
            best_result_df = pd.DataFrame() # Ensure empty DataFrame on major error
        finally:
            # Ensure page and context are closed if they were created
            if page:
                try: 
                    await page.close()
                    logger.debug("✅ Closed Playwright page.")
                except Exception as page_close_err:
                    logger.warning(f"⚠️ Error closing page: {page_close_err}")
            if context:
                try:
                    await context.close()
                    logger.debug("✅ Closed Playwright context.")
                except Exception as context_close_err:
                    logger.warning(f"⚠️ Error closing context: {context_close_err}")

        # Final log based on results
        if len(best_result_df) < min_results_threshold:
            logger.warning(f"⚠️ Could not find sufficient results ({min_results_threshold} needed) for '{original_keyword1}' after trying variations. Max found: {len(best_result_df)} items.")
        else:
            logger.info(f"✅ KoGift scraping finished for '{original_keyword1}'. Final result count: {len(best_result_df)} items.")

        # Map DataFrame columns before returning
        if not best_result_df.empty:
            try:
                # Define final column mapping
                column_mapping = {
                    'name': 'name', 
                    'price': 'price',
                    'href': 'link', 
                    'src': 'image_url',
                    'supplier': 'supplier'  # 공급사 컬럼 추가
                }
                # Select and rename columns that exist in the DataFrame
                rename_map = {k: v for k, v in column_mapping.items() if k in best_result_df.columns}
                best_result_df = best_result_df[list(rename_map.keys())].rename(columns=rename_map)
                
                # Ensure correct dtypes (e.g., price as float)
                if 'price' in best_result_df.columns:
                    best_result_df['price'] = pd.to_numeric(best_result_df['price'], errors='coerce').fillna(0.0)
                    
                # 수량-단가 정보 추출 (옵션)
                if fetch_price_tables and not best_result_df.empty:
                    logger.info(f"상세 페이지에서 수량-단가 정보 추출 시작 (총 {len(best_result_df)}개 상품)")
                    
                    # 상세 페이지 별도 컨텍스트 생성
                    detail_context = await browser.new_context()
                    detail_page = await detail_context.new_page()
                    
                    # 수량-단가 정보를 저장할 사전
                    price_tables = {}
                    
                    # 처음 5개 상품에 대해서만 상세 정보 추출 (시간 절약을 위해)
                    max_details = min(5, len(best_result_df))
                    for i, (idx, row) in enumerate(best_result_df.head(max_details).iterrows()):
                        product_link = row['link']
                        product_name = row['name']
                        
                        logger.info(f"상품 {i+1}/{max_details} 상세 정보 추출 중: {product_name[:30]}...")
                        price_table = await extract_price_table(detail_page, product_link)
                        
                        if price_table is not None and not price_table.empty:
                            price_tables[idx] = price_table
                            logger.info(f"상품 {i+1}/{max_details} 단가표 추출 성공: {len(price_table)}개 행")
                        else:
                            logger.warning(f"상품 {i+1}/{max_details} 단가표 추출 실패")
                    
                    # 페이지 및 컨텍스트 닫기
                    await detail_page.close()
                    await detail_context.close()
                    
                    # 추출된 단가표 정보 로깅
                    logger.info(f"총 {len(price_tables)}/{max_details} 상품에서 단가표 추출 성공")
                    
                    # 결과 DataFrame에 단가표 정보 컬럼 추가
                    best_result_df['price_table'] = pd.Series(price_tables)
                
                # 이미지 URL 정규화 및 다운로드
                logger.info("고려기프트 이미지 처리 시작")
                best_result_df_list = best_result_df.to_dict('records')
                
                # 이미지 URL 정규화 및 다운로드 수행
                best_result_df_list = await verify_kogift_images(best_result_df_list)
                
                # 리스트를 DataFrame으로 변환
                best_result_df = pd.DataFrame(best_result_df_list)
                
                # 이미지 다운로드 경로 확인 (디버깅)
                if 'local_image_path' in best_result_df.columns:
                    downloaded_count = best_result_df['local_image_path'].notnull().sum()
                    logger.info(f"이미지 다운로드 결과: {downloaded_count}/{len(best_result_df)} 파일 저장됨")
                else:
                    logger.warning("이미지 다운로드가 실행되었지만 로컬 경로 컬럼이 없습니다.")
                
            except KeyError as ke:
                logger.error(f"Error mapping columns for Kogift results: Missing key {ke}. Returning raw data.")
            except Exception as map_err:
                logger.error(f"Error during final column mapping for Kogift: {map_err}")

        return best_result_df

# Simple function to test direct image download
def test_kogift_scraper():
    """Test both image download and product information retrieval from Kogift"""
    import asyncio
    import sys
    import os
    import requests
    import logging
    import random
    import time
    import pandas as pd
    import argparse
    from datetime import datetime
    from urllib.parse import urlparse, urljoin
    from playwright.async_api import async_playwright
    from utils import load_config
    
    # Setup command-line arguments
    parser = argparse.ArgumentParser(description='Test Kogift scraper functionality')
    parser.add_argument('--test-type', choices=['all', 'images', 'products'], default='all',
                        help='Specify which tests to run (all, images, or products)')
    parser.add_argument('--keywords', nargs='+', default=["777", "쓰리쎄븐", "손톱깎이"],
                        help='Keywords to use for product search testing')
    parser.add_argument('--max-items', type=int, default=5,
                        help='Maximum number of items to fetch per keyword')
    
    # Parse command-line arguments
    if len(sys.argv) > 1 and sys.argv[1] == '--test-run':
        # Remove the --test-run argument that was added automatically when running the script
        sys.argv.pop(1)
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("kogift_test")
    
    # Test image download functionality
    def test_image_download():
        logger.info("=== TESTING IMAGE DOWNLOAD FUNCTIONALITY ===")
        
        # Example image URLs to test
        test_urls = [
            "https://koreagift.com/ez/upload/mall/shop_1707873892937710_0.jpg",
            "https://koreagift.com/upload/mall/shop_1736386408518966_0.jpg",  # Missing /ez/ path
            "https://adpanchok.co.kr/upload/mall/shop_1234567890_0.jpg"
        ]
        
        print(f"Testing direct image download for {len(test_urls)} URLs")
        
        # Create save directory
        save_dir = 'test_images'
        os.makedirs(save_dir, exist_ok=True)
        
        # Normalize URLs first
        normalized_urls = []
        for url in test_urls:
            normalized_url, is_valid = normalize_kogift_image_url(url)
            print(f"Original: {url}")
            print(f"Normalized: {normalized_url} (Valid: {is_valid})")
            if is_valid:
                normalized_urls.append(normalized_url)
        
        # Download images
        if normalized_urls:
            results = {}
            for url in normalized_urls:
                result = download_image(url, save_dir)
                if result:
                    results[url] = result
            
            # Print results
            print(f"\nDownload results: {len(results)}/{len(normalized_urls)} successful")
            for url, path in results.items():
                print(f"URL: {url}")
                print(f"Saved to: {path}")
                print(f"File exists: {os.path.exists(path)}")
                if os.path.exists(path):
                    size_kb = os.path.getsize(path) / 1024
                    print(f"File size: {size_kb:.2f} KB")
                print("-" * 50)
    
    # Test product information retrieval 
    async def test_product_info():
        logger.info("=== TESTING PRODUCT INFORMATION RETRIEVAL ===")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
        config = load_config(config_path)
        if not config.sections():
            logger.error(f"Could not load config from {config_path}")
            return
        
        # Use keywords from command line args
        test_products = args.keywords
        logger.info(f"Using keywords for testing: {test_products}")
        
        # Modify config to limit number of items
        if config.has_section('ScraperSettings'):
            config.set('ScraperSettings', 'kogift_max_items', str(args.max_items))
        else:
            config.add_section('ScraperSettings')
            config.set('ScraperSettings', 'kogift_max_items', str(args.max_items))
            
        # Launch browser
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=config.getboolean('Playwright', 'playwright_headless', fallback=False))
            
            for product in test_products:
                logger.info(f"Starting Kogift test scrape for: {product}")
                # Pass the ConfigParser object to scrape_data
                result = await scrape_data(browser, product, config=config, fetch_price_tables=True)
                
                print(f"\n--- Test Scrape Results for '{product}' ---")
                if not result.empty:
                    print(f"Found {len(result)} results.")
                    print(f"First 5 results:")
                    print(result.head())
                    
                    # Print all URLs in the result
                    print(f"\nAll product URLs found ({len(result)}):")
                    for i, (name, link) in enumerate(zip(result['name'], result['link']), 1):
                        print(f"{i}. {name[:30]}... : {link}")
                    
                    print(f"\nAll image URLs found ({len(result)}):")
                    for i, (name, img) in enumerate(zip(result['name'], result['image_url']), 1):
                        print(f"{i}. {name[:30]}... : {img}")
                    
                    # 다운로드된 이미지 경로 출력
                    if 'local_image_path' in result.columns:
                        print(f"\nDownloaded images ({result['local_image_path'].notnull().sum()}/{len(result)}):")
                        for i, (name, img_path) in enumerate(zip(result['name'], result['local_image_path']), 1):
                            if pd.notnull(img_path):
                                print(f"{i}. {name[:30]}... : {img_path}")
                    
                    # 단가표 정보 출력 (있는 경우)
                    if 'price_table' in result.columns:
                        print(f"\n수량-단가 정보 추출 결과:")
                        price_tables_found = 0
                        for idx, price_table in result['price_table'].items():
                            if isinstance(price_table, pd.DataFrame) and not price_table.empty:
                                price_tables_found += 1
                                product_name = result.loc[idx, 'name']
                                print(f"\n상품: {product_name[:30]}...")
                                print(price_table)
                        
                        print(f"\n총 {price_tables_found}개 상품에서 단가표 정보를 추출했습니다.")
                else:
                    print("No results found.")
                
                print(f"Total results: {len(result)}")
                print("-------------------------\n")
            
            await browser.close()
    
    # Run tests based on command-line arguments
    print(f"Running Kogift scraper tests (mode: {args.test_type})...")
    
    if args.test_type in ['all', 'images']:
        print("\n1. Testing image download functionality")
        test_image_download()
    
    if args.test_type in ['all', 'products']:
        print("\n2. Testing product information retrieval")
        asyncio.run(test_product_info())
    
    print("\nAll Kogift tests completed")

if __name__ == "__main__":
    # If this file is run directly, run the test
    if os.path.basename(__file__) == "crowling_kogift.py":
        # Setup basic logging FOR THE TEST ONLY
        # In production, logging is set up by initialize_environment
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger.info("Running Kogift scraper test...")
        
        # Run the comprehensive test function
        test_kogift_scraper()