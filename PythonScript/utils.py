import os
import logging
import requests
import httpx
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
from PIL import Image
import pandas as pd
import re
from typing import List, Dict, Any, Optional, Set, Tuple, Union
import asyncio
import configparser # Import configparser
import hashlib # Import hashlib
from pathlib import Path # Import Path
import time # Import time
from concurrent.futures import ThreadPoolExecutor # Keep for potential sync tasks
from datetime import datetime
from image_downloader import download_images, predownload_kogift_images

# --- Configuration Loading ---

# Default config structure aligned with INI sections
DEFAULT_CONFIG = {
    'Paths': {
        'input_dir': 'C:\\RPA\\Input',
        'temp_dir': 'C:\\RPA\\Temp',
        'output_dir': 'C:\\RPA\\Output',
        'image_main_dir': 'C:\\RPA\\Image\\Main',
        'image_target_dir': 'C:\\RPA\\Image\\Target',
        'log_file': 'shoprpa_log.txt',
    },
    'Logging': {
        'log_level': 'INFO',
    },
    'API_Keys': {
        'naver_client_id': '',
        'naver_client_secret': '',
    },
    'Matching': {
        'text_threshold': '0.7',
        'image_threshold': '0.6',
        'text_weight': '0.7',
        'image_weight': '0.3',
        'text_model_name': 'jhgan/ko-sroberta-multitask',
        'image_model_name': 'EfficientNetB0',
        'use_background_removal': 'True',
        'process_type': 'A',
    },
    'Concurrency': {
        'max_crawl_workers': '4',
        'max_match_workers': '4',
    },
    'Network': {
        'request_timeout': '15',
        'connect_timeout': '5',
        'read_timeout': '15',
        'max_retries': '5',
        'backoff_factor': '0.3',
        'retry_status_codes_requests': '429, 500, 502, 503, 504',
        'retry_status_codes_httpx': '429, 500, 502, 503, 504',
        'max_connections': '100',
        'max_keepalive_connections': '20',
    },
    'ScraperSettings': {
        'crawl_timeout': '120',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'kogift_scrape_limit': '100',
        'naver_scrape_limit': '50',
        'kogift_urls': 'https://koreagift.com/ez/index.php,https://adpanchok.co.kr/ez/index.php',
        'kogift_min_results_threshold': '5',
        'kogift_max_items': '200',
        'kogift_max_pages': '10',
        'haereum_main_url': 'https://www.jclgift.com/',
        'haereum_image_base_url': 'http://i.jclgift.com/',
        'haereum_selectors_json': '{"search_input": "input#keyword, input[name=\"search_word\"]", "search_button": "input[type=\"image\"][src*=\"b_search.gif\"]", "product_list_item": "td[width=\"160\"][bgcolor=\"ffffff\"]", "product_name_list": "td[align=\"center\"][style*=\"line-height:130%\"] > a", "product_image_list": "td[align=\"center\"] > a > img", "product_list_wrapper": "form[name=\"ListForm\"]"}',
    },
    'Playwright': {
        'playwright_headless': 'True',
        'playwright_task_concurrency': '4',
        'playwright_default_timeout_ms': '60000',
        'playwright_navigation_timeout_ms': '60000',
        'playwright_action_timeout_ms': '10000',
        'playwright_block_resources': 'True',
    }
}

def load_config(config_file_path: str = 'config.ini') -> configparser.ConfigParser:
    """Loads configuration from an INI file, setting defaults first."""
    # Initialize with string values from DEFAULT_CONFIG
    parser = configparser.ConfigParser(defaults=None, interpolation=None)
    try:
        parser.read_dict(DEFAULT_CONFIG)
        logging.debug("Default configuration loaded into parser.")
    except Exception as e:
        # This should ideally not happen with the predefined structure
        logging.error(f"Error loading default configuration structure: {e}. Parser might be empty.")
        # Fallback: create parser without defaults if loading dict fails
        parser = configparser.ConfigParser(interpolation=None)

    # Read the actual file, overriding defaults
    if os.path.exists(config_file_path):
        try:
            read_files = parser.read(config_file_path, encoding='utf-8')
            if read_files:
                 logging.info(f"Successfully read and merged configuration from: {config_file_path}")
            else:
                 logging.warning(f"Config file found at {config_file_path}, but could not be parsed or was empty. Using defaults only.")
        except configparser.Error as e:
            logging.error(f"Error parsing config file {config_file_path}: {e}. Using defaults only.")
        except Exception as e:
            logging.error(f"Unexpected error reading config file {config_file_path}: {e}. Using defaults only.")
            # Reset parser to only defaults in case of catastrophic read error
            parser = configparser.ConfigParser(defaults=None, interpolation=None)
            try:
                 parser.read_dict(DEFAULT_CONFIG)
            except Exception:
                 logging.error("Failed to reload defaults after config read error.")
    else:
        logging.warning(f"Config file not found at {config_file_path}. Using default configuration values only.")

    return parser

# --- Network Utilities ---

def get_requests_session(config: configparser.ConfigParser) -> requests.Session:
    """Get a requests session with retry configuration."""
    session = requests.Session()
    
    # Get settings from config
    try:
        retry_codes = [int(code.strip()) for code in config.get('Network', 'retry_status_codes_requests', fallback='429,500,502,503,504').split(',')]
        max_retries = config.getint('Network', 'max_retries', fallback=3)
        backoff_factor = config.getfloat('Network', 'backoff_factor', fallback=0.3)
        verify_ssl = config.getboolean('Network', 'verify_ssl', fallback=True)
        allow_redirects = config.getboolean('Network', 'allow_redirects', fallback=True)
        user_agent = config.get('Network', 'user_agent', fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36')
    except (configparser.Error, ValueError) as e:
        logging.warning(f"Error reading session settings: {e}. Using defaults.")
        retry_codes = [429, 500, 502, 503, 504]
        max_retries = 3
        backoff_factor = 0.3
        verify_ssl = True
        allow_redirects = True
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=retry_codes,
    )
    
    # Configure session
    session.mount('http://', HTTPAdapter(max_retries=retry_strategy))
    session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
    session.verify = verify_ssl
    session.allow_redirects = allow_redirects
    session.headers.update({'User-Agent': user_agent})
    
    return session

def get_async_httpx_client(config: configparser.ConfigParser, user_agent: Optional[str] = None) -> httpx.AsyncClient:
    """Creates an httpx AsyncClient with configured timeouts, limits, and retries using ConfigParser."""
    try:
        max_retries = config.getint('Network', 'max_retries')
        connect_timeout = config.getfloat('Network', 'connect_timeout')
        read_timeout = config.getfloat('Network', 'read_timeout')
        pool_timeout = config.getfloat('Network', 'pool_timeout', fallback=5.0)
        max_connections = config.getint('Network', 'max_connections')
        max_keepalive = config.getint('Network', 'max_keepalive_connections')
    except (configparser.Error, ValueError) as e:
        logging.warning(f"Error reading network httpx config from [Network]: {e}. Using hardcoded defaults.")
        max_retries = 5
        connect_timeout = 5.0
        read_timeout = 15.0
        pool_timeout = 5.0
        max_connections = 100
        max_keepalive = 20
        
    final_user_agent = user_agent or config.get('ScraperSettings', 'user_agent', fallback=DEFAULT_CONFIG['ScraperSettings']['user_agent'])

    # Set all timeout parameters explicitly
    timeout_config = httpx.Timeout(
        connect=connect_timeout,
        read=read_timeout,
        write=read_timeout,  # Use same as read timeout
        pool=pool_timeout
    )
    limits_config = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_keepalive)
    transport = httpx.AsyncHTTPTransport(retries=max_retries, http2=True)
    headers = {"User-Agent": final_user_agent}

    try:
        client = httpx.AsyncClient(
            headers=headers,
            timeout=timeout_config,
            limits=limits_config,
            transport=transport,
            follow_redirects=True
        )
        logging.debug(f"Created httpx.AsyncClient: Timeout={timeout_config}, Limits={limits_config}, Retries={max_retries}")
        return client
    except Exception as e:
         logging.error(f"Failed to create httpx.AsyncClient: {e}")
         # Return a default client or raise error depending on requirements
         return httpx.AsyncClient() # Example: return default client

# --- File Utilities ---

def download_image(url: str, save_path: Union[str, Path], config: configparser.ConfigParser) -> bool:
    """Downloads image using requests session, validates, returns success bool."""
    if pd.isna(url) or not isinstance(url, str) or not url.startswith('http'):
        logging.debug(f"Skipping download: Invalid URL '{url}'")
        return False
        
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        connect_timeout = config.getfloat('Network', 'connect_timeout', fallback=5.0)
        read_timeout = config.getfloat('Network', 'read_timeout', fallback=15.0)
        max_retries = config.getint('Network', 'max_retries', fallback=3)
        retry_delay = config.getfloat('Network', 'backoff_factor', fallback=0.3)
    except (configparser.Error, ValueError) as e:
        logging.warning(f"Download image: Error reading network settings: {e}. Using defaults.")
        connect_timeout = 5.0
        read_timeout = 15.0
        max_retries = 3
        retry_delay = 0.3
        
    session = get_requests_session(config)

    # Check if it's a kogift URL
    is_kogift = "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower()

    # Normalize URL for kogift
    if is_kogift and not url.startswith('https://'):
        url = 'https://' + url.lstrip('/')

    for attempt in range(max_retries):
        try:
            logging.debug(f"Attempting to download image: {url} -> {save_path} (attempt {attempt + 1}/{max_retries})")
            response = session.get(url, timeout=(connect_timeout, read_timeout), stream=True)
            response.raise_for_status()

            # Check content type
            content_type = response.headers.get('Content-Type', '')
            if not content_type.startswith('image/'):
                if is_kogift:
                    logging.warning(f"Non-image content type for Kogift URL: {content_type}, proceeding anyway")
                else:
                    logging.warning(f"Non-image content type: {content_type}, URL: {url}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue

            # Check content length
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) < 100:
                if is_kogift:
                    logging.warning(f"Small content length for Kogift image: {content_length} bytes")
                else:
                    logging.warning(f"Content too small: {content_length} bytes")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue

            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Validate downloaded file
            if not os.path.exists(save_path) or os.path.getsize(save_path) < 100:
                logging.warning(f"Downloaded file is too small or missing: {save_path}")
                if os.path.exists(save_path):
                    os.remove(save_path)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                return False

            # Validate image format
            try:
                img = Image.open(save_path)
                img.verify()
                img = Image.open(save_path)  # Re-open after verify
                
                # Check image dimensions
                if img.width < 10 or img.height < 10:
                    logging.warning(f"Image dimensions too small: {img.width}x{img.height}")
                    if not is_kogift and attempt < max_retries - 1:
                        os.remove(save_path)
                        time.sleep(retry_delay * (attempt + 1))
                        continue

                if img.format.lower() not in ['jpeg', 'png', 'gif', 'bmp', 'webp']:
                    if is_kogift:
                        logging.warning(f"Unusual image format for Kogift image: {img.format}")
                    else:
                        logging.warning(f"Unsupported image format: {img.format}")
                        if attempt < max_retries - 1:
                            os.remove(save_path)
                            time.sleep(retry_delay * (attempt + 1))
                            continue

                logging.debug(f"Image validated successfully: {save_path}")
                return True

            except (IOError, SyntaxError, Image.DecompressionBombError) as img_err:
                logging.warning(f"Invalid image file ({url}): {img_err}")
                if os.path.exists(save_path):
                    os.remove(save_path)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                return False

        except requests.exceptions.Timeout as err:
            logging.warning(f"Timeout downloading image {url} (attempt {attempt + 1}): {err}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
        except requests.exceptions.RequestException as err:
            logging.error(f"Request error downloading image {url} (attempt {attempt + 1}): {err}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
        except OSError as err:
            logging.error(f"OS error saving image to {save_path} (attempt {attempt + 1}): {err}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
        except Exception as err:
            logging.error(f"Unexpected error downloading image {url} (attempt {attempt + 1}): {err}", exc_info=True)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue

    return False

async def download_image_async(url: str, save_path: Union[str, Path], client: httpx.AsyncClient, config: configparser.ConfigParser) -> bool:
    """Asynchronously download an image from a URL to a local file path using httpx."""
    if not url or not url.startswith('http'):
        logging.warning(f"Invalid URL for download: {url}")
        return False

    save_path = Path(save_path)
    # Create parent directories if they don't exist
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Skip download if file already exists and is not empty
    if save_path.exists() and save_path.stat().st_size > 0:
        logging.debug(f"Image already exists, skipping download: {save_path}")
        return True

    # Check if it's a kogift URL for special handling
    is_kogift = "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower()
    
    # Ensure kogift images always have jpg extension
    if is_kogift and not str(save_path).lower().endswith('.jpg'):
        new_path = save_path.with_suffix('.jpg')
        logging.debug(f"Converting kogift image path to jpg: {save_path} -> {new_path}")
        save_path = new_path

    # URL 디버깅: 도메인 정보 등 확인
    try:
        parsed_url = urlparse(url)
        logging.debug(f"다운로드 URL 분석: 도메인={parsed_url.netloc}, 경로={parsed_url.path}, 프로토콜={parsed_url.scheme}")
        
        # URL 유효성 검사 추가
        if not parsed_url.netloc or not parsed_url.scheme:
            logging.warning(f"URL 형식이 올바르지 않음: {url}")
            return False
            
        # 특수 도메인 처리 (고려기프트 관련)
        if is_kogift:
            logging.debug(f"고려기프트/판촉 도메인 이미지 URL 감지: {url}")
            # 여기에 도메인별 특수 처리 추가 가능
    except Exception as e:
        logging.error(f"URL 파싱 오류: {url}, 오류: {e}")

    logging.debug(f"Downloading image from {url} to {save_path}")
    
    # Get timeout settings from config
    try:
        timeout = config.getfloat('Network', 'request_timeout', fallback=30.0)
    except (configparser.Error, ValueError):
        timeout = 30.0  # Default timeout if not in config

    max_retries = 3
    retry_delay = 1.0
    
    # 파일 확장자 추정
    file_ext = save_path.suffix.lower()
    valid_image_exts = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
    
    for attempt in range(max_retries):
        try:
            # Set timeout and follow redirects
            response = await client.get(url, follow_redirects=True, timeout=timeout)
            response.raise_for_status()  # Raise an error for bad responses
            
            # Check Content-Type header for image
            content_type = response.headers.get('content-type', '')
            
            # For Kogift/Koreagift websites, be more lenient with content types
            if not content_type.startswith('image/'):
                if is_kogift:
                    # For kogift, log the issue but still try to save as image
                    logging.warning(f"Kogift URL returned non-image content-type: {url}, Content-Type: {content_type}")
                    # Continue with saving anyway for kogift
                else:
                    # For non-kogift, log and continue to next attempt
                    logging.warning(f"Downloaded content is not an image: {url}, Content-Type: {content_type}")
                    
                    # For non-image content, log details
                    if attempt == max_retries - 1:  # Only log on last attempt
                        if content_type.startswith('text/html'):
                            logging.warning(f"URL returned HTML instead of image: {url} (content length: {len(response.content)} bytes)")
                        else:
                            logging.warning(f"URL returned non-image content: {url}, Content-Type: {content_type}, Length: {len(response.content)} bytes")
                    
                    # Only skip saving for non-kogift
                    if not is_kogift:
                        await asyncio.sleep(retry_delay)
                        continue
            
            # 이미지 데이터 확인
            if len(response.content) < 100:  # 너무 작은 응답
                logging.warning(f"Image download seems too small ({len(response.content)} bytes): {url}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
            
            # 파일 저장
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            # 저장된 파일 확인
            if save_path.exists() and save_path.stat().st_size > 0:
                logging.debug(f"Image successfully downloaded: {url} -> {save_path}")
                
                # 이미지 파일 유효성 검사
                try:
                    from PIL import Image
                    img = Image.open(save_path)
                    # 간단한 검증: 이미지를 로드하고 크기 확인
                    if img.width < 10 or img.height < 10:
                        logging.warning(f"Downloaded image is too small: {img.width}x{img.height} pixels")
                        # 작은 이미지여도 일단 사용
                except Exception as img_err:
                    # For kogift images, keep the file even if it's not a valid image
                    if is_kogift:
                        logging.warning(f"Downloaded kogift file is not a valid image, but keeping it: {save_path}, Error: {img_err}")
                        return True
                    else:
                        logging.warning(f"Downloaded file is not a valid image: {save_path}, Error: {img_err}")
                        # 이미지 로드 실패해도 파일은 보존
                
                return True
            else:
                logging.warning(f"Downloaded file missing or empty: {save_path}")
                
        except httpx.TimeoutException:
            logging.warning(f"Timeout downloading image (attempt {attempt+1}/{max_retries}): {url}")
        except httpx.HTTPStatusError as http_err:
            status_code = http_err.response.status_code
            logging.warning(f"HTTP error {status_code} downloading image (attempt {attempt+1}/{max_retries}): {url}")
            
            # 404 오류면 더 시도하지 않음
            if status_code == 404:
                logging.error(f"Image not found (404): {url}")
                break
                
        except httpx.RequestError as req_err:
            logging.warning(f"Request error downloading image (attempt {attempt+1}/{max_retries}): {url}, Error: {req_err}")
        except Exception as e:
            logging.error(f"Error downloading image (attempt {attempt+1}/{max_retries}): {url}, Error: {e}", exc_info=True)
        
        # Exponential backoff
        if attempt < max_retries - 1:
            # 재시도 전 지연 시간 증가
            retry_delay *= 2
            await asyncio.sleep(retry_delay)
            
    return False

# --- Text Processing Utilities ---

def jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Calculate the Jaccard similarity between two sets of strings."""
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return float(intersection) / union if union > 0 else 0.0

def tokenize_korean(text: str) -> Set[str]:
    """Simple Korean tokenizer based on whitespace and removing punctuation."""
    if not isinstance(text, str):
        return set()
    # Remove punctuation and split by whitespace
    cleaned_text = re.sub(r"[^\w\s]", "", text)
    tokens = set(cleaned_text.lower().split())
    # Optional: Add more sophisticated tokenization if needed
    return tokens

def get_base_keyword(product_name: str) -> str:
    """Extracts a base keyword from the product name, removing common prefixes/suffixes."""
    # Example: Remove trailing specifics like color or size indicators if needed
    # This is a placeholder, customize based on actual product name patterns
    base_name = product_name.split('(')[0].strip()
    # Further cleaning could be added here
    return base_name

def generate_keyword_variations(product_name: str, max_variations: int = 4) -> List[str]:
    """
    고도화된 키워드 변형 생성 기능:
    1. 원본 키워드 유지
    2. 모델명/제품코드 제거
    3. 브랜드명 + 핵심 제품명 추출
    4. 숫자 제거 변형
    5. 한글/영문 조합 분리
    6. 특수 기호 처리
    7. 브랜드명 특수처리 (예: 777은 쓰리쎄븐으로도 검색)
    
    Args:
        product_name: 원본 제품명
        max_variations: 최대 변형 개수
        
    Returns:
        List[str]: 생성된 키워드 변형 목록
    """
    if not product_name or pd.isna(product_name):
        return []

    variations = [product_name.strip()]  # 원본 유지
    
    # 특수 브랜드명 매핑 (필요시 확장)
    brand_mapping = {
        "777": "쓰리쎄븐",
        "쓰리쎄븐": "777",
    }
    
    # 브랜드명 처리를 위한 브랜드 패턴 (필요시 확장)
    brand_patterns = [
        (r'\b777\b', '쓰리쎄븐'),  # 777 -> 쓰리쎄븐
        (r'\b쓰리쎄븐\b', '777'),  # 쓰리쎄븐 -> 777
    ]
    
    cleaned_name = product_name.strip()
    
    # 1. 모델명/제품코드 제거 (더 정교한 패턴)
    # 알파벳 + 숫자 조합의 패턴 (TS-16000VG, 399VC 등)
    code_patterns = [
        r'\b([A-Z]{1,5}[-]?\d+[-]?[A-Za-z0-9]*)\b',  # 일반적인 모델코드 (TS-16000VG)
        r'\b(\d+[A-Z]{1,5})\b',                       # 숫자+알파벳 코드 (399VC)
        r'\b([A-Z]{1,5}[-]?\d+[-]?[A-Za-z0-9]*[-][A-Za-z0-9]*)\b'  # 복합 코드
    ]
    
    # 코드 패턴 제거
    code_free_name = cleaned_name
    for pattern in code_patterns:
        code_free_name = re.sub(pattern, '', code_free_name, flags=re.IGNORECASE)
    
    # 공백 정리
    code_free_name = ' '.join(code_free_name.split()).strip()
    
    # 코드가 제거된 버전 추가
    if code_free_name and code_free_name != product_name and len(code_free_name) > 3:
        if code_free_name not in variations:
            variations.append(code_free_name)
    
    # 2. 브랜드+핵심 제품명 추출
    parts = code_free_name.split()
    if len(parts) >= 3:
        # 첫 단어(브랜드) + 마지막 단어들(핵심 제품명)
        # 긴 이름은 브랜드 + 마지막 2~3 단어만 사용
        if len(parts) >= 5:
            simplified = f"{parts[0]} {' '.join(parts[-2:])}"
        else:
            simplified = f"{parts[0]} {parts[-1]}"
            
        if simplified and simplified not in variations and len(simplified) > 3:
            variations.append(simplified)
    
    # 3. 브랜드명 변형 처리 (777 <-> 쓰리쎄븐)
    for original, mapped in brand_patterns:
        brand_variant = re.sub(original, mapped, product_name)
        if brand_variant != product_name and brand_variant not in variations:
            variations.append(brand_variant)
            
        # 코드 없는 버전에도 브랜드 변형 적용
        if code_free_name:
            brand_code_free = re.sub(original, mapped, code_free_name)
            if brand_code_free != code_free_name and brand_code_free not in variations:
                variations.append(brand_code_free)
    
    # 4. 특수기호 대체/제거
    special_chars_name = re.sub(r'[-_+.,#]', ' ', product_name)
    special_chars_name = ' '.join(special_chars_name.split()).strip()
    
    if special_chars_name and special_chars_name != product_name and special_chars_name not in variations:
        variations.append(special_chars_name)
    
    # 5. 한글/영문 분리 (영문 또는 한글만 있는 버전 생성)
    if re.search(r'[가-힣]', product_name) and re.search(r'[a-zA-Z]', product_name):
        # 한글만 추출
        hangul_only = ''.join(re.findall(r'[가-힣]+', product_name))
        if len(hangul_only) > 3 and hangul_only not in variations:
            hangul_only = ' '.join(re.findall(r'[가-힣]+', product_name))
            variations.append(hangul_only)
            
        # 영문만 추출
        eng_only = ''.join(re.findall(r'[a-zA-Z]+', product_name))
        if len(eng_only) > 3 and eng_only not in variations:
            eng_only = ' '.join(re.findall(r'[a-zA-Z]+', product_name))
            variations.append(eng_only)
    
    # 중복 제거 및 최대 개수 제한
    unique_variations = []
    for v in variations:
        v = v.strip()
        if v and v not in unique_variations and len(v) > 2:
            unique_variations.append(v)
    
    return unique_variations[:max_variations]

# --- Image Preprocessing Function --- 
async def _process_single_image_wrapper(args: Tuple) -> Tuple[Any, Optional[str]]:
    """Internal helper for running download and optional bg removal for one image."""
    idx, row_id, image_url, save_dir, prefix, config, client = args
    
    if pd.isna(image_url) or not isinstance(image_url, str) or not image_url.startswith('http'):
        return row_id, None

    try:
        # Generate filename
        url_hash = hashlib.md5(image_url.encode()).hexdigest()[:10]
        # Basic file extension extraction and sanitization
        file_ext = os.path.splitext(urlparse(image_url).path)[1]
        file_ext = ''.join(c for c in file_ext if c.isalnum() or c == '.')[:5].lower()
        if not file_ext.startswith('.') or len(file_ext) < 2: file_ext = '.jpg'
        
        row_id_str = str(row_id).replace(os.path.sep, '_')
        target_filename = f"{prefix}_{row_id_str}_{url_hash}{file_ext}"
        target_path = Path(save_dir) / target_filename
        
        logging.debug(f"Processing image for ID {row_id_str}: {image_url} -> {target_path}")
        
        download_success = await download_image_async(image_url, target_path, client, config)
        
        if not download_success:
            logging.warning(f"Failed download for ID {row_id_str} from {image_url}")
            return row_id, None
            
        final_path = str(target_path) # Default to original path
        
        try:
             use_bg_removal = config.getboolean('Matching', 'use_background_removal')
        except (configparser.Error, ValueError):
             logging.warning("Error reading use_background_removal setting, defaulting to False for this image.")
             use_bg_removal = False
             
        if use_bg_removal:
            # Lazily import to avoid circular dependency if image_utils imports utils
            try:
                 from image_utils import remove_background_async
                 # Generate background removed path
                 bg_removed_path = target_path.with_name(f"{target_path.stem}_no_bg{target_path.suffix}")
                 
                 bg_success = await remove_background_async(target_path, bg_removed_path)
                 if bg_success:
                     final_path = str(bg_removed_path)
                     logging.debug(f"Background removed for ID {row_id_str}: {final_path}")
                     # Optional: Delete original?
                     # if target_path.exists() and target_path != bg_removed_path:
                     #    try: os.remove(target_path); logging.debug(f"Deleted original: {target_path}")
                     #    except OSError as e: logging.warning(f"Could not delete original {target_path}: {e}")
                 else:
                     logging.warning(f"BG removal failed for ID {row_id_str} ({target_path}), using original.")
                     final_path = str(target_path) # Ensure fallback
            except ImportError:
                 logging.error("Could not import remove_background_async from image_utils. Skipping background removal.")
                 final_path = str(target_path)
            except Exception as bg_err:
                logging.error(f"Error during background removal for ID {row_id_str} ({target_path}): {bg_err}", exc_info=True)
                final_path = str(target_path) # Fallback
                
        return row_id, final_path
        
    except Exception as e:
        logging.error(f"Error in _process_single_image_wrapper for ID {row_id} ({image_url}): {e}", exc_info=True)
        return row_id, None

async def preprocess_and_download_images(
    df: pd.DataFrame, 
    url_column_name: str, 
    id_column_name: str, 
    prefix: str, 
    config: configparser.ConfigParser, 
    max_workers: Optional[int] = None
) -> Dict[Any, Optional[str]]:
    """Wrapper function that uses image_downloader.py functionality."""
    logging.info("Using enhanced image downloader for preprocessing images...")
    
    if df is None or df.empty:
        logging.info(f"Skipping image preprocessing (prefix: '{prefix}'): DataFrame is empty.")
        return {}
        
    if url_column_name not in df.columns or id_column_name not in df.columns:
        logging.error(f"Missing required columns '{url_column_name}' or '{id_column_name}'. Cannot preprocess (prefix: '{prefix}').")
        return {}

    # Extract URLs and IDs
    image_urls = []
    row_ids = []
    for idx, row in df.iterrows():
        image_url = row.get(url_column_name)
        row_id = row.get(id_column_name)
        if pd.isna(image_url) or not isinstance(image_url, str) or not image_url.startswith('http'):
            continue
        image_urls.append(image_url)
        row_ids.append(row_id)

    # Use image_downloader's functionality
    results = await download_images(image_urls)
    
    # Convert results to expected format
    image_path_map = {}
    for row_id, url in zip(row_ids, image_urls):
        image_path_map[row_id] = results.get(url)

    return image_path_map

# --- DataFrame Utilities ---
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning for DataFrames (e.g., strip whitespace)."""
    for col in df.select_dtypes(include=['object']).columns:
        # Handle potential non-string data gracefully
        if pd.api.types.is_string_dtype(df[col]):
             df[col] = df[col].str.strip()
        # Optionally add handling for other types if needed
    return df

def setup_logging(config: configparser.ConfigParser = None):
    """
    Setup logging configuration for the application.
    
    Args:
        config: Optional ConfigParser object. If None, will use default settings.
    """
    import logging
    import os
    from datetime import datetime
    
    # Default log directory and file
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Default log file with timestamp
    default_log_file = os.path.join(log_dir, f'shoprpa_{datetime.now().strftime("%Y%m%d")}.log')
    
    if config is not None:
        try:
            # Try to get log settings from config
            log_level = config.get('Logging', 'log_level', fallback='INFO').upper()
            log_file = config.get('Logging', 'log_file', fallback=default_log_file)
        except:
            log_level = 'INFO'
            log_file = default_log_file
    else:
        log_level = 'INFO'
        log_file = default_log_file
    
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure logging
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Logging initialized. Level: {log_level}, File: {log_file}")