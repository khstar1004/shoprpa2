import os
import logging
import asyncio
import aiohttp
import aiofiles
import hashlib
import configparser
from typing import List, Dict, Set, Tuple, Optional, Union
from urllib.parse import urlparse, unquote
import time
from PIL import Image
from pathlib import Path
import random
import io  # Add missing io import

# 로거 설정
logger = logging.getLogger(__name__)

# config.ini 파일 로드
config = configparser.ConfigParser()
config_ini_path = Path(__file__).resolve().parent.parent / 'config.ini'

try:
    config.read(config_ini_path, encoding='utf-8')
    if not config.sections():
        raise FileNotFoundError(f"Config file not found or empty: {config_ini_path}")

    # 설정 로드
    MAX_RETRIES = config.getint('Matching', 'max_retries_downloads', fallback=3)
    VERIFY_SAMPLE_PERCENT = config.getint('Matching', 'verify_sample_percent', fallback=10)
    VERIFY_IMAGE_URLS = config.getboolean('Matching', 'verify_image_urls', fallback=True)
    PREDOWNLOAD_KOGIFT_IMAGES = config.getboolean('Matching', 'predownload_kogift_images', fallback=True)

    # 이미지 저장 경로 - 항상 대문자로 시작하는 디렉토리명 사용
    try:
        # 기본 Main 경로를 사용
        image_main_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
        KOGIFT_IMAGE_DIR = Path(image_main_dir) / 'Kogift'  # Always use 'Kogift' with capital K
        logger.info(f"Using Kogift image directory from config (Main): {KOGIFT_IMAGE_DIR}")
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.error(f"Error getting image_main_dir from config: {e}. Using default.")
        # Fallback to default Main/Kogift with proper capitalization
        KOGIFT_IMAGE_DIR = Path('C:\\RPA\\Image\\Main') / 'Kogift'
        logger.info(f"Using default Kogift image directory (Main): {KOGIFT_IMAGE_DIR}")

except Exception as e:
    logger.error(f"Error loading config from {config_ini_path}: {e}, using default values")
    # 기본값 설정
    MAX_RETRIES = 3
    VERIFY_SAMPLE_PERCENT = 10
    VERIFY_IMAGE_URLS = True
    PREDOWNLOAD_KOGIFT_IMAGES = True
    # Default Kogift image directory (Main) with proper capitalization
    KOGIFT_IMAGE_DIR = Path('C:\\RPA\\Image\\Main') / 'Kogift' 
    logger.info(f"Using default Kogift image directory (Main): {KOGIFT_IMAGE_DIR}")

# 이미지 경로 생성 및 권한 확인
try:
    # Use the specific KOGIFT_IMAGE_DIR
    KOGIFT_IMAGE_DIR.mkdir(parents=True, exist_ok=True) 
    # 쓰기 권한 확인
    if not os.access(KOGIFT_IMAGE_DIR, os.W_OK):
        # 대체 경로 사용 - config에서 정의된 경로 사용 (Main 우선)
        try:
            image_main_dir = config.get('Paths', 'image_main_dir') # 대체 경로도 Main으로
            fallback_dir = Path(image_main_dir) / 'Kogift'  # Always use 'Kogift' with capital K
            fallback_dir.mkdir(parents=True, exist_ok=True)
            logger.warning(f"No write permission to {KOGIFT_IMAGE_DIR}, using fallback directory: {fallback_dir}")
            KOGIFT_IMAGE_DIR = fallback_dir
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logger.error(f"Error getting image_main_dir from config: {e}. Using default RPA Main path.")
            fallback_dir = Path('C:\\RPA\\Image\\Main') / 'Kogift'  # Always use 'Kogift' with capital K
            fallback_dir.mkdir(parents=True, exist_ok=True)
            KOGIFT_IMAGE_DIR = fallback_dir
except Exception as e:
    # 기본 대체 경로 사용 - config에서 정의된 경로 사용 (Main 우선)
    try:
        image_main_dir = config.get('Paths', 'image_main_dir') # 기본 대체 경로도 Main
        fallback_dir = Path(image_main_dir) / 'Kogift'  # Always use 'Kogift' with capital K
        fallback_dir.mkdir(parents=True, exist_ok=True)
        logger.error(f"Error creating image directory {KOGIFT_IMAGE_DIR}: {e}, using fallback: {fallback_dir}")
        KOGIFT_IMAGE_DIR = fallback_dir
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.error(f"Error getting image_main_dir from config: {e}. Using default RPA Main path.")
        fallback_dir = Path('C:\\RPA\\Image\\Main') / 'Kogift'  # Always use 'Kogift' with capital K
        fallback_dir.mkdir(parents=True, exist_ok=True)
        KOGIFT_IMAGE_DIR = fallback_dir

# 파일 작업을 위한 세마포어 생성
file_semaphore = asyncio.Semaphore(1)
async def verify_image_url(session: aiohttp.ClientSession, url: str, timeout: int = 10) -> Tuple[str, bool, Optional[str]]:
    """이미지 URL이 유효한지 확인하는 함수"""
    if not url:
        return url, False, "Empty URL"
    
    # URL 정규화
    if not url.startswith(('http://', 'https://')):
        if "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower():
            url = f"https://{url}" if not url.startswith('//') else f"https:{url}"
        else:
            return url, False, "Invalid URL scheme"

    # Check if URL ends with .asp, .aspx, or other non-image extensions to avoid processing
    parsed_url = urlparse(url)
    path = unquote(parsed_url.path)
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    non_image_extensions = ['.asp', '.aspx', '.php', '.jsp', '.html', '.htm']
    
    if ext in non_image_extensions:
        logger.warning(f"Skipping non-image file with extension {ext}: {url}")
        return url, False, f"Non-image file extension: {ext}"

    try:
        # GET 요청으로 이미지 확인
        try:
            async with session.get(url, timeout=timeout, allow_redirects=True) as response:
                if response.status != 200:
                    return url, False, f"HTTP status {response.status}"
                
                content_type = response.headers.get('Content-Type', '')
                # 네이버 이미지 URL은 별도 처리
                is_naver = "pstatic.net" in url.lower()
                
                if not content_type.startswith('image/') and not is_naver:
                    return url, False, f"Not an image (content-type: {content_type})"
                
                # 응답 크기 확인 - 네이버는 실제 이미지가 아닌 URL도 허용
                if 'Content-Length' in response.headers and not is_naver:
                    content_length = int(response.headers['Content-Length'])
                    if content_length < 100:
                        return url, False, f"Content too small: {content_length} bytes"
                
                # 응답 헤더만으로는 불충분할 수 있으므로 일부 데이터를 읽어 확인
                chunk = await response.content.read(10240)  # 최대 10KB만 읽음
                
                # 네이버 이미지는 데이터가 작아도 허용
                if len(chunk) < 100 and not is_naver:
                    return url, False, f"Response too small: {len(chunk)} bytes"
                
                try:
                    # 네이버 이미지는 실제 이미지가 아닐 수 있으므로 검증 스킵
                    if is_naver:
                        return url, True, None
                        
                    img = Image.open(io.BytesIO(chunk))
                    img.verify()  # 이미지 데이터 검증
                    return url, True, None
                except Exception as e:
                    # 네이버 이미지 URL인 경우 어떤 오류가 나도 항상 유효하다고 간주
                    if is_naver:
                        logger.warning(f"Naver image validation skipped completely: {url}")
                        return url, True, None
                    return url, False, f"Invalid image data: {str(e)}"
                
            return url, True, None
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            return url, False, f"Request error: {str(e)}"
            
    except asyncio.TimeoutError:
        return url, False, "Request timeout"
    except aiohttp.ClientError as e:
        return url, False, f"Client error: {str(e)}"
    except Exception as e:
        return url, False, f"Unexpected error: {str(e)}"

def get_image_path(url: str) -> str:
    """이미지 URL에 대한 로컬 파일 경로 생성 (Kogift 전용 경로 사용)"""
    if not url:
        return None
        
    # URL 정규화
    if not url.startswith(('http://', 'https://')):
        if "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower():
            url = f"https://{url}" if not url.startswith('//') else f"https:{url}"
            
    # URL 해시를 파일명으로 사용
    url_hash = hashlib.md5(url.encode()).hexdigest()
    
    # Check if it's a kogift URL
    is_kogift = "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower()
    
    # 원본 파일 확장자 가져오기
    parsed_url = urlparse(url)
    path = unquote(parsed_url.path)
    _, ext = os.path.splitext(path)
    ext = ext.lower() or '.jpg'  # 확장자가 없으면 .jpg로 기본 설정
    
    # 허용된 확장자 목록 (확장)
    allowed_exts = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
    non_image_extensions = ['.asp', '.aspx', '.php', '.jsp', '.html', '.htm']
    
    # Filter out non-image extensions
    if ext in non_image_extensions:
        logger.warning(f"Skipping non-image file with extension {ext}: {url}")
        return None
        
    if ext not in allowed_exts or is_kogift:
        ext = '.jpg'  # 허용되지 않은 확장자 또는 kogift 이미지는 .jpg로 변환
    
    filename = f"{url_hash}{ext}"
    
    # 이미지 경로 생성 (Use the globally defined KOGIFT_IMAGE_DIR)
    image_path = KOGIFT_IMAGE_DIR / filename
    
    # 디렉토리 존재 확인 및 생성 (Redundant if done globally, but safe)
    image_path.parent.mkdir(parents=True, exist_ok=True)
    
    return str(image_path)

async def download_image(session: aiohttp.ClientSession, url: str, retry_count: int = 0) -> Tuple[str, bool, str]:
    """Download an image from a URL and save it locally.
    
    Args:
        session: The aiohttp client session to use for requests.
        url: The URL of the image to download.
        retry_count: Current retry attempt (used internally for recursion).
        
    Returns:
        Tuple of (url, success_bool, local_path or error_message)
    """
    if not url:
        return url, False, "Empty URL"
        
    # Fix URL format issues (especially with backslashes)
    if isinstance(url, str) and "\\" in url:
        url = url.replace("\\", "/")
    
    # Normalize URL
    if not url.startswith(('http://', 'https://')):
        if any(domain in url.lower() for domain in ["kogift", "koreagift", "adpanchok", "jclgift"]):
            # Handle different URL formats
            if url.startswith('//'):
                url = f"https:{url}"
            elif ":" in url and not url.startswith(('http:', 'https:')):
                # Handle case where URL is like 'https:\www...'
                parts = url.split(':', 1)
                if len(parts) == 2:
                    scheme = parts[0].lower()
                    path = parts[1].lstrip('/').lstrip('\\')
                    url = f"{scheme}://{path}"
            else:
                url = f"https://{url}"
        else:
            return url, False, f"Invalid URL scheme: {url}"
    
    # Determine the appropriate directory based on URL
    if "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower():
        save_dir = KOGIFT_IMAGE_DIR # Use the globally defined Main/Kogift directory
    elif "jclgift" in url.lower():
        # Save jclgift images in a haereum directory within Main
        base_main_dir = KOGIFT_IMAGE_DIR.parent.parent / 'Main' # Get C:\RPA\Image\Main
        haereum_dir = base_main_dir / 'Haereum'  # Always use 'Haereum' with capital H
        haereum_dir.mkdir(parents=True, exist_ok=True)
        save_dir = haereum_dir
    elif "pstatic.net" in url.lower() or "naver" in url.lower():
        # Save Naver images in a naver directory within Main
        base_main_dir = KOGIFT_IMAGE_DIR.parent.parent / 'Main' # Get C:\RPA\Image\Main
        naver_dir = base_main_dir / 'Naver'  # Always use 'Naver' with capital N
        naver_dir.mkdir(parents=True, exist_ok=True)
        save_dir = naver_dir
    else:
        # Use generic directory for other URLs (default to Main/Kogift for safety, though unlikely)
        logger.warning(f"URL source undetermined, saving to Kogift directory: {url}")
        save_dir = KOGIFT_IMAGE_DIR
    
    # Generate a filename based on URL hash
    url_hash = hashlib.md5(url.encode()).hexdigest()
    
    # Get file extension from URL
    parsed_url = urlparse(url)
    path = unquote(parsed_url.path)
    _, ext = os.path.splitext(path)
    
    # Handle problematic extensions
    if ext.lower() in ['.asp', '.aspx', '.php', '.jsp', '.html', '.htm']:
        ext = '.jpg'  # Default to jpg for non-image extensions
    elif not ext:
        ext = '.jpg'  # Default extension if none provided
    elif len(ext) > 5:
        ext = '.jpg'  # If extension is unusually long, use default
    
    # Ensure extension starts with a dot
    if not ext.startswith('.'):
        ext = '.' + ext
    
    # Create a filename with source info
    if "jclgift" in url.lower():
        source_prefix = "haereum"
    elif "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower():
        source_prefix = "kogift"
    elif "pstatic" in url.lower() or "naver" in url.lower():
        source_prefix = "naver"
    else:
        source_prefix = "other"
        
    filename = f"{source_prefix}_{url_hash[:10]}{ext}"
    image_path = save_dir / filename
    
    # For jclgift URLs, also save a copy in Main folder without nobg
    create_nobg_version = False
    if "jclgift" in url.lower():
        # save_dir is already C:\RPA\Image\Main\Haereum, so no need to recalculate
        main_dir = save_dir 
        main_filename = f"{source_prefix}_{url_hash[:10]}{ext}"
        main_path = main_dir / main_filename
        create_nobg_version = True
    
    # Check if file already exists and is of sufficient size
    if os.path.exists(image_path) and os.path.getsize(image_path) > 1000:  # 1KB minimum
        logger.debug(f"Image already exists: {image_path}")
        return url, True, str(image_path)
    
    # Process source-specific headers
    headers = {}
    is_jclgift = "jclgift" in url.lower()
    is_kogift = "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower()
    
    if is_kogift:
        headers = {
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://koreagift.com/'
        }
    elif is_jclgift:
        headers = {
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.jclgift.com/'
        }
    
    # Try to download the image
    try:
        async with session.get(url, headers=headers, timeout=30) as response:
            if response.status != 200:
                # Handle special cases for certain status codes
                if response.status == 404:
                    logger.warning(f"Image not found (404): {url}")
                    return url, False, "404 Not Found"
                    
                if retry_count < MAX_RETRIES:
                    logger.warning(f"Got HTTP {response.status} for {url}, retrying ({retry_count + 1}/{MAX_RETRIES})...")
                    await asyncio.sleep(0.5 * (retry_count + 1))  # Exponential backoff
                    return await download_image(session, url, retry_count + 1)
                else:
                    return url, False, f"HTTP error {response.status} after {MAX_RETRIES} retries"
            
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            is_naver = "pstatic.net" in url.lower()
            
            if not content_type.startswith('image/') and not is_naver and not is_kogift and not is_jclgift:
                logger.warning(f"Non-image content type: {content_type} for {url}")
                if retry_count < MAX_RETRIES:
                    await asyncio.sleep(0.5 * (retry_count + 1))  # Exponential backoff
                    return await download_image(session, url, retry_count + 1)
                else:
                    return url, False, f"Non-image content type: {content_type}"
            
            # Get content
            data = await response.read()
            
            # Verify data is an image before saving
            try:
                img = Image.open(io.BytesIO(data))
                img.verify()  # Verify it's a valid image
                
                # Get the actual image format
                img = Image.open(io.BytesIO(data))
                img_format = img.format.lower() if img.format else 'jpeg'
                
                # Check dimensions
                width, height = img.size
                if width < 20 or height < 20:
                    logger.warning(f"Image dimensions too small: {width}x{height} for {url}")
                    if not is_kogift and not is_jclgift and retry_count < MAX_RETRIES:
                        await asyncio.sleep(0.5 * (retry_count + 1))
                        return await download_image(session, url, retry_count + 1)
                    
                # Update extension based on actual format
                proper_ext = f".{img_format}" if img_format != 'jpeg' else '.jpg'
                if proper_ext != ext:
                    filename = f"{source_prefix}_{url_hash[:10]}{proper_ext}"
                    image_path = save_dir / filename
                    
                    # Update main_path too if applicable
                    if create_nobg_version:
                        main_filename = f"{source_prefix}_{url_hash[:10]}{proper_ext}"
                        main_path = main_dir / main_filename
                
                # Use semaphore for file operations to prevent race conditions
                async with file_semaphore:
                    # Make sure parent directory exists
                    os.makedirs(image_path.parent, exist_ok=True)
                    
                    # Write image to file
                    async with aiofiles.open(image_path, 'wb') as f:
                        await f.write(data)
                    
                    # If this is jclgift, also save to Main folder 
                    if create_nobg_version:
                        os.makedirs(main_path.parent, exist_ok=True)
                        async with aiofiles.open(main_path, 'wb') as f:
                            await f.write(data)
                            
                        # Also create a _nobg.png version for the background-removed image path
                        # Ensure the base path exists before creating the nobg version path
                        os.makedirs(main_path.parent, exist_ok=True) 
                        nobg_path = main_path.with_suffix('').with_name(main_path.stem + "_nobg.png")
                        # Copy the original data to the _nobg path placeholder
                        async with aiofiles.open(nobg_path, 'wb') as f:
                            await f.write(data)
                        
                        # Return the Main path to use for images
                        return url, True, str(main_path)
                
                # Return success with the path to the saved image
                return url, True, str(image_path)
                
            except Exception as img_err:
                logger.warning(f"Invalid image data for {url}: {img_err}")
                if retry_count < MAX_RETRIES:
                    await asyncio.sleep(0.5 * (retry_count + 1))
                    return await download_image(session, url, retry_count + 1)
                else:
                    return url, False, f"Invalid image data: {img_err}"
    
    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
        if retry_count < MAX_RETRIES:
            logger.warning(f"Connection error for {url}, retrying ({retry_count + 1}/{MAX_RETRIES}): {e}")
            await asyncio.sleep(0.5 * (retry_count + 1))
            return await download_image(session, url, retry_count + 1)
        else:
            logger.error(f"Connection error for {url} after {MAX_RETRIES} retries: {e}")
            return url, False, f"Connection error: {e}"
            
    except Exception as e:
        logger.error(f"Unexpected error downloading {url}: {e}")
        return url, False, f"Unexpected error: {e}"

async def download_images(image_urls: List[str]) -> Dict[str, Optional[str]]:
    """Download multiple images asynchronously.
    
    Args:
        image_urls: List of image URLs to download.
        
    Returns:
        Dictionary mapping original URLs to local file paths.
    """
    if not image_urls:
        return {}
    
    # Normalize all URLs first - fix backslashes
    normalized_urls = []
    original_to_normalized = {}
    
    for url in image_urls:
        if not url:
            continue
            
        # Fix backslashes
        if isinstance(url, str) and "\\" in url:
            normalized_url = url.replace("\\", "/")
        else:
            normalized_url = url
            
        # Add proper scheme if needed
        if isinstance(normalized_url, str) and not normalized_url.startswith(('http://', 'https://')):
            domain_keywords = ["kogift", "koreagift", "adpanchok", "jclgift"]
            if any(kw in normalized_url.lower() for kw in domain_keywords):
                # Handle different URL formats
                if normalized_url.startswith('//'):
                    normalized_url = f"https:{normalized_url}"
                elif ":" in normalized_url and not normalized_url.startswith(('http:', 'https:')):
                    # Handle case where URL is like 'https:\www...'
                    parts = normalized_url.split(':', 1)
                    if len(parts) == 2:
                        scheme = parts[0].lower()
                        path = parts[1].lstrip('/').lstrip('\\')
                        normalized_url = f"{scheme}://{path}"
                else:
                    normalized_url = f"https://{normalized_url}"
                    
        normalized_urls.append(normalized_url)
        original_to_normalized[url] = normalized_url
    
    # Create result dictionary to store URLs and their local paths
    result_dict = {}
    
    # Log basic info
    logger.info(f"Downloading {len(normalized_urls)} images...")
    
    # Create directories for each source type if they don't exist (Standardize to Main)
    try:
        base_main_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    except Exception as e:
        logger.warning(f"Error getting image_main_dir from config: {e}. Using default C:\\RPA\\Image\\Main")
        base_main_dir = 'C:\\RPA\\Image\\Main'
        
    source_dirs = {
        'haereum': Path(base_main_dir) / 'Haereum',
        'kogift': Path(base_main_dir) / 'Kogift',
        'naver': Path(base_main_dir) / 'Naver',
        # No need for Target directories here anymore
    }
    
    for d in source_dirs.values():
        os.makedirs(d, exist_ok=True)
    
    # Create aiohttp session 
    conn = aiohttp.TCPConnector(ssl=False, limit=5)
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_connect=10, sock_read=30)
    
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        # Create tasks for downloading each image
        tasks = [download_image(session, url) for url in normalized_urls]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful = 0
        failed = 0
        
        for original_url, result in zip(image_urls, results):
            if isinstance(result, Exception):
                logger.error(f"Error downloading {original_url}: {result}")
                result_dict[original_url] = None
                failed += 1
            elif isinstance(result, tuple) and len(result) == 3:
                _, success, path_or_error = result
                if success:
                    successful += 1
                    # Store both the image path and additional metadata to aid in Excel embedding
                    if "jclgift" in original_url.lower():
                        source = "haereum"
                    elif "kogift" in original_url.lower() or "koreagift" in original_url.lower() or "adpanchok" in url.lower():
                        source = "kogift" 
                    elif "pstatic" in original_url.lower() or "naver" in original_url.lower():
                        source = "naver"
                    else:
                        source = "other"
                    
                    # Include additional metadata for image in result
                    result_dict[original_url] = {
                        'url': original_url,
                        'local_path': path_or_error,
                        'source': source
                    }
                else:
                    result_dict[original_url] = None
                    failed += 1
                    logger.warning(f"Failed to download {original_url}: {path_or_error}")
            else:
                result_dict[original_url] = None
                failed += 1
                logger.error(f"Unexpected result format for {original_url}: {result}")
    
    # Log stats
    logger.info(f"Image download complete: {successful} successful, {failed} failed out of {len(image_urls)} total")
    
    return result_dict

async def predownload_kogift_images(product_list: List[Dict]) -> Dict[str, Optional[str]]:
    """고려기프트 제품 이미지를 미리 다운로드"""
    if not PREDOWNLOAD_KOGIFT_IMAGES:
        logger.info("Pre-downloading of 고려기프트 images is disabled in config")
        return {}
        
    if not product_list:
        logger.warning("No product list provided for Kogift image predownload")
        return {}
        
    logger.info(f"Pre-downloading images for {len(product_list)} 고려기프트 products")
    
    # 모든 이미지 URL 추출
    image_urls = []
    for product in product_list:
        if not isinstance(product, dict):
            continue
            
        # 여러 필드명 지원 (하위 호환성)
        img_url = None
        for field in ['image', 'image_path', 'src', 'img_src', 'image_url']:
            if field in product and product[field]:
                img_url = product[field]
                break
                
        if img_url:
            image_urls.append(img_url)
    
    if not image_urls:
        logger.warning("No image URLs found in the product list")
        return {}
    
    # 중복 제거
    unique_urls = list(set(image_urls))
    logger.info(f"Found {len(unique_urls)} unique Kogift image URLs to download")
    
    # 이미지 URL 검증
    if VERIFY_IMAGE_URLS and VERIFY_SAMPLE_PERCENT > 0:
        # VERIFY_SAMPLE_PERCENT에 따라 일부만 검증
        # 개수 계산 (최소 1개, 최대 고려기프트 이미지 전체)
        sample_size = max(1, min(len(unique_urls), int(len(unique_urls) * VERIFY_SAMPLE_PERCENT / 100)))
        
        # 임의 샘플링을 위한 간단한 방법 (균등 간격)
        step = max(1, len(unique_urls) // sample_size)
        urls_to_verify = unique_urls[::step][:sample_size]
        
        logger.info(f"Verifying {len(urls_to_verify)} sample Kogift image URLs ({VERIFY_SAMPLE_PERCENT}%)")
        
        # TCP 연결 재사용을 위한 세션 설정
        timeout = aiohttp.ClientTimeout(total=15)
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 샘플 URL 검증
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
            tasks = [verify_image_url(session, url) for url in urls_to_verify]
            verify_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 검증 결과 확인
            valid_count = 0
            invalid_count = 0
            error_count = 0
            
            for i, result in enumerate(verify_results):
                url = urls_to_verify[i]
                if isinstance(result, Exception):
                    logger.error(f"Error verifying {url}: {str(result)}")
                    error_count += 1
                else:
                    _, valid, _ = result
                    if valid:
                        valid_count += 1
                    else:
                        invalid_count += 1
            
            if invalid_count + error_count:
                logger.warning(f"Found {invalid_count} invalid and {error_count} error URLs out of {len(urls_to_verify)} sample")
                # 문제가 많은 경우 경고 출력
                if (invalid_count + error_count) > len(urls_to_verify) // 2:
                    logger.error(f"More than 50% of Kogift image URLs are problematic! Consider checking the source.")
    
    # 이미지 다운로드
    image_paths = await download_images(unique_urls)
    
    # 성공적으로 다운로드한 이미지 수 계산
    success_count = sum(1 for path in image_paths.values() if path is not None)
    logger.info(f"Pre-downloaded {success_count}/{len(unique_urls)} 고려기프트 images")
    
    return image_paths

async def download_all_images(products: List[Dict]) -> Dict[str, Optional[str]]:
    """모든 제품의 이미지를 다운로드하는 함수"""
    image_urls = []
    
    # 제품 목록에서 이미지 URL 추출
    for product in products:
        if isinstance(product, dict):
            # 이미지 URL 필드 이름이 다양할 수 있으므로 모든 가능성 체크
            image_url = product.get('image') or product.get('image_url') or product.get('imageUrl') or product.get('img_url')
            if image_url:
                image_urls.append(image_url)
    
    # 중복 제거
    image_urls = list(set(image_urls))
    
    # 이미지 다운로드 실행
    return await download_images(image_urls)

async def main():
    """테스트 함수"""
    # 테스트 이미지 URL 목록
    test_urls = [
        "https://www.kogift.com/web/product/big/202010/758bfe210ff0765832a812a6f4893762.jpg",
        "https://www.kogift.com/web/product/extra/small/202010/92b2c92a05c3b4cc7b84a0b763784332.jpg",
        "https://img.kogift.com/web/product/medium/202105/210edd76a72d2356f9d3af01da6c5dcb.jpg",
        # 상대 경로 URL 테스트
        "www.kogift.com/web/product/big/202010/758bfe210ff0765832a812a6f4893762.jpg",
        # 잘못된 URL 테스트
        "https://example.com/not-an-image.html",
        # 존재하지 않는 이미지 URL
        "https://www.kogift.com/nonexistent-image.jpg",
        # 네이버 이미지 URL 테스트
        "https://shopping-phinf.pstatic.net/main_8463641/84636418311.1.jpg",
        # 하른 이미지 URL 테스트
        "https://www.jclgift.com/upload/product/simg3/EECZ00010000s(332).jpg"
    ]
    
    logger.info("Testing image URL verification...")
    # TCP 연결 재사용을 위한 세션 설정
    timeout = aiohttp.ClientTimeout(total=15)
    connector = aiohttp.TCPConnector(limit=5, ssl=False)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        for url in test_urls:
            result = await verify_image_url(session, url)
            if isinstance(result, Exception):
                status = f"Error: {str(result)}"
            else:
                url, valid, error_msg = result
                status = "Valid" if valid else f"Invalid: {error_msg}"
            logger.info(f"URL: {url} - {status}")
    
    logger.info("\nTesting image downloads...")
    # 이미지 다운로드 테스트
    result = await download_images(test_urls)
    
    # 결과 출력
    success_count = sum(1 for path in result.values() if path)
    logger.info(f"Successfully downloaded {success_count}/{len(test_urls)} images")
    
    for url, path in result.items():
        status = f"-> {path}" if path else "-> Failed"
        logger.info(f"{url} {status}")
    
    # 저장된 이미지 파일 존재 확인
    logger.info("\nVerifying downloaded image files...")
    for url, path in result.items():
        if path:
            file_exists = os.path.exists(path)
            file_size = os.path.getsize(path) if file_exists else 0
            status = f"Exists ({file_size} bytes)" if file_exists else "Missing"
            logger.info(f"{path}: {status}")

# 스크립트가 직접 실행될 때만 메인 함수 호출
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("shopRPA_log.log"),
            logging.StreamHandler()
        ]
    )
    asyncio.run(main()) 