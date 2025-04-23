import os
import logging
import asyncio
import aiohttp
import aiofiles
import hashlib
import configparser
from typing import List, Dict, Set, Tuple, Optional
from urllib.parse import urlparse, unquote
import time
from PIL import Image
import io
from pathlib import Path
import random
import re
import requests
import shutil

# 로거 설정
logger = logging.getLogger(__name__)

# config.ini 파일 로드
config = configparser.ConfigParser()
# Use Path object for robustness
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
    KOGIFT_SPECIAL_DOMAIN_HANDLING = config.getboolean('Matching', 'kogift_special_domain_handling', fallback=True)

    # 이미지 저장 경로 - Use Target/kogift for this downloader
    try:
        image_target_dir = config.get('Paths', 'image_target_dir')
        KOGIFT_IMAGE_DIR = Path(image_target_dir) / 'kogift'
        logger.info(f"Using Kogift image directory from config: {KOGIFT_IMAGE_DIR}")
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.error(f"Error getting image_target_dir from config: {e}. Using default.")
        KOGIFT_IMAGE_DIR = Path('C:\\RPA\\Image\\Target') / 'kogift'
        logger.info(f"Using default Kogift image directory: {KOGIFT_IMAGE_DIR}")

except Exception as e:
    logger.error(f"Error loading config from {config_ini_path}: {e}, using default values")
    # 기본값 설정
    MAX_RETRIES = 3
    VERIFY_SAMPLE_PERCENT = 10
    VERIFY_IMAGE_URLS = True
    PREDOWNLOAD_KOGIFT_IMAGES = True
    KOGIFT_SPECIAL_DOMAIN_HANDLING = True
    # Default Kogift image directory
    KOGIFT_IMAGE_DIR = Path('C:\\RPA\\Image\\Target') / 'kogift' 
    logger.info(f"Using default Kogift image directory: {KOGIFT_IMAGE_DIR}")

# 이미지 경로 생성 및 권한 확인
try:
    # Use the specific KOGIFT_IMAGE_DIR
    KOGIFT_IMAGE_DIR.mkdir(parents=True, exist_ok=True) 
    
    # 쓰기 권한 확인
    if not os.access(KOGIFT_IMAGE_DIR, os.W_OK):
        # 대체 경로 사용
        fallback_dir = Path.cwd() / "downloaded_images" / "kogift" # Add kogift subfolder here too
        fallback_dir.mkdir(parents=True, exist_ok=True)
        logger.warning(f"No write permission to {KOGIFT_IMAGE_DIR}, using fallback directory: {fallback_dir}")
        KOGIFT_IMAGE_DIR = fallback_dir
except Exception as e:
    # 기본 대체 경로 사용
    fallback_dir = Path.cwd() / "downloaded_images" / "kogift"
    fallback_dir.mkdir(parents=True, exist_ok=True)
    logger.error(f"Error creating image directory {KOGIFT_IMAGE_DIR}: {e}, using fallback: {fallback_dir}")
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
                
                # 응답 크기 확인
                if 'Content-Length' in response.headers:
                    content_length = int(response.headers['Content-Length'])
                    if content_length < 100:
                        return url, False, f"Content too small: {content_length} bytes"
                
                # 응답 헤더만으로는 불충분할 수 있으므로 일부 데이터를 읽어 확인
                chunk = await response.content.read(10240)  # 최대 10KB만 읽음
                if len(chunk) < 100:
                    return url, False, f"Response too small: {len(chunk)} bytes"
                
                try:
                    img = Image.open(io.BytesIO(chunk))
                    img.verify()  # 이미지 데이터 검증
                    return url, True, None
                except Exception as e:
                    # 네이버 이미지 URL인 경우 이미지 검증 실패해도 계속 진행
                    if is_naver:
                        logger.warning(f"Naver image validation failed but proceeding: {url}, Error: {str(e)}")
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
    
    # 허용된 확장자 목록
    allowed_exts = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
    if ext not in allowed_exts or is_kogift:
        ext = '.jpg'  # 허용되지 않은 확장자 또는 kogift 이미지는 .jpg로 변환
    
    filename = f"{url_hash}{ext}"
    
    # 이미지 경로 생성 (Use the globally defined KOGIFT_IMAGE_DIR)
    image_path = KOGIFT_IMAGE_DIR / filename
    
    # 디렉토리 존재 확인 및 생성 (Redundant if done globally, but safe)
    image_path.parent.mkdir(parents=True, exist_ok=True)
    
    return str(image_path)

async def download_image(session: aiohttp.ClientSession, url: str, retry_count: int = 0) -> Tuple[str, bool, str]:
    """이미지 다운로드 함수"""
    if not url or retry_count >= MAX_RETRIES:
        logger.warning(f"Failed to download image after {retry_count} retries: {url}")
        return url, False, ""
    
    try:
        # URL 정규화
        if not url.startswith(('http://', 'https://')):
            if "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower() or "jclgift" in url.lower():
                url = f"https://{url}" if not url.startswith('//') else f"https:{url}"
            else:
                logger.warning(f"Invalid URL format: {url}")
                return url, False, ""
        
        # URL 검증
        if VERIFY_IMAGE_URLS:
            verified_url, valid, error_msg = await verify_image_url(session, url)
            if not valid:
                logger.warning(f"Invalid image URL: {url}, Error: {error_msg}")
                return url, False, ""
            url = verified_url
        
        # 이미지 경로 생성
        image_path = get_image_path(url)
        if not image_path:
            logger.warning(f"Could not generate image path for URL: {url}")
            return url, False, ""
            
        # 이미지 경로를 Path 객체로 변환
        image_path = Path(image_path)
        
        # 고유한 임시 파일 경로 생성 (충돌 방지)
        temp_path = image_path.with_name(f"{image_path.stem}_{random.randint(1000, 9999)}.tmp")
        
        # 이미지가 이미 있는지 확인
        if image_path.exists():
            file_size = image_path.stat().st_size
            if file_size > 0:  # 파일이 있고 내용이 있으면 다운로드 스킵
                logger.debug(f"Image already exists: {url} -> {image_path}")
                return url, True, str(image_path)
        
        # Kogift 이미지 다운로드를 위한 특별한 헤더 설정
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://koreagift.com/'
        }
        
        # 이미지 다운로드
        async with session.get(url, headers=headers, timeout=30) as response:
            if response.status != 200:
                logger.warning(f"Failed to download image: {url}, status: {response.status}")
                if retry_count < MAX_RETRIES - 1:
                    await asyncio.sleep(1.0 * (2 ** retry_count))  # Exponential backoff
                    return await download_image(session, url, retry_count + 1)
                return url, False, ""
            
            # 이미지 데이터 읽기
            data = await response.read()
            
            # 데이터 크기 검증
            if len(data) < 100:
                logger.warning(f"Invalid Kogift image data (too small): {len(data)} bytes")
                if retry_count < MAX_RETRIES - 1:
                    await asyncio.sleep(1.0 * (2 ** retry_count))
                    return await download_image(session, url, retry_count + 1)
                return url, False, ""
            
            # 임시 파일에 저장
            async with aiofiles.open(temp_path, 'wb') as f:
                await f.write(data)
            
            # 이미지 유효성 검사
            try:
                with Image.open(temp_path) as img:
                    img.verify()
                with Image.open(temp_path) as img:
                    if img.width < 10 or img.height < 10:
                        logger.warning(f"Image dimensions too small: {img.width}x{img.height}")
                        if retry_count < MAX_RETRIES - 1:
                            await asyncio.sleep(1.0 * (2 ** retry_count))
                            return await download_image(session, url, retry_count + 1)
                        return url, False, ""
            except Exception as img_err:
                logger.warning(f"Invalid image data: {img_err}")
                if retry_count < MAX_RETRIES - 1:
                    await asyncio.sleep(1.0 * (2 ** retry_count))
                    return await download_image(session, url, retry_count + 1)
                return url, False, ""
            
            # 임시 파일을 실제 파일로 이동
            if image_path.exists():
                image_path.unlink()
            temp_path.rename(image_path)
            
            logger.info(f"Successfully downloaded image: {url} -> {image_path}")
            return url, True, str(image_path)
            
    except Exception as e:
        logger.error(f"Error downloading image {url}: {e}")
        if retry_count < MAX_RETRIES - 1:
            await asyncio.sleep(1.0 * (2 ** retry_count))
            return await download_image(session, url, retry_count + 1)
        return url, False, ""

async def download_images(image_urls: List[str]) -> Dict[str, Optional[str]]:
    """이미지 URL 목록에서 비동기적으로 이미지 다운로드"""
    # 결과를 저장할 딕셔너리: {url: local_path}
    results = {}
    
    # 빈 URL 제거
    image_urls = [url for url in image_urls if url]
    
    if not image_urls:
        logger.warning("No image URLs provided for download")
        return results
    
    # URL 정규화
    normalized_urls = []
    for url in image_urls:
        if not url.startswith(('http://', 'https://')):
            if "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower():
                norm_url = f"https://{url}" if not url.startswith('//') else f"https:{url}"
                normalized_urls.append(norm_url)
            else:
                logger.warning(f"Skipping invalid URL: {url}")
        else:
            normalized_urls.append(url)
    
    # 이미 로컬에 다운로드된 이미지 필터링
    to_download = []
    for url in normalized_urls:
        path = get_image_path(url)
        if path and os.path.exists(path) and os.path.getsize(path) > 0:
            logger.debug(f"Image already exists: {url} -> {path}")
            results[url] = path
        else:
            to_download.append(url)
    
    if not to_download:
        logger.info("No new images to download")
        return results
    
    logger.info(f"Downloading {len(to_download)} images...")
    
    # TCP 연결 재사용을 위한 세션 설정
    timeout = aiohttp.ClientTimeout(total=30)
    conn_limit = min(10, len(to_download))  # 최대 10개 연결 제한
    
    # 사용자 에이전트 설정
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://koreagift.com/'
    }
    
    # 비동기 세션 생성 및 연결 제한 설정
    connector = aiohttp.TCPConnector(limit=conn_limit, ssl=False)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        # 동시 다운로드를 위한 태스크 생성
        tasks = []
        for url in to_download:
            task = asyncio.create_task(download_image(session, url))
            tasks.append(task)
        
        # 모든 태스크 완료 대기
        completed = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 처리
        for url, (_, success, path) in zip(to_download, completed):
            if success and path:
                results[url] = path
                logger.info(f"Successfully downloaded: {url} -> {path}")
            else:
                logger.warning(f"Failed to download: {url}")
    
    success_count = len(results)
    logger.info(f"Downloaded {success_count}/{len(image_urls)} images successfully")
    
    return results

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