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

# 로거 설정
logger = logging.getLogger(__name__)

# config.ini 파일 로드
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
config.read(config_path, encoding='utf-8')

# 설정 로드
MAX_RETRIES = config.getint('Matching', 'max_retries_downloads', fallback=3)
VERIFY_SAMPLE_PERCENT = config.getint('Matching', 'verify_sample_percent', fallback=10)
VERIFY_IMAGE_URLS = config.getboolean('Matching', 'verify_image_urls', fallback=True)
PREDOWNLOAD_KOGIFT_IMAGES = config.getboolean('Matching', 'predownload_kogift_images', fallback=True)
KOGIFT_SPECIAL_DOMAIN_HANDLING = config.getboolean('Matching', 'kogift_special_domain_handling', fallback=True)

# 이미지 저장 경로 - 'images_dir' 값 사용
IMAGE_DIR = config.get('Matching', 'images_dir', fallback='C:\\RPA\\Image\\Target')
logger.info(f"Using image directory from config: {IMAGE_DIR}")
os.makedirs(IMAGE_DIR, exist_ok=True)

async def verify_image_url(session: aiohttp.ClientSession, url: str, timeout: int = 5) -> Tuple[str, bool, Optional[str]]:
    """
    이미지 URL이 유효한지 확인하는 함수
    
    Args:
        session: aiohttp 세션
        url: 확인할 이미지 URL
        timeout: 요청 타임아웃 (초)
        
    Returns:
        Tuple[str, bool, Optional[str]]: (URL, 유효 여부, 오류 메시지)
    """
    if not url:
        return url, False, "Empty URL"
    
    try:
        # 먼저 HEAD 요청으로 빠르게 확인
        try:
            async with session.head(url, timeout=timeout) as response:
                if response.status != 200:
                    return url, False, f"HTTP status {response.status}"
                
                content_type = response.headers.get('Content-Type', '')
                # 고려기프트 URL에 대해서는 Content-Type 검사를 덜 엄격하게 적용
                is_kogift = "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower()
                
                if not content_type.startswith('image/') and not is_kogift:
                    # 일부 서버는 HEAD 요청에 제대로 응답하지 않을 수 있으므로,
                    # content-type이 이미지가 아니면 GET 요청으로 다시 시도
                    raise ValueError("Not an image content type in HEAD response")
                
                # 응답 헤더만으로는 불충분할 수 있으므로 일부 데이터를 읽어 확인
                chunk = await response.content.read(10240)  # 최대 10KB만 읽음
                try:
                    img = Image.open(io.BytesIO(chunk))
                    img.verify()  # 이미지 데이터 검증
                    return url, True, None
                except Exception as e:
                    # 고려기프트 URL인 경우 이미지 검증 실패해도 계속 진행할 수 있게 함
                    if is_kogift:
                        # 너무 작은 파일이면 오류로 간주
                        if len(chunk) < 100:
                            return url, False, f"Invalid Kogift image data (too small): {len(chunk)} bytes"
                        # 오류 메시지 기록하고 유효하다고 간주
                        logger.warning(f"Kogift image validation failed but proceeding: {url}, Error: {str(e)}")
                        return url, True, None
                    return url, False, f"Invalid image data: {str(e)}"
        except (asyncio.TimeoutError, ValueError, aiohttp.ClientError):
            # HEAD 요청이 실패하면 GET 요청으로 재시도
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    return url, False, f"HTTP status {response.status}"
                
                content_type = response.headers.get('Content-Type', '')
                # 고려기프트 URL에 대해서는 Content-Type 검사를 덜 엄격하게 적용
                is_kogift = "kogift" in url.lower() or "koreagift" in url.lower() or "adpanchok" in url.lower()
                
                if not content_type.startswith('image/') and not is_kogift:
                    return url, False, f"Not an image (content-type: {content_type})"
                
                # 응답 헤더만으로는 불충분할 수 있으므로 일부 데이터를 읽어 확인
                chunk = await response.content.read(10240)  # 최대 10KB만 읽음
                try:
                    img = Image.open(io.BytesIO(chunk))
                    img.verify()  # 이미지 데이터 검증
                    return url, True, None
                except Exception as e:
                    # 고려기프트 URL인 경우 이미지 검증 실패해도 계속 진행할 수 있게 함
                    if is_kogift:
                        # 너무 작은 파일이면 오류로 간주
                        if len(chunk) < 100:
                            return url, False, f"Invalid Kogift image data (too small): {len(chunk)} bytes"
                        # 오류 메시지 기록하고 유효하다고 간주
                        logger.warning(f"Kogift image validation failed but proceeding: {url}, Error: {str(e)}")
                        return url, True, None
                    return url, False, f"Invalid image data: {str(e)}"
                
        return url, True, None
        
    except asyncio.TimeoutError:
        return url, False, "Request timeout"
    except aiohttp.ClientError as e:
        return url, False, f"Client error: {str(e)}"
    except Exception as e:
        return url, False, f"Unexpected error: {str(e)}"

def get_image_path(url: str) -> str:
    """이미지 URL에 대한 로컬 파일 경로 생성"""
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
    return os.path.join(IMAGE_DIR, filename)

async def download_image(session: aiohttp.ClientSession, url: str, retry_count: int = 0) -> Tuple[str, bool, str]:
    """이미지 다운로드 함수"""
    if not url or retry_count >= MAX_RETRIES:
        logger.warning(f"Failed to download image after {retry_count} retries: {url}")
        return url, False, ""
    
    try:
        # URL 검증
        if VERIFY_IMAGE_URLS:
            url_valid, valid, error_msg = await verify_image_url(session, url)
            if not valid:
                logger.warning(f"Invalid image URL: {url}, Error: {error_msg}")
                return url, False, ""
        
        # 고려기프트 도메인 특별 처리
        if KOGIFT_SPECIAL_DOMAIN_HANDLING and "kogift" in url.lower():
            parsed_url = urlparse(url)
            if not parsed_url.scheme:  # URL에 스킴이 없는 경우
                url = "https://" + url
        
        image_path = get_image_path(url)
        
        # 이미지가 이미 있는지 확인
        if os.path.exists(image_path):
            file_size = os.path.getsize(image_path)
            if file_size > 0:  # 파일이 있고 내용이 있으면 다운로드 스킵
                return url, True, image_path
        
        # 이미지 다운로드
        async with session.get(url, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"HTTP error while downloading {url}: {response.status}")
                # 재시도
                return await download_image(session, url, retry_count + 1)
            
            # Content-Type 확인
            content_type = response.headers.get('Content-Type', '')
            logger.info(f"Content-Type for {url}: {content_type}")
            
            # 이미지 데이터 받기
            data = await response.read()
            
            # 고려기프트 URL의 경우 Content-Type 검사를 덜 엄격하게 적용
            # 작은 HTML 오류 페이지가 아니라면 계속 진행
            if content_type and 'text/html' in content_type and len(data) < 1000:
                logger.warning(f"URL likely returns HTML error page instead of image: {url}")
                return url, False, ""
            
            if not data or len(data) < 100:  # 이미지 데이터가 너무 작으면 의심
                logger.warning(f"Downloaded image too small: {len(data)} bytes, URL: {url}")
                return url, False, ""
            
            # 이미지 데이터 검증 시도
            try:
                img = Image.open(io.BytesIO(data))
                img.verify()
            except Exception as e:
                logger.warning(f"Invalid image data from {url}: {str(e)}")
                return url, False, ""
            
            # 이미지 저장
            async with aiofiles.open(image_path, 'wb') as f:
                await f.write(data)
            
            logger.info(f"Downloaded image: {url} -> {image_path}")
            return url, True, image_path
    
    except asyncio.TimeoutError:
        logger.warning(f"Timeout error while downloading {url}")
        # 재시도
        return await download_image(session, url, retry_count + 1)
    
    except aiohttp.ClientError as e:
        logger.warning(f"Client error while downloading {url}: {str(e)}")
        # 재시도
        return await download_image(session, url, retry_count + 1)
    
    except Exception as e:
        logger.error(f"Unexpected error while downloading {url}: {str(e)}", exc_info=True)
        # 재시도
        return await download_image(session, url, retry_count + 1)

async def download_images(image_urls: List[str]) -> Dict[str, Optional[str]]:
    """이미지 URL 목록에서 비동기적으로 이미지 다운로드"""
    # 결과를 저장할 딕셔너리: {url: local_path}
    results = {}
    
    # 이미 로컬에 다운로드된 이미지 필터링
    to_download = []
    for url in image_urls:
        if not url:
            results[url] = None
            continue
            
        path = get_image_path(url)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            logger.debug(f"Image already exists: {url} -> {path}")
            results[url] = path
        else:
            to_download.append(url)
    
    if not to_download:
        logger.info("No new images to download")
        return results
    
    logger.info(f"Downloading {len(to_download)} images...")
    
    # 비동기 세션 생성 및 동시 다운로드
    async with aiohttp.ClientSession() as session:
        # 병렬로 다운로드 작업 실행
        tasks = [download_image(session, url) for url in to_download]
        downloads = await asyncio.gather(*tasks)
        
        # 결과 처리
        for url, success, path in downloads:
            results[url] = path if success else None
    
    # 다운로드 결과 요약
    success_count = sum(1 for path in results.values() if path)
    logger.info(f"Downloaded {success_count}/{len(image_urls)} images successfully")
    
    return results

async def predownload_kogift_images(product_list: List[Dict]) -> Dict[str, Optional[str]]:
    """고려기프트 제품 이미지를 미리 다운로드"""
    if not PREDOWNLOAD_KOGIFT_IMAGES:
        logger.info("Pre-downloading of 고려기프트 images is disabled in config")
        return {}
        
    logger.info(f"Pre-downloading images for {len(product_list)} 고려기프트 products")
    
    # 모든 이미지 URL 추출
    image_urls = []
    for product in product_list:
        # 여러 필드명 지원 (하위 호환성)
        img_url = None
        for field in ['image', 'image_path', 'src']:
            if field in product and product[field]:
                img_url = product[field]
                break
                
        if img_url:
            # 고려기프트 도메인 URL 처리
            if KOGIFT_SPECIAL_DOMAIN_HANDLING and "kogift" in img_url.lower():
                parsed_url = urlparse(img_url)
                if not parsed_url.scheme:  # URL에 스킴이 없는 경우
                    img_url = "https://" + img_url
            
            image_urls.append(img_url)
    
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
        
        # 샘플 URL 검증
        async with aiohttp.ClientSession() as session:
            tasks = [verify_image_url(session, url) for url in urls_to_verify]
            verify_results = await asyncio.gather(*tasks)
            
            # 검증 결과 확인
            invalid_count = sum(1 for _, valid, _ in verify_results if not valid)
            if invalid_count:
                logger.warning(f"Found {invalid_count}/{len(urls_to_verify)} invalid Kogift image URLs in sample")
                # 문제가 많은 경우 경고 출력
                if invalid_count > len(urls_to_verify) // 2:
                    logger.error(f"More than 50% of Kogift image URLs are invalid! Consider checking the source.")
                
                # 문제가 많아도 계속 진행 (실패 이미지는 download_image에서 필터링됨)
    
    # 이미지 다운로드
    image_paths = await download_images(unique_urls)
    
    # 성공적으로 다운로드한 이미지 수 계산
    success_count = sum(1 for path in image_paths.values() if path is not None)
    logger.info(f"Pre-downloaded {success_count}/{len(unique_urls)} 고려기프트 images")
    
    return image_paths

async def main():
    """테스트 함수"""
    # 테스트 이미지 URL 목록
    test_urls = [
        "https://www.kogift.com/web/product/big/202010/758bfe210ff0765832a812a6f4893762.jpg",
        "https://www.kogift.com/web/product/extra/small/202010/92b2c92a05c3b4cc7b84a0b763784332.jpg",
        "https://img.kogift.com/web/product/medium/202105/210edd76a72d2356f9d3af01da6c5dcb.jpg",
        # 잘못된 URL 테스트
        "https://example.com/not-an-image.html",
        # 존재하지 않는 이미지 URL
        "https://www.kogift.com/nonexistent-image.jpg"
    ]
    
    logger.info("Testing image URL verification...")
    async with aiohttp.ClientSession() as session:
        for url in test_urls:
            url, valid, error_msg = await verify_image_url(session, url)
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

# 스크립트가 직접 실행될 때만 메인 함수 호출
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main()) 