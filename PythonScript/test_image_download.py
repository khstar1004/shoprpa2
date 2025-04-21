"""
Test script for downloading images from Kogift
This script is independent and doesn't require any imports from the main application.
"""

import requests
import os
import time
import random
import logging
from urllib.parse import urlparse, urljoin

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        
        # Full path to save the image
        save_path = os.path.join(save_dir, filename)
        
        # Check if file already exists
        if os.path.exists(save_path):
            logger.debug(f"Image already exists at {save_path}")
            return save_path
            
        # Download image
        response = requests.get(img_url, stream=True, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Check if response contains image data
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"Content-Type: {content_type} for URL: {img_url}")
        
        # Save image
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Image downloaded successfully: {save_path}")
        return save_path
        
    except Exception as e:
        logger.error(f"Failed to download image from {img_url}: {e}")
        return None

def normalize_kogift_image_url(img_url, base_url="https://www.kogift.com"):
    """
    Kogift 이미지 URL을 표준화하고 유효성을 검사합니다.
    
    Args:
        img_url: 원본 이미지 URL 또는 경로
        base_url: 기본 도메인 URL
        
    Returns:
        (normalized_url, is_valid): 정규화된 URL과 유효성 여부
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

def main():
    # Test URLs
    test_urls = [
        "https://koreagift.com/ez/upload/mall/shop_1707873892937710_0.jpg",
        "https://koreagift.com/upload/mall/shop_1736386408518966_0.jpg",  # Missing /ez/ path
        "https://adpanchok.co.kr/upload/mall/shop_1234567890_0.jpg"
    ]
    
    print(f"Testing direct image download for {len(test_urls)} URLs")
    
    # Create save directory
    save_dir = 'downloaded_images'
    os.makedirs(save_dir, exist_ok=True)
    
    # Normalize URLs and download images
    results = {}
    for url in test_urls:
        # Normalize URL
        normalized_url, is_valid = normalize_kogift_image_url(url)
        print(f"Original: {url}")
        print(f"Normalized: {normalized_url} (Valid: {is_valid})")
        
        if is_valid:
            # Download image
            path = download_image(normalized_url, save_dir)
            if path:
                results[url] = path
    
    # Print results
    print(f"\nDownload results: {len(results)}/{len(test_urls)} successful")
    for url, path in results.items():
        print(f"URL: {url}")
        print(f"Saved to: {path}")
        print(f"File exists: {os.path.exists(path)}")
        if os.path.exists(path):
            size_kb = os.path.getsize(path) / 1024
            print(f"File size: {size_kb:.2f} KB")
        print("-" * 50)

if __name__ == "__main__":
    main() 