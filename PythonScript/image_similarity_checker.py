import os
import sys
import requests
from bs4 import BeautifulSoup
import logging
import re
from urllib.parse import urljoin, urlparse
import time
import hashlib
import tempfile
import argparse
import configparser
from typing import Union, Optional, Dict, List, Tuple, Any

# Import centralized utilities
from utils import get_requests_session, download_image, load_config
from image_utils import calculate_image_similarity

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load config
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
config.read(config_path, encoding='utf-8')

# Get image directory from config
target_image_dir = config.get('Matching', 'images_dir', fallback='C:\\RPA\\Image\\Target')
logging.info(f"Using target image directory from config: {target_image_dir}")

# Create temporary directory - either use a subfolder of the target directory or system temp
if os.path.exists(target_image_dir) and os.access(target_image_dir, os.W_OK):
    TEMP_IMAGE_DIR = os.path.join(target_image_dir, 'temp')
else:
    TEMP_IMAGE_DIR = os.path.join(tempfile.gettempdir(), 'shoprpa_image_similarity_temp')
    
logging.info(f"Using temporary image directory: {TEMP_IMAGE_DIR}")
os.makedirs(TEMP_IMAGE_DIR, exist_ok=True)

# Network timeouts - try to load from config
try:
    CONNECT_TIMEOUT = config.getfloat('Network', 'connect_timeout', fallback=5)
    READ_TIMEOUT = config.getfloat('Network', 'read_timeout', fallback=15)
except (configparser.Error, ValueError):
    CONNECT_TIMEOUT = 5
    READ_TIMEOUT = 15

# --- Haeoeum Scraper Logic (Specific to this script's purpose) ---
HAEOEUM_BASE_URL = "http://www.jclgift.com"
HAEOEUM_SELECTORS = {
    "main_image": "img#target_img, img[style*='cursor:hand'][onclick*='view_big'], img[width='330'][height='330']",
    "alt_main_image": 'td[height="340"] img',
}
HAEOEUM_PATTERNS = {
    "onclick_image": re.compile(r"view_big\('([^']+)'", re.IGNORECASE),
}

def _is_valid_haeoeum_image_url(url: str) -> bool:
    """Checks if a URL is likely a valid product image from Haeoeum."""
    if not url or not isinstance(url, str):
        return False
    url = url.strip().lower()
    if not url.startswith(('http://', 'https://', '/')):
        return False
    # Check extension only if it has one
    parsed_path = urlparse(url).path
    _, ext = os.path.splitext(parsed_path)
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif']
    if ext and ext not in image_extensions:
        return False
    # Avoid icons, buttons, etc.
    forbidden_patterns = ['icon', 'button', 'btn_', 'pixel.gif', 'spacer.gif', 'no_image', '_s.'] # Added _s. for small thumbs
    if any(pattern in url for pattern in forbidden_patterns):
        return False
    # Domain check only needed for absolute URLs
    if url.startswith('http') and "jclgift.com" not in urlparse(url).netloc:
         return False
    return True

def extract_main_image_url_haeoeum(product_url: str, session: requests.Session) -> Union[str, None]:
    """Extracts the main product image URL from a Haeoeum Gift product page."""
    if not product_url or "product_view.asp" not in product_url:
        logging.warning(f"Invalid or non-product Haeoeum URL: {product_url}")
        return None

    try:
        # Use configured timeouts
        response = session.get(product_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False) # verify=False might be needed for older sites
        response.raise_for_status()

        # Handle encoding (common issue with older Korean sites)
        if not response.encoding or response.encoding.lower() == 'iso-8859-1':
             try:
                 import chardet # Optional dependency
                 detected = chardet.detect(response.content)
                 response.encoding = detected['encoding'] if detected and detected['confidence'] > 0.7 else 'cp949'
                 logging.debug(f"Detected encoding: {response.encoding} for {product_url}")
             except ImportError:
                 logging.warning("chardet library not found, falling back to cp949 encoding.")
                 response.encoding = 'cp949' # Default fallback for Korean
             except Exception as chardet_err:
                  logging.warning(f"Error detecting encoding: {chardet_err}, using cp949.")
                  response.encoding = 'cp949'

        soup = BeautifulSoup(response.content, "html.parser") # Use response.content to respect detected encoding
        extracted_images = []

        # 1. Primary Main Image Selector
        main_image = soup.select_one(HAEOEUM_SELECTORS["main_image"])
        if main_image:
            img_src = main_image.get('src')
            if img_src:
                extracted_images.append(urljoin(HAEOEUM_BASE_URL, img_src.strip()))

            # Check onclick for larger image
            onclick = main_image.get('onclick', '')
            if onclick:
                onclick_match = HAEOEUM_PATTERNS["onclick_image"].search(onclick)
                if onclick_match:
                    big_img_path = onclick_match.group(1)
                    if big_img_path:
                        # Construct absolute URL correctly
                        big_img_url = urljoin(HAEOEUM_BASE_URL + "/", big_img_path.strip().lstrip('./'))
                        extracted_images.append(big_img_url)

        # 2. Alternative Main Image Selector
        alt_image = soup.select_one(HAEOEUM_SELECTORS["alt_main_image"])
        if alt_image and alt_image.get('src'):
             extracted_images.append(urljoin(HAEOEUM_BASE_URL, alt_image.get('src').strip()))

        # 3. Look for other large images within the main content area (heuristic)
        # Example: Find images within a table structure often used
        content_table = soup.find('table', width='717') # Example heuristic selector
        if content_table:
            for img in content_table.select('img[src]'):
                 img_src = img.get('src')
                 if img_src:
                    full_url = urljoin(HAEOEUM_BASE_URL, img_src.strip())
                    # Basic dimension check (heuristic for larger images)
                    width = img.get('width')
                    height = img.get('height')
                    try:
                        if width and int(width) > 100 or height and int(height) > 100:
                             extracted_images.append(full_url)
                    except ValueError:
                         pass # Ignore if width/height are not integers

        # Find the first valid image URL from the extracted list
        logging.debug(f"Extracted potential image URLs for {product_url}: {extracted_images}")
        for img_url in extracted_images:
            if _is_valid_haeoeum_image_url(img_url):
                # Resolve potential relative URLs
                absolute_img_url = urljoin(HAEOEUM_BASE_URL + "/", img_url)
                logging.info(f"Found valid main image for {product_url}: {absolute_img_url}")
                return absolute_img_url

        logging.warning(f"No valid main image found for {product_url} after checking {len(extracted_images)} candidates.")
        return None

    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout requesting {product_url}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {product_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error parsing {product_url}: {e}", exc_info=True)
        return None

# --- Main Workflow ---
def get_image_similarity_for_urls(url1: str, url2: str) -> Union[float, None]:
    """
    Gets the main images from two Haeoeum URLs, downloads them temporarily,
    and calculates their similarity using centralized utilities.

    Args:
        url1: The first Haeoeum product URL.
        url2: The second Haeoeum product URL.

    Returns:
        The similarity score (0.0 to 1.0) or None if an error occurs.
    """
    session = get_requests_session() # Use utility session
    image_path1 = None
    image_path2 = None
    similarity = None

    try:
        # 1. Extract Image URLs
        logging.info(f"Extracting image URL from: {url1}")
        img_url1 = extract_main_image_url_haeoeum(url1, session)
        if not img_url1:
            logging.error(f"Could not extract image URL from {url1}")
            return None

        logging.info(f"Extracting image URL from: {url2}")
        img_url2 = extract_main_image_url_haeoeum(url2, session)
        if not img_url2:
            logging.error(f"Could not extract image URL from {url2}")
            return None

        # 2. Download Images Temporarily
        logging.info(f"Downloading image 1: {img_url1}")
        # Create temporary file paths
        _, file_ext1 = os.path.splitext(urlparse(img_url1).path)
        temp_file1 = tempfile.NamedTemporaryFile(delete=False, suffix=file_ext1 or '.jpg', dir=TEMP_IMAGE_DIR)
        image_path1 = temp_file1.name
        temp_file1.close() # Close handle so download can write

        if not download_image(img_url1, image_path1, config): # Pass config to the download_image function
            logging.error(f"Failed to download image from {img_url1}")
            return None

        logging.info(f"Downloading image 2: {img_url2}")
        _, file_ext2 = os.path.splitext(urlparse(img_url2).path)
        temp_file2 = tempfile.NamedTemporaryFile(delete=False, suffix=file_ext2 or '.jpg', dir=TEMP_IMAGE_DIR)
        image_path2 = temp_file2.name
        temp_file2.close()

        if not download_image(img_url2, image_path2, config): # Pass config to the download_image function
            logging.error(f"Failed to download image from {img_url2}")
            return None

        # 3. Calculate Similarity
        logging.info(f"Calculating similarity between {image_path1} and {image_path2}")
        similarity = calculate_image_similarity(image_path1, image_path2) # Use utility function
        logging.info(f"Calculated Similarity: {similarity:.4f}")

        return similarity

    except Exception as e:
        logging.error(f"An error occurred in get_image_similarity_for_urls: {e}", exc_info=True)
        return None # Return None on general errors

    finally:
        # 4. Clean up temporary files
        if image_path1 and os.path.exists(image_path1):
            try:
                os.remove(image_path1)
                logging.debug(f"Cleaned up temporary file: {image_path1}")
            except OSError as e:
                logging.warning(f"Could not remove temporary file {image_path1}: {e}")
        if image_path2 and os.path.exists(image_path2):
            try:
                os.remove(image_path2)
                logging.debug(f"Cleaned up temporary file: {image_path2}")
            except OSError as e:
                logging.warning(f"Could not remove temporary file {image_path2}: {e}")

# --- Command-Line Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate image similarity between two Haeoeum product URLs.")
    parser.add_argument("url1", help="First Haeoeum product URL")
    parser.add_argument("url2", help="Second Haeoeum product URL")
    args = parser.parse_args()

    logging.info(f"Starting comparison for:\nURL1: {args.url1}\nURL2: {args.url2}")

    similarity_score = get_image_similarity_for_urls(args.url1, args.url2)

    if similarity_score is not None:
        print(f"\nImage Similarity Score: {similarity_score:.4f}")
    else:
        print("\nFailed to calculate image similarity. Check logs for details.")
        sys.exit(1) # Exit with error code

    sys.exit(0) # Exit successfully 