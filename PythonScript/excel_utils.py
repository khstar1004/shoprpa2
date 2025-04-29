import os
import logging
import pandas as pd
import datetime
import openpyxl
from openpyxl.styles import Alignment, Border, Side, Font, PatternFill, NamedStyle
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
import hashlib
from urllib.parse import urlparse
from PIL import Image
import functools
from functools import wraps
import configparser
import time
import re
from pathlib import Path
import traceback
import uuid
import tempfile
import requests
from typing import Optional

# Check Python/PIL version for proper resampling constant
try:
    # Python 3.10+ with newer Pillow
    RESAMPLING_FILTER = Image.Resampling.LANCZOS
except (AttributeError, ImportError):
    try:
        # Older Pillow versions
        RESAMPLING_FILTER = Image.LANCZOS
    except (AttributeError, ImportError):
        # Very old Pillow versions
        RESAMPLING_FILTER = Image.ANTIALIAS

# Initialize logger
logger = logging.getLogger(__name__)

# Define constants for image source directory names
HAEREUM_DIR_NAME = 'Haereum'
KOGIFT_DIR_NAME = 'Kogift' # Changed from kogift_pre / Kogift
NAVER_DIR_NAME = 'Naver'   # Changed to lowercase 'naver'
OTHER_DIR_NAME = 'Other'


# Initialize config parser
CONFIG = configparser.ConfigParser()
config_ini_path = Path(__file__).resolve().parent.parent / 'config.ini'
try:
    CONFIG.read(config_ini_path, encoding='utf-8')
    logger.info(f"Successfully loaded configuration from {config_ini_path}")
    # Get main paths from config
    IMAGE_MAIN_DIR = Path(CONFIG.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'))
except Exception as e:
    logger.error(f"Error loading config from {config_ini_path}: {e}, using default values")
    # Default directory if config fails
    IMAGE_MAIN_DIR = Path('C:\\RPA\\Image\\Main') 

# --- Constants ---
PROMO_KEYWORDS = ['판촉', '기프트', '답례품', '기념품', '인쇄', '각인', '제작', '호갱', '몽키', '홍보']

# Column Rename Mapping (Update based on 엑셀골든_upload and potential variations)
# Ensure keys cover variations, values match FINAL_COLUMN_ORDER_UPLOAD
COLUMN_RENAME_MAP = {
    # 구분
    '구분': '구분',
    '구분(승인관리:A/가격관리:P)': '구분',
    # 담당자
    '담당자': '담당자',
    # 공급사명 (Input source? 네이버 공급사명 is separate)
    '업체명': '공급사명', # Assuming '업체명' from input maps to '공급사명' in output
    # 공급처코드
    '업체코드': '공급처코드',
    # 상품코드
    'Code': '상품코드',
    '상품코드': '상품코드',
    # 카테고리(중분류)
    '중분류카테고리': '카테고리(중분류)',
    '카테고리(중분류)': '카테고리(중분류)',
    # 상품명
    '상품명': '상품명',
    'name': '상품명',
    # 본사 기본수량
    '기본수량(1)': '본사 기본수량',
    '본사 기본수량': '본사 기본수량',
    # 판매단가1(VAT포함)
    '판매단가(V포함)': '판매단가1(VAT포함)',
    '판매단가1(VAT포함)': '판매단가1(VAT포함)',
    # 본사링크
    '본사상품링크': '본사링크',
    '본사링크': '본사링크',
    # 고려 기본수량
    '기본수량(2)': '고려 기본수량',
    '고려 기본수량': '고려 기본수량',
    # 판매단가2(VAT포함)
    '판매가(V포함)(2)': '판매단가2(VAT포함)',
    '판매단가(V포함)(2)': '판매단가2(VAT포함)',
    '판매단가2(VAT포함)': '판매단가2(VAT포함)',
    # 고려 가격차이
    '가격차이(2)': '고려 가격차이',
    # 고려 가격차이(%)
    '가격차이(2)(%)': '고려 가격차이(%)',
    # 고려 링크
    '고려기프트 상품링크': '고려 링크',
    '고려 링크': '고려 링크',
    # 네이버 기본수량
    '기본수량(3)': '네이버 기본수량',
    '네이버 기본수량': '네이버 기본수량',
    # 판매단가3 (VAT포함)
    '판매단가(V포함)(3)': '판매단가3 (VAT포함)',
    '판매단가3 (VAT포함)': '판매단가3 (VAT포함)',
    # 네이버 가격차이
    '가격차이(3)': '네이버 가격차이',
    # 네이버가격차이(%)
    '가격차이(3)(%)': '네이버가격차이(%)',
    # 네이버 공급사명
    '공급사명': '네이버 공급사명', # Note: Input '공급사명' maps to '네이버 공급사명' here? Review logic if needed.
    '네이버 공급사명': '네이버 공급사명',
    # 네이버 링크 (Input '공급사 상품링크' might map here?)
    '네이버 쇼핑 링크': '네이버 링크',
    '네이버 링크': '네이버 링크',
    '공급사 상품링크': '네이버 링크', # Check if this mapping is correct
    # 해오름(이미지링크)
    '본사 이미지': '해오름(이미지링크)',
    '해오름이미지경로': '해오름(이미지링크)',
    # 고려기프트(이미지링크)
    '고려기프트 이미지': '고려기프트(이미지링크)',
    '고려기프트(이미지링크)': '고려기프트(이미지링크)',
    # 네이버쇼핑(이미지링크)
    '네이버 이미지': '네이버쇼핑(이미지링크)',
    '네이버쇼핑(이미지링크)': '네이버쇼핑(이미지링크)'
}

# Final Target Column Order (Based on "엑셀골든_upload") - Use this for both files now?
# Let's define two orders: one for upload, one for result? Or just use upload for both?
# For now, update FINAL_COLUMN_ORDER to match the upload format strictly.
FINAL_COLUMN_ORDER = [
    '구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드',
    '카테고리(중분류)', '상품명', '본사 기본수량', '판매단가1(VAT포함)', '본사링크',
    '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)', '고려 링크',
    '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)',
    '네이버 공급사명', '네이버 링크',
    '해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'
]

# Columns that must be present in the input file for processing
# Update this based on the new FINAL_COLUMN_ORDER if necessary,
# focusing on the absolutely essential input fields needed.
REQUIRED_INPUT_COLUMNS = [
    # Keep the original core requirements, renaming handled by COLUMN_RENAME_MAP
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리',
    '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
]

# --- Column Type Definitions for Formatting ---
# Update these lists based on the new FINAL_COLUMN_ORDER names
PRICE_COLUMNS = [
    '판매단가1(VAT포함)', '판매단가2(VAT포함)', '판매단가3 (VAT포함)',
    '고려 가격차이', '네이버 가격차이'
]
QUANTITY_COLUMNS = ['본사 기본수량', '고려 기본수량', '네이버 기본수량']
PERCENTAGE_COLUMNS = ['고려 가격차이(%)', '네이버가격차이(%)']
TEXT_COLUMNS = ['구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드', '카테고리(중분류)', '상품명', '네이버 공급사명']
LINK_COLUMNS_FOR_HYPERLINK = {
    '본사링크': '본사링크',
    '고려 링크': '고려 링크',
    '네이버 링크': '네이버 링크'
    # Image columns handled separately
}
IMAGE_COLUMNS = ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']

# Error Messages Constants (Can be used for conditional formatting or checks)
ERROR_MESSAGES = {
    'no_match': '가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음',
    'no_price_match': '가격이 범위내에 없거나 검색된 상품이 없음',
    'low_similarity': '일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음',
    'no_results': '검색 결과 0',
    'no_image': '이미지를 찾을 수 없음',
    'file_not_found': '-이미지 없음-',
    'invalid_image': '유효하지 않은 이미지 형식',
    'processing_error': '-처리 오류-',
    'too_small': '이미지 크기가 너무 작음 (저해상도)',
    'format_error': '지원하지 않는 이미지 형식',
    'download_failed': '이미지 다운로드 실패',
    'excel_limit': '이미지 크기가 Excel 제한을 초과함'
}
ERROR_MESSAGE_VALUES = list(ERROR_MESSAGES.values()) # Cache list for faster checking

# --- Styling Constants ---
HEADER_FILL = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid") # Light green fill
HEADER_FONT = Font(bold=True, size=11, name='맑은 고딕')
HEADER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)

# Define alignments based on column type
LEFT_ALIGNMENT = Alignment(horizontal="left", vertical="center", wrap_text=True)
CENTER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)
RIGHT_ALIGNMENT = Alignment(horizontal="right", vertical="center", wrap_text=False) # Numbers right-aligned

DEFAULT_FONT = Font(name='맑은 고딕', size=10)

THIN_BORDER_SIDE = Side(style='thin')
DEFAULT_BORDER = Border(left=THIN_BORDER_SIDE, right=THIN_BORDER_SIDE, top=THIN_BORDER_SIDE, bottom=THIN_BORDER_SIDE)

LINK_FONT = Font(color="0000FF", underline="single", name='맑은 고딕', size=10)
INVALID_LINK_FONT = Font(color="FF0000", name='맑은 고딕', size=10) # Red for invalid links

NEGATIVE_PRICE_FILL = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid") # Yellow fill for negative diff < -1

# Image Processing Constants
IMAGE_COLUMNS = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
IMAGE_MAX_SIZE = (1200, 1200)  # Excel 2021 maximum supported image size
IMAGE_STANDARD_SIZE = (200, 200)  # Standard display size in Excel
IMAGE_QUALITY = 85  # JPEG compression quality
SUPPORTED_IMAGE_FORMATS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']  # Supported by Excel 2021

# Image cell specific styling
IMAGE_CELL_HEIGHT = 180  # Increased from 120 to accommodate both image and link
IMAGE_CELL_WIDTH = 22   # Column width for image cells

# --- Utility Functions ---

def retry_on_failure(max_retries=3, delay=1):
    """Decorator for retrying functions on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Function {func.__name__} failed after {max_retries} attempts: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

@retry_on_failure()
def find_excel_file(directory: str, extension: str = '.xlsx') -> Optional[str]:
    """Find the first Excel file with the specified extension in the directory."""
    try:
        # Ignore temporary Excel files starting with ~$
        files = [f for f in os.listdir(directory) if f.lower().endswith(extension) and not f.startswith('~$')]
        return files[0] if files else None
    except Exception as e:
        logger.error(f"Error finding Excel file in '{directory}': {str(e)}")
        raise

# validate_excel_file is now handled by check_excel_file.py
# def validate_excel_file(...) -> removed

def convert_text_to_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """(Deprecated/Simplified) Initial conversion, formatting is now primarily handled in _prepare_data_for_excel."""
    logger.debug("Skipping deprecated convert_text_to_numbers function. Formatting handled in _prepare_data.")
    return df

def preprocess_product_name(name: str) -> str:
    """Preprocess product name (basic cleaning)."""
    if not isinstance(name, str):
        return str(name)
    # Keep basic cleaning, more advanced logic might be in matching modules
    return re.sub(r'[\(\)\[\]{}]+', '', name).strip() # Example: Remove only brackets


# --- Core Excel Creation Logic ---

def _apply_column_widths(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Sets appropriate column widths based on column names/types."""
    # Define width hints (can be adjusted)
    width_hints = {
        'image': 15, # Width for image columns (images are scaled)
        'name': 45,  # 상품명
        'link': 35,
        'price': 14,
        'percent': 10,
        'quantity': 10,
        'code': 12,
        'category': 20,
        'text_short': 12, # 구분, 담당자 등
        'default': 15
    }
    logger.debug(f"Applying column widths. DataFrame columns: {df.columns.tolist()}")
    for idx, col_name in enumerate(df.columns, 1):
        column_letter = get_column_letter(idx)
        width = width_hints['default'] # Default width

        col_name_str = str(col_name) # Ensure col_name is string for checks

        # Determine width based on column name patterns
        if col_name_str in IMAGE_COLUMNS:
            width = width_hints['image']
        elif '상품명' in col_name_str:
            width = width_hints['name']
        elif col_name_str in LINK_COLUMNS_FOR_HYPERLINK or '링크' in col_name_str:
            width = width_hints['link']
        elif col_name_str in PRICE_COLUMNS:
            width = width_hints['price']
        elif col_name_str in PERCENTAGE_COLUMNS:
            width = width_hints['percent']
        elif col_name_str in QUANTITY_COLUMNS:
             width = width_hints['quantity']
        elif 'Code' in col_name_str or '코드' in col_name_str:
            width = width_hints['code']
        elif '카테고리' in col_name_str:
            width = width_hints['category']
        elif col_name_str in ['구분', '담당자']:
            width = width_hints['text_short']
        # Add more specific rules if needed

        worksheet.column_dimensions[column_letter].width = width
        # logger.debug(f"Set width for column '{col_name_str}' ({column_letter}) to {width}") # Reduce log verbosity
    logger.debug("Finished applying column widths.")

def _apply_cell_styles_and_alignment(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Applies formatting (font, border, alignment) to header and data cells."""
    logger.debug("Applying cell styles and alignments.")
    # Header Styling
    for cell in worksheet[1]: # First row is header
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGNMENT
        cell.border = DEFAULT_BORDER

    # Data Cell Styling
    for row_idx in range(2, worksheet.max_row + 1):
        for col_idx, col_name in enumerate(df.columns, 1):
            cell = worksheet.cell(row=row_idx, column=col_idx)
            cell.font = DEFAULT_FONT
            cell.border = DEFAULT_BORDER

            # Apply alignment based on column type
            col_name_str = str(col_name)
            is_pct_col = col_name_str in ['가격차이(2)(%)', '가격차이(3)(%)'] # Explicit check for percentage columns

            # Check if the cell value is likely numeric (ignoring error messages)
            is_numeric_value = False
            cell_value_str = str(cell.value)
            if cell_value_str not in ERROR_MESSAGE_VALUES and cell_value_str != '-':
                 # Basic check if it looks like a number (might need refinement)
                 try:
                      float(cell_value_str.replace(',', '').replace('%',''))
                      is_numeric_value = True
                 except ValueError:
                      is_numeric_value = False

            # Apply right alignment to numbers and specifically formatted percentage strings
            # Use the updated column names
            if is_pct_col or ((col_name_str in PRICE_COLUMNS or col_name_str in QUANTITY_COLUMNS) and is_numeric_value):
                cell.alignment = RIGHT_ALIGNMENT
            # Update checks for center alignment based on new names
            elif col_name_str in IMAGE_COLUMNS or '코드' in col_name_str or col_name_str == '구분(승인관리:A/가격관리:P)':
                 cell.alignment = CENTER_ALIGNMENT
            else:
                cell.alignment = LEFT_ALIGNMENT # Default left align for text/links/errors
    logger.debug("Finished applying cell styles.")

def _process_image_columns(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Processes image columns and embeds images into the worksheet."""
    logger.debug("Processing image columns...")
    
    # Get indices of image columns
    image_column_indices = {}
    for col_name in IMAGE_COLUMNS:
        if col_name in df.columns:
            idx = list(df.columns).index(col_name) + 1  # Excel is 1-indexed
            image_column_indices[col_name] = idx
            logger.debug(f"Found image column: {col_name} at index {idx}")

    if not image_column_indices:
        logger.debug("No image columns found in DataFrame")
        return

    # Image size settings - increased for better visibility
    img_width = 150  # Increased from 120
    img_height = 150  # Increased from 120

    # For each row in the data
    for row_idx in range(2, worksheet.max_row + 1):  # Start from 2 to skip header
        # Process each image column
        for col_name, col_idx in image_column_indices.items():
            image_path_to_embed = None # Reset for each cell
            cell = None # Ensure cell is defined
            try:
                # Get the cell value (image path or dictionary)
                cell = worksheet.cell(row=row_idx, column=col_idx)
                original_value = cell.value
                
                logger.debug(f"Processing cell {cell.coordinate} ({col_name}). Value type: {type(original_value)}, Value: '{str(original_value)[:100]}...'")
                
                # Skip empty cells or explicit error messages/placeholders
                if original_value is None or str(original_value).strip() == '' or str(original_value) == '-' or \
                   (isinstance(original_value, str) and any(err in original_value for err in ERROR_MESSAGE_VALUES)):
                    logger.debug(f"  Skipping cell {cell.coordinate}: Empty or error/placeholder value.")
                    continue
                
                img_dict = None
                # --- Priority 1: Handle Dictionary Format --- 
                if isinstance(original_value, dict):
                    img_dict = original_value
                    logger.debug(f"  Cell {cell.coordinate} contains dictionary: {img_dict}")
                elif isinstance(original_value, str) and original_value.startswith('{') and original_value.endswith('}'):
                    try:
                        import ast
                        parsed_dict = ast.literal_eval(original_value)
                        if isinstance(parsed_dict, dict):
                            img_dict = parsed_dict
                            logger.debug(f"  Cell {cell.coordinate} parsed dictionary from string: {img_dict}")
                        else:
                            logger.warning(f"  Cell {cell.coordinate} contained dictionary-like string but parsing failed.")
                    except (SyntaxError, ValueError) as e:
                        logger.warning(f"  Cell {cell.coordinate} failed to parse dictionary-like string: {e}")

                # --- Process Dictionary if found ---
                if img_dict:
                    # First, try to use local_path if available
                    local_path = img_dict.get('local_path')
                    if local_path and isinstance(local_path, str) and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        image_path_to_embed = local_path
                        logger.info(f"  Using local_path from dictionary for cell {cell.coordinate}: {image_path_to_embed}")
                    else:
                        logger.warning(f"  Dictionary in cell {cell.coordinate} missing valid local_path: {local_path}")
                        # Get source to determine image directory path
                        source = img_dict.get('source', '').lower()
                        
                        # Try to find the image by searching in the correct directory
                        search_dir = None
                        if source in ['haereum', 'haoreum']:
                            search_dir = IMAGE_MAIN_DIR / HAEREUM_DIR_NAME
                        elif source in ['kogift', 'koreagift']:
                            search_dir = IMAGE_MAIN_DIR / KOGIFT_DIR_NAME
                        elif source == 'naver':
                            search_dir = IMAGE_MAIN_DIR / NAVER_DIR_NAME
                        else:
                            search_dir = IMAGE_MAIN_DIR
                        
                        # Try to find an existing image file that matches our pattern
                        if search_dir and search_dir.exists(): # Check if search_dir is not None
                            # Extract filename from original_path if available
                            original_path = img_dict.get('original_path', '')
                            if original_path and isinstance(original_path, str):
                                original_filename = os.path.basename(original_path)
                                # Try direct filename match
                                potential_file = search_dir / original_filename
                                if potential_file.exists() and potential_file.stat().st_size > 0:
                                    image_path_to_embed = str(potential_file)
                                    logger.info(f"  Found image using original_path filename: {image_path_to_embed}")
                            
                            # If still not found, try URL-based naming pattern
                            if not image_path_to_embed:
                                url = img_dict.get('url', '')
                                if url and isinstance(url, str):
                                    # Get URL hash for filename pattern matching
                                    url_hash = hashlib.md5(url.encode('utf-8', errors='ignore')).hexdigest()[:10]
                                    
                                    # For each file in directory, check if it matches the pattern with hash
                                    pattern_prefix = f"{source}_" if source else ""
                                    
                                    # Try to find file by pattern matching (looser match)
                                    matching_files = []
                                    for file in search_dir.glob(f"{pattern_prefix}*{url_hash}*"):
                                        if file.is_file() and file.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif']:
                                            matching_files.append(file)
                                    
                                    # If multiple matches, prefer non-nobg version
                                    if matching_files:
                                        # Sort by whether 'nobg' is in the filename (non-nobg first)
                                        matching_files.sort(key=lambda f: 'nobg' in f.name.lower())
                                        image_path_to_embed = str(matching_files[0])
                                        logger.info(f"  Found image by URL hash pattern matching: {image_path_to_embed}")
                        
                        # If we still haven't found a local file, attempt download from URL if available
                        if not image_path_to_embed:
                            url = img_dict.get('url')
                            if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                                logger.info(f"  Attempting download from URL in dictionary: {url}")
                                try:
                                    # Create proper download directory based on source
                                    if source in ['haereum', 'haoreum'] or 'jclgift' in url.lower():
                                        save_dir = IMAGE_MAIN_DIR / HAEREUM_DIR_NAME
                                    elif source in ['kogift', 'koreagift'] or any(kw in url.lower() for kw in ['kogift', 'koreagift', 'adpanchok']):
                                        save_dir = IMAGE_MAIN_DIR / KOGIFT_DIR_NAME
                                    elif source == 'naver' or 'pstatic' in url.lower():
                                        save_dir = IMAGE_MAIN_DIR / NAVER_DIR_NAME
                                    else:
                                        save_dir = IMAGE_MAIN_DIR / OTHER_DIR_NAME
                                    
                                    # Ensure directory exists
                                    save_dir.mkdir(parents=True, exist_ok=True)
                                    
                                    # Generate filename from URL
                                    url_hash = hashlib.md5(url.encode('utf-8', errors='ignore')).hexdigest()[:10]
                                    ext = os.path.splitext(urlparse(url).path)[1].lower()
                                    if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                                        ext = '.jpg'  # Default extension
                                    
                                    # Create filename with source prefix
                                    filename = f"{source}_{url_hash}{ext}"
                                    output_path = save_dir / filename
                                    
                                    # Only download if file doesn't exist or is empty
                                    if not output_path.exists() or output_path.stat().st_size < 100:
                                        import requests
                                        # Use session with proper headers
                                        session = requests.Session()
                                        session.headers.update({
                                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                                            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                                            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
                                        })
                                        
                                        # Try to download with timeout
                                        timeout = int(CONFIG.get('Matching', 'download_image_timeout', fallback=30))
                                        response = session.get(url, timeout=timeout, stream=True)
                                        response.raise_for_status()
                                        
                                        # Check content type
                                        content_type = response.headers.get('Content-Type', '')
                                        # 고려기프트, adpanchok 사이트는 text/plain으로 이미지를 반환하므로 예외 처리
                                        is_kogift_url = any(domain in url.lower() for domain in ['koreagift.com', 'adpanchok.co.kr', 'kogift'])
                                        if not content_type.startswith('image/') and not is_kogift_url and not ('jclgift' in url or 'pstatic' in url):
                                            logger.warning(f"  URL doesn't return an image: {content_type}")
                                            cell.value = "이미지 아님 (URL)"
                                            continue
                                        
                                        # Save file
                                        with open(output_path, 'wb') as f:
                                            for chunk in response.iter_content(chunk_size=8192):
                                                if chunk:
                                                    f.write(chunk)
                                        
                                        # Verify download was successful
                                        if output_path.exists() and output_path.stat().st_size > 100:
                                            logger.info(f"  Successfully downloaded image to {output_path}")
                                            image_path_to_embed = str(output_path)
                                        else:
                                            logger.warning(f"  Downloaded file is too small or invalid: {output_path}")
                                            cell.value = "다운로드 실패 (파일 크기)"
                                            continue
                                    else:
                                        logger.info(f"  Using existing downloaded file: {output_path}")
                                        image_path_to_embed = str(output_path)
                                except requests.RequestException as e:
                                    logger.warning(f"  Network error downloading image: {e}")
                                    cell.value = "다운로드 실패 (네트워크)"
                                    continue
                                except Exception as e:
                                    logger.error(f"  Error downloading image: {e}")
                                    cell.value = "다운로드 실패 (기타 오류)"
                                    continue
                            else:
                                cell.value = "이미지 경로 없음 (URL 없음)" 
                                continue
                # --- Priority 2: Handle String Path/URL --- 
                elif isinstance(original_value, str):
                    path_str = original_value.strip()
                    # Check if it's an absolute or relative path that exists
                    if os.path.exists(path_str) and os.path.isfile(path_str) and os.path.getsize(path_str) > 0:
                        image_path_to_embed = path_str
                        logger.info(f"  Using direct file path for cell {cell.coordinate}: {image_path_to_embed}")
                    # Check if it's a URL (simplistic check)
                    elif path_str.startswith(('http://', 'https://')):
                        logger.warning(f"  Cell {cell.coordinate} contains URL string '{path_str[:60]}...'. Attempting to download.")
                        # Use similar logic to the dictionary URL handling
                        try:
                            # Determine source from URL or column name
                            source = 'other'
                            if '본사' in col_name or 'haereum' in path_str or 'jclgift' in path_str: 
                                source = 'haereum'
                                save_dir = IMAGE_MAIN_DIR / HAEREUM_DIR_NAME
                            elif '고려' in col_name or any(kw in path_str.lower() for kw in ['kogift', 'koreagift', 'adpanchok']): 
                                source = 'kogift'
                                save_dir = IMAGE_MAIN_DIR / KOGIFT_DIR_NAME
                            elif '네이버' in col_name or 'naver' in path_str or 'pstatic' in path_str: 
                                source = 'naver'
                                save_dir = IMAGE_MAIN_DIR / NAVER_DIR_NAME
                            else:
                                save_dir = IMAGE_MAIN_DIR / OTHER_DIR_NAME
                            
                            # Ensure directory exists
                            save_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Try to find existing file by hash first
                            url_hash = hashlib.md5(path_str.encode('utf-8', errors='ignore')).hexdigest()[:10]
                            found_path = None
                            
                            # Check if file already exists
                            for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                                potential_file = save_dir / f"{source}_{url_hash}{ext}"
                                if potential_file.exists() and potential_file.stat().st_size > 100:
                                    found_path = str(potential_file)
                                    logger.info(f"  Found existing file for URL: {found_path}")
                                    break
                            
                            if found_path:
                                image_path_to_embed = found_path
                            else:
                                # Download if not found
                                import requests
                                # Generate filename from URL
                                ext = os.path.splitext(urlparse(path_str).path)[1].lower()
                                if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                                    ext = '.jpg'  # Default extension
                                
                                filename = f"{source}_{url_hash}{ext}"
                                output_path = save_dir / filename
                                
                                # Use session with proper headers
                                session = requests.Session()
                                session.headers.update({
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                                    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
                                })
                                
                                # Try to download with timeout
                                timeout = int(CONFIG.get('Matching', 'download_image_timeout', fallback=30))
                                response = session.get(path_str, timeout=timeout, stream=True)
                                response.raise_for_status()
                                
                                # Save file
                                with open(output_path, 'wb') as f:
                                    for chunk in response.iter_content(chunk_size=8192):
                                        if chunk:
                                            f.write(chunk)
                                
                                # Verify download was successful
                                if output_path.exists() and output_path.stat().st_size > 100:
                                    logger.info(f"  Successfully downloaded image to {output_path}")
                                    image_path_to_embed = str(output_path)
                                else:
                                    logger.warning(f"  Downloaded file is too small or invalid: {output_path}")
                                    cell.value = "다운로드 실패 (파일 크기)"
                                    continue
                        except Exception as e:
                            logger.error(f"  Error handling URL string: {e}")
                            cell.value = "URL 처리 오류"
                            continue
                    else:
                        # Try to find the image in one of the standard directories based on column name
                        image_found = False
                        
                        # Determine source folders to search based on column name
                        search_dirs = []
                        if '본사' in col_name:
                            search_dirs = [IMAGE_MAIN_DIR / HAEREUM_DIR_NAME]
                        elif '고려' in col_name:
                            search_dirs = [IMAGE_MAIN_DIR / KOGIFT_DIR_NAME]
                        elif '네이버' in col_name:
                            search_dirs = [IMAGE_MAIN_DIR / NAVER_DIR_NAME]
                        else:
                            search_dirs = [
                                IMAGE_MAIN_DIR / HAEREUM_DIR_NAME,
                                IMAGE_MAIN_DIR / KOGIFT_DIR_NAME,
                                IMAGE_MAIN_DIR / NAVER_DIR_NAME,
                                IMAGE_MAIN_DIR / OTHER_DIR_NAME, # Add Other as potential search dir
                                IMAGE_MAIN_DIR
                            ]
                            
                        # First try direct filename match
                        for search_dir in search_dirs:
                            if search_dir.exists():
                                # Try exact filename
                                exact_match = search_dir / os.path.basename(path_str)
                                if exact_match.exists() and exact_match.stat().st_size > 0:
                                    image_path_to_embed = str(exact_match)
                                    image_found = True
                                    logger.info(f"  Found image by exact filename in {search_dir}: {image_path_to_embed}")
                                    break
                                
                                # If not found, try pattern matching (for product name encoded in filename)
                                # First clean up the product name to use for pattern matching
                                clean_name = re.sub(r'[^\w가-힣]', '_', path_str)[:20]  # Use first 20 chars of cleaned name
                                if clean_name:
                                    for file in search_dir.glob(f"*{clean_name}*"):
                                        if file.is_file() and file.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif']:
                                            image_path_to_embed = str(file)
                                            image_found = True
                                            logger.info(f"  Found image by pattern matching in {search_dir}: {image_path_to_embed}")
                                            break
                                
                                # If still not found, try to scan the directory for any .jpg files (excluding _nobg files)
                                if not image_found:
                                    jpg_files = [f for f in search_dir.glob("*.jpg") if "_nobg" not in f.name]
                                    if jpg_files:
                                        image_path_to_embed = str(jpg_files[0])  # Use the first jpg file
                                        image_found = True
                                        logger.info(f"  Found image by scanning directory {search_dir}: {image_path_to_embed}")
                                        break
                                
                        if not image_found:
                            logger.warning(f"  Invalid path/URL string in cell {cell.coordinate}: '{path_str[:60]}...'")
                            cell.value = "잘못된 이미지 경로"
                            continue
                else:
                    logger.warning(f"  Unsupported value type in cell {cell.coordinate}: {type(original_value)}")
                    cell.value = "지원되지 않는 형식"
                    continue
                
                # --- Embed Image if path is valid ---
                if image_path_to_embed:
                    try:
                        # Verify the image file is valid before embedding
                        with Image.open(image_path_to_embed) as img_check:
                            img_size = img_check.size
                            logger.debug(f"  Verified image {image_path_to_embed}, size: {img_size}")
                            
                            # Skip very small images
                            if img_size[0] < 10 or img_size[1] < 10:
                                logger.warning(f"  Image too small to embed: {img_size} for {image_path_to_embed}")
                                cell.value = "이미지 크기 작음"
                                continue
                            
                            # Excel 2021 compatibility: optimize large or problematic images
                            # Create an optimized version for Excel if needed
                            try_optimize = False
                            # Check if image is very large
                            if img_size[0] > 1000 or img_size[1] > 1000 or os.path.getsize(image_path_to_embed) > 500000:
                                try_optimize = True
                            # Check problematic file formats for Excel
                            img_format = img_check.format
                            if img_format and img_format.lower() not in ['jpeg', 'png']:
                                try_optimize = True
                                
                            if try_optimize:
                                logger.info(f"  Optimizing image for Excel compatibility: {image_path_to_embed}")
                                # Create a temp optimized version
                                import tempfile
                                import uuid
                                
                                # Create optimized version in temp directory
                                temp_dir = Path(tempfile.gettempdir()) / 'excel_image_cache'
                                temp_dir.mkdir(parents=True, exist_ok=True)
                                
                                # Generate unique filename
                                temp_path = temp_dir / f"excel_opt_{uuid.uuid4().hex[:8]}.jpg"
                                
                                # Convert to RGB if needed (Excel doesn't handle RGBA/transparency well)
                                if img_check.mode in ['RGBA', 'LA'] or (img_check.mode == 'P' and 'transparency' in img_check.info):
                                    img_rgb = img_check.convert('RGB')
                                else:
                                    img_rgb = img_check.convert('RGB')
                                
                                # Calculate new dimensions (preserve aspect ratio, max 800px)
                                max_dim = 800
                                if img_size[0] > max_dim or img_size[1] > max_dim:
                                    if img_size[0] > img_size[1]:
                                        new_width = max_dim
                                        new_height = int(img_size[1] * (max_dim / img_size[0]))
                                    else:
                                        new_height = max_dim
                                        new_width = int(img_size[0] * (max_dim / img_size[1]))
                                    
                                    # Resize image
                                    img_resized = img_rgb.resize((new_width, new_height), RESAMPLING_FILTER)
                                else:
                                    img_resized = img_rgb
                                
                                # Save optimized image as JPEG with good quality
                                img_resized.save(temp_path, 'JPEG', quality=85, optimize=True)
                                
                                if temp_path.exists() and temp_path.stat().st_size > 0:
                                    logger.info(f"  Using optimized version for Excel: {temp_path}")
                                    image_path_to_embed = str(temp_path)
                        
                        # Add the image to the worksheet
                        img = openpyxl.drawing.image.Image(image_path_to_embed)
                        img.width = img_width
                        img.height = img_height
                        
                        # Set image anchor to the cell
                        img.anchor = cell.coordinate
                        worksheet.add_image(img)
                        
                        # Keep URLs as plain text, not hyperlinks
                        if img_dict and 'url' in img_dict:
                            cell.value = img_dict['url']
                            # Don't set hyperlink
                            cell.font = DEFAULT_FONT  # Use regular font, not link style
                            cell.alignment = Alignment(horizontal='center', vertical='top', wrap_text=True)
                        elif isinstance(original_value, str) and ('http' in original_value or 'https' in original_value):
                            cell.value = original_value
                            # Don't set hyperlink
                            cell.font = DEFAULT_FONT  # Use regular font, not link style
                            cell.alignment = Alignment(horizontal='center', vertical='top', wrap_text=True)
                        else:
                            cell.value = ""
                        
                        logger.info(f"  Successfully added image {os.path.basename(image_path_to_embed)} to cell {cell.coordinate}")
                        
                    except FileNotFoundError:
                        logger.error(f"  Image file not found at path: {image_path_to_embed}")
                        cell.value = "이미지 파일 없음"
                    except Exception as img_err:
                        logger.error(f"  Failed to process/embed image {image_path_to_embed}: {img_err}")
                        cell.value = "이미지 처리 오류"
            
            except Exception as e:
                logger.error(f"Error processing image cell {cell.coordinate if cell else f'R{row_idx}C{col_idx}'} ({col_name}): {e}", exc_info=True)
                try:
                    # Ensure cell exists before setting error message
                    if cell:
                        cell.value = "이미지 처리 오류 (전역)"
                except Exception as final_err:
                    logger.error(f"Failed to set error message for cell R{row_idx}C{col_idx}: {final_err}")

    logger.debug("Finished processing image columns")

def _apply_conditional_formatting(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Applies conditional formatting (e.g., yellow fill for price difference < -1)."""
    logger.debug("Applying conditional formatting.")

    # Find price difference columns (non-percentage) using new names
    price_diff_cols = [
        col for col in df.columns
        if col in ['고려 가격차이', '네이버 가격차이']
    ]

    if not price_diff_cols:
        logger.debug("No price difference columns found for conditional formatting.")
        return

    # Define yellow fill
    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    # Process each row
    for row_idx in range(2, worksheet.max_row + 1):  # Start from 2 to skip header
        highlight_row = False # Flag to highlight the row
        for price_diff_col in price_diff_cols:
            if price_diff_col not in df.columns: # Check if column exists in df
                continue
            col_idx = df.columns.get_loc(price_diff_col) + 1  # 1-based index for openpyxl
            cell = worksheet.cell(row=row_idx, column=col_idx)

            # Get cell value and check if it's < -1
            try:
                if cell.value not in ['-', '', None]:  # Skip empty or placeholder values
                    # Remove commas and convert to float
                    value_str = str(cell.value).replace(',', '')
                    value = float(value_str)

                    # Highlight if value is less than -1
                    if value < -1:
                        highlight_row = True
                        break # Found a reason to highlight, no need to check other diff cols for this row
            except ValueError:
                # Skip if value cannot be converted to float (e.g., error messages)
                continue
            except Exception as e:
                logger.error(f"Error processing cell {cell.coordinate} for conditional formatting: {e}")
                continue

        # If the flag is set, highlight the entire row
        if highlight_row:
            for col in range(1, worksheet.max_column + 1):
                try:
                    worksheet.cell(row=row_idx, column=col).fill = yellow_fill
                except Exception as e:
                     logger.error(f"Error applying fill to cell R{row_idx}C{col}: {e}")


    logger.debug("Finished applying conditional formatting for price differences < -1.")

def _setup_page_layout(worksheet: openpyxl.worksheet.worksheet.Worksheet):
    """Sets up page orientation, print area, freeze panes, etc."""
    logger.debug("Setting up page layout.")
    try:
        worksheet.page_setup.orientation = worksheet.ORIENTATION_LANDSCAPE
        worksheet.page_setup.paperSize = worksheet.PAPERSIZE_A4
        worksheet.page_setup.fitToWidth = 1
        worksheet.page_setup.fitToHeight = 0 # Fit to width primarily
        worksheet.print_options.horizontalCentered = True
        # worksheet.print_options.verticalCentered = True # Optional
        worksheet.print_options.gridLines = False # Typically false for final reports
        worksheet.freeze_panes = 'A2'  # Freeze header row
        # Set print area to used range (optional, helps if there's stray data)
        # worksheet.print_area = worksheet.dimensions
        logger.debug("Page layout settings applied.")
    except Exception as e:
        logger.error(f"Failed to set page layout options: {e}")

def _add_hyperlinks_to_worksheet(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Process link columns but do NOT convert to hyperlinks - keep as plain text."""
    logger.debug(f"Processing links as plain text (hyperlinks disabled)")
    # Find column indices for defined link columns using new names
    link_col_indices = {col: idx for idx, col in enumerate(df.columns, 1) if col in LINK_COLUMNS_FOR_HYPERLINK}

    if not link_col_indices:
        logger.debug("No link columns found for processing.")
        return

    # Basic URL pattern check (simplified)
    url_pattern = re.compile(r'^https?://\S+$', re.IGNORECASE)

    url_count = 0
    for col_name, col_idx in link_col_indices.items():
        for row_idx in range(2, worksheet.max_row + 1):
            cell = worksheet.cell(row=row_idx, column=col_idx)
            link_text = str(cell.value) if cell.value else ''

            # Skip empty cells, placeholders, or error messages
            if not link_text or link_text.lower() in ['-', 'nan', 'none', ''] or link_text in ERROR_MESSAGE_VALUES:
                continue

            # If cell has a hyperlink attribute already set, remove it
            if hasattr(cell, 'hyperlink') and cell.hyperlink:
                cell.hyperlink = None
                
            # Use regular font (not blue/underlined)
            cell.font = DEFAULT_FONT
            
            # Count valid URLs just for logging
            if url_pattern.match(link_text):
                url_count += 1

    logger.info(f"Processed link columns as plain text. Found {url_count} URLs across link columns.")

def _add_header_footer(worksheet: openpyxl.worksheet.worksheet.Worksheet):
    """Adds standard header and footer."""
    try:
        # Check if header_footer attribute exists (some versions don't support it)
        if hasattr(worksheet, 'header_footer'):
            current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            worksheet.header_footer.center_header.text = "가격 비교 결과"
            worksheet.header_footer.right_header.text = f"생성일: {current_date}"
            worksheet.header_footer.left_footer.text = "해오름 RPA 가격 비교"
            worksheet.header_footer.right_footer.text = "페이지 &P / &N"
            logger.debug("Added header and footer to worksheet")
        else:
            logger.warning("Header/footer not supported in this Excel version - skipping")
    except Exception as e:
        logger.warning(f"Could not set header/footer: {e}")

def _apply_table_format(worksheet: openpyxl.worksheet.worksheet.Worksheet):
    """Applies Excel table formatting to the data range."""
    # 테이블 서식 적용 함수 비우기 - 필터 적용 방지
    logger.debug("Table formatting skipped as requested.")
    return

def verify_image_data(img_value, img_col_name):
    """Helper function to verify and format image data for the Excel output."""
    try:
        # If the value is a string that looks like a dictionary (from JSON)
        if isinstance(img_value, str) and img_value.startswith('{') and img_value.endswith('}'):
            try:
                # Convert string representation to actual dictionary
                import ast
                img_dict = ast.literal_eval(img_value)
                if isinstance(img_dict, dict):
                    # Return the parsed dictionary for further processing
                    return img_dict
            except (SyntaxError, ValueError):
                # If parsing fails, treat as a regular string
                pass
                
        # Handle dictionary format (expected for all image sources)
        if isinstance(img_value, dict):
            # If there's a local_path, verify it exists
            if 'local_path' in img_value and img_value['local_path']:
                local_path = img_value['local_path']
                if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    return img_value  # Return the valid dictionary
            
            # If no valid local_path but URL exists, keep the dictionary for the URL
            if 'url' in img_value and img_value['url']:
                return img_value  # Return dictionary with just URL
            
            return '-'  # No valid path or URL
            
        # Handle string path/URL
        elif isinstance(img_value, str) and img_value and img_value != '-':
            img_value = img_value.strip()
            
            # Determine source from column name
            source_map = {
                '본사': 'haoreum',
                '고려': 'kogift',
                '네이버': 'naver'
            }
            
            source = 'other'
            for key, value in source_map.items():
                if key in img_col_name:
                    source = value
                    break
            
            # For URL strings (not file paths)
            if img_value.startswith(('http:', 'https:')):
                # Return a dictionary format for consistency
                return {'url': img_value, 'source': source}

            # Fix backslashes in path
            if '\\' in img_value:
                img_value = img_value.replace('\\', '/')
            
            # For file path strings (absolute paths preferred)
            if os.path.isabs(img_value) and os.path.exists(img_value) and os.path.getsize(img_value) > 0:
                # Convert file path to dictionary format for consistency
                img_value_str = img_value.replace(os.sep, '/')
                placeholder_url = f"file:///{img_value_str}"
                return {
                    'url': placeholder_url, 
                    'local_path': img_value, 
                    'original_path': img_value, 
                    'source': source
                }
                
            # Handle relative paths by checking multiple base directories
            elif not os.path.isabs(img_value):
                # Try different base paths based on source type
                base_paths = []
                if source == 'haoreum':
                    base_paths = [
                        IMAGE_MAIN_DIR / HAEREUM_DIR_NAME,
                        IMAGE_MAIN_DIR / 'Target' / HAEREUM_DIR_NAME, # Assuming Target exists
                        IMAGE_MAIN_DIR # Fallback
                    ]
                elif source == 'kogift':
                    base_paths = [
                        IMAGE_MAIN_DIR / KOGIFT_DIR_NAME,
                        IMAGE_MAIN_DIR / 'Target' / KOGIFT_DIR_NAME,
                        IMAGE_MAIN_DIR
                    ]
                elif source == 'naver':
                    base_paths = [
                        IMAGE_MAIN_DIR / NAVER_DIR_NAME,
                        IMAGE_MAIN_DIR / 'Target' / NAVER_DIR_NAME,
                        IMAGE_MAIN_DIR
                    ]
                else: # source == 'other'
                    base_paths = [
                        IMAGE_MAIN_DIR / OTHER_DIR_NAME,
                        IMAGE_MAIN_DIR / 'Target' / OTHER_DIR_NAME,
                        IMAGE_MAIN_DIR # General fallback
                    ]
                
                # Try each base path for resolving the relative path
                for base_path in base_paths:
                    try:
                        abs_path = (base_path / img_value).resolve()
                        if abs_path.exists() and abs_path.stat().st_size > 0:
                            abs_path_str = str(abs_path).replace('\\', '/')
                            placeholder_url = f"file:///{abs_path_str}"
                            return {
                                'url': placeholder_url, 
                                'local_path': str(abs_path), 
                                'original_path': str(abs_path), 
                                'source': source
                            }
                    except Exception:
                        continue  # Try next base path
                
                # If we reach here, all base paths failed
                if source == 'haoreum':
                    # For Haoreum, try to check if the image might be in a common format
                    try:
                        # Try standard haereum image format
                        standard_paths = [
                            # Common Haoreum image patterns using the base dir constant
                            IMAGE_MAIN_DIR / HAEREUM_DIR_NAME / f"haoreum_{os.path.basename(img_value)}",
                            IMAGE_MAIN_DIR / HAEREUM_DIR_NAME / f"haoreum_{img_value}",
                            IMAGE_MAIN_DIR / HAEREUM_DIR_NAME / os.path.basename(img_value)
                        ]
                        
                        for std_path in standard_paths:
                            std_path_str = str(std_path).replace('\\', '/') # Ensure correct format for URL
                            if os.path.exists(std_path_str) and os.path.getsize(std_path_str) > 0:
                                placeholder_url = f"file:///{std_path_str}"
                                return {
                                    'url': placeholder_url,
                                    'local_path': std_path_str,
                                    'original_path': img_value, # Keep original value as original_path
                                    'source': 'haoreum'
                                }
                    except Exception:
                        pass  # Ignore errors in this speculative search

            # If all attempts fail, return the original string for further handling
            return {'original_path': img_value, 'source': source}

        return '-'  # None, NaN, empty string, etc.
    except Exception as e:
        logging.warning(f"Error verifying image data '{str(img_value)[:100]}...' for column {img_col_name}: {e}")
        return '-'  # Return placeholder on error

def _prepare_data_for_excel(df: pd.DataFrame, skip_images=False) -> pd.DataFrame:
    """
    Prepares the DataFrame for Excel output: column order, formatting.
    
    Args:
        df (pd.DataFrame): The DataFrame to prepare
        skip_images (bool): If True, skip image columns for upload file
        
    Returns:
        pd.DataFrame: Prepared DataFrame
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure all required columns from FINAL_COLUMN_ORDER exist
    for col in FINAL_COLUMN_ORDER:
        if col not in df.columns:
            df[col] = ""
            logger.debug(f"Added missing column '{col}' to DataFrame before ordering.")

    # Select and reorder columns based on FINAL_COLUMN_ORDER
    # Ensure only columns defined in FINAL_COLUMN_ORDER are kept and ordered correctly
    existing_cols_in_order = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
    df = df[existing_cols_in_order]
    logger.debug(f"Columns after reordering: {df.columns.tolist()}")

    # Apply renaming based on COLUMN_RENAME_MAP *before* preparing for excel
    # This ensures the DataFrame passed to _prepare_data has the target column names
    df.rename(columns=COLUMN_RENAME_MAP, inplace=True, errors='ignore') # Ignore errors if a column to rename doesn't exist
    # Ensure columns are still in the FINAL_COLUMN_ORDER after renaming
    # Add missing columns and reorder
    missing_cols = [col for col in FINAL_COLUMN_ORDER if col not in df.columns]
    for col in missing_cols:
        df[col] = '' # Add missing columns with empty string
        logger.debug(f"Added missing column '{col}' with empty values")
    df = df[FINAL_COLUMN_ORDER] # Enforce final order

    # For upload file, modify image column values to be web URLs or empty
    if skip_images:
        # Image columns now use new names from FINAL_COLUMN_ORDER
        image_columns = [col for col in df.columns if col in IMAGE_COLUMNS]
        for col in image_columns:
            # Replace image dict/path with web URL or empty string for upload file
            df[col] = df[col].apply(
                lambda x:
                    # Case 1: Input is a dictionary
                    x['url'] if isinstance(x, dict) and 'url' in x and isinstance(x['url'], str) and x['url'].startswith(('http://', 'https://'))
                    # Case 2: Input is a string that is already a web URL
                    else (x if isinstance(x, str) and x.startswith(('http://', 'https://'))
                    # Case 3: Anything else (dict without web URL, local path, file://, other types, None)
                    else '')
                if pd.notna(x) else ''
            )
        logger.debug(f"Processed image columns for upload file, keeping only web URLs: {image_columns}")

    # Format numeric columns (prices, quantities) using new names
    # numeric_keywords removed, using specific lists instead
    for col in df.columns:
        if any(keyword in col for keyword in ['단가', '가격', '수량']):
            try:
                # Attempt conversion, handle errors gracefully
                original_dtype = df[col].dtype
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Only fillna if conversion was successful (result is numeric)
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna('') # Fill NaN with empty string for Excel
                else:
                     # If coercion failed, maybe revert or log? Keep original if not numeric.
                     df[col] = df[col].astype(original_dtype) # Revert if conversion failed badly
                     df[col] = df[col].fillna('') # Still fill NaNs
            except Exception as e:
                logging.warning(f"Error formatting numeric column '{col}': {str(e)}")
                df[col] = df[col].fillna('') # Ensure NaNs are handled even on error
    
    logger.debug(f"Final columns for Excel output: {df.columns.tolist()}")
    return df

def safe_excel_operation(func):
    """
    데코레이터: Excel 작업 중 발생할 수 있는 예외를 안전하게 처리합니다.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Excel operation failed in {func.__name__}: {str(e)}", exc_info=True)
            return False
    return wrapper

# --- Main Public Function --- #

def create_split_excel_outputs(df: pd.DataFrame, output_path: str) -> tuple:
    """
    Creates two separate Excel files from the input DataFrame:
    1. A "result" file with both images and links (same as create_final_output_excel)
    2. An "upload" file with only links (no images)
    
    Args:
        df (pd.DataFrame): The DataFrame to save to Excel
        output_path (str): Path for the result file (with images)
        
    Returns:
        tuple: (result_success, upload_success, result_path, upload_path) - Booleans indicating if each file 
              was successfully created, and the paths to both files
    """
    logging.info(f"Starting creation of split Excel outputs (result and upload files)")
    
    # Generate the upload file path by adding _upload before the extension
    base_name, ext = os.path.splitext(output_path)
    # Add _result suffix to the result file path
    result_path = f"{base_name}_result{ext}"
    upload_path = f"{base_name}_upload{ext}"
    
    # Log the paths that will be created
    logging.info(f"Result file path (with images): {result_path}")
    logging.info(f"Upload file path (links only): {upload_path}")
    
    # --- Rename Columns Early --- #
    # Apply renaming based on COLUMN_RENAME_MAP to the original DataFrame
    # This ensures filtering uses the correct, final column names
    df_renamed = df.rename(columns=COLUMN_RENAME_MAP, errors='ignore')
    # Add any missing columns from FINAL_COLUMN_ORDER before filtering
    for col in FINAL_COLUMN_ORDER:
        if col not in df_renamed.columns:
            df_renamed[col] = ''
            logger.debug(f"Added missing column '{col}' before filtering")
    df_renamed = df_renamed[FINAL_COLUMN_ORDER] # Ensure order before filtering

    # Check if either file is locked
    if os.path.exists(result_path):
        try:
            # Try to open file for append/binary to check lock without modifying
            with open(result_path, 'a+b'):
                pass  # Just checking if we can open it for writing
        except (IOError, PermissionError):
            logging.error(f"Result file is locked: {result_path}")
            return False, False, result_path, upload_path
    
    if os.path.exists(upload_path):
        try:
            with open(upload_path, 'a'):
                pass
        except PermissionError:
            logging.error(f"Upload file is locked: {upload_path}")
            return False, False, result_path, upload_path
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(result_path), exist_ok=True)
    
    # --- Row Filtering Logic --- #
    # Use the renamed DataFrame (df_renamed) for filtering
    df_filtered = df_renamed.copy()
    initial_rows = len(df_filtered)

    # Define columns relevant to Kogift and Naver based on FINAL_COLUMN_ORDER
    kogift_cols = [
        '구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드',
        '카테고리(중분류)', '상품명', '본사 기본수량', '판매단가1(VAT포함)', '본사링크',
        '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)', '고려 링크'
    ]
    naver_cols = [
        '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)',
        '네이버 공급사명', '네이버 링크', '네이버쇼핑(이미지링크)'
    ]

    # Ensure these columns exist in the DataFrame before filtering
    actual_kogift_cols = [col for col in kogift_cols if col in df_filtered.columns]
    actual_naver_cols = [col for col in naver_cols if col in df_filtered.columns]

    if not actual_kogift_cols or not actual_naver_cols:
        logging.warning("Cannot perform row filtering: Missing key Kogift or Naver columns.")
    else:
        logging.info(f"Filtering rows where both Kogift ({len(actual_kogift_cols)} cols) and Naver ({len(actual_naver_cols)} cols) data are missing...")

        # Function to check if a value is considered "empty"
        # Handles None, NaN, empty strings, '-', and checks dictionary image values
        def is_empty(value, col_name):
            if pd.isna(value): return True
            if isinstance(value, str) and value.strip() in ['', '-']: return True
            # For image columns, check if the dictionary has a valid local_path or web url
            if col_name in IMAGE_COLUMNS and isinstance(value, dict):
                has_local = 'local_path' in value and value['local_path'] and os.path.exists(value['local_path'])
                has_web_url = 'url' in value and isinstance(value['url'], str) and value['url'].startswith(('http://', 'https://'))
                return not (has_local or has_web_url)
            # Check for empty image strings (non-URLs)
            if col_name in IMAGE_COLUMNS and isinstance(value, str) and not value.startswith(('http://', 'https://')):
                 return value.strip() in ['', '-']
            return False

        # Identify rows to drop
        rows_to_drop = []
        for index, row in df_filtered.iterrows():
            # Check if all Kogift columns are empty
            # Pass column name to is_empty for specific image checks
            kogift_empty = all(is_empty(row[col], col) for col in actual_kogift_cols)
            # Check if all Naver columns are empty
            naver_empty = all(is_empty(row[col], col) for col in actual_naver_cols)

            if kogift_empty and naver_empty:
                rows_to_drop.append(index)

        # Drop rows with empty Kogift and Naver data
        df_filtered = df_filtered.drop(rows_to_drop)
        logging.info(f"Filtered out {initial_rows - len(df_filtered)} rows with empty Kogift and Naver data")

    # Create a Pandas Excel writer using openpyxl engine for better style support
    # Use context manager for automatic saving/closing
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:

        # Write DataFrame to Excel
        df_filtered.to_excel(writer, sheet_name='Sheet1', index=False, na_rep='') # Use empty string for NaN

        # Get workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Apply formatting using openpyxl (consistent with result file)
        _apply_excel_formatting(workbook, worksheet, df_filtered, include_images=True)

        # --- Apply Formatting AFTER data is written ---
        try:
            # 3. Apply Column Widths and Cell Styles
            _apply_column_widths(worksheet, df_filtered)
            _apply_cell_styles_and_alignment(worksheet, df_filtered)
        except Exception as e:
            logger.error(f"Error during formatting: {e}")

        try:
            # 4. Apply Conditional Formatting
            _apply_conditional_formatting(worksheet, df_filtered)
        except Exception as e:
            logger.error(f"Error during conditional formatting: {e}")

        try:
            # 5. Handle Images (Embedding)
            _process_image_columns(worksheet, df_filtered)
        except Exception as e:
            logger.error(f"Error during image processing: {e}")
        
        try:
            # 6. Adjust dimensions for image cells
            _adjust_image_cell_dimensions(worksheet, df_filtered)
        except Exception as e:
            logger.error(f"Error adjusting image cell dimensions: {e}")

        try:
            # 7. Add Hyperlinks
            _add_hyperlinks_to_worksheet(worksheet, df_filtered)
        except Exception as e:
            logger.error(f"Error adding hyperlinks: {e}")

        try:
            # 8. Page Setup and Header/Footer
            _setup_page_layout(worksheet)
            _add_header_footer(worksheet)
        except Exception as e:
            logger.error(f"Error setting up page layout: {e}")

        try:
            # 9. Apply Table Format (Apply last after other formatting)
            _apply_table_format(worksheet)
        except Exception as e:
            logger.error(f"Error applying table format: {e}")

    logger.info(f"Successfully created and formatted Excel file: {output_path}")
    return True, True, result_path, upload_path

def _apply_excel_formatting(workbook, worksheet, df, include_images=True):
    """
    Apply Excel formatting including headers, column widths, and cell styles.
    
    Args:
        workbook: xlsxwriter workbook object
        worksheet: xlsxwriter worksheet object
        df: DataFrame being written
        include_images: Whether to include image formatting
    """
    # Define formats
    header_format = workbook.add_format({
        'bold': True, 
        'text_wrap': True,
        'valign': 'top',
        'border': 1
    })
    
    # Define a regular cell format (not hyperlink)
    text_format = workbook.add_format({
        'text_wrap': True
    })
    
    # Write column headers with the defined format
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
    
    # Set column widths based on content
    for col_num, column in enumerate(df.columns):
        column_width = max(
            df[column].astype(str).map(len).max(),
            len(str(column))
        ) + 2  # Add a little extra space
        
        # Limit column width to reasonable size
        column_width = min(column_width, 50)
        
        # For image columns, set specific width when including images
        if include_images and '이미지' in column:
            column_width = 22  # IMAGE_CELL_WIDTH
        
        # For link columns, ensure they're wide enough but don't create hyperlinks
        if column in LINK_COLUMNS_FOR_HYPERLINK or '링크' in column:
            column_width = max(column_width, 35)  # Ensure links have enough width
            
            # Write URLs as plain text
            for row_num, value in enumerate(df[column], start=1):
                if pd.notna(value) and isinstance(value, str):
                    # Write the cell with regular format (not a hyperlink)
                    worksheet.write(row_num, col_num, value, text_format)
        
        worksheet.set_column(col_num, col_num, column_width)
    
    # If not including images, we're done
    if not include_images:
        return
    
    # Add images to cells if include_images is True
    # (This would be handled by the image insertion functions in create_final_output_excel)

@safe_excel_operation
def create_final_output_excel(df: pd.DataFrame, output_path: str) -> bool:
    """
    Creates the final formatted Excel file.
    Orchestrates data preparation, styling, image handling, and saving.
    
    Args:
        df: DataFrame containing the data to save
        output_path: Path where the Excel file will be saved
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None:
        logger.error("Cannot create Excel file: Input DataFrame is None.")
        return False

    logger.info(f"Starting creation of final Excel output: {output_path}")
    try:
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir:
             os.makedirs(output_dir, exist_ok=True)

        # 1. Prepare the data (column order, formatting)
        try:
            df_prepared = _prepare_data_for_excel(df.copy())
        except TypeError as te:
            if "copy" in str(te):
                logger.warning("Pandas version compatibility issue detected. Trying with basic fillna.")
                # Fall back to a simpler approach without using copy=False
                df_temp = df.copy()
                # Ensure all required columns exist
                for col in FINAL_COLUMN_ORDER:
                    if col not in df_temp.columns:
                        df_temp[col] = '-'
                # Basic formatting only - just fill NaN and convert to string
                df_prepared = df_temp[FINAL_COLUMN_ORDER].fillna('').astype(str)
            else:
                raise

        if df_prepared.empty and not df.empty:
             logger.error("Data preparation resulted in an empty DataFrame. Cannot save Excel.")
             return False
        elif df_prepared.empty and df.empty:
             logger.warning("Input DataFrame was empty, saving an Excel file with only headers.")
             df_prepared = pd.DataFrame(columns=FINAL_COLUMN_ORDER)

        # Check if file is already open
        try:
            # Try to open the file for writing to check if it's locked
            if os.path.exists(output_path):
                with open(output_path, 'a+b'):
                    pass  # Just checking if we can open it for writing
        except (IOError, PermissionError):
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            alternative_path = f"{os.path.splitext(output_path)[0]}_{timestamp}{os.path.splitext(output_path)[1]}"
            logger.warning(f"Output file {output_path} is locked. Using alternative path: {alternative_path}")
            output_path = alternative_path

        # 2. Save prepared data to Excel using openpyxl engine
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_prepared.to_excel(writer, index=False, sheet_name='Results', na_rep='') # Use empty string for NaN
            worksheet = writer.sheets['Results']
            logger.debug(f"DataFrame written to sheet 'Results'. Max Row: {worksheet.max_row}, Max Col: {worksheet.max_column}")

            # --- Apply Formatting AFTER data is written ---
            try:
                # 3. Apply Column Widths and Cell Styles
                _apply_column_widths(worksheet, df_prepared)
                _apply_cell_styles_and_alignment(worksheet, df_prepared)
            except Exception as e:
                logger.error(f"Error during formatting: {e}")

            try:
                # 4. Apply Conditional Formatting
                _apply_conditional_formatting(worksheet, df_prepared)
            except Exception as e:
                logger.error(f"Error during conditional formatting: {e}")

            try:
                # 5. Handle Images (Embedding)
                _process_image_columns(worksheet, df_prepared)
            except Exception as e:
                logger.error(f"Error during image processing: {e}")
            
            try:
                # 6. Adjust dimensions for image cells
                _adjust_image_cell_dimensions(worksheet, df_prepared)
            except Exception as e:
                logger.error(f"Error adjusting image cell dimensions: {e}")

            try:
                # 7. Add Hyperlinks
                _add_hyperlinks_to_worksheet(worksheet, df_prepared)
            except Exception as e:
                logger.error(f"Error adding hyperlinks: {e}")

            try:
                # 8. Page Setup and Header/Footer
                _setup_page_layout(worksheet)
                _add_header_footer(worksheet)
            except Exception as e:
                logger.error(f"Error setting up page layout: {e}")

            try:
                # 9. Apply Table Format (Apply last after other formatting)
                _apply_table_format(worksheet)
            except Exception as e:
                logger.error(f"Error applying table format: {e}")

        logger.info(f"Successfully created and formatted Excel file: {output_path}")
        return True

    except PermissionError as pe:
        logger.error(f"Permission denied when trying to save Excel file: {output_path}. Check if the file is open. Error: {pe}")
        try:
            # Try with a different filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            alternative_path = f"{os.path.splitext(output_path)[0]}_{timestamp}{os.path.splitext(output_path)[1]}"
            logger.info(f"Attempting to save with alternative filename: {alternative_path}")
            
            try:
                df_prepared = _prepare_data_for_excel(df.copy())
            except TypeError as te:
                if "copy" in str(te):
                    logger.warning("Pandas version compatibility issue in alternative save. Using basic formatting.")
                    # Fall back to a simpler approach
                    df_temp = df.copy()
                    # Ensure all required columns exist
                    for col in FINAL_COLUMN_ORDER:
                        if col not in df_temp.columns:
                            df_temp[col] = '-'
                    # Basic formatting only
                    df_prepared = df_temp[FINAL_COLUMN_ORDER].fillna('').astype(str)
                else:
                    raise

            df_prepared.to_excel(alternative_path, index=False, engine='openpyxl', sheet_name='Results', na_rep='') # Use empty string for NaN
            logger.info(f"Successfully saved data to alternative path (without formatting): {alternative_path}")
            return True
        except Exception as alt_err:
            logger.error(f"Also failed to save to alternative path: {alt_err}")
            return False
    except Exception as e:
        logger.error(f"Error creating Excel file: {e}", exc_info=True)
        return False

def apply_excel_styles(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """
    Apply Excel styles to the worksheet.
    This is a wrapper around _apply_cell_styles_and_alignment for backward compatibility.
    """
    _apply_cell_styles_and_alignment(worksheet, df)

def _adjust_image_cell_dimensions(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Adjusts row heights and column widths for cells containing images."""
    logger.debug("Adjusting dimensions for image cells...")
    
    # Get image column indices
    image_cols = {col: idx for idx, col in enumerate(df.columns, 1) if col in IMAGE_COLUMNS}
    
    if not image_cols:
        return
        
    # Increase column widths for image columns to accommodate larger images
    for col_name, col_idx in image_cols.items():
        try:
            col_letter = get_column_letter(col_idx)
            # Use larger column width for image columns
            worksheet.column_dimensions[col_letter].width = 22  # Increased from 15
        except Exception as e:
            logger.error(f"Error adjusting column width for {col_name}: {e}")
    
    # Create a set of rows that need height adjustment
    rows_with_images = set()
    
    try:
        # Find rows that have actual images (not error messages or empty cells)
        for row_idx in range(2, worksheet.max_row + 1):
            for col_name, col_idx in image_cols.items():
                try:
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    
                    # If the cell is empty, it likely has an image
                    if cell.value == "" or cell.value is None:
                        rows_with_images.add(row_idx)
                        break
                        
                    # Check for image data in dictionary format
                    cell_value = str(cell.value) if cell.value else ""
                    if cell_value and cell_value.startswith('{') and cell_value.endswith('}'):
                        try:
                            import ast
                            img_dict = ast.literal_eval(cell_value)
                            if isinstance(img_dict, dict) and ('local_path' in img_dict or 'url' in img_dict):
                                rows_with_images.add(row_idx)
                                break
                        except:
                            pass
                            
                    # Check for path-like strings
                    if (cell_value and cell_value != '-' and 
                        not any(err_msg in cell_value for err_msg in ERROR_MESSAGE_VALUES) and
                        ('\\' in cell_value or '/' in cell_value or '.jpg' in cell_value.lower() or 
                         '.png' in cell_value.lower() or '.jpeg' in cell_value.lower() or
                         'http' in cell_value.lower())):
                        rows_with_images.add(row_idx)
                        break
                except Exception as e:
                    logger.error(f"Error checking cell at row {row_idx}, column {col_idx}: {e}")
    except Exception as e:
        logger.error(f"Error finding rows with images: {e}")
    
    # Apply increased height to rows with images
    for row_idx in rows_with_images:
        try:
            # Set larger row height to accommodate the bigger images
            worksheet.row_dimensions[row_idx].height = 120  # Increased from 90
            
            # Center-align all cells in this row for better appearance with images
            for col_idx in range(1, worksheet.max_column + 1):
                try:
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    # Preserve horizontal alignment, set vertical to center
                    current_alignment = cell.alignment
                    cell.alignment = Alignment(
                        horizontal=current_alignment.horizontal,
                        vertical="center",
                        wrap_text=current_alignment.wrap_text
                    )
                except Exception as e:
                    logger.error(f"Error adjusting cell alignment at row {row_idx}, column {col_idx}: {e}")
        except Exception as e:
            logger.error(f"Error adjusting row height for row {row_idx}: {e}")
    
    logger.debug(f"Adjusted dimensions for {len(rows_with_images)} rows with images")

