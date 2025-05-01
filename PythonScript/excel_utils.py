import os
import logging
import pandas as pd
import datetime
from datetime import datetime, timedelta
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
import numpy as np

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

# Column Rename Mapping (Ensure keys cover variations, values match FINAL_COLUMN_ORDER)
COLUMN_RENAME_MAP = {
    # Standard column renames - map FROM old names TO new names
    '구분(승인관리:A/가격관리:P)': '구분',
    '공급사명': '업체명',
    '공급처코드': '업체코드',
    '상품코드': 'Code',
    '카테고리(중분류)': '중분류카테고리',
    '본사 기본수량': '기본수량(1)',
    '판매단가1(VAT포함)': '판매단가(V포함)',
    '본사링크': '본사상품링크',
    '고려 기본수량': '기본수량(2)',
    '판매단가2(VAT포함)': '판매가(V포함)(2)',
    '고려 가격차이': '가격차이(2)',
    '고려 가격차이(%)': '가격차이(2)(%)',
    '고려 링크': '고려기프트 상품링크',
    '네이버 기본수량': '기본수량(3)',
    '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
    '네이버 가격차이': '가격차이(3)',
    '네이버가격차이(%)': '가격차이(3)(%)',
    '네이버 공급사명': '공급사명',
    '네이버 링크': '네이버 쇼핑 링크',
    '해오름(이미지링크)': '본사 이미지',
    '고려기프트(이미지링크)': '고려기프트 이미지',
    '네이버쇼핑(이미지링크)': '네이버 이미지',
    
    # Self-maps (columns that already have correct names)
    '구분': '구분',
    '담당자': '담당자',
    '업체명': '업체명',
    '업체코드': '업체코드',
    'Code': 'Code',
    '중분류카테고리': '중분류카테고리',
    '상품명': '상품명',
    '기본수량(1)': '기본수량(1)',
    '판매단가(V포함)': '판매단가(V포함)',
    '본사상품링크': '본사상품링크',
    '기본수량(2)': '기본수량(2)',
    '판매가(V포함)(2)': '판매가(V포함)(2)',
    '가격차이(2)': '가격차이(2)',
    '가격차이(2)(%)': '가격차이(2)(%)',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '기본수량(3)': '기본수량(3)',
    '판매단가(V포함)(3)': '판매단가(V포함)(3)',
    '가격차이(3)': '가격차이(3)',
    '가격차이(3)(%)': '가격차이(3)(%)',
    '공급사명': '공급사명',
    '네이버 쇼핑 링크': '네이버 쇼핑 링크',
    '공급사 상품링크': '공급사 상품링크',
    '본사 이미지': '본사 이미지',
    '고려기프트 이미지': '고려기프트 이미지',
    '네이버 이미지': '네이버 이미지'
}

# Final Target Column Order (Based on "엑셀 골든" sample)
# THIS IS THE STRICT ORDER AND NAMING FOR THE OUTPUT FILE
FINAL_COLUMN_ORDER = [
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명',
    '기본수량(1)', '판매단가(V포함)', '본사상품링크',
    '기본수량(2)', '판매가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
    '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', 
    '네이버 쇼핑 링크', '공급사 상품링크',
    '본사 이미지', '고려기프트 이미지', '네이버 이미지'
]

# Columns that must be present in the input file for processing
# Update this based on the new FINAL_COLUMN_ORDER if necessary,
# focusing on the absolutely essential input fields needed.
REQUIRED_INPUT_COLUMNS = [
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리',
    '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
]

# --- Column Type Definitions for Formatting ---
# Update these lists based on the FINAL_COLUMN_ORDER names
PRICE_COLUMNS = [
    '판매단가(V포함)', '판매가(V포함)(2)', '판매단가(V포함)(3)',
    '가격차이(2)', '가격차이(3)'
]
QUANTITY_COLUMNS = ['기본수량(1)', '기본수량(2)', '기본수량(3)']
PERCENTAGE_COLUMNS = ['가격차이(2)(%)', '가격차이(3)(%)']
TEXT_COLUMNS = ['구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명', '공급사명']
LINK_COLUMNS_FOR_HYPERLINK = {
    # Map final column names used for links
    '본사상품링크': '본사상품링크',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '네이버 쇼핑 링크': '네이버 쇼핑 링크',
    '공급사 상품링크': '공급사 상품링크'
}
# Define IMAGE_COLUMNS based on FINAL_COLUMN_ORDER
IMAGE_COLUMNS = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']

# Upload file columns (based on '엑셀골든_upload' notepad)
UPLOAD_COLUMN_ORDER = [
    '구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드', '카테고리(중분류)', '상품명',
    '본사 기본수량', '판매단가1(VAT포함)', '본사링크',
    '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)', '고려 링크',
    '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)', '네이버 공급사명', 
    '네이버 링크', '해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'
]

# Mapping between FINAL_COLUMN_ORDER and UPLOAD_COLUMN_ORDER
COLUMN_MAPPING_FINAL_TO_UPLOAD = {
    '구분': '구분(승인관리:A/가격관리:P)',
    '담당자': '담당자',  
    '업체명': '공급사명',
    '업체코드': '공급처코드',
    'Code': '상품코드',
    '중분류카테고리': '카테고리(중분류)',
    '상품명': '상품명',
    '기본수량(1)': '본사 기본수량',
    '판매단가(V포함)': '판매단가1(VAT포함)',
    '본사상품링크': '본사링크',
    '기본수량(2)': '고려 기본수량',
    '판매가(V포함)(2)': '판매단가2(VAT포함)',
    '가격차이(2)': '고려 가격차이',
    '가격차이(2)(%)': '고려 가격차이(%)',
    '고려기프트 상품링크': '고려 링크',
    '기본수량(3)': '네이버 기본수량',
    '판매단가(V포함)(3)': '판매단가3 (VAT포함)',
    '가격차이(3)': '네이버 가격차이',
    '가격차이(3)(%)': '네이버가격차이(%)',
    '공급사명': '네이버 공급사명',
    '네이버 쇼핑 링크': '네이버 링크',
    '본사 이미지': '해오름(이미지링크)',
    '고려기프트 이미지': '고려기프트(이미지링크)',
    '네이버 이미지': '네이버쇼핑(이미지링크)'
}

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

# --- Image Processing Constants ---
IMAGE_MAX_SIZE = (2000, 2000)  # Excel 2021 maximum supported image size (increased from 1200x1200)
IMAGE_STANDARD_SIZE = (600, 600)  # Standard display size in Excel (increased from 400x400)
IMAGE_QUALITY = 85  # JPEG compression quality
SUPPORTED_IMAGE_FORMATS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']  # Supported by Excel 2021

# Image cell specific styling
IMAGE_CELL_HEIGHT = 420  # Increased from 360 for larger images
IMAGE_CELL_WIDTH = 60   # Increased from 44 for wider image cells

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
        elif '카테고리' in col_name_str or '분류' in col_name_str: # Added '분류'
            width = width_hints['category']
        elif col_name_str in ['구분', '담당자']: # Use new '구분' name
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
            elif col_name_str in IMAGE_COLUMNS or '코드' in col_name_str or 'Code' in col_name_str or col_name_str == '구분': # Use new '구분', added 'Code'
                 cell.alignment = CENTER_ALIGNMENT
            else:
                cell.alignment = LEFT_ALIGNMENT # Default left align for text/links/errors
    logger.debug("Finished applying cell styles.")

def _process_image_columns(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Process image columns in the DataFrame and add images to the worksheet.
    
    This function handles complex nested data structures for images, which may be dictionaries
    with paths or URLs, or plain string paths. The function will try to find the image files
    and embed them in the Excel file if they exist.
    
    Args:
        worksheet: The worksheet to add images to
        df: DataFrame containing the data with image columns
    """
    import openpyxl
    from openpyxl.drawing.image import Image
    
    # Initialize tracking variables
    successful_embeddings = 0
    attempted_embeddings = 0
    kogift_successful = 0
    kogift_attempted = 0
    
    # Only handle these image-specific columns
    global IMAGE_COLUMNS
    columns_to_process = [col for col in IMAGE_COLUMNS if col in df.columns]
    
    if not columns_to_process:
        logger.debug("No image columns found in DataFrame")
        return 0
        
    # Fallback image to use if an image file is not found
    default_img_path = os.environ.get('RPA_DEFAULT_IMAGE', None)
    default_exists = default_img_path and os.path.exists(default_img_path)
    if not default_exists and default_img_path:
        logger.warning(f"Default image not found at {default_img_path}")
        default_img_path = None
        
    # Track if the fallback image was used
    used_fallback = False
    
    # Note: This is a reference to PIL.Image to avoid confusion with openpyxl.drawing.image.Image
    from PIL import Image as PILImage
    
    # Function to safely load and resize an image for Excel
    def safe_load_image(path, max_height=150, max_width=150):
        try:
            img = PILImage.open(path)
            # Calculate new dimensions preserving aspect ratio
            width, height = img.size
            if width > max_width or height > max_height:
                ratio = min(max_width / width, max_height / height)
                new_width = int(width * ratio)
                new_height = int(height * ratio)
                img = img.resize((new_width, new_height), PILImage.LANCZOS)
                
                # Save temporary resized version
                temp_dir = os.environ.get('TEMP_DIR', os.path.join(os.path.dirname(path), 'temp'))
                os.makedirs(temp_dir, exist_ok=True)
                temp_path = os.path.join(temp_dir, f"resized_{os.path.basename(path)}")
                img.save(temp_path)
                return temp_path
            return path
        except Exception as e:
            logger.warning(f"Error loading/resizing image {path}: {e}")
            return None
    
    # Track the count of images per column
    img_counts = {col: 0 for col in columns_to_process}
    err_counts = {col: 0 for col in columns_to_process}
    
    logger.debug(f"Processing {len(columns_to_process)} image columns")
    
    # For each image column in the DataFrame
    for col_idx, column in enumerate(columns_to_process):
        is_kogift_image = 'kogift' in column.lower() or '고려기프트' in column  # Track whether it's a Kogift column
        
        # Excel column letter for this column (e.g., 'A', 'B', ...)
        excel_col = get_column_letter(df.columns.get_loc(column) + 1)
        
        # For each row in the DataFrame
        for row_idx, cell_value in enumerate(df[column]):
            img_path = None  # Initialize image path
            fallback_img_path = default_img_path  # Use default fallback
            
            # Skip empty cells (None, NaN, empty strings)
            if pd.isna(cell_value) or cell_value == "":
                continue
                
            # Skip cells with placeholder dash
            if cell_value == "-":
                continue
            
            # Handle dictionary format (most complete info)
            if isinstance(cell_value, dict):
                # Try local path first, then URL
                if 'local_path' in cell_value and cell_value['local_path']:
                    img_path = cell_value['local_path']
                    if is_kogift_image:
                        logger.debug(f"Found Kogift local_path: {img_path}")
                elif 'url' in cell_value and cell_value['url'] and cell_value['url'].startswith(('http', 'https', 'file:')):
                    # For URLs, we need to find corresponding downloaded file
                    url = cell_value['url']
                    # Try to use URL as path directly
                    if url.startswith('file:///'):
                        # Convert file URL to actual path
                        img_path = url.replace('file:///', '').replace('/', os.sep)
                        if is_kogift_image:
                            logger.debug(f"Converted Kogift file URL to path: {img_path}")
                    else:
                        # Try to deduce local path from related data
                        if is_kogift_image:
                            logger.debug(f"Kogift URL-only image data, attempting to find local file: {url[:50]}...")
                        
                        # Recognize standard image paths based on domain
                        if 'jclgift.com' in url:
                            # Try to find corresponding downloaded file
                            filename = os.path.basename(url)
                            base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                            
                            # Common image locations
                            possible_locations = [
                                os.path.join(base_img_dir, 'Main', 'Haereum', filename),
                                os.path.join(base_img_dir, 'Main', 'Haereum', f"haereum_{filename}"),
                                os.path.join(base_img_dir, 'Target', 'Haereum', filename),
                                os.path.join(base_img_dir, 'Target', 'Haereum', f"haereum_{filename}")
                            ]
                            
                            for loc in possible_locations:
                                if os.path.exists(loc):
                                    img_path = loc
                                    logger.debug(f"Found local file for URL: {img_path}")
                                    break
                        elif 'koreagift.com' in url or 'kogift.com' in url or 'adpanchok.co.kr' in url:  # FIXED: Added full domain list
                            # Similar pattern for Kogift
                            filename = os.path.basename(url)
                            base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                            
                            # FIXED: More extensive search patterns for Kogift images
                            possible_locations = [
                                os.path.join(base_img_dir, 'Main', 'Kogift', filename),
                                os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{filename}"),
                                os.path.join(base_img_dir, 'Main', 'kogift', filename),
                                os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{filename}"),
                                # Add more variations - lowercased directory
                                os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{url.split('/')[-1]}"),
                                os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{url.split('/')[-1]}"),
                                # Check in the root image directories too
                                os.path.join(base_img_dir, 'Kogift', filename),
                                os.path.join(base_img_dir, 'Kogift', f"kogift_{filename}"),
                                os.path.join(base_img_dir, 'kogift', filename),
                                os.path.join(base_img_dir, 'kogift', f"kogift_{filename}"),
                                # Check in Shop_* variations
                                os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{filename.replace('shop_', '')}"),
                                os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{filename.replace('shop_', '')}")
                            ]
                            
                            # Add MD5 hash pattern searches for kogift URLs
                            if 'koreagift.com' in url or 'kogift.com' in url or 'adpanchok.co.kr' in url:
                                import hashlib
                                url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
                                # Add hash-based patterns
                                possible_locations.extend([
                                    os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{url_hash}.jpg"),
                                    os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{url_hash}.jpg"),
                                    os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{url_hash}.png"),
                                    os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{url_hash}.png"),
                                    os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{url_hash}_nobg.png"),
                                    os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{url_hash}_nobg.png")
                                ])
                            
                            for loc in possible_locations:
                                if os.path.exists(loc):
                                    img_path = loc
                                    logger.debug(f"Found local Kogift file for URL: {img_path}")
                                    break
                                    
                            # If still not found, try broader search
                            if not img_path and is_kogift_image:
                                logger.debug("Performing broader search for Kogift image...")
                                for root_dir in [os.path.join(base_img_dir, 'Main'), os.path.join(base_img_dir, 'Target'), base_img_dir]:
                                    if os.path.exists(root_dir):
                                        for subdir, _, files in os.walk(root_dir):
                                            if 'kogift' in subdir.lower():
                                                for file in files:
                                                    # Check for partial filename match
                                                    # Look for similarity in both the URL's filename part and the full basename
                                                    url_part = url.split('/')[-1].lower()
                                                    if url_part in file.lower() or (
                                                        file.lower().startswith('kogift_') and 
                                                        any(hashed_part in file.lower() for hashed_part in [
                                                            url_hash[:8] if 'url_hash' in locals() else "", 
                                                            filename[:8] if len(filename) > 8 else filename
                                                        ])
                                                    ):
                                                        img_path = os.path.join(subdir, file)
                                                        logger.debug(f"Found Kogift file via broad search: {img_path}")
                                                        break
                                            if img_path:
                                                break
                                    if img_path:
                                        break
                                        
                # FIXED: Try 'original_path' for Kogift images if local_path and URL don't work
                elif is_kogift_image and 'original_path' in cell_value and cell_value['original_path']:
                    orig_path = cell_value['original_path']
                    logger.debug(f"Checking Kogift original_path: {orig_path}")
                    
                    if os.path.exists(orig_path):
                        img_path = orig_path
                        logger.debug(f"Using Kogift original_path directly: {img_path}")
                    else:
                        # Try to find the file by basename
                        base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                        filename = os.path.basename(orig_path)
                        
                        # FIXED: Search for the file in Kogift directories
                        for root_dir in [os.path.join(base_img_dir, 'Main'), os.path.join(base_img_dir, 'Target'), base_img_dir]:
                            if os.path.exists(root_dir):
                                for subdir, _, files in os.walk(root_dir):
                                    if 'kogift' in subdir.lower():
                                        for file in files:
                                            if filename.lower() in file.lower():
                                                img_path = os.path.join(subdir, file)
                                                logger.debug(f"Found Kogift file from original_path: {img_path}")
                                                break
                                    if img_path:
                                        break
                            if img_path:
                                break
            
            # Handle string path
            elif isinstance(cell_value, str) and cell_value not in ['-', '']:
                if cell_value.startswith(('http://', 'https://')):
                    # Web URL - we would need a downloaded version
                    if is_kogift_image:
                        logger.debug(f"Kogift string URL (needs downloaded version): {cell_value[:50]}...")
                    # For Kogift, try to find downloaded version
                    if is_kogift_image and ('koreagift.com' in cell_value or 'kogift.com' in cell_value):
                        filename = os.path.basename(cell_value)
                        base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                        
                        # Look for downloaded versions
                        for root_dir in [os.path.join(base_img_dir, 'Main'), os.path.join(base_img_dir, 'Target'), base_img_dir]:
                            if os.path.exists(root_dir):
                                for subdir, _, files in os.walk(root_dir):
                                    if 'kogift' in subdir.lower():
                                        for file in files:
                                            if filename.lower() in file.lower():
                                                img_path = os.path.join(subdir, file)
                                                logger.debug(f"Found Kogift downloaded file: {img_path}")
                                                break
                                    if img_path:
                                        break
                            if img_path:
                                break
                elif cell_value.startswith('file:///'):
                    # Local file URL
                    img_path = cell_value.replace('file:///', '').replace('/', os.sep)
                    if is_kogift_image:
                        logger.debug(f"Converted Kogift file URL to path: {img_path}")
                elif os.path.exists(cell_value):
                    # Direct file path
                    img_path = cell_value
                    if is_kogift_image:
                        logger.debug(f"Using direct Kogift file path: {img_path}")
                elif '\\' in cell_value or '/' in cell_value:
                    # Looks like a path but might not exist
                    if is_kogift_image:
                        logger.debug(f"Kogift path-like string but file not found: {cell_value[:50]}...")
                    
                    # Try to find similar file by name
                    filename = os.path.basename(cell_value)
                    base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                    
                    # Special handling for Kogift
                    if is_kogift_image:
                        # FIXED: More extensive search for Kogift images
                        for root_dir in [os.path.join(base_img_dir, 'Main'), os.path.join(base_img_dir, 'Target'), base_img_dir]:
                            if os.path.exists(root_dir):
                                for subdir, _, files in os.walk(root_dir):
                                    if 'kogift' in subdir.lower():
                                        for file in files:
                                            if filename.lower() in file.lower():
                                                img_path = os.path.join(subdir, file)
                                                logger.debug(f"Found Kogift file via path search: {img_path}")
                                                break
                                    if img_path:
                                        break
                            if img_path:
                                break
                    
                    # General search if not found yet
                    if not img_path:
                        found = False
                        for root_dir in [os.path.join(base_img_dir, 'Main'), os.path.join(base_img_dir, 'Target')]:
                            if os.path.exists(root_dir):
                                for subdir, _, files in os.walk(root_dir):
                                    for file in files:
                                        if filename in file:
                                            img_path = os.path.join(subdir, file)
                                            found = True
                                            logger.debug(f"Found similar file by name: {img_path}")
                                            break
                                    if found:
                                        break
                            if found:
                                break
            
            # If no image path could be determined, use fallback
            if not img_path and fallback_img_path:
                img_path = fallback_img_path
                if is_kogift_image:
                    logger.debug(f"Using fallback image for Kogift row {row_idx}")
            
            # Skip if no valid path was found
            if not img_path:
                if is_kogift_image:
                    logger.debug(f"No valid image path found for Kogift row {row_idx}")
                continue
            
            # Add image to worksheet if file exists and has content
            try:
                attempted_embeddings += 1
                if is_kogift_image:
                    kogift_attempted += 1
                
                # Verify file exists and is not empty
                if not os.path.exists(img_path):
                    if is_kogift_image:
                        logger.warning(f"Kogift image file not found: {img_path}")
                    else:
                        logger.warning(f"Image file not found: {img_path}")
                    continue
                
                if os.path.getsize(img_path) == 0:
                    if is_kogift_image:
                        logger.warning(f"Kogift image file is empty: {img_path}")
                    else:
                        logger.warning(f"Image file is empty: {img_path}")
                    continue
                
                # Create and resize the image
                try:
                    img = openpyxl.drawing.image.Image(img_path)
                    
                    # FIXED: Set larger image size for better visibility
                    img.width = 360  # pixels - increased from 240
                    img.height = 360  # pixels - increased from 240
                    
                    # Position image in the cell
                    img.anchor = f"{get_column_letter(col_idx)}{row_idx}"
                    
                    # Add image to worksheet
                    worksheet.add_image(img)
                    
                    # Clear text in cell to avoid showing both image and text
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.value = ""
                    
                    successful_embeddings += 1
                    if is_kogift_image:
                        kogift_successful += 1
                        logger.debug(f"Successfully added Kogift image at row {row_idx}, column {col_idx}")
                    
                except Exception as img_err:
                    if is_kogift_image:
                        logger.warning(f"Failed to add Kogift image at row {row_idx}, column {col_idx}: {img_err}")
                    else:
                        logger.warning(f"Failed to add image at row {row_idx}, column {col_idx}: {img_err}")
                    # Don't clear the cell value here - keep text as fallback
                    
            except Exception as e:
                if is_kogift_image:
                    logger.warning(f"Error processing Kogift image at row {row_idx}, column {col_idx}: {e}")
                else:
                    logger.warning(f"Error processing image at row {row_idx}, column {col_idx}: {e}")
                # Keep cell value as is for reference
    
    logger.info(f"Image processing complete. Embedded {successful_embeddings}/{attempted_embeddings} images.")
    if kogift_attempted > 0:
        logger.info(f"Kogift image processing: {kogift_successful}/{kogift_attempted} images embedded successfully.")
    
    # Track image columns for dimension adjustment
    image_cols = [(df.columns.get_loc(col) + 1, col) for col in columns_to_process]
    
    # Adjust row heights where images are embedded
    for row_idx in range(2, worksheet.max_row + 1):
        has_image = False
        for col_idx, _ in image_cols:
            cell = worksheet.cell(row=row_idx, column=col_idx)
            if cell.value == "": # Cell was cleared for image
                has_image = True
                break
        
        if has_image:
            # FIXED: Set taller row height to accommodate larger images
            worksheet.row_dimensions[row_idx].height = 380  # Increased from 280
    
    return successful_embeddings

def _apply_conditional_formatting(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Applies conditional formatting (e.g., yellow fill for price difference < -1)."""
    logger.debug("Applying conditional formatting.")

    # Find price difference columns (both regular and percentage) using new names
    price_diff_cols = [
        col for col in df.columns
        if col in ['가격차이(2)', '가격차이(3)', '가격차이(2)(%)', '가격차이(3)(%)'] # Include percentage columns
    ]

    if not price_diff_cols:
        logger.debug("No price difference columns found for conditional formatting.")
        return

    # Define yellow fill
    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    # Check dtypes of the relevant columns in the DataFrame
    for col in price_diff_cols:
        if col in df.columns:
            logger.debug(f"Conditional formatting: Column '{col}' dtype is {df[col].dtype}")
        else:
            logger.warning(f"Conditional formatting: Column '{col}' not found in DataFrame.")

    # Add detailed logging for debugging
    logger.info(f"가격차이 조건부 서식 적용 (음수 강조): {price_diff_cols}")
    logger.info(f"총 확인할 행 수: {worksheet.max_row - 1}")  # Subtract 1 for header row
    
    rows_highlighted = 0
    rows_checked = 0
    errors = 0

    # Process each row - Rely PRIMARILY on DataFrame values for consistency
    for df_row_idx in range(len(df)):
        excel_row_idx = df_row_idx + 2 # Adjust for 1-based indexing and header row
        highlight_row = False # Flag to highlight the row
        rows_checked += 1
        
        # 먼저 DataFrame에서 확인 - 더 신뢰할 수 있는 데이터
        for price_diff_col in price_diff_cols:
            if price_diff_col not in df.columns: # Skip if column doesn't exist
                continue
                
            try:
                # Get value directly from DataFrame
                value = df.iloc[df_row_idx].get(price_diff_col)

                # Check if the value is numeric and less than -1
                if pd.notna(value) and value not in ['-', '']:
                    try:
                        # 다양한 형식 처리
                        if isinstance(value, (int, float)):
                            numeric_value = float(value)
                        elif isinstance(value, str) and value.strip():
                            # 문자열 처리 - 콤마 및 기타 문자 제거
                            cleaned_value = value.replace(',', '').replace(' ', '')
                            # 음수 표시 처리 ("(100)" 형식을 "-100"으로 변환)
                            if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
                                cleaned_value = '-' + cleaned_value[1:-1]
                            numeric_value = float(cleaned_value)
                        else:
                            # 변환 불가능한 값
                            continue

                        # Apply highlight if value is less than -1 (negative)
                        if numeric_value < -1:
                            highlight_row = True
                            logger.debug(f"행 {excel_row_idx}: 가격차이 {numeric_value} < -1 (컬럼 {price_diff_col}). 하이라이팅 적용.")
                            break  # Found a reason to highlight this row
                    except (ValueError, TypeError) as e:
                        # Log if conversion fails, but don't highlight
                        logger.debug(f"행 {excel_row_idx}: 숫자 변환 실패 '{value}' (컬럼 {price_diff_col}): {e}")
                        # 변환 오류는 무시하고 계속 진행
            except IndexError:
                 logger.warning(f"인덱스 오류: DataFrame 행 {df_row_idx} 접근 실패 (조건부 서식 적용 중)")
                 continue # Skip this row if index is out of bounds
            except Exception as e:
                 logger.error(f"DataFrame 행 {df_row_idx}, 컬럼 {price_diff_col} 처리 중 오류: {e}")
                 errors += 1

        # 이제 실제 Excel 워크시트에서 확인 (데이터프레임에서 찾지 못한 경우)
        if not highlight_row:
            try:
                # Excel columns are 1-indexed
                for col_idx in range(1, worksheet.max_column + 1):
                    # Get header to identify price difference columns
                    header = worksheet.cell(row=1, column=col_idx).value
                    
                    if header in price_diff_cols:
                        cell = worksheet.cell(row=excel_row_idx, column=col_idx)
                        if cell.value and cell.value != '-':
                            try:
                                # Similar conversion logic as above
                                if isinstance(cell.value, (int, float)):
                                    numeric_value = float(cell.value)
                                elif isinstance(cell.value, str) and cell.value.strip():
                                    cleaned_value = cell.value.replace(',', '').replace(' ', '')
                                    if cleaned_value.startswith('(') and cleaned_value.endswith(')'):
                                        cleaned_value = '-' + cleaned_value[1:-1]
                                    numeric_value = float(cleaned_value)
                                else:
                                    continue
                                    
                                if numeric_value < -1:
                                    highlight_row = True
                                    logger.debug(f"Excel에서 직접 찾음: 행 {excel_row_idx}, 컬럼 {header} 값 {numeric_value} < -1")
                                    break
                            except (ValueError, TypeError):
                                # Invalid value, just continue
                                pass
            except Exception as excel_err:
                logger.warning(f"Excel 확인 중 오류 발생 (행 {excel_row_idx}): {excel_err}")
                # Continue to use the DataFrame result

        # If the flag is set, highlight the entire row in Excel
        if highlight_row:
            rows_highlighted += 1
            for col_idx_excel in range(1, worksheet.max_column + 1):
                try:
                    cell_to_fill = worksheet.cell(row=excel_row_idx, column=col_idx_excel)
                    # 현재 값 및 서식 보존
                    current_value = cell_to_fill.value
                    
                    # 기존 서식에 노란색 배경 추가
                    cell_to_fill.fill = yellow_fill
                except Exception as e:
                    logger.error(f"셀 서식 적용 오류 R{excel_row_idx}C{col_idx_excel}: {e}")
                    errors += 1

    # Log summary of highlighting results
    logger.info(f"조건부 서식 적용 완료: {rows_highlighted}개 행에 가격차이 < -1 하이라이팅 적용됨 (검사 행: {rows_checked}, 오류: {errors})")

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

def _add_hyperlinks_to_worksheet(worksheet, df, hyperlinks_as_formulas=False):
    """
    Adds hyperlinks to URL cells in the worksheet.
    If hyperlinks_as_formulas=True, use Excel formulas for hyperlinks.
    Otherwise, use openpyxl's Hyperlink object.
    """
    try:
        # Define columns that should contain hyperlinks
        link_columns = [col for col in df.columns if any(term in col.lower() for term in ['링크', 'link', 'url'])]
        
        # Process each URL column
        total_urls_processed = 0
        
        for col in link_columns:
            if col in df.columns:
                col_idx = list(df.columns).index(col) + 1  # 1-based indexing for Excel
                
                # Loop through each cell in this column
                for row_idx, value in enumerate(df[col], 2):  # Start from row 2 (after header)
                    # Handle Series objects
                    if isinstance(value, pd.Series):
                        # Take the first non-empty value
                        for item in value:
                            if pd.notna(item) and item not in ['-', '']:
                                value = item
                                break
                        else:
                            value = ''
                    
                    # Skip empty values
                    if pd.isna(value) or value in ['', '-', 'None', 'nan']:
                        continue
                        
                    # Convert to string
                    url = str(value)
                    
                    # Extract URL from dictionary if needed
                    if isinstance(value, dict) and 'url' in value:
                        url = value['url']
                    
                    # Skip non-URL values
                    if not ('http://' in url or 'https://' in url or 'file:///' in url):
                        continue
                        
                    # Clean URL if needed
                    url = url.strip()
                    
                    try:
                        # Cell to apply hyperlink
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        
                        if hyperlinks_as_formulas:
                            # Use Excel HYPERLINK formula
                            display_text = url
                            if len(display_text) > 50:
                                display_text = display_text[:47] + "..."
                            
                            cell.value = f'=HYPERLINK("{url}","{display_text}")'
                        else:
                            # Use openpyxl hyperlink object
                            cell.hyperlink = url
                            cell.value = url
                            
                            # Style for hyperlink
                            cell.font = Font(color="0563C1", underline="single")
                        
                        total_urls_processed += 1
                    except Exception as hyperlink_err:
                        logger.warning(f"Error adding hyperlink in row {row_idx}, col {col}: {hyperlink_err}")
                        # Keep original text if hyperlink fails
                        cell.value = url
                        
        logger.info(f"Processed link columns as plain text. Found {total_urls_processed} URLs across link columns.")
    except Exception as e:
        logger.warning(f"Error processing hyperlinks: {e}")
        logger.debug(traceback.format_exc())

def _add_header_footer(worksheet: openpyxl.worksheet.worksheet.Worksheet):
    """Adds standard header and footer."""
    try:
        # Check if header_footer attribute exists (some versions don't support it)
        if hasattr(worksheet, 'header_footer'):
            current_date = datetime.now().strftime("%Y-%m-%d %H:%M")
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
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # 1) Rename columns EARLY so that original names are preserved before we drop/reorder columns
    df.rename(columns=COLUMN_RENAME_MAP, inplace=True, errors='ignore')

    # 2) Ensure all required columns from FINAL_COLUMN_ORDER exist
    for col in FINAL_COLUMN_ORDER:
        if col not in df.columns:
            df[col] = ""
            logger.debug(f"Added missing column '{col}' to DataFrame before ordering.")

    # 3) Re-order columns based on FINAL_COLUMN_ORDER (keep only expected columns)
    df = df[[col for col in FINAL_COLUMN_ORDER if col in df.columns]]

    # For upload file, modify image column values to be web URLs or empty
    if skip_images:
        # Image columns now use new names from FINAL_COLUMN_ORDER / IMAGE_COLUMNS constant
        # final_image_columns = ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'] # Already defined
        image_columns = [col for col in df.columns if col in IMAGE_COLUMNS] # Use the constant

        for col in image_columns:
            # Replace image dict/path with web URL or empty string for upload file
            df[col] = df[col].apply(
                lambda x:
                    # Case 1: Input is a dictionary with 'url' key
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

def create_split_excel_outputs(df_finalized: pd.DataFrame, output_path_base: str) -> tuple:
    """
    Creates two Excel outputs:
    1. Result file (with images embedded)
    2. Upload file (with image links only)
    
    Expects df_finalized to have image data dictionaries in columns defined by IMAGE_COLUMNS.
    """
    # Default return values (used in case of error)
    result_path = None
    result_success = False
    upload_path = None
    upload_success = False
    
    try:
        logger.info(f"Starting creation of split Excel outputs from finalized DataFrame (Shape: {df_finalized.shape})")
        
        # Validate the DataFrame
        if df_finalized is None or df_finalized.empty:
            logger.error("Input DataFrame is None or empty. Cannot create Excel files.")
            return False, False, None, None
        
        # Ensure columns are properly ordered and contain expected final names
        # This assumes finalize_dataframe_for_excel already did its job
        if not all(col in df_finalized.columns for col in FINAL_COLUMN_ORDER):
             missing_req = [col for col in FINAL_COLUMN_ORDER if col not in df_finalized.columns]
             logger.error(f"Finalized DataFrame is missing required columns: {missing_req}. Cannot proceed.")
             return False, False, None, None
        # Reorder just in case
        df_finalized = df_finalized[FINAL_COLUMN_ORDER]
        
        # Get file source info for naming (Simplified)
        source_info = "Unknown"
        mgmt_type = "승인관리"  # Default type
        row_count = len(df_finalized)
        
        try:
            if '구분' in df_finalized.columns:
                source_val = df_finalized['구분'].mode()[0] if not df_finalized['구분'].mode().empty else 'A'
                mgmt_type = "승인관리" if source_val == 'A' else "가격관리" if source_val == 'P' else str(source_val)
            if '업체명' in df_finalized.columns:
                source_info = df_finalized['업체명'].mode()[0] if not df_finalized['업체명'].mode().empty else "Unknown"
        except Exception as e:
            logger.warning(f"Error getting source/company name: {e}")
        
        # Create timestamped filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_part = datetime.now().strftime("%Y%m%d")
        result_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
        upload_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
        
        # Ensure output directory exists
        output_dir = Path(output_path_base)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        result_path = str(output_dir / result_filename)
        upload_path = str(output_dir / upload_filename)
        
        logger.info(f"Result file path (with images): {result_path}")
        logger.info(f"Upload file path (links only): {upload_path}")
        
        # -----------------------------------------
        # 1. Create Result File (with images)
        # -----------------------------------------
        try:
            logger.info(f"Attempting to write result file: {result_path}")
            
            # Create workbook and worksheet
            workbook_result = openpyxl.Workbook()
            worksheet_result = workbook_result.active
            worksheet_result.title = "제품 가격 비교"
            
            # Prepare DataFrame for writing (convert complex objects to strings temporarily)
            df_for_write = df_finalized.copy()
            image_columns_in_df = [col for col in IMAGE_COLUMNS if col in df_for_write.columns]
            for img_col in image_columns_in_df:
                 # Store local path string for image insertion later, or '-' if invalid
                 df_for_write[img_col] = df_finalized[img_col].apply(
                     lambda x: x.get('local_path') if isinstance(x, dict) and x.get('local_path') else '-'
                 )
            # Convert any remaining non-scalar types (just in case)
            df_for_write = df_for_write.astype(str).replace({'nan': '-', 'None': '-'})
            
            # Write header
            for col_idx, col_name in enumerate(df_finalized.columns, 1):
                worksheet_result.cell(row=1, column=col_idx, value=col_name)
            
            # Write data from df_for_write
            for row_idx, row in enumerate(df_for_write.itertuples(index=False), 2):
                for col_idx, value in enumerate(row, 1):
                    worksheet_result.cell(row=row_idx, column=col_idx, value=value if value != '-' else "")
            
            # Apply basic formatting and hyperlinks
            _apply_basic_excel_formatting(worksheet_result, df_finalized.columns.tolist())
            _add_hyperlinks_to_worksheet(worksheet_result, df_finalized, hyperlinks_as_formulas=False)
            _add_header_footer(worksheet_result)
            
            # Remove auto filter if present
            if hasattr(worksheet_result, 'auto_filter') and worksheet_result.auto_filter:
                worksheet_result.auto_filter.ref = None
            
            # Save workbook before adding images
            workbook_result.save(result_path)
            
            # --- Add Images --- 
            try:
                logger.info("Adding images to result file...")
                # Re-load the workbook
                workbook_with_images = openpyxl.load_workbook(result_path)
                worksheet_with_images = workbook_with_images.active
                
                # Adjust row/column dimensions
                for row_idx in range(2, len(df_finalized) + 2):
                    worksheet_with_images.row_dimensions[row_idx].height = 120
                for img_col in image_columns_in_df:
                    col_idx = df_finalized.columns.get_loc(img_col) + 1
                    worksheet_with_images.column_dimensions[get_column_letter(col_idx)].width = 22
                
                # Insert images using local_path from the original df_finalized
                images_added = 0
                for row_idx, (_, row_data) in enumerate(df_finalized.iterrows(), 2):
                    for img_col in image_columns_in_df:
                        col_idx = df_finalized.columns.get_loc(img_col) + 1
                        cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                        
                        img_dict = row_data[img_col]
                        local_path = img_dict.get('local_path') if isinstance(img_dict, dict) else None
                        
                        if local_path and os.path.exists(local_path):
                            try:
                                img = openpyxl.drawing.image.Image(local_path)
                                img.width = 160
                                img.height = 160
                                img.anchor = cell.coordinate
                                worksheet_with_images.add_image(img)
                                cell.value = "" # Clear the placeholder path
                                images_added += 1
                            except Exception as img_err:
                                logger.warning(f"Could not add image {local_path} to cell {cell.coordinate}: {img_err}")
                                cell.value = ERROR_MESSAGES.get('invalid_image', 'Invalid Image')
                                cell.font = INVALID_LINK_FONT # Use red font for error
                        elif isinstance(img_dict, dict): # Dictionary exists but path is missing/invalid
                             logger.debug(f"Image path missing or invalid in dict for cell {cell.coordinate}: {img_dict}")
                             cell.value = ERROR_MESSAGES.get('file_not_found', 'No Image File')
                             cell.font = INVALID_LINK_FONT
                        else: # Value wasn't a dict or was None/placeholder already
                             pass # Keep the existing placeholder value ('-' or empty)

                logger.info(f"Added {images_added} images to result file.")
                # Save the final workbook with images
                workbook_with_images.save(result_path)
                result_success = True
                logger.info(f"Result file saved successfully: {result_path}")
            except Exception as img_add_err:
                 logger.error(f"Error adding images to result file: {img_add_err}", exc_info=True)
                 # result_success remains False, result_path might exist but without images

        except Exception as e:
            logger.error(f"Error creating result Excel file: {e}", exc_info=True)
            result_success = False

        # -----------------------------------------
        # 2. Create Upload File (links only)
        # -----------------------------------------
        try:
            logger.info(f"Attempting to write upload file: {upload_path}")
            
            # Create DataFrame for upload file
            df_upload = pd.DataFrame()
            upload_cols_map = {v: k for k, v in COLUMN_MAPPING_FINAL_TO_UPLOAD.items()} # Map UPLOAD name -> FINAL name
            
            for upload_col in UPLOAD_COLUMN_ORDER:
                 if upload_col in upload_cols_map:
                     final_col = upload_cols_map[upload_col]
                     if final_col in df_finalized.columns:
                          # Check if it's an image column needing URL extraction
                          if final_col in IMAGE_COLUMNS:
                              logger.debug(f"Extracting URL for '{upload_col}' from '{final_col}'")
                              # Apply function to extract URL from dict, handle errors
                              df_upload[upload_col] = df_finalized[final_col].apply(
                                  lambda x: x.get('url') if isinstance(x, dict) and x.get('url') else ''
                              )
                          else:
                              # Copy data directly for non-image columns
                              df_upload[upload_col] = df_finalized[final_col]
                     else:
                          logger.warning(f"Source column '{final_col}' for upload column '{upload_col}' not found in finalized df. Adding empty.")
                          df_upload[upload_col] = None # Or appropriate default
                 else:
                      logger.warning(f"Upload column '{upload_col}' not found in mapping. Adding empty.")
                      df_upload[upload_col] = None
            
            # Fill NA values with empty strings for upload file
            df_upload.fillna('', inplace=True)
            
            # Create upload workbook
            workbook_upload = openpyxl.Workbook()
            worksheet_upload = workbook_upload.active
            worksheet_upload.title = "업로드용 데이터"
            
            # Write header for upload file
            for col_idx, col_name in enumerate(df_upload.columns, 1):
                worksheet_upload.cell(row=1, column=col_idx, value=col_name)
            
            # Write data for upload file
            for row_idx, row in enumerate(df_upload.itertuples(index=False), 2):
                for col_idx, value in enumerate(row, 1):
                    worksheet_upload.cell(row=row_idx, column=col_idx, value=value)
            
            # Apply basic formatting (optional for upload file)
            _apply_basic_excel_formatting(worksheet_upload, df_upload.columns.tolist())
            
            # FIXED: Add filter to upload file
            worksheet_upload.auto_filter.ref = worksheet_upload.dimensions
            logger.info("Added filter to upload Excel file")
            
            # Save upload workbook
            workbook_upload.save(upload_path)
            upload_success = True
            logger.info(f"Upload file saved successfully: {upload_path}")

        except Exception as e:
            logger.error(f"Error creating upload Excel file: {e}", exc_info=True)
            upload_success = False
            
    except Exception as outer_err:
        logger.error(f"Major error during split Excel output creation: {outer_err}", exc_info=True)
        # Ensure flags are False and paths are None
        result_success = False
        upload_success = False
        result_path = None
        upload_path = None

    return result_success, upload_success, result_path, upload_path


@safe_excel_operation
def create_final_output_excel(df: pd.DataFrame, output_path: str) -> bool:
    """
    (Revised) Creates a single final formatted Excel file.
    This function now utilizes finalize_dataframe_for_excel and applies full formatting.
    It's kept for potential direct use but create_split_excel_outputs is preferred
    if both result and upload files are needed.

    Args:
        df: DataFrame containing the data to save
        output_path: Path where the Excel file will be saved

    Returns:
        bool: True if successful, False otherwise
    """
    if df is None:
        logger.error("Cannot create Excel file: Input DataFrame is None.")
        return False

    logger.info(f"Starting creation of single final Excel output: {output_path}")
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)


    # 1. Finalize the DataFrame (Rename, Order, Clean)
    df_finalized = finalize_dataframe_for_excel(df) # Use the refactored function

    if df_finalized.empty and not df.empty: # Check if finalization failed or cleared data
        logger.error("DataFrame became empty after finalization step. Cannot save Excel.")
        return False
    elif df_finalized.empty and df.empty:
        logger.warning("Input DataFrame was empty. Saving Excel file with only headers.")
        # Allow proceeding to create an empty file with headers


    # 2. Check if file is locked
    if os.path.exists(output_path):
        try:
            with open(output_path, 'a+b'):
                 pass # Check lock
        except (IOError, PermissionError) as lock_err:
             logger.error(f"Output file {output_path} is locked: {lock_err}. Cannot save.")
             # Optional: Could try alternative path like in split function
             # timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
             # alternative_path = f"{os.path.splitext(output_path)[0]}_{timestamp}{os.path.splitext(output_path)[1]}"
             # logger.warning(f"Attempting alternative path: {alternative_path}")
             # output_path = alternative_path
             # But for now, just fail if locked.
             return False


    # 3. Save finalized data to Excel using openpyxl engine
    try:
        logger.info(f"Attempting to write final Excel: {output_path} with {len(df_finalized)} rows.")
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_finalized.to_excel(writer, index=False, sheet_name='Results', na_rep='')
            worksheet = writer.sheets['Results']
            workbook = writer.book # Get workbook if needed later
            logger.info(f"Data ({worksheet.max_row -1} rows) written to sheet 'Results'. Applying formatting...")

            # --- Apply Full Formatting ---
            _apply_column_widths(worksheet, df_finalized)
            _apply_cell_styles_and_alignment(worksheet, df_finalized)
            if not df_finalized.empty: # Avoid processing images on empty df
                _process_image_columns(worksheet, df_finalized)
                _adjust_image_cell_dimensions(worksheet, df_finalized)
            else:
                 logger.info("Skipping image processing and dimension adjustment for empty DataFrame.")
            _add_hyperlinks_to_worksheet(worksheet, df_finalized)
            _apply_conditional_formatting(worksheet, df_finalized)
            _setup_page_layout(worksheet)
            _add_header_footer(worksheet)
            # _apply_table_format(worksheet) # Keep disabled

        logger.info(f"Successfully created and formatted final Excel file: {output_path}")
        return True

    except PermissionError as pe:
        logger.error(f"Permission denied writing final Excel file '{output_path}'. Is it open? Error: {pe}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error creating final Excel file '{output_path}': {e}", exc_info=True)
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
    
    # Get image column indices using the IMAGE_COLUMNS constant
    image_cols = {col: idx for idx, col in enumerate(df.columns, 1) if col in IMAGE_COLUMNS}
    
    if not image_cols:
        return

    # Increase column widths for image columns to accommodate larger images
    for col_name, col_idx in image_cols.items():
        try:
            col_letter = get_column_letter(col_idx)
            # FIXED: Use larger column width for image columns
            worksheet.column_dimensions[col_letter].width = 80  # Increased from 60
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
            # FIXED: Set larger row height to accommodate bigger images
            worksheet.row_dimensions[row_idx].height = 380  # Increased from 280
            
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

# --- Refactored Data Finalization ---
def finalize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finalizes the DataFrame for Excel output: Renames columns, ensures all required columns exist,
    sets the final column order, and applies basic type formatting.
    Assumes the final image columns (e.g., '본사 이미지') already contain the image data dictionaries.
    """
    if df is None:
        logger.error("Input DataFrame is None, cannot finalize.")
        return pd.DataFrame(columns=FINAL_COLUMN_ORDER)

    logger.info(f"Finalizing DataFrame for Excel. Input shape: {df.shape}")
    logger.debug(f"Input columns: {df.columns.tolist()}")

    # --- Column Mapping for final output (inverse of FINAL_TO_UPLOAD) --- 
    # Needed if input df uses upload-style names
    COLUMN_UPLOAD_TO_FINAL_MAP = {v: k for k, v in COLUMN_MAPPING_FINAL_TO_UPLOAD.items()}
    
    # --- Final Image Columns (no longer need temporary map) ---
    # final_image_columns = IMAGE_COLUMNS # Use constant defined above

    # --- Deduplicate Columns --- 
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        logger.warning(f"Found {len(duplicate_cols)} duplicate column names: {duplicate_cols}")
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        logger.info(f"Removed duplicate columns. New shape: {df.shape}")

    df_processed = df.copy()

    # --- Rename columns to FINAL names (handle both possible input conventions) ---
    # First, try renaming from potential UPLOAD names to FINAL names
    df_processed = df_processed.rename(columns=COLUMN_UPLOAD_TO_FINAL_MAP, errors='ignore')
    # Then, ensure any original FINAL names are kept (self-map)
    df_processed = df_processed.rename(columns=COLUMN_RENAME_MAP, errors='ignore') 
    logger.debug(f"Columns after renaming: {df_processed.columns.tolist()}")

    # --- Create final DataFrame with FINAL_COLUMN_ORDER --- 
    output_df = pd.DataFrame()
    processed_cols = df_processed.columns

    for col in FINAL_COLUMN_ORDER:
        if col in processed_cols:
            # Directly copy the column, including final image columns which should have dicts
            output_df[col] = df_processed[col]
        else:
            # Column from FINAL_COLUMN_ORDER is missing entirely
            logger.warning(f"Final required column '{col}' is missing from processed data. Adding as None.")
            output_df[col] = None

    # --- Consolidate Price Columns (using df_processed as source) --- 
    price_alternates = {
        '판매가(V포함)(2)': ['판매단가2(VAT포함)', '판매단가(V포함)(2)'],
        '판매단가(V포함)(3)': ['판매단가3 (VAT포함)', '판매단가(V포함)(3)']
    }
    logger.info("Consolidating data from alternate price columns...")
    for target_col, alt_cols in price_alternates.items():
        if target_col in output_df.columns:
            target_is_empty = output_df[target_col].isnull().all()
            if target_is_empty:
                logger.debug(f"Target column '{target_col}' is currently empty.")
                # Find the first available alternate column in df_processed with data
                for alt_col in alt_cols:
                    if alt_col in df_processed.columns and not df_processed[alt_col].isnull().all():
                        logger.info(f"Found data in alternate column '{alt_col}'. Copying to '{target_col}'.")
                        try:
                             output_df[target_col] = pd.to_numeric(df_processed[alt_col], errors='coerce')
                        except Exception as copy_err:
                             logger.error(f"Error coercing/copying from {alt_col} to {target_col}: {copy_err}")
                             output_df[target_col] = df_processed[alt_col] # Fallback copy
                        break # Stop after finding the first alternate with data

    # --- Format Numeric Columns --- 
    # Use the constant IMAGE_COLUMNS directly
    logger.info("Applying numeric formatting...")
    for col in output_df.columns:
        if col in IMAGE_COLUMNS: continue # Skip final image columns (contain dicts)
        
        is_numeric_col = (
            col in PRICE_COLUMNS or
            col in QUANTITY_COLUMNS or
            col in PERCENTAGE_COLUMNS or
            col in ['가격차이(2)', '가격차이(3)']
        )
        if is_numeric_col:
            try:
                output_df[col] = pd.to_numeric(output_df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Could not convert column '{col}' to numeric: {e}")

    # --- Handle Missing Values --- 
    # Replace specific pandas NA types with None for broader compatibility (like JSON, Excel writers)
    # Important: Convert nullable integers/floats AFTER numeric conversion
    for col in output_df.columns:
         if pd.api.types.is_integer_dtype(output_df[col].dtype) and output_df[col].isnull().any():
              # For nullable integer columns, keep pd.NA or convert to float then None if needed
              # output_df[col] = output_df[col].astype(float).replace({np.nan: None}) # Option 1: Convert to float
              pass # Option 2: Keep as nullable int (pd.NA), Excel writer should handle
         elif pd.api.types.is_float_dtype(output_df[col].dtype):
              output_df[col] = output_df[col].replace({np.nan: None})
         elif output_df[col].dtype == object:
              # Replace pd.NA and np.nan in object columns
              output_df[col] = output_df[col].replace({pd.NA: None, np.nan: None})
         
    # Set default '-' for remaining None/empty strings in non-image columns
    logger.info("Setting default values for empty cells ('-')...")
    for col in output_df.columns:
        if col not in IMAGE_COLUMNS:
            output_df[col] = output_df[col].apply(
                lambda x: '-' if (x is None or x == '') else x
            )

    logger.info(f"DataFrame finalized. Output shape: {output_df.shape}")
    logger.debug(f"Final columns: {output_df.columns.tolist()}")
    
    # --- Final Verification (Optional) ---
    # Example: Check image columns contain dictionaries or '-'
    for img_col in IMAGE_COLUMNS:
         if img_col in output_df.columns:
              # Allow None/NaN in addition to dict and '-'
              invalid_types = output_df[img_col].apply(
                  lambda x: not isinstance(x, dict) and pd.notna(x) and x != '-'
              ).sum()
              if invalid_types > 0:
                   logger.warning(f"Column '{img_col}' contains {invalid_types} entries that are not dict, None/NA, or '-'")

    return output_df

def _apply_basic_excel_formatting(worksheet, column_list):
    """
    Applies basic Excel formatting to the worksheet:
    - Sets column widths
    - Applies header styles
    - Applies basic cell formatting
    """
    try:
        # 1. Set column widths based on content type
        for col_idx, col_name in enumerate(column_list, 1):
            # Default width based on column type
            if '이미지' in col_name or 'image' in col_name.lower():
                width = 30  # Image columns
            elif 'URL' in col_name or '링크' in col_name or 'link' in col_name.lower():
                width = 40  # URL columns
            elif '상품명' in col_name or '제품명' in col_name:
                width = 35  # Product name columns
            elif '코드' in col_name or 'code' in col_name.lower():
                width = 15  # Code columns
            else:
                width = 20  # Default width
            
            # Set column width
            column_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[column_letter].width = width
        
        # 2. Apply header style
        header_style = NamedStyle(name='header_style')
        header_style.font = Font(bold=True, size=11)
        header_style.fill = PatternFill(start_color='E0E0E0', end_color='E0E0E0', fill_type='solid')
        header_style.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        header_style.border = Border(
            left=Side(style='thin'), 
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        # Apply header style to first row
        for col_idx in range(1, len(column_list) + 1):
            cell = worksheet.cell(row=1, column=col_idx)
            cell.style = header_style
        
        # Make header row taller
        worksheet.row_dimensions[1].height = 30
        
        # 3. Apply basic data cell formatting
        data_style = NamedStyle(name='data_style')
        data_style.alignment = Alignment(vertical='center', wrap_text=True)
        data_style.border = Border(
            left=Side(style='thin'), 
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        # Get the number of rows in the worksheet (excluding header)
        max_row = worksheet.max_row
        
        # Apply data style to all data cells
        for row_idx in range(2, max_row + 1):
            for col_idx in range(1, len(column_list) + 1):
                cell = worksheet.cell(row=row_idx, column=col_idx)
                cell.style = data_style
                
                # Specific formatting for certain column types
                col_name = column_list[col_idx - 1]
                
                # Price columns - right align and format as number
                if '단가' in col_name or '가격' in col_name or 'price' in col_name.lower():
                    cell.alignment = Alignment(horizontal='right', vertical='center')
                    
                # Code/ID columns - center align
                elif '코드' in col_name or 'ID' in col_name or 'id' in col_name.lower():
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    
                # URL/Link columns - left align
                elif 'URL' in col_name or '링크' in col_name or 'link' in col_name.lower():
                    cell.alignment = Alignment(horizontal='left', vertical='center')
                    
                # Regular text columns - left align
                else:
                    cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
        
        # 4. Freeze the header row
        worksheet.freeze_panes = 'A2'
        
        # FIXED: Remove the auto-filter functionality
        # Explicitly remove any existing filter
        if hasattr(worksheet, 'auto_filter') and worksheet.auto_filter:
            worksheet.auto_filter.ref = None
        
        logger.debug(f"Applied basic Excel formatting to worksheet (header + {max_row-1} data rows)")
        
    except Exception as e:
        logger.warning(f"Error applying basic Excel formatting: {e}")
        logger.debug(traceback.format_exc())

