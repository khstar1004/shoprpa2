import os
import logging
import pandas as pd
import datetime
from datetime import datetime, timedelta
import openpyxl
from openpyxl.styles import Alignment, Border, Side, Font, PatternFill, NamedStyle
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from PIL import Image
import functools
from functools import wraps
import configparser
import time
import re
from pathlib import Path
import traceback
import numpy as np
import json
from copy import copy
from decimal import Decimal
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
    # Use default directory if config fails
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
        'image': 21.44, # Width for image columns - UPDATED to 21.44 per requirement
        'name': 45,  # 상품명
        'link': 35,
        'price': 14,
        'percent': 10,
        'quantity': 10,
        'code': 12,
        'category': 20,
        'text_short': 7, # UPDATED to 7 per requirement
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
        elif col_name_str in ['구분', '담당자', '업체명', '업체코드']: # UPDATED: Added more columns to use short width
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
        
        # UPDATED: Enable wrap text for headers to display in 2 lines
        header_alignment = copy(HEADER_ALIGNMENT)
        header_alignment.wrap_text = True
        cell.alignment = header_alignment
        
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
            # UPDATED: Added explicit right alignment for requested columns
            quantity_columns = ['기본수량(1)', '기본수량(2)', '판매가(V포함)(2)', '기본수량(3)']
            if col_name_str in quantity_columns:
                cell.alignment = RIGHT_ALIGNMENT
            elif is_pct_col or ((col_name_str in PRICE_COLUMNS or col_name_str in QUANTITY_COLUMNS) and is_numeric_value):
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
        
    TODO: This function needs refactoring due to its length (500+ lines) and complexity.
    Consider breaking it down into smaller, more focused functions for each image source type
    (Haereum, Kogift, Naver) and separating the URL extraction logic from image embedding logic.
    """
    import openpyxl
    from openpyxl.drawing.image import Image
    
    # Initialize tracking variables
    successful_embeddings = 0
    attempted_embeddings = 0
    kogift_successful = 0
    kogift_attempted = 0
    naver_successful = 0
    naver_attempted = 0
    
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
        is_naver_image = 'naver' in column.lower() or '네이버' in column       # Add tracking for Naver columns
        
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
                    
                    # FIXED: Special handling for Naver images - log and verify paths
                    if is_naver_image:
                        logger.debug(f"Found Naver local_path: {img_path}")
                        
                        # Verify the path exists and is absolute
                        if not os.path.isabs(img_path):
                            abs_path = os.path.abspath(img_path)
                            logger.debug(f"Converting relative Naver path to absolute: {img_path} -> {abs_path}")
                            img_path = abs_path
                        
                        # Verify the file exists
                        if not os.path.exists(img_path):
                            logger.warning(f"Naver image path doesn't exist: {img_path}")
                            
                            # Try alternative extensions
                            base_path = os.path.splitext(img_path)[0]
                            for ext in ['.jpg', '.jpeg', '.png', '.gif']:
                                alt_path = f"{base_path}{ext}"
                                if os.path.exists(alt_path):
                                    logger.info(f"Found alternative Naver image path: {alt_path}")
                                    img_path = alt_path
                                    break
                            else:
                                # If no alternative found, try looking for _nobg version
                                nobg_path = f"{base_path}_nobg.png"
                                if os.path.exists(nobg_path):
                                    logger.info(f"Found _nobg version of Naver image: {nobg_path}")
                                    img_path = nobg_path
                    elif is_kogift_image:
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
                            
                            # ADDED: Additional _nobg pattern search
                            # Extract base filename and check for _nobg variants
                            if '_nobg' not in filename.lower():
                                base_name = os.path.splitext(filename)[0]
                                nobg_variant = f"{base_name}_nobg.png"
                                possible_locations.extend([
                                    os.path.join(base_img_dir, 'Main', 'Kogift', nobg_variant),
                                    os.path.join(base_img_dir, 'Main', 'kogift', nobg_variant),
                                    os.path.join(base_img_dir, 'Kogift', nobg_variant),
                                    os.path.join(base_img_dir, 'kogift', nobg_variant)
                                ])
                                
                                # If filename doesn't start with kogift_, also try with prefix
                                if not base_name.lower().startswith('kogift_'):
                                    prefixed_nobg = f"kogift_{base_name}_nobg.png"
                                    possible_locations.extend([
                                        os.path.join(base_img_dir, 'Main', 'Kogift', prefixed_nobg),
                                        os.path.join(base_img_dir, 'Main', 'kogift', prefixed_nobg),
                                        os.path.join(base_img_dir, 'Kogift', prefixed_nobg),
                                        os.path.join(base_img_dir, 'kogift', prefixed_nobg)
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
                                            # ENHANCED: Check for both exact matches and _nobg variants
                                            filename_to_check = os.path.basename(cell_value)
                                            
                                            # Direct match
                                            if filename_to_check.lower() in file.lower():
                                                img_path = os.path.join(subdir, file)
                                                logger.debug(f"Found Kogift file via path search: {img_path}")
                                                break
                                                
                                            # Check if this could be a _nobg variant of our target
                                            if '_nobg' in file.lower() and filename_to_check.lower().endswith(('.jpg', '.png', '.jpeg')):
                                                # Extract the base part of our filename (remove extension)
                                                base_filename = os.path.splitext(filename_to_check)[0]
                                                # Check if this file is the _nobg variant
                                                if f"{base_filename}_nobg" in file.lower():
                                                    img_path = os.path.join(subdir, file)
                                                    logger.debug(f"Found Kogift _nobg variant via path search: {img_path}")
                                                    break
                                                    
                                            # Check if this is a regular file that has a matching _nobg variant
                                            if not '_nobg' in file.lower() and file.lower() == filename_to_check.lower():
                                                # Check if there's a corresponding _nobg file
                                                base_file = os.path.splitext(file)[0]
                                                nobg_variant = f"{base_file}_nobg.png"
                                                nobg_path = os.path.join(subdir, nobg_variant)
                                                if os.path.exists(nobg_path):
                                                    img_path = nobg_path  # Use the _nobg version instead
                                                    logger.debug(f"Found and using Kogift _nobg variant for regular file: {img_path}")
                                                    break
                                                else:
                                                    # Still use the regular file if no _nobg exists
                                                    img_path = os.path.join(subdir, file)
                                                    logger.debug(f"Found Kogift regular file (no _nobg variant): {img_path}")
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
                if is_naver_image:
                    naver_attempted += 1
                
                # Verify file exists and is not empty
                if not os.path.exists(img_path):
                    if is_kogift_image:
                        logger.warning(f"Kogift image file not found: {img_path}")
                    elif is_naver_image:
                        logger.warning(f"Naver image file not found: {img_path}")
                    else:
                        logger.warning(f"Image file not found: {img_path}")
                    continue
                
                if os.path.getsize(img_path) == 0:
                    if is_kogift_image:
                        logger.warning(f"Kogift image file is empty: {img_path}")
                    elif is_naver_image:
                        logger.warning(f"Naver image file is empty: {img_path}")
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
                    if is_naver_image:
                        naver_successful += 1
                        logger.debug(f"Successfully added Naver image at row {row_idx}, column {col_idx}")
                    
                except Exception as img_err:
                    if is_kogift_image:
                        logger.warning(f"Failed to add Kogift image at row {row_idx}, column {col_idx}: {img_err}")
                    elif is_naver_image:
                        logger.warning(f"Failed to add Naver image at row {row_idx}, column {col_idx}: {img_err}")
                    else:
                        logger.warning(f"Failed to add image at row {row_idx}, column {col_idx}: {img_err}")
                    # Don't clear the cell value here - keep text as fallback
                    
            except Exception as e:
                if is_kogift_image:
                    logger.warning(f"Error processing Kogift image at row {row_idx}, column {col_idx}: {e}")
                elif is_naver_image:
                    logger.warning(f"Error processing Naver image at row {row_idx}, column {col_idx}: {e}")
                else:
                    logger.warning(f"Error processing image at row {row_idx}, column {col_idx}: {e}")
                # Keep cell value as is for reference
    
    logger.info(f"Image processing complete. Embedded {successful_embeddings}/{attempted_embeddings} images.")
    if kogift_attempted > 0:
        logger.info(f"Kogift image processing: {kogift_successful}/{kogift_attempted} images embedded successfully.")
    if naver_attempted > 0:
        logger.info(f"Naver image processing: {naver_successful}/{naver_attempted} images embedded successfully.")
    
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

    # First check if these columns actually exist in the DataFrame
    existing_diff_cols = [col for col in price_diff_cols if col in df.columns]
    if not existing_diff_cols:
        logger.warning("None of the price difference columns exist in the DataFrame. Skipping conditional formatting.")
        return

    # Add detailed logging for debugging
    logger.info(f"가격차이 조건부 서식 적용 시작 (음수 강조): {existing_diff_cols}")
    logger.info(f"총 확인할 행 수: {worksheet.max_row - 1}")  # Subtract 1 for header row
    
    # Log column data types for debugging
    for col in existing_diff_cols:
        logger.info(f"열 '{col}' 데이터 타입: {df[col].dtype}")
        # Try to count negative values in each column
        try:
            if df[col].dtype in ['int64', 'float64']:
                # For numeric columns, count directly
                neg_count = (df[col] < -1).sum()
                logger.info(f"열 '{col}'에서 -1 미만인 값: {neg_count}개")
            else:
                # For non-numeric columns, try to convert first
                try:
                    neg_count = (pd.to_numeric(df[col], errors='coerce') < -1).sum()
                    logger.info(f"열 '{col}'에서 -1 미만인 값: {neg_count}개 (변환 후)")
                except:
                    logger.warning(f"열 '{col}'의 값을 숫자로 변환할 수 없습니다.")
        except Exception as e:
            logger.warning(f"열 '{col}'에서 음수 값 계산 중 오류: {e}")
    
    rows_highlighted = 0
    rows_checked = 0
    errors = 0

    # Get the column indices in the Excel worksheet
    col_indices = {}
    for i, header in enumerate(df.columns, 1):
        col_indices[header] = i

    # Process each row
    for row_idx in range(2, worksheet.max_row + 1):  # Excel is 1-indexed, row 1 is header
        rows_checked += 1
        highlight_row = False
        
        # Check each price difference column
        for diff_col in existing_diff_cols:
            # Get the Excel column index
            if diff_col not in col_indices:
                continue
                
            col_idx = col_indices[diff_col]
            
            # Get the cell value directly from the worksheet
            cell = worksheet.cell(row=row_idx, column=col_idx)
            cell_value = cell.value
            
            # Skip empty cells
            if cell_value is None or cell_value == '' or cell_value == '-':
                continue
                
            try:
                # Handle different types of values
                numeric_value = None
                
                if isinstance(cell_value, (int, float)):
                    # Direct numeric value
                    numeric_value = float(cell_value)
                elif isinstance(cell_value, str):
                    # Strip any whitespace, commas, currency symbols
                    clean_value = cell_value.strip().replace(',', '').replace(' ', '')
                    
                    # Handle parentheses format for negative numbers like (100)
                    if clean_value.startswith('(') and clean_value.endswith(')'):
                        clean_value = '-' + clean_value[1:-1]
                        
                    # Attempt conversion to float if it's not just a dash or empty
                    if clean_value and clean_value != '-':
                        try:
                            numeric_value = float(clean_value)
                        except ValueError:
                            # If conversion fails, skip this cell
                            continue
                
                # If we successfully got a numeric value and it's < -1, highlight the row
                if numeric_value is not None and numeric_value < -1:
                    highlight_row = True
                    logger.debug(f"음수 가격차이 발견: 행 {row_idx}, 열 '{diff_col}', 값 {numeric_value} < -1")
                    break
                    
            except Exception as e:
                logger.warning(f"행 {row_idx}, 열 '{diff_col}' 처리 중 오류: {e}")
                errors += 1
                continue
                
        # Apply highlighting to the entire row if needed
        if highlight_row:
            rows_highlighted += 1
            for col_idx in range(1, worksheet.max_column + 1):
                try:
                    # Apply yellow fill to all cells in the row
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.fill = yellow_fill
                except Exception as e:
                    logger.error(f"셀 서식 적용 오류 (행 {row_idx}, 열 {col_idx}): {e}")
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

def flatten_nested_image_dicts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten any nested dictionaries in image data structures to prevent Excel conversion errors.
    """
    df_result = df.copy()
    
    # Define image-related columns (can be customized)
    image_cols = ['본사 이미지', '고려기프트 이미지', '네이버 이미지', '해오름 이미지 URL']
    # Add any columns that might contain image data
    image_cols.extend([col for col in df.columns if '이미지' in col])
    # Remove duplicates while preserving order
    image_cols = list(dict.fromkeys(image_cols))
    
    # Only process columns that actually exist in the dataframe
    image_cols = [col for col in image_cols if col in df.columns]
    
    if not image_cols:
        return df_result  # No processing needed
    
    for col in image_cols:
        for idx in df_result.index:
            value = df_result.loc[idx, col]
            
            # Process dictionaries
            if isinstance(value, dict):
                # Handle nested URL
                if 'url' in value and isinstance(value['url'], dict):
                    if 'url' in value['url']:
                        value['original_nested_url'] = value['url']  # Save original for reference
                        value['url'] = value['url']['url']  # Extract the inner URL
                
                # Ensure all dictionary values are strings if needed
                for k, v in list(value.items()):
                    if isinstance(v, dict):
                        value[k] = str(v)
            
            # Process the special case where the entire cell is a URL dict
            elif isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                try:
                    # Try to parse as JSON (though this is rarely the issue)
                    import json
                    json_value = json.loads(value.replace("'", '"'))
                    if isinstance(json_value, dict) and 'url' in json_value:
                        df_result.at[idx, col] = json_value['url']
                except:
                    # If parsing fails, just keep the original
                    pass
    
    return df_result


@safe_excel_operation
def create_split_excel_outputs(df_finalized: pd.DataFrame, output_path_base: str) -> tuple:
    """
    Create two Excel files:
    1. Result file: With images, for viewing
    2. Upload file: URL links only, for uploading to systems

    Args:
        df_finalized: The finalized DataFrame with all data
        output_path_base: The base path for output files

    Returns:
        tuple: (result_file_path, upload_file_path)
    """
    # Ensure we have valid data
    if df_finalized is None or df_finalized.empty:
        logger.error("No data to write to Excel. DataFrame is empty or None.")
        return None, None

    # Flatten any nested image dictionaries to prevent Excel conversion errors
    df_finalized = flatten_nested_image_dicts(df_finalized)
    
    logger.info(f"Starting creation of split Excel outputs from finalized DataFrame (Shape: {df_finalized.shape})")
    
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
        
        # Ensure columns are properly ordered (defense against the caller passing mal-formed data)
        if not all(col in FINAL_COLUMN_ORDER for col in df_finalized.columns):
            logger.warning("Input DataFrame columns are not in the exact FINAL_COLUMN_ORDER. Reordering again.")
            # Recreate with only the expected columns in the correct order
            ordered_df = pd.DataFrame()
            for col in FINAL_COLUMN_ORDER:
                if col in df_finalized.columns:
                    ordered_df[col] = df_finalized[col]
            df_finalized = ordered_df
        
        # Get file source info for naming
        source_info = "Unknown"
        mgmt_type = "승인관리"  # Default type
        row_count = len(df_finalized)
        
        try:
            # Check the appropriate column based on format (use both old and new column names)
            if '구분' in df_finalized.columns:
                # Get the most common value to use in naming
                source_val = df_finalized['구분'].iloc[0]
                if source_val == 'A':
                    mgmt_type = "승인관리"
                elif source_val == 'P':
                    mgmt_type = "가격관리"
                else:
                    mgmt_type = str(source_val)
            elif '구분(승인관리:A/가격관리:P)' in df_finalized.columns:
                source_val = df_finalized['구분(승인관리:A/가격관리:P)'].iloc[0]
                if source_val == 'A':
                    mgmt_type = "승인관리"
                elif source_val == 'P':
                    mgmt_type = "가격관리"
                else:
                    mgmt_type = str(source_val)
                    
            # Get company name for filename
            if '업체명' in df_finalized.columns:
                # Use the most common company name or the first one
                company_counts = df_finalized['업체명'].value_counts()
                if not company_counts.empty:
                    source_info = company_counts.index[0]
            elif '공급사명' in df_finalized.columns:
                company_counts = df_finalized['공급사명'].value_counts()
                if not company_counts.empty:
                    source_info = company_counts.index[0]
        except Exception as e:
            logger.warning(f"Error getting source name: {e}")
            source_info = "Mixed"
        
        # Create timestamped filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_part = datetime.now().strftime("%Y%m%d")
        
        # Format: {company}({count})-{mgmt_type}-{date}_{type}_{timestamp}.xlsx
        result_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
        upload_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
        
        # Make sure output_path_base is a directory, not a file
        # If it ends with .xlsx, use its directory instead
        if output_path_base.lower().endswith('.xlsx'):
            output_path_base = os.path.dirname(output_path_base)
            
        # Ensure the output directory exists
        os.makedirs(output_path_base, exist_ok=True)
        
        # Build full paths
        result_path = os.path.join(output_path_base, result_filename)
        upload_path = os.path.join(output_path_base, upload_filename)
        
        logger.info(f"Result file path (with images): {result_path}")
        logger.info(f"Upload file path (links only): {upload_path}")
        
        # -----------------------------------------
        # 1. Create Result File (with images)
        # -----------------------------------------
        try:
            logger.info(f"Attempting to write result file: {result_path} with {len(df_finalized)} rows.")
            
            # Create a new workbook for result file
            workbook_result = openpyxl.Workbook()
            worksheet_result = workbook_result.active
            worksheet_result.title = "제품 가격 비교"
            
            logger.info("Writing result data to Excel sheet...")
            
            # Convert image dictionaries to strings for initial data writing
            # This prevents "Cannot convert dict to Excel" errors
            df_for_excel = df_finalized.copy()
            
            # Convert any dictionary or complex objects to strings
            for col in df_for_excel.columns:
                for idx in df_for_excel.index:
                    value = df_for_excel.loc[idx, col]
                    if isinstance(value, dict):
                        # For dictionary values, store just the URL to make Excel happy
                        if 'url' in value:
                            # Handle case where url itself is a dictionary (nested dict)
                            if isinstance(value['url'], dict) and 'url' in value['url']:
                                df_for_excel.at[idx, col] = value['url']['url']
                            else:
                                df_for_excel.at[idx, col] = value['url']
                        else:
                            # Just convert to string representation if no URL
                            df_for_excel.at[idx, col] = str(value)
                    elif isinstance(value, pd.Series):
                        # For Series objects, convert to string
                        for item in value:
                            if pd.notna(item) and item not in ['-', '']:
                                if isinstance(item, dict) and 'url' in item:
                                    # Handle case where url itself is a dictionary (nested dict)
                                    if isinstance(item['url'], dict) and 'url' in item['url']:
                                        df_for_excel.at[idx, col] = item['url']['url']
                                    else:
                                        df_for_excel.at[idx, col] = item['url']
                                else:
                                    df_for_excel.at[idx, col] = str(item)
                                break
                        else:
                            df_for_excel.at[idx, col] = "-"
            
            # Write header
            for col_idx, col_name in enumerate(df_for_excel.columns, 1):
                worksheet_result.cell(row=1, column=col_idx, value=col_name)
            
            # Write data
            for row_idx, row in enumerate(df_for_excel.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    # Convert None to empty string to avoid writing 'None' to cells
                    worksheet_result.cell(row=row_idx, column=col_idx, value=value if not pd.isna(value) else "")
            
            # Apply common formatting (basic without images)
            _apply_basic_excel_formatting(worksheet_result, df_for_excel.columns.tolist())
            _add_hyperlinks_to_worksheet(worksheet_result, df_for_excel, hyperlinks_as_formulas=False)
            _add_header_footer(worksheet_result)
            
            # FIXED: Explicitly remove auto filter in result file
            if hasattr(worksheet_result, 'auto_filter') and worksheet_result.auto_filter:
                worksheet_result.auto_filter.ref = None
                logger.info("Removed filter from result Excel file")
            
            # Save without images first to ensure we can write to file
            workbook_result.save(result_path)
            
            # Now load the saved file to add images
            # Create a new workbook with images
            try:
                logger.info("Adding images to result file...")
                # Load the workbook
                workbook_with_images = openpyxl.load_workbook(result_path)
                worksheet_with_images = workbook_with_images.active
                
                # Process image columns
                image_cols = [col for col in df_finalized.columns if col in IMAGE_COLUMNS]
                images_added = 0
                
                # Adjust row heights to accommodate images
                for row_idx in range(2, len(df_finalized) + 2):
                    worksheet_with_images.row_dimensions[row_idx].height = 120  # Adjust row height
                
                # Adjust column widths for image columns
                for img_col in image_cols:
                    col_idx = df_finalized.columns.get_loc(img_col) + 1
                    col_letter = get_column_letter(col_idx)
                    worksheet_with_images.column_dimensions[col_letter].width = 22  # Wider columns for images
                
                # Add images to the worksheet
                for row_idx, (_, row) in enumerate(df_finalized.iterrows(), 2):
                    for img_col in image_cols:
                        col_idx = df_finalized.columns.get_loc(img_col) + 1
                        img_value = row[img_col]
                        
                        # Check if we have a dictionary with image data
                        if isinstance(img_value, dict):
                            # FIXED: Enhanced image path validation for Naver images
                            is_naver_col = 'naver' in img_col.lower() or '네이버' in img_col
                            
                            # Try to reconstruct/find URLs for images if missing
                            if ('url' not in img_value or not img_value['url'] or 
                                not isinstance(img_value['url'], str) or
                                not img_value['url'].startswith(('http://', 'https://'))):
                                
                                # Handle case where url is a nested dictionary
                                if isinstance(img_value.get('url'), dict) and 'url' in img_value['url'] and isinstance(img_value['url']['url'], str):
                                    img_value['url'] = img_value['url']['url']
                                
                                # Continue with regular processing if URL is still invalid
                                if ('url' not in img_value or not img_value['url'] or 
                                    not isinstance(img_value['url'], str) or
                                    not img_value['url'].startswith(('http://', 'https://'))):
                                
                                    # Determine the source based on column name or image data
                                    source = img_value.get('source', '').lower()
                                    if not source:
                                        if '본사' in img_col:
                                            source = 'haereum'
                                        elif '고려' in img_col:
                                            source = 'kogift'
                                        elif '네이버' in img_col:
                                            source = 'naver'
                                
                                # Try to reconstruct URL based on source
                                if source == 'haereum' or source == 'haoreum':
                                    # Try to extract product code from path if available
                                    product_code = None
                                    if 'original_path' in img_value and isinstance(img_value['original_path'], str):
                                        path = img_value['original_path']
                                        # Look for product code pattern (e.g., BBCA0009349, CCBK0001873)
                                        code_match = re.search(r'([A-Z]{4}\d{7})', path)
                                        if code_match:
                                            product_code = code_match.group(1)
                                            
                                    # Use p_idx if available (highest priority)
                                    if 'p_idx' in img_value:
                                        product_code = img_value['p_idx']
                                    
                                    if product_code:
                                        # Try multiple extensions with proper suffix (typically 's')
                                        extensions = ['.jpg', '.png', '.gif']
                                        # Default suffix (usually 's' for small image)
                                        suffix = 's'
                                        
                                        # Extract suffix from path if possible
                                        if 'original_path' in img_value:
                                            suffix_match = re.search(r'([A-Z]{4}\d{7})(.*?)(\.[a-z]+)$', str(img_value['original_path']))
                                            if suffix_match and suffix_match.group(2):
                                                suffix = suffix_match.group(2)
                                        
                                        # Generate URL with the first extension (could be enhanced to verify actual existence)
                                        img_value['url'] = f"https://www.jclgift.com/upload/product/simg3/{product_code}{suffix}{extensions[0]}"
                                        logger.debug(f"Generated URL for Haereum image based on product code: {img_value['url']}")
                                elif source == 'kogift':
                                    if 'original_path' in img_value and isinstance(img_value['original_path'], str):
                                        orig_path = img_value['original_path']
                                        if 'upload' in orig_path:
                                            parts = orig_path.split('upload/')
                                            if len(parts) > 1:
                                                # Preserve the original file extension if present
                                                img_value['url'] = f"https://koreagift.com/ez/upload/{parts[1]}"
                                                logger.debug(f"Generated URL for Kogift image based on path: {img_value['url']}")
                                        else:
                                            # Try to extract product code or ID
                                            code_match = re.search(r'kogift_(.*?)_[a-f0-9]{8,}', orig_path)
                                            if code_match:
                                                product_name = code_match.group(1)
                                                # Try various extensions
                                                extensions = ['.jpg', '.png', '.gif']
                                                img_value['url'] = f"https://koreagift.com/ez/upload/mall/shop_{product_name}{extensions[0]}"
                                                logger.debug(f"Generated alternative URL for Kogift image: {img_value['url']}")
                                elif source == 'naver':
                                    if 'product_id' in img_value:
                                        # Try multiple extensions for Naver images
                                        extensions = ['.jpg', '.png', '.gif']
                                        img_value['url'] = f"https://shopping-phinf.pstatic.net/main_{img_value['product_id']}/{img_value['product_id']}{extensions[0]}"
                                        logger.debug(f"Generated URL for Naver image based on product_id: {img_value['url']}")
                                    elif 'original_path' in img_value and isinstance(img_value['original_path'], str):
                                        # Try to extract product ID from path
                                        path = img_value['original_path']
                                        id_match = re.search(r'naver_(.*?)_[a-f0-9]{8,}', path)
                                        if id_match:
                                            product_name = id_match.group(1)
                                            # Skip generating front URLs as they are unreliable
                                            logger.warning(f"Skipping generation of unreliable 'front' URL for Naver image: {path}")
                                            # If URL already exists and is a front URL, remove it
                                            if 'url' in img_value and isinstance(img_value['url'], str) and "pstatic.net/front/" in img_value['url']:
                                                logger.warning(f"Removing unreliable 'front' URL: {img_value['url']}")
                                                img_value['url'] = ''
                                
                            # Now check if we have a local path to add the image
                            if 'local_path' in img_value:
                                img_path = img_value['local_path']
                                has_url = 'url' in img_value and isinstance(img_value['url'], str) and img_value['url'].startswith(('http://', 'https://'))
                                
                                # FIXED: Ensure path is absolute and standardized
                                if img_path and not os.path.isabs(img_path):
                                    try:
                                        img_path = os.path.abspath(img_path)
                                        img_value['local_path'] = img_path  # Update the dictionary with absolute path
                                        logger.debug(f"Converted relative path to absolute: {img_path}")
                                    except Exception as path_err:
                                        logger.warning(f"Error converting to absolute path: {path_err}")
                                
                                # FIXED: For Naver images, do extra validation and try harder to find the file
                                if is_naver_col and img_path and not os.path.exists(img_path):
                                    logger.warning(f"Naver image file not found: {img_path}")
                                    
                                    # Try to find the image with different extensions
                                    base_path = os.path.splitext(img_path)[0]
                                    for ext in ['.jpg', '.jpeg', '.png', '.gif']:
                                        alt_path = f"{base_path}{ext}"
                                        if os.path.exists(alt_path):
                                            logger.info(f"Found alternative path for Naver image: {alt_path}")
                                            img_path = alt_path
                                            img_value['local_path'] = img_path  # Update the dictionary
                                            break
                                    
                                    # If still not found, try _nobg version
                                    if not os.path.exists(img_path):
                                        nobg_path = f"{base_path}_nobg.png"
                                        if os.path.exists(nobg_path):
                                            logger.info(f"Found _nobg version of Naver image: {nobg_path}")
                                            img_path = nobg_path
                                            img_value['local_path'] = img_path  # Update the dictionary
                                
                                # Extra verification for any image type
                                if img_path and not os.path.exists(img_path):
                                    logger.warning(f"Image file not found after all attempts: {img_path}")
                                    
                                    # Try to find any image with similar name in expected directory
                                    try:
                                        dir_path = os.path.dirname(img_path)
                                        file_base = os.path.basename(img_path)
                                        base_name = os.path.splitext(file_base)[0]
                                        
                                        if os.path.exists(dir_path):
                                            # List files in directory
                                            for file in os.listdir(dir_path):
                                                # Check if base name is contained in this file
                                                if base_name[:8] in file and os.path.isfile(os.path.join(dir_path, file)):
                                                    found_path = os.path.join(dir_path, file)
                                                    logger.info(f"Found similar file: {found_path}")
                                                    img_path = found_path
                                                    img_value['local_path'] = img_path  # Update the dictionary
                                                    break
                                    except Exception as e:
                                        logger.warning(f"Error searching for similar files: {e}")
                                
                                if os.path.exists(img_path):
                                    try:
                                        # Create and add the image
                                        img = openpyxl.drawing.image.Image(img_path)
                                        # FIXED: Larger images in the result file
                                        img.width = 160  # Set image width - increased from 80
                                        img.height = 160  # Set image height - increased from 80
                                        img.anchor = f"{get_column_letter(col_idx)}{row_idx}"
                                        
                                        # FIXED: Add error handling for image loading
                                        try:
                                            worksheet_with_images.add_image(img)
                                            
                                            # Clear the cell content
                                            cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                            cell.value = ""
                                            
                                            # Add hyperlink to the image URL if available (the image will have a link)
                                            if has_url:
                                                # Use openpyxl hyperlink object - will show on hover
                                                cell.hyperlink = img_value['url']
                                                
                                                # Add a very small marker to indicate there's a URL (using a comment)
                                                try:
                                                    comment = openpyxl.comments.Comment(f"이미지 URL: {img_value['url']}", "시스템")
                                                    comment.width = 300
                                                    comment.height = 50
                                                    cell.comment = comment
                                                except Exception as comment_err:
                                                    logger.warning(f"코멘트 추가 실패 (행 {row_idx}, 열 {col_idx}): {comment_err}")
                                            
                                            images_added += 1
                                            logger.debug(f"이미지 추가 성공 (행 {row_idx}, 열 {col_idx})")
                                        except Exception as img_add_err:
                                            # More specific handling for common image errors
                                            if "not an image file" in str(img_add_err).lower():
                                                logger.warning(f"Not a valid image file: {img_path}")
                                                # Try to verify with PIL
                                                try:
                                                    from PIL import Image as PILImage
                                                    img_test = PILImage.open(img_path)
                                                    img_test.verify()  # Verify it's a valid image
                                                    logger.warning(f"Image verified with PIL but failed in openpyxl: {img_path}")
                                                except Exception as pil_err:
                                                    logger.warning(f"Image verification with PIL also failed: {pil_err}")
                                            
                                            logger.warning(f"Failed to add image (행 {row_idx}, 열 {col_idx}): {img_add_err}")
                                            
                                            # Fallback to displaying URL if available
                                            if has_url:
                                                cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                                cell.value = img_value['url']
                                                cell.hyperlink = img_value['url']
                                                cell.font = Font(color="0563C1", underline="single")
                                                logger.debug(f"Falling back to URL display for failed image: {img_value['url'][:50]}...")
                                    except Exception as e:
                                        logger.error(f"Error creating/configuring image object for {img_path}: {e}")
                                        # Fallback to displaying URL if available
                                        if has_url:
                                            cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                            cell.value = img_value['url']
                                            cell.hyperlink = img_value['url']
                                            cell.font = Font(color="0563C1", underline="single")
                
                # FIXED: Ensure filter is removed after image addition too
                if hasattr(worksheet_with_images, 'auto_filter') and worksheet_with_images.auto_filter:
                    worksheet_with_images.auto_filter.ref = None
                    logger.info("Removed filter from result Excel file after adding images")
                
                # Save the workbook with images
                workbook_with_images.save(result_path)
                logger.info(f"Successfully added {images_added} images to result file")
            except Exception as img_err:
                logger.error(f"Error adding images to result file: {img_err}")
                # Continue with the file without images
            
            result_success = True
            logger.info(f"Successfully created result file: {result_path}")
            
        except Exception as e:
            logger.error(f"Error creating result file: {e}")
            logger.debug(traceback.format_exc())
            result_success = False
        
        # -----------------------------------------
        # 2. Create Upload File (with links only and different column names)
        # -----------------------------------------
        try:
            logger.info(f"Preparing data for upload file: {upload_path}")
            
            # Create a deep copy of the original DataFrame to avoid modifying it
            df_upload = pd.DataFrame()
            
            # FIX: First extract image URLs from the DataFrame before column mapping
            # Create a new DataFrame with the image URLs
            df_with_image_urls = df_finalized.copy()
            
            # Process each image column to extract only web URLs
            for img_col in IMAGE_COLUMNS:
                if img_col in df_finalized.columns:
                    logger.info(f"Extracting image URLs from {img_col} column...")
                    
                    # Map the column names to the upload file column names
                    # This maps: '본사 이미지' -> '해오름(이미지링크)', '고려기프트 이미지' -> '고려기프트(이미지링크)', '네이버 이미지' -> '네이버쇼핑(이미지링크)'
                    upload_img_col = COLUMN_MAPPING_FINAL_TO_UPLOAD.get(img_col, img_col) # Use mapping, fallback to original if not found

                    # Create the target upload column if it doesn't exist in the intermediate df
                    if upload_img_col not in df_with_image_urls.columns:
                        df_with_image_urls[upload_img_col] = ""
                    
                    # Track URL extraction results for this column
                    urls_found = 0
                    fallback_urls_generated = 0
                    url_errors = 0
                    
                    # Process all rows to extract image URLs
                    for idx in df_finalized.index:
                        value = df_finalized.at[idx, img_col]
                        
                        # Default to empty string (not "-") to avoid showing placeholders in cells
                        image_url = ""
                        url_source = "unknown"
                        
                        try:
                            # FIX: Explicitly check dictionary structure and extract 'url' key if it's a web URL
                            if isinstance(value, dict):
                                # Check for product_url first (for Naver) - NEW ADDITION
                                if 'product_url' in value and isinstance(value['product_url'], str) and value['product_url'].startswith(('http://', 'https://')):
                                    image_url = value['product_url'].strip()
                                    url_source = "direct_from_product_url_key"
                                    logger.debug(f"Found product URL in {img_col} at idx {idx} using 'product_url' key: {image_url[:50]}...")
                                # Then check for regular 'url' key as fallback
                                elif 'url' in value and isinstance(value['url'], str) and value['url'].startswith(('http://', 'https://')):
                                    image_url = value['url'].strip()
                                    url_source = "direct_from_url_key"
                                    logger.debug(f"Found web URL in {img_col} at idx {idx} using 'url' key: {image_url[:50]}...")
                                else:
                                    # Fallback: Check other potential keys if 'url' is missing or invalid
                                    for url_key in ['image_url', 'original_url', 'src']:
                                        fallback_url = value.get(url_key)
                                        if fallback_url and isinstance(fallback_url, str) and fallback_url.startswith(('http://', 'https://')):
                                            image_url = fallback_url.strip()
                                            url_source = f"fallback_from_{url_key}"
                                            logger.debug(f"Found web URL in {img_col} at idx {idx} using fallback key '{url_key}': {image_url[:50]}...")
                                            break # Stop checking keys once a valid URL is found
                            
                            # Handle string URL format (if the value is not a dictionary)
                            elif isinstance(value, str) and value.strip() and value != '-':
                                url = value.strip()
                                if url.startswith(('http://', 'https://')):
                                    # Basic check if it looks like an image URL (more lenient)
                                    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
                                    image_identifiers = ['upload/', 'simg', 'pstatic.net', 'phinf', '/image/', 'thumb']
                                    
                                    if any(url.lower().endswith(ext) for ext in image_extensions) or any(ident in url.lower() for ident in image_identifiers):
                                        image_url = url
                                        url_source = "direct_string_url"
                                        logger.debug(f"Found image URL string in {img_col} at idx {idx}: {url[:50]}...")
                            
                            # --- Reconstruction logic (REMOVED) ---
                            # Reconstruction logic is removed here as the input `df_finalized`
                            # should already contain the correct image URL in the dictionary
                            # provided by `image_integration.py`. We only need to extract it.
                            # If the dictionary or the 'url' key is missing, we should not reconstruct.
                            
                            # Final validation for the extracted URL
                            if image_url:
                                # Validate the URL format
                                if not image_url.startswith(('http://', 'https://')):
                                    logger.warning(f"정상적인 URL이 아님 (행 {idx+1}, {img_col}): '{image_url[:50]}' - http:// 또는 https://로 시작해야 함")
                                    image_url = ""  # Reset invalid URL
                                    url_errors += 1
                                else:
                                    # URL validation passed
                                    urls_found += 1
                                    logger.debug(f"유효한 이미지 URL 추출 완료 (행 {idx+1}, {img_col}, 소스: {url_source}): {image_url[:50]}...")
                        except Exception as e:
                            logger.error(f"이미지 URL 추출 중 오류 발생 (행 {idx+1}, {img_col}): {str(e)[:100]}")
                            image_url = ""  # Reset on error
                            url_errors += 1
                        
                        # Store the extracted image URL in the intermediate DataFrame under the UPLOAD column name
                        df_with_image_urls.at[idx, upload_img_col] = image_url if image_url else "" # Use empty string if no valid URL

                    # Log summary for this column
                    logger.info(f"URL 추출 결과 ({upload_img_col}): 총 {urls_found}개 URL 추출 성공, {url_errors}개 오류")

            # NEW: Special handling for Naver image column - replace with product links
            df_with_image_urls = prepare_naver_image_urls_for_upload(df_with_image_urls)

            # Check if we extracted any image URLs
            for img_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']: # Use upload file column names
                if img_col in df_with_image_urls.columns:
                    url_count = (df_with_image_urls[img_col].astype(str).str.startswith(('http://', 'https://'))).sum()
                    logger.info(f"Extracted {url_count} image URLs for {img_col} in upload data")
                    
                    # Log a few examples if any URLs were found
                    if url_count > 0:
                        sample_urls = df_with_image_urls[df_with_image_urls[img_col].astype(str).str.startswith(('http://', 'https://'))][img_col].head(3).tolist()
                        logger.info(f"Sample image URLs for {img_col}: {sample_urls}")

            # Map columns from result format to upload format 
            df_upload = pd.DataFrame() # Start with an empty DataFrame for the upload file
            
            for target_col in UPLOAD_COLUMN_ORDER:
                # Find corresponding source column from the original result format
                source_col = None
                for result_col, upload_col in COLUMN_MAPPING_FINAL_TO_UPLOAD.items():
                    if upload_col == target_col:
                        source_col = result_col
                        break
                
                # Determine where to get the data:
                # - If it's an upload image link column, get it from df_with_image_urls[target_col]
                # - Otherwise, get it from df_finalized[source_col]
                
                if target_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']:
                    # Get the already processed image URL from df_with_image_urls
                    if target_col in df_with_image_urls.columns:
                        df_upload[target_col] = df_with_image_urls[target_col]
                    else:
                        df_upload[target_col] = '' # Should not happen, but safety check
                        logger.warning(f"Processed image URL column '{target_col}' not found in intermediate df.")
                elif source_col and source_col in df_finalized.columns:
                    # Get non-image data from the original finalized DataFrame
                    df_upload[target_col] = df_finalized[source_col]
                else:
                    # If no matching column found, add an empty column
                    df_upload[target_col] = ''
                    logger.warning(f"Could not find source column for upload column '{target_col}' or source column missing.")

            # Log image columns in the final upload file to confirm extraction worked
            for img_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']:
                if img_col in df_upload.columns:
                    non_empty = df_upload[img_col].astype(str).str.strip().str.len() > 0
                    non_empty_urls = df_upload[img_col].astype(str).str.startswith(('http://', 'https://'))
                    count = non_empty.sum()
                    url_count = non_empty_urls.sum()
                    logger.info(f"Upload file: {img_col} column has {count} non-empty values, {url_count} are URLs")
                    
                    # Log sample values
                    if url_count > 0:
                        sample_values = df_upload.loc[non_empty_urls, img_col].head(3).tolist()
                        logger.info(f"Sample URL values in final upload df: {sample_values}")
                    elif count > 0:
                         sample_non_urls = df_upload.loc[non_empty & ~non_empty_urls, img_col].head(3).tolist()
                         logger.warning(f"Sample non-URL values in final upload df {img_col}: {sample_non_urls}")

            # Create new workbook for upload file (now with properly extracted image URLs)
            workbook_upload = openpyxl.Workbook()
            worksheet_upload = workbook_upload.active
            worksheet_upload.title = "제품 가격 비교 (업로드용)"
            
            logger.info(f"Writing upload file (with image links): {upload_path} with {len(df_upload)} rows.")
            
            # Write header
            for col_idx, col_name in enumerate(df_upload.columns, 1):
                worksheet_upload.cell(row=1, column=col_idx, value=col_name)
            
            # Write data (now with properly extracted image URLs)
            for row_idx, row in enumerate(df_upload.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    # Handle NaN/None values
                    if pd.isna(value) or value is None:
                        cell_value = ""
                    else:
                        cell_value = value
                    
                    # Write the cell value
                    worksheet_upload.cell(row=row_idx, column=col_idx, value=cell_value)
                
                # Add direct verification of image URL columns for logging
                if row_idx <= 5:  # Log only the first 5 rows
                    img_idx_start = len(UPLOAD_COLUMN_ORDER) - 3
                    for i in range(3):  # Check all 3 image columns
                        col_idx = img_idx_start + i + 1
                        cell_value = worksheet_upload.cell(row=row_idx, column=col_idx).value
                        col_name = UPLOAD_COLUMN_ORDER[img_idx_start + i]
                        if cell_value and isinstance(cell_value, str) and cell_value.startswith(('http://', 'https://')):
                            logger.debug(f"Row {row_idx}, {col_name}: Valid URL found: {cell_value[:50]}...")
                        else:
                            logger.debug(f"Row {row_idx}, {col_name}: No valid URL found. Value: '{cell_value}'")
            
            # Apply upload file specific formatting (new function with requested formatting)
            _apply_upload_file_formatting(worksheet_upload, df_upload.columns.tolist())
            
            # Add hyperlinks to all image URL cells
            try:
                logger.info("Adding hyperlinks to image URLs in upload file...")
                
                # Define upload image columns
                upload_image_cols = ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']
                
                # Get the column indices for these columns
                col_indices = {}
                for i, col_name in enumerate(df_upload.columns, 1):
                    if col_name in upload_image_cols:
                        col_indices[col_name] = i
                
                # Add hyperlinks to the cells
                for row_idx in range(2, len(df_upload) + 2):  # Start from row 2 (after header)
                    for col_name, col_idx in col_indices.items():
                        cell = worksheet_upload.cell(row=row_idx, column=col_idx)
                        url = cell.value
                        
                        # Only add hyperlink if the cell contains a valid URL
                        if isinstance(url, str) and url.strip() and url.startswith(('http://', 'https://')):
                            cell.hyperlink = url
                            cell.font = Font(color="0563C1", underline="single")
                
                logger.info("Hyperlinks added to upload file successfully")
            except Exception as e:
                logger.warning(f"Error adding hyperlinks to upload file: {e}")
            
            # Save upload file
            workbook_upload.save(upload_path)
            upload_success = True
            logger.info(f"Successfully created upload file (with image links): {upload_path}")
                
            return result_success, upload_success, result_path, upload_path
        
        except Exception as upload_err:
            logger.error(f"Error creating upload file: {upload_err}")
            logger.debug(traceback.format_exc())
            upload_success = False
        
        return result_success, upload_success, result_path, upload_path
        
    except Exception as main_error:
        logger.error(f"Unexpected error in create_split_excel_outputs: {main_error}")
        logger.debug(traceback.format_exc())
        return False, False, None, None

@safe_excel_operation
def create_final_output_excel(df: pd.DataFrame, output_path: str) -> bool:
    """
    Create a combined Excel output file with images and various formatting.
    Unlike create_split_excel_outputs, this creates a single Excel file with advanced formatting.
    It's kept for potential direct use but create_split_excel_outputs is preferred
    for most use cases.

    Args:
        df: DataFrame with the data
        output_path: Path where to save the Excel file

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
            worksheet.column_dimensions[col_letter].width = 85  # Increased from 80
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
            worksheet.row_dimensions[row_idx].height = 400  # Increased from 380
            
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
    Assumes image data (paths or dicts) is already present.
    """
    if df is None:
        logger.error("Input DataFrame is None, cannot finalize.")
        # Return empty df with correct columns to avoid downstream errors
        return pd.DataFrame(columns=FINAL_COLUMN_ORDER)

    logger.info(f"Finalizing DataFrame for Excel. Input shape: {df.shape}")
    logger.debug(f"Input columns: {df.columns.tolist()}")
    
    # Check for duplicate column names - this can cause the 'dtype' error
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        logger.warning(f"Found {len(duplicate_cols)} duplicate column names: {duplicate_cols}")
        # Create a new DataFrame with deduplicated columns
        # For each duplicate, keep only the first instance
        unique_cols = []
        
        for col in df.columns:
            if col in unique_cols:
                # Skip this column as we already have it
                continue
            unique_cols.append(col)
        
        # Create new DataFrame with only unique columns
        df = df[unique_cols]
        logger.info(f"Removed duplicate columns. New shape: {df.shape}")
    
    # Step 1: Create a new DataFrame to avoid modifying the original
    df_final = df.copy()
    
    # Step 2: Rename columns to the target names
    df_final = df_final.rename(columns=COLUMN_RENAME_MAP, errors='ignore')
    logger.debug(f"Columns after rename: {df_final.columns.tolist()}")
    
    # FIXED: Debug logging for Kogift price data
    logger.info("Checking Kogift price data before column processing:")
    kogift_price_col = '판매단가(V포함)(2)'
    if kogift_price_col in df_final.columns:
        price_count = df_final[kogift_price_col].notnull().sum()
        logger.info(f"Found {price_count} non-null values in '{kogift_price_col}' column")
        
        # Log a few sample values
        if price_count > 0:
            sample_values = df_final[kogift_price_col].head(3).tolist()
            logger.info(f"Sample Kogift price values: {sample_values}")
    else:
        logger.warning(f"Column '{kogift_price_col}' not found in dataframe!")
    
    # Step 3: Create an output DataFrame with columns in the proper order
    output_df = pd.DataFrame()
    
    # Identify which columns in the final_order exist in the input
    available_cols = [col for col in FINAL_COLUMN_ORDER if col in df_final.columns]
    
    # Log which columns from FINAL_COLUMN_ORDER are missing
    missing_cols = [col for col in FINAL_COLUMN_ORDER if col not in df_final.columns]
    if missing_cols:
        logger.warning(f"The following columns from FINAL_COLUMN_ORDER are missing: {missing_cols}")
    
    # Copy data from original to new dataframe
    for col in available_cols:
        try:
            output_df[col] = df_final[col]
        except Exception as e:
            logger.error(f"Error copying column '{col}' during finalization: {e}")
            output_df[col] = None # Add empty column on error

    # Step 4: Add missing columns with empty values
    for col in FINAL_COLUMN_ORDER:
        if col not in output_df.columns:
            output_df[col] = None # Add missing column with None values
            logger.debug(f"Added missing column '{col}' with None values")
    
    # FIXED: Ensure the Kogift price column exists and has data copied correctly
    # First, check for the specific price column again:
    if kogift_price_col in df_final.columns and kogift_price_col in output_df.columns:
        # Explicitly copy the price column data again to ensure it's not lost
        output_df[kogift_price_col] = df_final[kogift_price_col]
        logger.info(f"Explicitly copied '{kogift_price_col}' data to ensure preservation")
        
        # Verify the copy worked
        kogift_price_count_after = output_df[kogift_price_col].notnull().sum()
        logger.info(f"After explicit copy: {kogift_price_count_after} non-null values in '{kogift_price_col}'")
    
    # FIXED: Special handling for price columns to avoid data loss
    # Check for alternate column names that might hold price data
    price_alternates = {
        '판매가(V포함)(2)': ['판매단가2(VAT포함)', '판매단가(V포함)(2)', '고려가격', '고려기프트판매가'], # Added original processing name
        '판매단가(V포함)(3)': ['판매단가3 (VAT포함)', '판매단가(V포함)(3)', '네이버가격', '네이버판매가'] # Added original processing name
    }

    logger.info("Checking for and consolidating data from alternate price columns...")
    for target_col, alt_cols in price_alternates.items():
        if target_col in output_df.columns:
            # Count valid data points in the target column currently
            target_valid_count = output_df[target_col].apply(lambda x: pd.notna(x) and x not in ['-', '']).sum()
            logger.debug(f"Target '{target_col}' currently has {target_valid_count} valid entries.")

            # Check each alternate column found in the *renamed* df_final
            best_alt_col = None
            best_alt_count = target_valid_count # Start with current count

            for alt_col_potential in alt_cols:
                # Check if this alternate exists *after renaming*
                if alt_col_potential in df_final.columns:
                    alt_valid_count = df_final[alt_col_potential].apply(lambda x: pd.notna(x) and x not in ['-', '']).sum()
                    logger.debug(f"  Checking alternate '{alt_col_potential}': Found {alt_valid_count} valid entries.")
                    # If this alternate has more valid data, consider it
                    if alt_valid_count > best_alt_count:
                        best_alt_count = alt_valid_count
                        best_alt_col = alt_col_potential

            # If a better alternate was found, copy its data to the target column
            if best_alt_col:
                logger.info(f"Found better data in alternate column '{best_alt_col}' ({best_alt_count} valid). Copying to '{target_col}'.")
                # Ensure the source column exists before copying
                if best_alt_col in df_final.columns:
                     # Copy data, converting potential errors during copy
                     try:
                          output_df[target_col] = pd.to_numeric(df_final[best_alt_col], errors='coerce')
                     except Exception as copy_err:
                          logger.error(f"Error coercing/copying from {best_alt_col} to {target_col}: {copy_err}")
                          # Fallback: copy raw data if numeric conversion fails
                          output_df[target_col] = df_final[best_alt_col]
                else:
                     logger.warning(f"Attempted to copy from non-existent column '{best_alt_col}'")

    # Step 5: Format numeric columns (Ensure this runs AFTER alternate consolidation)
    # Get image columns for exclusion from numeric formatting
    image_cols = [col for col in output_df.columns if col in IMAGE_COLUMNS]

    logger.info("Applying numeric formatting to relevant columns...")
    for col in output_df.columns:
        # Skip image columns
        if col in image_cols:
            continue

        # Check if column should be numeric (Prices, Quantities, Differences)
        # Use final column names here
        is_numeric_col = (
            col in PRICE_COLUMNS or
            col in QUANTITY_COLUMNS or
            col in PERCENTAGE_COLUMNS or
            col in ['가격차이(2)', '가격차이(3)'] # Explicitly include difference columns
        )

        if is_numeric_col:
            # Log before conversion
            pre_conversion_sample = output_df[col].head(3).tolist()
            pre_conversion_dtype = output_df[col].dtype
            logger.debug(f"Converting column '{col}' (dtype: {pre_conversion_dtype}, sample: {pre_conversion_sample}) to numeric.")

            try:
                # Store original data before attempting conversion
                original_data = output_df[col].copy()
                
                # Attempt conversion to numeric, coercing errors to NaN
                output_df[col] = pd.to_numeric(output_df[col], errors='coerce')
                
                # Log after conversion
                post_conversion_sample = output_df[col].head(3).tolist()
                post_conversion_dtype = output_df[col].dtype
                nan_count = output_df[col].isna().sum()
                logger.debug(f"  -> Post-conversion '{col}' (dtype: {post_conversion_dtype}, sample: {post_conversion_sample}, NaNs: {nan_count})")

                # If conversion resulted in all NaNs, consider reverting
                if nan_count == len(output_df[col]):
                    logger.warning(f"Numeric conversion resulted in all NaN for column '{col}'. Reverting to original data.")
                    output_df[col] = original_data

            except Exception as e:
                logger.warning(f"Error converting column '{col}' to numeric: {e}. Keeping original data.")
                # Optionally revert if needed, but pd.to_numeric usually handles errors well with coerce

    # Step 6: Replace NaN/NaT with None for Excel compatibility
    output_df = output_df.replace({pd.NA: None, np.nan: None, pd.NaT: None})

    # Step 7: Set default values for empty cells ('-' for non-image, handling None)
    logger.info("Setting default values for empty cells ('-')...")
    for col in output_df.columns:
        if col not in image_cols:
            # Apply '-' default to cells that are None or empty strings after NaN replacement
            # Ensure we don't overwrite 0 or False
            output_df[col] = output_df[col].apply(
                lambda x: '-' if (x is None or x == '') else x
            )

    # Final verification of key data
    logger.info(f"DataFrame finalized. Output shape: {output_df.shape}")
    logger.debug(f"Final columns: {output_df.columns.tolist()}")

    # Final verification of price data
    for price_col in ['판매단가(V포함)', '판매가(V포함)(2)', '판매단가(V포함)(3)', '가격차이(2)', '가격차이(3)']:
        if price_col in output_df.columns:
            try:
                 # Count non-empty, non-'', non-None values
                 non_empty_count = output_df[price_col].apply(lambda x: pd.notna(x) and x not in ['-', '']).sum()
                 # Log sample non-empty values if they exist
                 sample_values = output_df.loc[output_df[price_col].apply(lambda x: pd.notna(x) and x not in ['-', '']), price_col].head(3).tolist()
                 logger.info(f"Final check: Column '{price_col}' has {non_empty_count} valid entries. Sample: {sample_values}")
            except Exception as e:
                 logger.error(f"Error during final check for column '{price_col}': {e}")
                 logger.info(f"Final check: Column '{price_col}' dtype: {output_df[price_col].dtype}")


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

# Add a new function specifically for upload file formatting
def _apply_upload_file_formatting(worksheet, column_list):
    """
    Applies specific formatting for the upload Excel file:
    - Headers with gray background and 2 lines display
    - Content with wrap text
    - Specific cell dimensions and borders
    """
    try:
        # 1. Set standard column width (7) for all columns
        for col_idx in range(1, len(column_list) + 1):
            column_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[column_letter].width = 7
        
        # Adjust specific columns that need different widths
        special_width_columns = {
            '상품명': 35,  # 상품명 needs more width
            '상품코드': 12, # Product code
            '카테고리(중분류)': 15,  # Category
            '해오름(이미지링크)': 40, # Image URLs
            '고려기프트(이미지링크)': 40,
            '네이버쇼핑(이미지링크)': 40,
            '본사링크': 30, # Product links
            '고려 링크': 30,
            '네이버 링크': 30
        }
        
        for idx, col_name in enumerate(column_list, 1):
            column_letter = get_column_letter(idx)
            # Check if this column needs special width
            for special_name, width in special_width_columns.items():
                if special_name in col_name:
                    worksheet.column_dimensions[column_letter].width = width
                    break
        
        # 2. Set row heights - header row = 34.5, data rows = 16.9
        worksheet.row_dimensions[1].height = 34.5  # Header row height
        
        # Set data row heights
        for row_idx in range(2, worksheet.max_row + 1):
            worksheet.row_dimensions[row_idx].height = 16.9
        
        # 3. Apply header formatting - gray background and wrap text
        gray_fill = PatternFill(start_color='D9D9D9', end_color='D9D9D9', fill_type='solid')
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for col_idx in range(1, len(column_list) + 1):
            cell = worksheet.cell(row=1, column=col_idx)
            # Apply gray background
            cell.fill = gray_fill
            # Enable text wrapping for 2-line display
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            # Add borders
            cell.border = thin_border
            # Bold font
            cell.font = Font(bold=True, size=10)
        
        # 4. Apply data row formatting - wrap text and borders
        for row_idx in range(2, worksheet.max_row + 1):
            for col_idx in range(1, len(column_list) + 1):
                cell = worksheet.cell(row=row_idx, column=col_idx)
                # Enable text wrapping (fit to cell)
                cell.alignment = Alignment(vertical='center', wrap_text=True)
                # Add borders
                cell.border = thin_border
                
                # Adjust alignment based on column content
                col_name = column_list[col_idx - 1]
                # Right-align numeric columns
                if any(term in col_name for term in ['단가', '가격차이', '기본수량']):
                    cell.alignment = Alignment(horizontal='right', vertical='center', wrap_text=True)
                # Center-align code/ID columns
                elif any(term in col_name for term in ['코드', 'Code', '구분']):
                    cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                # Left-align everything else
                else:
                    cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
        
        # 5. Freeze header row
        worksheet.freeze_panes = 'A2'
        
        # Remove any existing auto-filter
        if hasattr(worksheet, 'auto_filter') and worksheet.auto_filter:
            worksheet.auto_filter.ref = None
        
        logger.info(f"Applied upload file specific formatting to worksheet with {worksheet.max_row} rows.")
        
    except Exception as e:
        logger.warning(f"Error applying upload file formatting: {e}")
        logger.debug(traceback.format_exc())

@safe_excel_operation
def create_split_excel_outputs(df_finalized: pd.DataFrame, output_path_base: str) -> tuple:
    """
    Create two Excel files:
    1. Result file: With images, for viewing
    2. Upload file: URL links only, for uploading to systems

    Args:
        df_finalized: The finalized DataFrame with all data
        output_path_base: The base path for output files

    Returns:
        tuple: (result_file_path, upload_file_path)
    """
    # Ensure we have valid data
    if df_finalized is None or df_finalized.empty:
        logger.error("No data to write to Excel. DataFrame is empty or None.")
        return None, None

    # Flatten any nested image dictionaries to prevent Excel conversion errors
    df_finalized = flatten_nested_image_dicts(df_finalized)
    
    logger.info(f"Starting creation of split Excel outputs from finalized DataFrame (Shape: {df_finalized.shape})")
    
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
        
        # Ensure columns are properly ordered (defense against the caller passing mal-formed data)
        if not all(col in FINAL_COLUMN_ORDER for col in df_finalized.columns):
            logger.warning("Input DataFrame columns are not in the exact FINAL_COLUMN_ORDER. Reordering again.")
            # Recreate with only the expected columns in the correct order
            ordered_df = pd.DataFrame()
            for col in FINAL_COLUMN_ORDER:
                if col in df_finalized.columns:
                    ordered_df[col] = df_finalized[col]
            df_finalized = ordered_df
        
        # Get file source info for naming
        source_info = "Unknown"
        mgmt_type = "승인관리"  # Default type
        row_count = len(df_finalized)
        
        try:
            # Check the appropriate column based on format (use both old and new column names)
            if '구분' in df_finalized.columns:
                # Get the most common value to use in naming
                source_val = df_finalized['구분'].iloc[0]
                if source_val == 'A':
                    mgmt_type = "승인관리"
                elif source_val == 'P':
                    mgmt_type = "가격관리"
                else:
                    mgmt_type = str(source_val)
            elif '구분(승인관리:A/가격관리:P)' in df_finalized.columns:
                source_val = df_finalized['구분(승인관리:A/가격관리:P)'].iloc[0]
                if source_val == 'A':
                    mgmt_type = "승인관리"
                elif source_val == 'P':
                    mgmt_type = "가격관리"
                else:
                    mgmt_type = str(source_val)
                    
            # Get company name for filename
            if '업체명' in df_finalized.columns:
                # Use the most common company name or the first one
                company_counts = df_finalized['업체명'].value_counts()
                if not company_counts.empty:
                    source_info = company_counts.index[0]
            elif '공급사명' in df_finalized.columns:
                company_counts = df_finalized['공급사명'].value_counts()
                if not company_counts.empty:
                    source_info = company_counts.index[0]
        except Exception as e:
            logger.warning(f"Error getting source name: {e}")
            source_info = "Mixed"
        
        # Create timestamped filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_part = datetime.now().strftime("%Y%m%d")
        
        # Format: {company}({count})-{mgmt_type}-{date}_{type}_{timestamp}.xlsx
        result_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_result_{timestamp}.xlsx"
        upload_filename = f"{source_info}({row_count}개)-{mgmt_type}-{date_part}_upload_{timestamp}.xlsx"
        
        # Make sure output_path_base is a directory, not a file
        # If it ends with .xlsx, use its directory instead
        if output_path_base.lower().endswith('.xlsx'):
            output_path_base = os.path.dirname(output_path_base)
            
        # Ensure the output directory exists
        os.makedirs(output_path_base, exist_ok=True)
        
        # Build full paths
        result_path = os.path.join(output_path_base, result_filename)
        upload_path = os.path.join(output_path_base, upload_filename)
        
        logger.info(f"Result file path (with images): {result_path}")
        logger.info(f"Upload file path (links only): {upload_path}")
        
        # -----------------------------------------
        # 1. Create Result File (with images)
        # -----------------------------------------
        try:
            logger.info(f"Attempting to write result file: {result_path} with {len(df_finalized)} rows.")
            
            # Create a new workbook for result file
            workbook_result = openpyxl.Workbook()
            worksheet_result = workbook_result.active
            worksheet_result.title = "제품 가격 비교"
            
            logger.info("Writing result data to Excel sheet...")
            
            # Convert image dictionaries to strings for initial data writing
            # This prevents "Cannot convert dict to Excel" errors
            df_for_excel = df_finalized.copy()
            
            # Convert any dictionary or complex objects to strings
            for col in df_for_excel.columns:
                for idx in df_for_excel.index:
                    value = df_for_excel.loc[idx, col]
                    if isinstance(value, dict):
                        # For dictionary values, store just the URL to make Excel happy
                        if 'url' in value:
                            # Handle case where url itself is a dictionary (nested dict)
                            if isinstance(value['url'], dict) and 'url' in value['url']:
                                df_for_excel.at[idx, col] = value['url']['url']
                            else:
                                df_for_excel.at[idx, col] = value['url']
                        else:
                            # Just convert to string representation if no URL
                            df_for_excel.at[idx, col] = str(value)
                    elif isinstance(value, pd.Series):
                        # For Series objects, convert to string
                        for item in value:
                            if pd.notna(item) and item not in ['-', '']:
                                if isinstance(item, dict) and 'url' in item:
                                    # Handle case where url itself is a dictionary (nested dict)
                                    if isinstance(item['url'], dict) and 'url' in item['url']:
                                        df_for_excel.at[idx, col] = item['url']['url']
                                    else:
                                        df_for_excel.at[idx, col] = item['url']
                                else:
                                    df_for_excel.at[idx, col] = str(item)
                                break
                        else:
                            df_for_excel.at[idx, col] = "-"
            
            # Write header
            for col_idx, col_name in enumerate(df_for_excel.columns, 1):
                worksheet_result.cell(row=1, column=col_idx, value=col_name)
            
            # Write data
            for row_idx, row in enumerate(df_for_excel.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    # Convert None to empty string to avoid writing 'None' to cells
                    worksheet_result.cell(row=row_idx, column=col_idx, value=value if not pd.isna(value) else "")
            
            # Apply common formatting (basic without images)
            _apply_basic_excel_formatting(worksheet_result, df_for_excel.columns.tolist())
            _add_hyperlinks_to_worksheet(worksheet_result, df_for_excel, hyperlinks_as_formulas=False)
            _add_header_footer(worksheet_result)
            
            # FIXED: Explicitly remove auto filter in result file
            if hasattr(worksheet_result, 'auto_filter') and worksheet_result.auto_filter:
                worksheet_result.auto_filter.ref = None
                logger.info("Removed filter from result Excel file")
            
            # Save without images first to ensure we can write to file
            workbook_result.save(result_path)
            
            # Now load the saved file to add images
            # Create a new workbook with images
            try:
                logger.info("Adding images to result file...")
                # Load the workbook
                workbook_with_images = openpyxl.load_workbook(result_path)
                worksheet_with_images = workbook_with_images.active
                
                # Process image columns
                image_cols = [col for col in df_finalized.columns if col in IMAGE_COLUMNS]
                images_added = 0
                
                # Adjust row heights to accommodate images
                for row_idx in range(2, len(df_finalized) + 2):
                    worksheet_with_images.row_dimensions[row_idx].height = 120  # Adjust row height
                
                # Adjust column widths for image columns
                for img_col in image_cols:
                    col_idx = df_finalized.columns.get_loc(img_col) + 1
                    col_letter = get_column_letter(col_idx)
                    worksheet_with_images.column_dimensions[col_letter].width = 22  # Wider columns for images
                
                # Add images to the worksheet
                for row_idx, (_, row) in enumerate(df_finalized.iterrows(), 2):
                    for img_col in image_cols:
                        col_idx = df_finalized.columns.get_loc(img_col) + 1
                        img_value = row[img_col]
                        
                        # Check if we have a dictionary with image data
                        if isinstance(img_value, dict):
                            # Try to reconstruct/find URLs for images if missing
                            if ('url' not in img_value or not img_value['url'] or 
                                not isinstance(img_value['url'], str) or
                                not img_value['url'].startswith(('http://', 'https://'))):
                                
                                # Handle case where url is a nested dictionary
                                if isinstance(img_value.get('url'), dict) and 'url' in img_value['url'] and isinstance(img_value['url']['url'], str):
                                    img_value['url'] = img_value['url']['url']
                                
                                # Continue with regular processing if URL is still invalid
                                if ('url' not in img_value or not img_value['url'] or 
                                    not isinstance(img_value['url'], str) or
                                    not img_value['url'].startswith(('http://', 'https://'))):
                                
                                    # Determine the source based on column name or image data
                                    source = img_value.get('source', '').lower()
                                    if not source:
                                        if '본사' in img_col:
                                            source = 'haereum'
                                        elif '고려' in img_col:
                                            source = 'kogift'
                                        elif '네이버' in img_col:
                                            source = 'naver'
                                
                                # Try to reconstruct URL based on source
                                if source == 'haereum' or source == 'haoreum':
                                    # Try to extract product code from path if available
                                    product_code = None
                                    if 'original_path' in img_value and isinstance(img_value['original_path'], str):
                                        path = img_value['original_path']
                                        # Look for product code pattern (e.g., BBCA0009349, CCBK0001873)
                                        code_match = re.search(r'([A-Z]{4}\d{7})', path)
                                        if code_match:
                                            product_code = code_match.group(1)
                                            
                                    # Use p_idx if available (highest priority)
                                    if 'p_idx' in img_value:
                                        product_code = img_value['p_idx']
                                    
                                    if product_code:
                                        # Try multiple extensions with proper suffix (typically 's')
                                        extensions = ['.jpg', '.png', '.gif']
                                        # Default suffix (usually 's' for small image)
                                        suffix = 's'
                                        
                                        # Extract suffix from path if possible
                                        if 'original_path' in img_value:
                                            suffix_match = re.search(r'([A-Z]{4}\d{7})(.*?)(\.[a-z]+)$', str(img_value['original_path']))
                                            if suffix_match and suffix_match.group(2):
                                                suffix = suffix_match.group(2)
                                        
                                        # Generate URL with the first extension (could be enhanced to verify actual existence)
                                        img_value['url'] = f"https://www.jclgift.com/upload/product/simg3/{product_code}{suffix}{extensions[0]}"
                                        logger.debug(f"Generated URL for Haereum image based on product code: {img_value['url']}")
                                elif source == 'kogift':
                                    if 'original_path' in img_value and isinstance(img_value['original_path'], str):
                                        orig_path = img_value['original_path']
                                        if 'upload' in orig_path:
                                            parts = orig_path.split('upload/')
                                            if len(parts) > 1:
                                                # Preserve the original file extension if present
                                                img_value['url'] = f"https://koreagift.com/ez/upload/{parts[1]}"
                                                logger.debug(f"Generated URL for Kogift image based on path: {img_value['url']}")
                                        else:
                                            # Try to extract product code or ID
                                            code_match = re.search(r'kogift_(.*?)_[a-f0-9]{8,}', orig_path)
                                            if code_match:
                                                product_name = code_match.group(1)
                                                # Try various extensions
                                                extensions = ['.jpg', '.png', '.gif']
                                                img_value['url'] = f"https://koreagift.com/ez/upload/mall/shop_{product_name}{extensions[0]}"
                                                logger.debug(f"Generated alternative URL for Kogift image: {img_value['url']}")
                                elif source == 'naver':
                                    if 'product_id' in img_value:
                                        # Try multiple extensions for Naver images
                                        extensions = ['.jpg', '.png', '.gif']
                                        img_value['url'] = f"https://shopping-phinf.pstatic.net/main_{img_value['product_id']}/{img_value['product_id']}{extensions[0]}"
                                        logger.debug(f"Generated URL for Naver image based on product_id: {img_value['url']}")
                                    elif 'original_path' in img_value and isinstance(img_value['original_path'], str):
                                        # Try to extract product ID from path
                                        path = img_value['original_path']
                                        id_match = re.search(r'naver_(.*?)_[a-f0-9]{8,}', path)
                                        if id_match:
                                            product_name = id_match.group(1)
                                            # Skip generating front URLs as they are unreliable
                                            logger.warning(f"Skipping generation of unreliable 'front' URL for Naver image: {path}")
                                            # If URL already exists and is a front URL, remove it
                                            if 'url' in img_value and isinstance(img_value['url'], str) and "pstatic.net/front/" in img_value['url']:
                                                logger.warning(f"Removing unreliable 'front' URL: {img_value['url']}")
                                                img_value['url'] = ''
                                
                            # Now check if we have a local path to add the image
                            if 'local_path' in img_value:
                                img_path = img_value['local_path']
                                has_url = 'url' in img_value and isinstance(img_value['url'], str) and img_value['url'].startswith(('http://', 'https://'))
                                
                                # Log URL information for debugging
                                if has_url:
                                    logger.debug(f"이미지에 URL 있음 (행 {row_idx}, 열 {col_idx}): {img_value['url'][:50]}...")
                                else:
                                    logger.debug(f"이미지에 URL 없음 (행 {row_idx}, 열 {col_idx})")
                                
                                if os.path.exists(img_path):
                                    try:
                                        # Create and add the image
                                        img = openpyxl.drawing.image.Image(img_path)
                                        # FIXED: Larger images in the result file
                                        img.width = 160  # Set image width - increased from 80
                                        img.height = 160  # Set image height - increased from 80
                                        img.anchor = f"{get_column_letter(col_idx)}{row_idx}"
                                        
                                        # FIXED: Add error handling for image loading
                                        try:
                                            worksheet_with_images.add_image(img)
                                            
                                            # Clear the cell content
                                            cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                            cell.value = ""
                                            
                                            # Add hyperlink to the image URL if available (the image will have a link)
                                            if has_url:
                                                # Use openpyxl hyperlink object - will show on hover
                                                cell.hyperlink = img_value['url']
                                                
                                                # Add a very small marker to indicate there's a URL (using a comment)
                                                try:
                                                    comment = openpyxl.comments.Comment(f"이미지 URL: {img_value['url']}", "시스템")
                                                    comment.width = 300
                                                    comment.height = 50
                                                    cell.comment = comment
                                                except Exception as comment_err:
                                                    logger.warning(f"코멘트 추가 실패 (행 {row_idx}, 열 {col_idx}): {comment_err}")
                                            
                                            images_added += 1
                                            logger.debug(f"이미지 추가 성공 (행 {row_idx}, 열 {col_idx})")
                                        except Exception as img_add_err:
                                            # More specific handling for common image errors
                                            if "not an image file" in str(img_add_err).lower():
                                                logger.warning(f"Not a valid image file: {img_path}")
                                                # Try to verify with PIL
                                                try:
                                                    from PIL import Image as PILImage
                                                    img_test = PILImage.open(img_path)
                                                    img_test.verify()  # Verify it's a valid image
                                                    logger.warning(f"Image verified with PIL but failed in openpyxl: {img_path}")
                                                except Exception as pil_err:
                                                    logger.warning(f"Image verification with PIL also failed: {pil_err}")
                                            
                                            logger.warning(f"Failed to add image (행 {row_idx}, 열 {col_idx}): {img_add_err}")
                                            
                                            # Fallback to displaying URL if available
                                            if has_url:
                                                cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                                cell.value = img_value['url']
                                                cell.hyperlink = img_value['url']
                                                cell.font = Font(color="0563C1", underline="single")
                                                logger.debug(f"Falling back to URL display for failed image: {img_value['url'][:50]}...")
                                    except Exception as e:
                                        logger.error(f"Error creating/configuring image object for {img_path}: {e}")
                                        # Fallback to displaying URL if available
                                        if has_url:
                                            cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                            cell.value = img_value['url']
                                            cell.hyperlink = img_value['url']
                                            cell.font = Font(color="0563C1", underline="single")
                                else:
                                    logger.warning(f"로컬 이미지 파일 없음 (행 {row_idx}, 열 {col_idx}): {img_path}")
                                    cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                    if has_url:
                                        # If we have a URL but no local image, show the URL
                                        cell.value = img_value['url']
                                        cell.hyperlink = img_value['url']
                                        cell.font = Font(color="0563C1", underline="single")
                                        logger.debug(f"로컬 이미지 없어 URL만 추가 (행 {row_idx}, 열 {col_idx}): {img_value['url'][:50]}...")
                                    else:
                                        # No image and no URL, add an error message
                                        cell.value = "이미지 및 URL 없음"
                                        logger.warning(f"이미지 및 URL 모두 없음 (행 {row_idx}, 열 {col_idx})")
                            else:
                                # No local path in the dictionary
                                logger.warning(f"이미지 로컬 경로 정보 없음 (행 {row_idx}, 열 {col_idx})")
                                cell = worksheet_with_images.cell(row=row_idx, column=col_idx)
                                if 'url' in img_value and isinstance(img_value['url'], str) and img_value['url'].startswith(('http://', 'https://')):
                                    # If we have only a URL, show it
                                    cell.value = img_value['url']
                                    cell.hyperlink = img_value['url']
                                    cell.font = Font(color="0563C1", underline="single")
                                    logger.debug(f"이미지 로컬 경로 없어 URL만 추가 (행 {row_idx}, 열 {col_idx}): {img_value['url'][:50]}...")
                                else:
                                    # No local path and no valid URL
                                    cell.value = "유효한 이미지 정보 없음"
                                    logger.warning(f"유효한 이미지 정보가 없음 (행 {row_idx}, 열 {col_idx})")
                
                # FIXED: Ensure filter is removed after image addition too
                if hasattr(worksheet_with_images, 'auto_filter') and worksheet_with_images.auto_filter:
                    worksheet_with_images.auto_filter.ref = None
                    logger.info("Removed filter from result Excel file after adding images")
                
                # Save the workbook with images
                workbook_with_images.save(result_path)
                logger.info(f"Successfully added {images_added} images to result file")
            except Exception as img_err:
                logger.error(f"Error adding images to result file: {img_err}")
                # Continue with the file without images
            
            result_success = True
            logger.info(f"Successfully created result file: {result_path}")
            
        except Exception as e:
            logger.error(f"Error creating result file: {e}")
            logger.debug(traceback.format_exc())
            result_success = False
        
        # -----------------------------------------
        # 2. Create Upload File (with links only and different column names)
        # -----------------------------------------
        try:
            logger.info(f"Preparing data for upload file: {upload_path}")
            
            # Create a deep copy of the original DataFrame to avoid modifying it
            df_upload = pd.DataFrame()
            
            # FIX: First extract image URLs from the DataFrame before column mapping
            # Create a new DataFrame with the image URLs
            df_with_image_urls = df_finalized.copy()
            
            # Process each image column to extract only web URLs
            for img_col in IMAGE_COLUMNS:
                if img_col in df_finalized.columns:
                    logger.info(f"Extracting image URLs from {img_col} column...")
                    
                    # Map the column names to the upload file column names
                    # This maps: '본사 이미지' -> '해오름(이미지링크)', '고려기프트 이미지' -> '고려기프트(이미지링크)', '네이버 이미지' -> '네이버쇼핑(이미지링크)'
                    upload_img_col = COLUMN_MAPPING_FINAL_TO_UPLOAD.get(img_col, img_col) # Use mapping, fallback to original if not found

                    # Create the target upload column if it doesn't exist in the intermediate df
                    if upload_img_col not in df_with_image_urls.columns:
                        df_with_image_urls[upload_img_col] = ""
                    
                    # Track URL extraction results for this column
                    urls_found = 0
                    fallback_urls_generated = 0
                    url_errors = 0
                    
                    # Process all rows to extract image URLs
                    for idx in df_finalized.index:
                        value = df_finalized.at[idx, img_col]
                        
                        # Default to empty string (not "-") to avoid showing placeholders in cells
                        image_url = ""
                        url_source = "unknown"
                        
                        try:
                            # FIX: Explicitly check dictionary structure and extract 'url' key if it's a web URL
                            if isinstance(value, dict):
                                # Check for product_url first (for Naver) - NEW ADDITION
                                if 'product_url' in value and isinstance(value['product_url'], str) and value['product_url'].startswith(('http://', 'https://')):
                                    image_url = value['product_url'].strip()
                                    url_source = "direct_from_product_url_key"
                                    logger.debug(f"Found product URL in {img_col} at idx {idx} using 'product_url' key: {image_url[:50]}...")
                                # Then check for regular 'url' key as fallback
                                elif 'url' in value and isinstance(value['url'], str) and value['url'].startswith(('http://', 'https://')):
                                    image_url = value['url'].strip()
                                    url_source = "direct_from_url_key"
                                    logger.debug(f"Found web URL in {img_col} at idx {idx} using 'url' key: {image_url[:50]}...")
                                else:
                                    # Fallback: Check other potential keys if 'url' is missing or invalid
                                    for url_key in ['image_url', 'original_url', 'src']:
                                        fallback_url = value.get(url_key)
                                        if fallback_url and isinstance(fallback_url, str) and fallback_url.startswith(('http://', 'https://')):
                                            image_url = fallback_url.strip()
                                            url_source = f"fallback_from_{url_key}"
                                            logger.debug(f"Found web URL in {img_col} at idx {idx} using fallback key '{url_key}': {image_url[:50]}...")
                                            break # Stop checking keys once a valid URL is found
                            
                            # Handle string URL format (if the value is not a dictionary)
                            elif isinstance(value, str) and value.strip() and value != '-':
                                url = value.strip()
                                if url.startswith(('http://', 'https://')):
                                    # Basic check if it looks like an image URL (more lenient)
                                    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
                                    image_identifiers = ['upload/', 'simg', 'pstatic.net', 'phinf', '/image/', 'thumb']
                                    
                                    if any(url.lower().endswith(ext) for ext in image_extensions) or any(ident in url.lower() for ident in image_identifiers):
                                        image_url = url
                                        url_source = "direct_string_url"
                                        logger.debug(f"Found image URL string in {img_col} at idx {idx}: {url[:50]}...")
                            
                            # --- Reconstruction logic (REMOVED) ---
                            # Reconstruction logic is removed here as the input `df_finalized`
                            # should already contain the correct image URL in the dictionary
                            # provided by `image_integration.py`. We only need to extract it.
                            # If the dictionary or the 'url' key is missing, we should not reconstruct.
                            
                            # Final validation for the extracted URL
                            if image_url:
                                # Validate the URL format
                                if not image_url.startswith(('http://', 'https://')):
                                    logger.warning(f"정상적인 URL이 아님 (행 {idx+1}, {img_col}): '{image_url[:50]}' - http:// 또는 https://로 시작해야 함")
                                    image_url = ""  # Reset invalid URL
                                    url_errors += 1
                                else:
                                    # URL validation passed
                                    urls_found += 1
                                    logger.debug(f"유효한 이미지 URL 추출 완료 (행 {idx+1}, {img_col}, 소스: {url_source}): {image_url[:50]}...")
                        except Exception as e:
                            logger.error(f"이미지 URL 추출 중 오류 발생 (행 {idx+1}, {img_col}): {str(e)[:100]}")
                            image_url = ""  # Reset on error
                            url_errors += 1
                        
                        # Store the extracted image URL in the intermediate DataFrame under the UPLOAD column name
                        df_with_image_urls.at[idx, upload_img_col] = image_url if image_url else "" # Use empty string if no valid URL

                    # Log summary for this column
                    logger.info(f"URL 추출 결과 ({upload_img_col}): 총 {urls_found}개 URL 추출 성공, {url_errors}개 오류")

            # Check if we extracted any image URLs
            for img_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']: # Use upload file column names
                if img_col in df_with_image_urls.columns:
                    url_count = (df_with_image_urls[img_col].astype(str).str.startswith(('http://', 'https://'))).sum()
                    logger.info(f"Extracted {url_count} image URLs for {img_col} in upload data")
                    
                    # Log a few examples if any URLs were found
                    if url_count > 0:
                        sample_urls = df_with_image_urls[df_with_image_urls[img_col].astype(str).str.startswith(('http://', 'https://'))][img_col].head(3).tolist()
                        logger.info(f"Sample image URLs for {img_col}: {sample_urls}")

            # Map columns from result format to upload format 
            df_upload = pd.DataFrame() # Start with an empty DataFrame for the upload file
            
            for target_col in UPLOAD_COLUMN_ORDER:
                # Find corresponding source column from the original result format
                source_col = None
                for result_col, upload_col in COLUMN_MAPPING_FINAL_TO_UPLOAD.items():
                    if upload_col == target_col:
                        source_col = result_col
                        break
                
                # Determine where to get the data:
                # - If it's an upload image link column, get it from df_with_image_urls[target_col]
                # - Otherwise, get it from df_finalized[source_col]
                
                if target_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']:
                    # Get the already processed image URL from df_with_image_urls
                    if target_col in df_with_image_urls.columns:
                        df_upload[target_col] = df_with_image_urls[target_col]
                    else:
                        df_upload[target_col] = '' # Should not happen, but safety check
                        logger.warning(f"Processed image URL column '{target_col}' not found in intermediate df.")
                elif source_col and source_col in df_finalized.columns:
                    # Get non-image data from the original finalized DataFrame
                    df_upload[target_col] = df_finalized[source_col]
                else:
                    # If no matching column found, add an empty column
                    df_upload[target_col] = ''
                    logger.warning(f"Could not find source column for upload column '{target_col}' or source column missing.")

            # Log image columns in the final upload file to confirm extraction worked
            for img_col in ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']:
                if img_col in df_upload.columns:
                    non_empty = df_upload[img_col].astype(str).str.strip().str.len() > 0
                    non_empty_urls = df_upload[img_col].astype(str).str.startswith(('http://', 'https://'))
                    count = non_empty.sum()
                    url_count = non_empty_urls.sum()
                    logger.info(f"Upload file: {img_col} column has {count} non-empty values, {url_count} are URLs")
                    
                    # Log sample values
                    if url_count > 0:
                        sample_values = df_upload.loc[non_empty_urls, img_col].head(3).tolist()
                        logger.info(f"Sample URL values in final upload df: {sample_values}")
                    elif count > 0:
                         sample_non_urls = df_upload.loc[non_empty & ~non_empty_urls, img_col].head(3).tolist()
                         logger.warning(f"Sample non-URL values in final upload df {img_col}: {sample_non_urls}")

            # Create new workbook for upload file (now with properly extracted image URLs)
            workbook_upload = openpyxl.Workbook()
            worksheet_upload = workbook_upload.active
            worksheet_upload.title = "제품 가격 비교 (업로드용)"
            
            logger.info(f"Writing upload file (with image links): {upload_path} with {len(df_upload)} rows.")
            
            # Write header
            for col_idx, col_name in enumerate(df_upload.columns, 1):
                worksheet_upload.cell(row=1, column=col_idx, value=col_name)
            
            # Write data (now with properly extracted image URLs)
            for row_idx, row in enumerate(df_upload.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    # Handle NaN/None values
                    if pd.isna(value) or value is None:
                        cell_value = ""
                    else:
                        cell_value = value
                    
                    # Write the cell value
                    worksheet_upload.cell(row=row_idx, column=col_idx, value=cell_value)
                
                # Add direct verification of image URL columns for logging
                if row_idx <= 5:  # Log only the first 5 rows
                    img_idx_start = len(UPLOAD_COLUMN_ORDER) - 3
                    for i in range(3):  # Check all 3 image columns
                        col_idx = img_idx_start + i + 1
                        cell_value = worksheet_upload.cell(row=row_idx, column=col_idx).value
                        col_name = UPLOAD_COLUMN_ORDER[img_idx_start + i]
                        if cell_value and isinstance(cell_value, str) and cell_value.startswith(('http://', 'https://')):
                            logger.debug(f"Row {row_idx}, {col_name}: Valid URL found: {cell_value[:50]}...")
                        else:
                            logger.debug(f"Row {row_idx}, {col_name}: No valid URL found. Value: '{cell_value}'")
            
            # Apply upload file specific formatting (new function with requested formatting)
            _apply_upload_file_formatting(worksheet_upload, df_upload.columns.tolist())
            
            # Add hyperlinks to all image URL cells
            try:
                logger.info("Adding hyperlinks to image URLs in upload file...")
                
                # Define upload image columns
                upload_image_cols = ['해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)']
                
                # Get the column indices for these columns
                col_indices = {}
                for i, col_name in enumerate(df_upload.columns, 1):
                    if col_name in upload_image_cols:
                        col_indices[col_name] = i
                
                # Add hyperlinks to the cells
                for row_idx in range(2, len(df_upload) + 2):  # Start from row 2 (after header)
                    for col_name, col_idx in col_indices.items():
                        cell = worksheet_upload.cell(row=row_idx, column=col_idx)
                        url = cell.value
                        
                        # Only add hyperlink if the cell contains a valid URL
                        if isinstance(url, str) and url.strip() and url.startswith(('http://', 'https://')):
                            cell.hyperlink = url
                            cell.font = Font(color="0563C1", underline="single")
                
                logger.info("Hyperlinks added to upload file successfully")
            except Exception as e:
                logger.warning(f"Error adding hyperlinks to upload file: {e}")
            
            # Save upload file
            workbook_upload.save(upload_path)
            upload_success = True
            logger.info(f"Successfully created upload file (with image links): {upload_path}")
                
            return result_success, upload_success, result_path, upload_path
        
        except Exception as upload_err:
            logger.error(f"Error creating upload file: {upload_err}")
            logger.debug(traceback.format_exc())
            upload_success = False
        
        return result_success, upload_success, result_path, upload_path
        
    except Exception as main_error:
        logger.error(f"Unexpected error in create_split_excel_outputs: {main_error}")
        logger.debug(traceback.format_exc())
        return False, False, None, None

def extract_naver_image_urls(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Extracts Naver image URLs from a DataFrame column and filters out unreliable 'front' URLs.
    
    Args:
        df: DataFrame containing image data
        column_name: Name of the column containing image data
        
    Returns:
        DataFrame with processed image URLs
    """
    if column_name not in df.columns:
        logger.warning(f"Column '{column_name}' not found in DataFrame")
        return df
    
    result_df = df.copy()
    front_url_count = 0
    
    for idx in df.index:
        value = df.at[idx, column_name]
        
        # Skip non-dictionary values
        if not isinstance(value, dict):
            continue
        
        # Check if this is a Naver image dict with a URL
        if 'source' in value and value['source'] == 'naver' and 'url' in value:
            url = value['url']
            
            # Check if it's an unreliable front URL
            if is_unreliable_naver_url(url):
                front_url_count += 1
                
                # Either remove the URL or the entire image data
                if 'local_path' in value and os.path.exists(value['local_path']):
                    # Keep the local image but remove the URL
                    value['url'] = ''
                    logger.info(f"Row {idx}: Removed unreliable 'front' URL but kept local image")
                    result_df.at[idx, column_name] = value
                else:
                    # Remove the entire image data
                    result_df.at[idx, column_name] = '-'
                    logger.info(f"Row {idx}: Removed unreliable 'front' URL image data entirely")
    
    if front_url_count > 0:
        logger.warning(f"Removed or modified {front_url_count} unreliable 'front' URLs in '{column_name}' column")
    
    return result_df

def use_naver_product_links_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace Naver image URLs with the actual product website links for the upload file.
    
    Args:
        df: DataFrame with image information
        
    Returns:
        DataFrame with Naver image URLs replaced with product website links
    """
    # Create a copy to avoid modifying the original DataFrame
    result_df = df.copy()
    
    # We need to find both the Naver image column and the Naver product link column
    naver_img_col = '네이버 이미지'
    naver_link_col = '네이버 쇼핑 링크'
    
    # Ensure both columns exist
    if naver_img_col not in df.columns or naver_link_col not in df.columns:
        logger.warning(f"Either the Naver image column '{naver_img_col}' or link column '{naver_link_col}' is missing. Cannot replace URLs.")
        return df
    
    # Track processed items
    replaced_count = 0
    processed_count = 0
    
    # Process each row
    for idx in df.index:
        try:
            # Get the image data
            img_value = df.at[idx, naver_img_col]
            # Get the product link value
            product_link = df.at[idx, naver_link_col]
            
            processed_count += 1
            
            # Skip if no product link exists
            if pd.isna(product_link) or product_link in ['', '-', 'None', None]:
                continue
            
            # Handle dictionary format for image value
            if isinstance(img_value, dict):
                # Check if this is a Naver image dictionary
                if 'source' in img_value and img_value['source'] == 'naver':
                    # Create a copy of the image data
                    new_img_value = img_value.copy()
                    # Replace the URL with the product link
                    if product_link and isinstance(product_link, str) and product_link.startswith(('http://', 'https://')):
                        new_img_value['url'] = product_link
                        new_img_value['product_url'] = product_link  # Also store as product_url for clarity
                        result_df.at[idx, naver_img_col] = new_img_value
                        replaced_count += 1
                        logger.debug(f"Row {idx}: Replaced Naver image URL with product link: {product_link[:50]}...")
            # If the image value is a string URL
            elif isinstance(img_value, str) and img_value.startswith(('http://', 'https://')):
                # Only replace if we have a valid product link
                if product_link and isinstance(product_link, str) and product_link.startswith(('http://', 'https://')):
                    result_df.at[idx, naver_img_col] = {
                        'url': product_link,
                        'source': 'naver',
                        'product_url': product_link,
                        'original_image_url': img_value  # Keep the original image URL for reference
                    }
                    replaced_count += 1
                    logger.debug(f"Row {idx}: Replaced Naver image string URL with product link: {product_link[:50]}...")
        except Exception as e:
            logger.error(f"Error processing row {idx} in use_naver_product_links_for_upload: {e}")
            
    logger.info(f"Replaced {replaced_count}/{processed_count} Naver image URLs with product website links")
    return result_df


# Function to handle extraction and replacement of image URLs for upload file
def prepare_naver_image_urls_for_upload(df_with_image_urls: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare Naver image URLs for the upload file by prioritizing product links over image URLs.
    
    Args:
        df_with_image_urls: DataFrame with extracted image URLs
        
    Returns:
        DataFrame with processed Naver image URLs
    """
    if df_with_image_urls.empty:
        return df_with_image_urls
        
    # Naver image column in upload format
    naver_img_col = '네이버쇼핑(이미지링크)'
    # Naver link column in upload format 
    naver_link_col = '네이버 링크'
    
    # Check if necessary columns exist
    if naver_img_col not in df_with_image_urls.columns:
        logger.warning(f"Naver image column '{naver_img_col}' not found in DataFrame. Skipping preparation.")
        return df_with_image_urls
    
    if naver_link_col not in df_with_image_urls.columns:
        logger.warning(f"Naver link column '{naver_link_col}' not found in DataFrame. Cannot replace with product links.")
        return df_with_image_urls
    
    # Track processed items
    replaced_count = 0
    processed_count = 0
    
    # Create a copy of the DataFrame
    result_df = df_with_image_urls.copy()
    
    # Process each row
    for idx in df_with_image_urls.index:
        try:
            # Get the image URL value
            img_url = df_with_image_urls.at[idx, naver_img_col]
            # Get the product link value
            product_link = df_with_image_urls.at[idx, naver_link_col]
            
            processed_count += 1
            
            # Skip if no product link exists or if image URL is already empty
            if pd.isna(product_link) or product_link in ['', '-', 'None', None] or pd.isna(img_url) or img_url == '':
                continue
            
            # Replace image URL with product link
            if product_link and isinstance(product_link, str) and product_link.startswith(('http://', 'https://')):
                # Only replace if the current image URL is from pstatic.net
                if isinstance(img_url, str) and ('pstatic.net' in img_url or not img_url.strip()):
                    result_df.at[idx, naver_img_col] = product_link
                    replaced_count += 1
                    logger.debug(f"Row {idx}: Replaced Naver image URL with product link in upload file: {product_link[:50]}...")
        except Exception as e:
            logger.error(f"Error processing row {idx} in prepare_naver_image_urls_for_upload: {e}")
    
    logger.info(f"Prepared {replaced_count}/{processed_count} Naver image URLs for upload file")
    return result_df

def is_unreliable_naver_url(url: str) -> bool:
    """
    Checks if a URL is an unreliable Naver 'front' URL.
    
    Args:
        url: The URL to check
        
    Returns:
        True if it's an unreliable front URL, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    # Check for the problematic pattern
    if "pstatic.net/front/" in url:
        logger.warning(f"Detected unreliable 'front' URL: {url}")
        return True
    
    return False

