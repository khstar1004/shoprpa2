import pandas as pd
import numpy as np
import logging
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Alignment, Border, Side, Font
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.pagebreak import PageBreak
import os
import datetime
import configparser
from typing import Optional, Dict, List, Any, Tuple, Union
import re
import time
import traceback
import hashlib
from urllib.parse import urlparse, unquote
from PIL import Image
import sys
import openpyxl
import io
import requests
from functools import wraps
import functools
import os.path

# --- Setup Logger ---
logger = logging.getLogger(__name__)

# --- Constants ---
PROMO_KEYWORDS = ['판촉', '기프트', '답례품', '기념품', '인쇄', '각인', '제작', '호갱', '몽키', '홍보']

# Column Rename Mapping (Keep for potential input variations)
COLUMN_RENAME_MAP = {
    '날짜': '구분',
    '담당자': '담당자',
    '담 당자': '담당자',
    '업체명': '업체명',
    '업체코드': '업체코드',
    'Code': 'Code',
    '상품코드': 'Code',
    '중분류카테고리': '중분류카테고리',
    '카테고리(중분류)': '중분류카테고리',
    '상품명': '상품명',
    'name': '상품명',
    '기본수량(1)': '기본수량(1)',
    '본사 기본수량': '기본수량(1)',
    '판매단가(V포함)': '판매단가(V포함)',
    '판매단가1(VAT포함)': '판매단가(V포함)',
    '본사상품링크': '본사상품링크',
    '본사링크': '본사상품링크',
    '기본수량(2)': '기본수량(2)',
    '고려 기본수량': '기본수량(2)',
    '판매가(V포함)(2)': '판매가(V포함)(2)',
    '판매단가(V포함)(2)': '판매단가(V포함)(2)',
    '판매단가2(VAT포함)': '판매단가(V포함)(2)',
    '가격차이(2)': '가격차이(2)',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '고 려기프트 상품링크': '고려기프트 상품링크',  # Add variant with space
    '고려 링크': '고려기프트 상품링크',
    '고 려 링크': '고려기프트 상품링크',  # Add variant with spaces
    '기본수량(3)': '기본수량(3)',
    '네이버 기본수량': '기본수량(3)',
    '판매단가(V포함)(3)': '판매단가(V포함)(3)',
    '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
    '가격차이(3)': '가격차이(3)',
    '공급사명': '공급사명',
    '네이버 공급사명': '공급사명',
    '공급사 상품링크': '공급사 상품링크',
    '네이버 쇼핑 링크': '네이버 쇼핑 링크',
    '네이버 링크': '네이버 쇼핑 링크',
    '본사 이미지': '본사 이미지',
    '해오름이미지경로': '본사 이미지',
    '고려기프트 이미지': '고려기프트 이미지',
    '고려기프트(이미지링크)': '고려기프트 이미지',
    '네이버 이미지': '네이버 이미지',
    '네이버쇼핑(이미지링크)': '네이버 이미지'
}

# Final Target Column Order (Based on "엑셀 골든")
FINAL_COLUMN_ORDER = [
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명',
    '기본수량(1)', '판매단가(V포함)', '본사상품링크',
    '기본수량(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
    '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크',
    '본사 이미지', '고려기프트 이미지', '네이버 이미지',
    '매칭_여부', '매칭_정확도', '텍스트_유사도', '이미지_유사도', '매칭_품질'
]

# Columns that must be present in the input file for processing
# This can be a subset of FINAL_COLUMN_ORDER
REQUIRED_INPUT_COLUMNS = [
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리',
    '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
]

# --- Column Type Definitions for Formatting ---
# Define columns for specific formatting rules
PRICE_COLUMNS = [
    '판매단가(V포함)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '판매단가(V포함)(3)',
    '가격차이(2)', '가격차이(3)'
]
QUANTITY_COLUMNS = ['기본수량(1)', '기본수량(2)', '기본수량(3)']
PERCENTAGE_COLUMNS = [] # 퍼센트 컬럼 목록 비우기
TEXT_COLUMNS = ['구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명', '공급사명']
LINK_COLUMNS_FOR_HYPERLINK = {
    '본사상품링크': '본사상품링크',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '공급사 상품링크': '공급사 상품링크',
    '네이버 쇼핑 링크': '네이버 쇼핑 링크'
    # Image columns are handled separately
}
IMAGE_COLUMNS = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']

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

NEGATIVE_PRICE_FILL = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid") # Yellow fill for negative diff

# Image Processing Constants
IMAGE_COLUMNS = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
IMAGE_MAX_SIZE = (1200, 1200)  # Excel 2021 maximum supported image size
IMAGE_STANDARD_SIZE = (200, 200)  # Standard display size in Excel
IMAGE_QUALITY = 85  # JPEG compression quality
SUPPORTED_IMAGE_FORMATS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp']  # Supported by Excel 2021

# Image cell specific styling
IMAGE_CELL_HEIGHT = 90  # Row height for image cells
IMAGE_CELL_WIDTH = 15   # Column width for image cells

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
            if is_pct_col or ((col_name_str in PRICE_COLUMNS or col_name_str in QUANTITY_COLUMNS) and is_numeric_value):
                cell.alignment = RIGHT_ALIGNMENT
            elif col_name_str in IMAGE_COLUMNS or 'Code' in col_name_str or '코드' in col_name_str or col_name_str == '구분':
                 cell.alignment = CENTER_ALIGNMENT
            else:
                cell.alignment = LEFT_ALIGNMENT # Default left align for text/links/errors
    logger.debug("Finished applying cell styles.")

def _process_image_columns(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Process and embed images in Excel worksheet."""
    logger.debug("Processing image columns...")
    
    # Get image column indices
    image_cols = {col: idx for idx, col in enumerate(df.columns, 1) 
                 if col in IMAGE_COLUMNS}
    
    if not image_cols:
        logger.info("No image columns found to process")
        return
    
    # Create a temporary directory for image processing if needed
    temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'temp_images')
    os.makedirs(temp_dir, exist_ok=True)
    
    # Track processed images to avoid duplicate processing
    processed_images = {}
    
    # Count successful/failed images for reporting
    success_count = 0
    failed_count = 0
    
    # Create recovery directories if they don't exist
    recovery_dirs = [
        os.path.join('C:', 'RPA', 'Image', 'Main', 'Haereum'),
        os.path.join('C:', 'RPA', 'Image', 'Main', 'Kogift'),
        os.path.join('C:', 'RPA', 'Image', 'Main', 'Naver'),
        os.path.join('C:', 'RPA', 'Image', 'Main', 'kogift_pre'),
        os.path.join('C:', 'RPA', 'Image', 'Target', 'Haereum'),
        os.path.join('C:', 'RPA', 'Image', 'Target', 'Kogift'),
        os.path.join('C:', 'RPA', 'Image', 'Target', 'Naver')
    ]
    for directory in recovery_dirs:
        os.makedirs(directory, exist_ok=True)
        
    for row_idx in range(2, worksheet.max_row + 1):
        for col_name, col_idx in image_cols.items():
            cell = worksheet.cell(row=row_idx, column=col_idx)
            if not cell.value or cell.value == '-':
                continue

            try:
                # Initialize img_url to None at the very beginning
                img_url = None
                
                # Handle both string paths and dictionary image info
                if isinstance(cell.value, dict):
                    img_info = cell.value
                    img_path = None
                    
                    # Keep track of original data for logging
                    original_data = str(img_info)
                    
                    # Get source from the dictionary
                    source = img_info.get('source', '')
                    
                    # Try to get URL from the dictionary - do this first for img_url initialization
                    if 'url' in img_info:
                        img_url = img_info['url']
                        # Fix backslashes in URLs immediately
                        if img_url and '\\' in img_url:
                            img_url = img_url.replace('\\', '/')
                        # Fix URL format if it's missing proper scheme
                        if img_url and img_url.startswith('https:') and not img_url.startswith('https://'):
                            img_url = 'https://' + img_url[6:]
                        elif img_url and ':' in img_url and not img_url.startswith(('http:', 'https:')):
                            # Handle case where URL is like 'https:\www...'
                            scheme, path = img_url.split(':', 1)
                            path = path.replace('\\', '').lstrip('/')
                            img_url = f"{scheme}://{path}"
                else:
                    # Handle plain string path
                    img_path = str(cell.value)
                    # Check if it might be a URL
                    if img_path.startswith('http'):
                        img_url = img_path  # Store as URL for potential fallback
                    
                    # Create a dummy img_info for consistent handling
                    img_info = {'source': 'unknown'}
                    original_data = img_path
                    source = 'unknown'
                
                # Normalize image paths (replace backslashes with forward slashes)
                if 'local_path' in img_info and img_info['local_path']:
                    img_info['local_path'] = img_info['local_path'].replace('\\', '/')
                
                # First try the 'original_path' key if it exists
                if 'original_path' in img_info and img_info['original_path']:
                    orig_path = img_info['original_path'].replace('\\', '/')
                    # Check if the path exists
                    if os.path.exists(orig_path):
                        img_path = orig_path
                        logger.debug(f"Using original_path: {img_path}")
                
                # If original_path doesn't exist, try local_path
                if not img_path and 'local_path' in img_info and img_info['local_path']:
                    local_path = img_info['local_path'].replace('\\', '/')
                        
                    # First check if the exact path exists
                    if os.path.exists(local_path):
                        img_path = local_path
                        logger.debug(f"Using exact local_path: {img_path}")
                    else:
                        # If still no image found, try parsing from local_path
                        try:
                            # Extract file name components
                            file_dir = os.path.dirname(local_path)
                            file_name = os.path.basename(local_path)
                            file_base, file_ext = os.path.splitext(file_name)
                            
                            # Check if the path has _nobg suffix and try to find original file
                            if '_nobg.png' in file_name:
                                base_name = file_base.replace('_nobg', '')
                                # Try with multiple extensions
                                for ext in ['.jpg', '.jpeg', '.png', '.gif']:
                                    test_path = os.path.join(file_dir, base_name + ext)
                                    if os.path.exists(test_path):
                                        img_path = test_path
                                        logger.debug(f"Found original image using base name: {img_path}")
                                        break
                            
                            # If still no path, try to find _nobg version
                            if not img_path and not file_name.endswith('_nobg.png'):
                                nobg_name = os.path.splitext(file_name)[0] + '_nobg.png'
                                nobg_path = os.path.join(file_dir, nobg_name)
                                if os.path.exists(nobg_path):
                                    img_path = nobg_path
                                    logger.debug(f"Found _nobg version: {img_path}")
                            
                            # Try searching in alternate directories
                            if not img_path:
                                search_dirs = []
                                if 'naver' in source.lower():
                                    search_dirs = [
                                        os.path.join('C:', 'RPA', 'Image', 'Main', 'Naver'),
                                        os.path.join('C:', 'RPA', 'Image', 'Target', 'Naver')
                                    ]
                                elif 'kogift' in source.lower() or 'koreagift' in source.lower():
                                    search_dirs = [
                                        os.path.join('C:', 'RPA', 'Image', 'Main', 'Kogift'),
                                        os.path.join('C:', 'RPA', 'Image', 'Main', 'kogift_pre'),
                                        os.path.join('C:', 'RPA', 'Image', 'Target', 'Kogift')
                                    ]
                                else:
                                    search_dirs = [
                                        os.path.join('C:', 'RPA', 'Image', 'Main', 'Haereum'),
                                        os.path.join('C:', 'RPA', 'Image', 'Target', 'Haereum')
                                    ]
                                
                                # Try each directory
                                for search_dir in search_dirs:
                                    if os.path.exists(search_dir):
                                        # Try to find file with similar name
                                        for fname in os.listdir(search_dir):
                                            # Extract product name from filename
                                            parts = file_name.split('_', 1)
                                            if len(parts) > 1 and parts[1] in fname:
                                                img_path = os.path.join(search_dir, fname)
                                                logger.debug(f"Found in alternate directory: {img_path}")
                                                break
                                    if img_path:
                                        break
                        except Exception as path_err:
                            logger.warning(f"Error trying to find similar file: {path_err}")
                
                # Try a hash-based search if we have a URL but no path
                if not img_path and img_url:
                    try:
                        # Normalize URL for hash creation
                        url_to_hash = img_url
                        # Create hash for searching
                        url_hash = hashlib.md5(url_to_hash.encode()).hexdigest()[:10]
                        
                        # Define directories to search based on source
                        search_dirs = []
                        if 'naver' in source.lower():
                            search_dirs = [
                                os.path.join('C:', 'RPA', 'Image', 'Main', 'Naver'),
                                os.path.join('C:', 'RPA', 'Image', 'Target', 'Naver')
                            ]
                        elif 'kogift' in source.lower() or 'koreagift' in source.lower():
                            search_dirs = [
                                os.path.join('C:', 'RPA', 'Image', 'Main', 'Kogift'),
                                os.path.join('C:', 'RPA', 'Image', 'Main', 'kogift_pre'),
                                os.path.join('C:', 'RPA', 'Image', 'Target', 'Kogift')
                            ]
                        else:
                            search_dirs = [
                                os.path.join('C:', 'RPA', 'Image', 'Main', 'Haereum'),
                                os.path.join('C:', 'RPA', 'Image', 'Target', 'Haereum')
                            ]
                        
                        # Search all directories for this hash
                        for directory in search_dirs:
                            if os.path.exists(directory):
                                for filename in os.listdir(directory):
                                    if url_hash in filename and os.path.isfile(os.path.join(directory, filename)):
                                        img_path = os.path.join(directory, filename)
                                        logger.debug(f"Found file using URL hash: {img_path}")
                                        break
                            if img_path:
                                break
                    except Exception as hash_err:
                        logger.warning(f"Error searching by hash: {hash_err}")
                
                # If still no image found but we have a URL, try to download it now
                if not img_path and img_url:
                    try:
                        # Normalize URL for download
                        normalized_url = img_url
                        
                        # Ensure proper URL scheme
                        if normalized_url.startswith('//'):
                            normalized_url = 'https:' + normalized_url
                        elif not normalized_url.startswith(('http://', 'https://')):
                            if ":" in normalized_url:
                                scheme, path = normalized_url.split(":", 1)
                                normalized_url = f"{scheme}://{path.lstrip('/')}"
                            else:
                                normalized_url = 'https://' + normalized_url.lstrip('/')
                        
                        # Fix common URL issues with specific sites
                        if 'jclgift.com' in normalized_url and '\\upload\\' in normalized_url:
                            normalized_url = normalized_url.replace('\\upload\\', '/upload/')
                        elif 'koreagift.com' in normalized_url and '\\ez\\upload\\' in normalized_url:
                            normalized_url = normalized_url.replace('\\ez\\upload\\', '/ez/upload/')
                        
                        # Handle URLs with whitespace
                        if ' ' in normalized_url:
                            normalized_url = normalized_url.replace(' ', '%20')
                        
                        logger.info(f"Attempting to download from URL: {normalized_url}")
                        
                        # Generate a unique filename
                        url_hash = hashlib.md5(normalized_url.encode()).hexdigest()[:10]
                        timestamp = int(time.time())
                        
                        # Determine appropriate directory based on source
                        if 'naver' in source.lower():
                            save_dir = os.path.join('C:', 'RPA', 'Image', 'Main', 'Naver')
                        elif 'kogift' in source.lower() or 'koreagift' in source.lower():
                            save_dir = os.path.join('C:', 'RPA', 'Image', 'Main', 'Kogift')
                        else:
                            save_dir = os.path.join('C:', 'RPA', 'Image', 'Main', 'Haereum')
                        
                        # Ensure directory exists
                        os.makedirs(save_dir, exist_ok=True)
                        
                        # Generate a descriptive filename
                        if 'local_path' in img_info and img_info['local_path']:
                            # Extract product name from local_path if possible
                            base_name = os.path.basename(img_info['local_path'])
                            # Remove extension and any hash part
                            product_parts = base_name.split('_')
                            if len(product_parts) > 2:
                                # Keep only the first parts that likely represent the product name
                                product_name = '_'.join(product_parts[1:-1]) 
                            else:
                                product_name = os.path.splitext(base_name)[0]
                        else:
                            product_name = f"recovery_{source}"
                        
                        # Clean filename to avoid special characters
                        product_name = re.sub(r'[^\w\-_.]', '_', product_name)
                        filename = f"{source}_{product_name}_{url_hash}_{timestamp}.jpg"
                        download_path = os.path.join(save_dir, filename)
                        
                        # Download image
                        logger.info(f"Attempting to download missing image: {normalized_url} -> {download_path}")
                        
                        # Try multiple user agents and referers
                        headers_list = [
                            {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                                'Referer': 'https://www.google.com/'
                            },
                            {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
                                'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
                            },
                            {
                                'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
                                'Accept': 'image/webp,image/*,*/*;q=0.8',
                            }
                        ]
                        
                        # Site-specific headers
                        if 'jclgift' in normalized_url:
                            for header in headers_list:
                                header['Referer'] = 'https://www.jclgift.com/'
                        elif 'kogift' in normalized_url or 'koreagift' in normalized_url:
                            for header in headers_list:
                                header['Referer'] = 'https://koreagift.com/'
                        
                        # Make multiple attempts with different headers
                        response = None
                        for headers in headers_list:
                            try:
                                response = requests.get(normalized_url, headers=headers, timeout=10)
                                if response.status_code == 200:
                                    break
                                logger.warning(f"Download attempt with {headers['User-Agent']} failed with status {response.status_code}")
                            except Exception as req_err:
                                logger.warning(f"Download attempt failed with error: {req_err}")
                        
                        # Process response if we got a good status
                        if response and response.status_code == 200:
                            # Check it's actually an image
                            content_type = response.headers.get('Content-Type', '')
                            content_length = int(response.headers.get('Content-Length', 0)) or len(response.content)
                            
                            # Accept as image if content type indicates or if from known sites and has reasonable size
                            is_likely_image = (
                                content_type.startswith('image/') or
                                (('jclgift' in normalized_url or 'koreagift' in normalized_url or 'pstatic' in normalized_url) and content_length > 1000)
                            )
                            
                            if is_likely_image:
                                # Save image to file
                                with open(download_path, 'wb') as f:
                                    f.write(response.content)
                                
                                # Verify it's a valid image file
                                try:
                                    from PIL import Image
                                    img = Image.open(download_path)
                                    img.verify()  # Basic verification
                                    # Reset file pointer after verify
                                    img = Image.open(download_path)
                                    # Save with consistent format
                                    img_path = download_path
                                    logger.info(f"Successfully downloaded missing image: {download_path}")
                                    
                                    # Update the dictionary with the new local path
                                    if isinstance(cell.value, dict):
                                        cell.value['local_path'] = img_path
                                        logger.debug(f"Updated image dictionary with downloaded path: {img_path}")
                                except Exception as img_err:
                                    logger.error(f"Downloaded file is not a valid image: {img_err}")
                                    # Try to delete invalid image
                                    try:
                                        if os.path.exists(download_path):
                                            os.remove(download_path)
                                    except:
                                        pass
                            else:
                                logger.warning(f"Downloaded content is not an image. Content-Type: {content_type}, Size: {content_length} bytes")
                        else:
                            status = response.status_code if response else "No response"
                            logger.warning(f"Failed to download image after multiple attempts. Final status: {status}")
                    except Exception as download_err:
                        logger.error(f"Failed to download image as last resort: {download_err}")
                
                # If we still don't have an image path, log and skip
                if not img_path:
                    logger.warning(f"Image file not found: {original_data}")
                    
                    # Final attempt to fix URL before using it as hyperlink
                    if img_url:
                        # Make sure the URL is properly formatted for hyperlink
                        if '\\' in img_url:
                            img_url = img_url.replace('\\', '/')
                        if img_url.startswith('https:') and not img_url.startswith('https://'):
                            img_url = 'https://' + img_url[6:]
                        elif ':' in img_url and not img_url.startswith(('http:', 'https://')):
                            # Handle case where URL is like 'https:\www...'
                            scheme, path = img_url.split(':', 1)
                            path = path.replace('\\', '').lstrip('/')
                            img_url = f"{scheme}://{path}"
                        
                        cell.value = img_url
                        cell.hyperlink = img_url
                        cell.font = LINK_FONT  # Apply hyperlink style
                    else:
                        cell.value = ERROR_MESSAGES['file_not_found']
                    failed_count += 1
                    continue
                
                # Normalize path
                img_path = os.path.normpath(img_path)
                
                # Skip processing if this image was already processed
                # IMPORTANT: Cache key should be the ORIGINAL path if available, or the primary path used
                cache_key = img_info.get('original_path', img_path) if isinstance(cell.value, dict) else img_path
                cache_key = os.path.normpath(cache_key)

                if cache_key in processed_images:
                    # Use the cached Excel image object
                    excel_img = processed_images[cache_key]['image']

                    # Recalculate anchor based on current cell
                    excel_img.anchor = cell.coordinate

                    # Add the cached image object to the worksheet
                    worksheet.add_image(excel_img)
                    logger.debug(f"Reused cached image for key: {cache_key} -> {cell.coordinate}")
                    success_count += 1
                    continue

                # --- Final Path Existence Check and Fallback --- 
                primary_path_to_use = None
                # 1. Check if img_path (potentially _nobg version) exists
                if os.path.exists(img_path) and os.path.getsize(img_path) > 0:
                    primary_path_to_use = img_path
                    logger.debug(f"Using primary path (might be _nobg): {primary_path_to_use}")
                # 2. If not, check original_path from dict if available
                elif isinstance(cell.value, dict) and 'original_path' in cell.value:
                    original_path = os.path.normpath(cell.value['original_path'])
                    if os.path.exists(original_path) and os.path.getsize(original_path) > 0:
                        primary_path_to_use = original_path
                        logging.warning(f"Primary path '{img_path}' not found, falling back to original_path: {primary_path_to_use}")
                    else:
                        logging.warning(f"Neither primary path '{img_path}' nor original_path '{original_path}' exist or are valid.")
                else:
                     logging.warning(f"Primary path '{img_path}' not found and no original_path available.")

                # If no valid path found after checks, handle fallback
                if not primary_path_to_use:
                    logger.warning(f"Final image path check failed - no valid file found for data: {original_data}")
                    # Ensure img_url is set to something valid or None
                    if img_url is None and isinstance(cell.value, dict) and 'url' in cell.value:
                        img_url = cell.value['url']
                        # Fix URL format if needed (moved URL fixing logic here)
                        if img_url and '\\' in img_url:
                            img_url = img_url.replace('\\', '/')
                        if img_url and img_url.startswith('https:') and not img_url.startswith('https://'):
                            img_url = 'https://' + img_url[6:]
                        elif img_url and ':' in img_url and not img_url.startswith(('http:', 'https:')):
                            scheme, path = img_url.split(':', 1)
                            path = path.replace('\\', '').lstrip('/')
                            img_url = f"{scheme}://{path}"

                    # If we have a URL, set it as a hyperlink as fallback
                    if img_url:
                        cell.value = img_url
                        cell.hyperlink = img_url
                        cell.font = LINK_FONT  # Apply hyperlink style
                    else:
                        cell.value = ERROR_MESSAGES['file_not_found']
                    failed_count += 1
                    continue
                # --- End Fallback Check ---

                # Use the verified existing path for processing
                img_path_to_process = primary_path_to_use

                try:
                    # Log the file type
                    file_ext = os.path.splitext(img_path_to_process)[1].lower()
                    logger.debug(f"Processing image: {img_path_to_process} (type: {file_ext}) -> Cell: {cell.coordinate}")

                    # Generate a hash-based filename for the processed image to avoid name conflicts
                    # Use the path being processed for the hash
                    img_hash = hashlib.md5(img_path_to_process.encode()).hexdigest()[:10]
                    temp_path = os.path.join(temp_dir, f"temp_{img_hash}.jpg")

                    # Check if we've already processed this image in a previous run
                    if os.path.exists(temp_path) and os.path.getmtime(temp_path) > os.path.getmtime(img_path_to_process):
                        # Use cached processed image if it exists and is newer than source
                        processed_img_final_path = temp_path
                        logger.debug(f"Using cached processed image: {processed_img_final_path}")
                    else:
                        # Open and validate image using the verified path
                        with Image.open(img_path_to_process) as img:
                            # Get dimensions for logging
                            orig_width, orig_height = img.size
                            logger.debug(f"Original image dimensions: {orig_width}x{orig_height}")
                            
                            # Convert to RGB if needed
                            if img.mode in ('RGBA', 'LA'):
                                img = img.convert('RGB')
                            
                            # Resize if too large
                            if img.size[0] > IMAGE_MAX_SIZE[0] or img.size[1] > IMAGE_MAX_SIZE[1]:
                                img.thumbnail(IMAGE_MAX_SIZE, Image.LANCZOS)
                                logger.debug(f"Resized image to: {img.size[0]}x{img.size[1]}")
                                
                            # Save as optimized JPG 
                            img.save(temp_path, 'JPEG', quality=IMAGE_QUALITY, optimize=True)
                            processed_img_final_path = temp_path
                            logger.debug(f"Saved optimized image: {processed_img_final_path}")
                    
                    # Add to worksheet with proper positioning
                    excel_img = openpyxl.drawing.image.Image(processed_img_final_path)

                    # Adjust size proportionally based on standard size
                    # Re-open the *processed* image to get its final dimensions
                    with Image.open(processed_img_final_path) as final_img:
                        width, height = final_img.size
                        ratio = min(IMAGE_STANDARD_SIZE[0] / width, IMAGE_STANDARD_SIZE[1] / height)
                        excel_img.width = int(width * ratio)
                        excel_img.height = int(height * ratio)

                    # Calculate cell position for proper image placement
                    excel_img.anchor = cell.coordinate

                    # Store processed image info in cache using the original key
                    processed_images[cache_key] = {
                        'image': excel_img, # Store the Excel Image object itself
                        'temp_path': processed_img_final_path if processed_img_final_path != img_path_to_process else None
                    }

                    # Add image to worksheet
                    worksheet.add_image(excel_img)
                    logger.debug(f"Successfully added image {processed_img_final_path} to cell {cell.coordinate}")
                    success_count += 1
                            
                except Exception as img_e:
                    logger.error(f"Error processing image {img_path_to_process}: {img_e}", exc_info=True)
                    # Ensure img_url is set to something valid or None
                    if img_url is None and isinstance(cell.value, dict) and 'url' in cell.value:
                        img_url = cell.value['url']
                        # Fix URL format if needed
                        if img_url and '\\' in img_url:
                            img_url = img_url.replace('\\', '/')
                        if img_url and img_url.startswith('https:') and not img_url.startswith('https://'):
                            img_url = 'https://' + img_url[6:]
                        elif img_url and ':' in img_url and not img_url.startswith(('http:', 'https://')):
                            # Handle case where URL is like 'https:\www...'
                            scheme, path = img_url.split(':', 1)
                            path = path.replace('\\', '').lstrip('/')
                            img_url = f"{scheme}://{path}"
                    
                    # Try to save a basic error message and URL as fallback
                    if img_url:
                        cell.value = img_url
                        try:
                            cell.hyperlink = img_url
                            cell.font = LINK_FONT
                        except:
                            pass
                    else:
                        cell.value = ERROR_MESSAGES['processing_error']
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing image in cell {cell.coordinate}: {e}", exc_info=True)
                # Initialize img_url before using it
                img_url = None
                # Try to save URL as fallback
                if isinstance(cell.value, dict) and 'url' in cell.value:
                    img_url = cell.value['url']
                    if img_url:
                        # Fix URL format if needed
                        if '\\' in img_url:
                            img_url = img_url.replace('\\', '/')
                        if img_url.startswith('https:') and not img_url.startswith('https://'):
                            img_url = 'https://' + img_url[6:]
                        elif ':' in img_url and not img_url.startswith(('http:', 'https://')):
                            # Handle case where URL is like 'https:\www...'
                            scheme, path = img_url.split(':', 1)
                            path = path.replace('\\', '').lstrip('/')
                            img_url = f"{scheme}://{path}"
                        
                        cell.value = img_url
                        try:
                            cell.hyperlink = img_url
                            cell.font = LINK_FONT
                        except:
                            pass
                else:
                    cell.value = ERROR_MESSAGES['processing_error']
                failed_count += 1
    
    logger.info(f"Finished processing images: {success_count} succeeded, {failed_count} failed, {len(processed_images)} unique images in {len(image_cols)} columns")

def _apply_conditional_formatting(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Applies conditional formatting (e.g., yellow fill for negative price difference rows)."""
    logger.debug("Applying conditional formatting.")
    
    # Find price difference columns (non-percentage)
    price_diff_cols = [
        col for col in df.columns
        if '가격차이' in str(col) and '%' not in str(col)
    ]

    if not price_diff_cols:
        logger.debug("No price difference columns found for conditional formatting.")
        return

    # Define yellow fill for negative values
    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

    # Process each row
    for row_idx in range(2, worksheet.max_row + 1):  # Start from 2 to skip header
        for price_diff_col in price_diff_cols:
            col_idx = df.columns.get_loc(price_diff_col) + 1  # 1-based index for openpyxl
            cell = worksheet.cell(row=row_idx, column=col_idx)
            
            # Get cell value and check if it's negative
            try:
                if cell.value not in ['-', '', None]:  # Skip empty or placeholder values
                    # Remove commas and convert to float
                    value_str = str(cell.value).replace(',', '')
                    value = float(value_str)
                    
                    # If value is negative, highlight entire row
                    if value < 0:
                        for col in range(1, worksheet.max_column + 1):
                            worksheet.cell(row=row_idx, column=col).fill = yellow_fill
                        break  # Break inner loop once row is highlighted
            except ValueError:
                # Skip if value cannot be converted to float (e.g., error messages)
                continue
            except Exception as e:
                logger.error(f"Error processing cell {cell.coordinate}: {e}")
                continue

    logger.debug("Finished applying conditional formatting for negative price differences.")

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
    """Adds hyperlinks to specified link columns."""
    logger.debug(f"Adding hyperlinks. Link columns defined: {list(LINK_COLUMNS_FOR_HYPERLINK.keys())}")
    # Find column indices for defined link columns
    link_col_indices = {col: idx for idx, col in enumerate(df.columns, 1) if col in LINK_COLUMNS_FOR_HYPERLINK}

    if not link_col_indices:
        logger.debug("No columns found for adding hyperlinks.")
        return

    # Basic URL pattern check (simplified)
    url_pattern = re.compile(r'^https?://\S+$', re.IGNORECASE)

    link_added_count = 0
    invalid_link_count = 0
    for col_name, col_idx in link_col_indices.items():
        # logger.debug(f"Processing hyperlinks for column: {col_name} (Index: {col_idx})") # Reduce verbosity
        for row_idx in range(2, worksheet.max_row + 1):
            cell = worksheet.cell(row=row_idx, column=col_idx)
            link_text = str(cell.value) if cell.value else ''

            # Skip empty cells, placeholders, or error messages
            if not link_text or link_text.lower() in ['-', 'nan', 'none', ''] or link_text in ERROR_MESSAGE_VALUES:
                continue

            try:
                # Attempt to match URL pattern
                if url_pattern.match(link_text):
                    # Check if it already has a hyperlink (rare, but possible)
                    if not cell.hyperlink:
                         cell.hyperlink = link_text
                         cell.font = LINK_FONT # Apply link style
                         link_added_count += 1
                    # else: # Already has hyperlink, ensure style is correct
                    #      cell.font = LINK_FONT
                else:
                    # If it's not a valid-looking URL, treat as text
                    invalid_link_count += 1
                    # logger.debug(f"Non-URL or invalid format in link column {cell.coordinate}: '{link_text[:50]}...'")
                    pass # Keep default font/style for non-links
            except Exception as e:
                logger.warning(f"Error processing link cell {cell.coordinate} ('{link_text[:50]}...'): {e}")

    logger.info(f"Finished adding hyperlinks. Added {link_added_count} links. Found {invalid_link_count} non-URL values in link columns.")

def _add_header_footer(worksheet: openpyxl.worksheet.worksheet.Worksheet):
    """Adds standard header and footer."""
    try:
        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        worksheet.header_footer.center_header.text = "가격 비교 결과"
        worksheet.header_footer.right_header.text = f"생성일: {current_date}"
        worksheet.header_footer.left_footer.text = "해오름 RPA 가격 비교"
        worksheet.header_footer.right_footer.text = "페이지 &P / &N"
        logger.debug("Added header and footer to worksheet")
    except Exception as e:
        logger.warning(f"Could not set header/footer: {e}")

def _apply_table_format(worksheet: openpyxl.worksheet.worksheet.Worksheet):
    """Applies Excel table formatting to the data range."""
    # 테이블 서식 적용 함수 비우기 - 필터 적용 방지
    logger.debug("Table formatting skipped as requested.")
    return

def _prepare_data_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares the DataFrame for Excel export: ensures columns, formats data.
    Handles numeric conversion carefully to preserve error strings.
    """
    if df is None:
        logger.error("_prepare_data_for_excel received None DataFrame.")
        return pd.DataFrame(columns=FINAL_COLUMN_ORDER)

    df_prepared = df.copy()
    logger.info(f"Preparing data for Excel export. Initial rows: {len(df_prepared)}, Initial columns: {df_prepared.columns.tolist()}")

    # 1. Ensure all FINAL columns exist, add if missing with '-'
    missing_cols = [col for col in FINAL_COLUMN_ORDER if col not in df_prepared.columns]
    if missing_cols:
        logger.warning(f"Missing columns in input data: {missing_cols}")
        for col in missing_cols:
            df_prepared[col] = '-'

    # 2. Select and Reorder columns STRICTLY according to FINAL_COLUMN_ORDER
    try:
        df_prepared = df_prepared[FINAL_COLUMN_ORDER]
        logger.debug(f"Columns reordered and selected. Final columns: {df_prepared.columns.tolist()}")
    except KeyError as ke:
        logger.error(f"KeyError during column selection: {ke}")
        # Fall back to ensuring all required columns exist
        available_final_cols = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
        df_prepared = df[available_final_cols]
        # Add missing ones with '-'
        for col in FINAL_COLUMN_ORDER:
            if col not in df_prepared.columns:
                df_prepared[col] = '-'
        df_prepared = df_prepared[FINAL_COLUMN_ORDER]  # Try reordering again

    # 3. Fill NaN values with '-' for consistency
    df_prepared.fillna('-', inplace=True)
    
    # 4. Convert all columns to string type to avoid dtype issues
    # This ensures all values are treated as strings for formatting
    for col in df_prepared.columns:
        df_prepared[col] = df_prepared[col].astype(str)
    
    # 5. Format data by column type
    for col_name in df_prepared.columns:
        is_price_col = col_name in PRICE_COLUMNS
        is_qty_col = col_name in QUANTITY_COLUMNS
        is_pct_col = col_name in PERCENTAGE_COLUMNS

        if is_price_col or is_qty_col or is_pct_col:
            # Handle each row individually
            for idx in df_prepared.index:
                value = df_prepared.at[idx, col_name]
                
                # Skip processing for error messages and placeholders
                if pd.isna(value) or value.strip() == '-' or any(err_msg in str(value) for err_msg in ERROR_MESSAGE_VALUES):
                    continue
                
                # Ensure value is string before checks
                value_str = str(value)

                # Format numeric values
                try:
                    # Skip empty values and placeholders
                    if value_str.lower().strip() in ['.', '-', '', 'none', 'nan']:
                        df_prepared.at[idx, col_name] = '-'
                        continue

                    # Clean numeric string and convert
                    clean_value = value_str.replace(',', '').replace('%', '')
                    numeric_value = float(clean_value)

                    # Format based on column type
                    if is_price_col:
                        formatted_value = '-' if numeric_value == 0 else f"{numeric_value:,.0f}"
                    elif is_qty_col:
                        formatted_value = '-' if numeric_value == 0 else f"{int(numeric_value):,}"
                    elif is_pct_col:
                        # Ensure integer formatting for percentage
                        formatted_value = f"{int(round(numeric_value))}%"

                    # Update the cell with formatted value (always as string)
                    # This avoids the dtype incompatibility warning
                    df_prepared.at[idx, col_name] = formatted_value
                    
                except (ValueError, TypeError):
                    # Keep the value as is if conversion fails
                    continue
                except Exception as e:
                    logger.warning(f"Error formatting value '{value}' in column {col_name}: {e}")
    
    # Create a copy to ensure all data is properly converted to strings
    # This helps avoid dtype issues during Excel writing
    df_prepared = df_prepared.astype(str)
    
    logger.info(f"Data preparation finished. Final rows: {len(df_prepared)}")
    return df_prepared

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

@safe_excel_operation
def create_final_output_excel(df: pd.DataFrame, output_path: str) -> bool:
    """
    Creates the final formatted Excel file.
    Orchestrates data preparation, styling, image handling, and saving.
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
        df_prepared = _prepare_data_for_excel(df.copy())

        if df_prepared.empty and not df.empty:
             logger.error("Data preparation resulted in an empty DataFrame. Cannot save Excel.")
             return False
        elif df_prepared.empty and df.empty:
             logger.warning("Input DataFrame was empty, saving an Excel file with only headers.")
             df_prepared = pd.DataFrame(columns=FINAL_COLUMN_ORDER)

        # 2. Save prepared data to Excel using openpyxl engine
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_prepared.to_excel(writer, index=False, sheet_name='Results', na_rep='-')
            worksheet = writer.sheets['Results']
            logger.debug(f"DataFrame written to sheet 'Results'. Max Row: {worksheet.max_row}, Max Col: {worksheet.max_column}")

            # --- Apply Formatting AFTER data is written ---
            # 3. Apply Column Widths and Cell Styles
            _apply_column_widths(worksheet, df_prepared)
            _apply_cell_styles_and_alignment(worksheet, df_prepared)

            # 4. Apply Conditional Formatting
            _apply_conditional_formatting(worksheet, df_prepared)

            # 5. Handle Images (Embedding)
            _process_image_columns(worksheet, df_prepared)
            
            # 6. Adjust dimensions for image cells
            _adjust_image_cell_dimensions(worksheet, df_prepared)

            # 7. Add Hyperlinks
            _add_hyperlinks_to_worksheet(worksheet, df_prepared)

            # 8. Page Setup and Header/Footer
            _setup_page_layout(worksheet)
            _add_header_footer(worksheet)

            # 9. Apply Table Format (Apply last after other formatting)
            _apply_table_format(worksheet)

        logger.info(f"Successfully created and formatted Excel file: {output_path}")
        return True

    except PermissionError as pe:
         logger.error(f"Permission denied when trying to save Excel file: {output_path}. Check if the file is open. Error: {pe}")
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
        
    # Adjust column widths for image columns consistently
    for col_name, col_idx in image_cols.items():
        col_letter = get_column_letter(col_idx)
        worksheet.column_dimensions[col_letter].width = IMAGE_CELL_WIDTH
    
    # Create a set of rows that need height adjustment
    rows_with_images = set()
    
    # Find rows that have actual images (not error messages or empty cells)
    for row_idx in range(2, worksheet.max_row + 1):
        for col_name, col_idx in image_cols.items():
            cell = worksheet.cell(row=row_idx, column=col_idx)
            cell_value = str(cell.value) if cell.value else ""
            
            # If the cell has content that looks like a path and not an error message
            if (cell_value and cell_value != '-' and 
                not any(err_msg in cell_value for err_msg in ERROR_MESSAGE_VALUES) and
                ('\\' in cell_value or '/' in cell_value or '.jpg' in cell_value.lower() or 
                 '.png' in cell_value.lower() or '.jpeg' in cell_value.lower())):
                rows_with_images.add(row_idx)
                break
    
    # Apply height to rows with images
    for row_idx in rows_with_images:
        worksheet.row_dimensions[row_idx].height = IMAGE_CELL_HEIGHT
        
        # Also center-align all cells in this row to ensure uniform appearance
        for col_idx in range(1, worksheet.max_column + 1):
            cell = worksheet.cell(row=row_idx, column=col_idx)
            # Only adjust vertical alignment to ensure content displays correctly with images
            current_alignment = cell.alignment
            cell.alignment = Alignment(
                horizontal=current_alignment.horizontal,
                vertical="center",
                wrap_text=current_alignment.wrap_text
            )
    
    logger.debug(f"Adjusted dimensions for {len(rows_with_images)} rows with images")

