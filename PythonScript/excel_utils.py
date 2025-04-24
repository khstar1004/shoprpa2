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
    '기본수량(2)', '판매단가(V포함)(2)', '가격차이(2)', '고려기프트 상품링크',
    '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크',
    '본사 이미지', '고려기프트 이미지', '네이버 이미지'
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
    'file_not_found': '이미지 파일을 찾을 수 없음',
    'invalid_image': '유효하지 않은 이미지 형식',
    'processing_error': '이미지 처리 중 오류가 발생',
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
IMAGE_STANDARD_SIZE = (120, 120)  # Standard display size in Excel
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

            if (col_name_str in PRICE_COLUMNS or col_name_str in QUANTITY_COLUMNS or col_name_str in PERCENTAGE_COLUMNS) and is_numeric_value:
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
        return
    
    # Create a temporary directory for image processing if needed
    temp_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'temp_images')
    os.makedirs(temp_dir, exist_ok=True)
    
    # Track processed images to avoid duplicate processing
    processed_images = {}
    
    # Count successful/failed images for reporting
    success_count = 0
    failed_count = 0
        
    for row_idx in range(2, worksheet.max_row + 1):
        for col_name, col_idx in image_cols.items():
            cell = worksheet.cell(row=row_idx, column=col_idx)
            if not cell.value or cell.value == '-':
                continue

            try:
                # Handle both string paths and dictionary image info
                if isinstance(cell.value, dict):
                    img_info = cell.value
                    img_path = None
                    img_url = None
                    
                    # Keep track of original data for logging
                    original_data = str(img_info)
                    
                    # Try to get URL from the dictionary
                    if 'url' in img_info:
                        img_url = img_info['url']
                        if img_url and '\\' in img_url:
                            # Fix backslashes in URLs
                            img_url = img_url.replace('\\', '/')
                            if img_url.startswith('https:') and not img_url.startswith('https://'):
                                img_url = 'https://' + img_url[6:]
                    
                    # First try the 'original_path' key if it exists (new field we added)
                    if 'original_path' in img_info and img_info['original_path']:
                        orig_path = img_info['original_path']
                        # Normalize path - replace double backslashes and ensure proper format
                        if '\\\\' in orig_path:
                            orig_path = orig_path.replace('\\\\', '\\')
                        # Check if the path exists
                        if os.path.exists(orig_path):
                            img_path = orig_path
                            logger.debug(f"Using original_path: {img_path}")
                    
                    # If original_path doesn't exist, try local_path
                    if not img_path and 'local_path' in img_info and img_info['local_path']:
                        local_path = img_info['local_path']
                        # Normalize path
                        if '\\\\' in local_path:
                            local_path = local_path.replace('\\\\', '\\')
                            
                        # First check if the exact path exists
                        if os.path.exists(local_path):
                            img_path = local_path
                            logger.debug(f"Using exact local_path: {img_path}")
                        else:
                            # Try without _nobg suffix if it's there
                            if '_nobg.png' in local_path:
                                orig_path = local_path.replace('_nobg.png', '.jpg')
                                if os.path.exists(orig_path):
                                    img_path = orig_path
                                    logger.debug(f"Found non-background removed version: {img_path}")
                                else:
                                    # Try other extensions
                                    for ext in ['.png', '.jpeg', '.gif']:
                                        test_path = local_path.replace('_nobg.png', ext)
                                        if os.path.exists(test_path):
                                            img_path = test_path
                                            logger.debug(f"Found alternate extension: {img_path}")
                                            break
                    
                    # If still no image found, try to find any file with the hash in the appropriate directory
                    if not img_path and img_url:
                        # Extract hash and try to find matching files
                        source = img_info.get('source', '')
                        url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
                        
                        # Determine the appropriate directory
                        if source == 'haereum':
                            target_dir = 'C:\\RPA\\Image\\Main\\Haereum'
                        elif source in ['kogift', 'kogift_pre']:
                            target_dir = 'C:\\RPA\\Image\\Main\\kogift_pre'
                        elif source == 'naver':
                            target_dir = 'C:\\RPA\\Image\\Main\\Naver'
                        else:
                            # Try all directories in case source is not specified
                            possible_dirs = [
                                'C:\\RPA\\Image\\Main\\Haereum',
                                'C:\\RPA\\Image\\Main\\kogift_pre',
                                'C:\\RPA\\Image\\Main\\Naver'
                            ]
                            
                            # Search all directories for files containing the hash
                            for dir_path in possible_dirs:
                                if os.path.exists(dir_path):
                                    for filename in os.listdir(dir_path):
                                        # Look for hash in filename and prefer non-background removed images
                                        if url_hash in filename and os.path.isfile(os.path.join(dir_path, filename)):
                                            candidate_path = os.path.join(dir_path, filename)
                                            if not '_nobg' in filename:
                                                img_path = candidate_path
                                                logger.debug(f"Found by hash in {dir_path}: {img_path}")
                                                break
                                            elif not img_path:  # Use nobg version if that's all we have
                                                img_path = candidate_path
                                    
                                    if img_path:
                                        break
                            
                            # Skip to next iteration if no image path is found
                            if not img_path:
                                continue
                        
                        # If we have a specific target directory, look there for matching files
                        if not img_path and os.path.exists(target_dir):
                            for filename in os.listdir(target_dir):
                                if url_hash in filename and os.path.isfile(os.path.join(target_dir, filename)):
                                    candidate_path = os.path.join(target_dir, filename)
                                    if not '_nobg' in filename:
                                        img_path = candidate_path
                                        logger.debug(f"Found by hash in {target_dir}: {img_path}")
                                        break
                                    elif not img_path:  # Use nobg version if that's all we have
                                        img_path = candidate_path
                    
                    # If we still don't have an image path, try parsing from local_path
                    if not img_path and 'local_path' in img_info:
                        try:
                            # Extract file name components
                            file_dir = os.path.dirname(img_info['local_path'])
                            file_name = os.path.basename(img_info['local_path'])
                            
                            # If directory exists, try to find files with similar names
                            if os.path.exists(file_dir):
                                for f in os.listdir(file_dir):
                                    # Look for files that share significant parts of the name
                                    name_parts = file_name.split('_')
                                    if len(name_parts) > 2:
                                        # Look for files with the same prefix and same product name part
                                        if f.startswith(name_parts[0]) and name_parts[1] in f:
                                            img_path = os.path.join(file_dir, f)
                                            # Prefer non-background removed images
                                            if not '_nobg' in f:
                                                logger.debug(f"Found by name matching: {img_path}")
                                                break
                        except Exception as path_err:
                            logger.warning(f"Error trying to find similar file: {path_err}")
                    
                    # If still no image found but we have a URL, save it for display
                    if not img_path:
                        logger.warning(f"Image file not found for dictionary input: {original_data}")
                        if img_url:
                            cell.value = img_url
                        else:
                            cell.value = ERROR_MESSAGES['file_not_found']
                        failed_count += 1
                        continue
                else:
                    # Handle plain string path
                    img_path = str(cell.value)
                
                # Final check for common path issues
                if img_path and '\\' in img_path:
                    # Ensure backslashes are consistent (no double backslashes)
                    if '\\\\' in img_path:
                        img_path = img_path.replace('\\\\', '\\')
                
                # Handle _nobg suffix in paths - prefer original images
                if img_path and '_nobg.png' in img_path:
                    orig_path = img_path.replace('_nobg.png', '.jpg')
                    if os.path.exists(orig_path):
                        img_path = orig_path
                        logger.debug(f"Using non-background removed version: {img_path}")
                
                # Normalize path
                img_path = os.path.normpath(img_path) if img_path else None
                
                # Skip processing if this image was already processed
                if img_path in processed_images:
                    excel_img = processed_images[img_path]['image']
                    # Calculate cell position for proper image placement
                    cell_anchor = f"{cell.coordinate}"
                    excel_img.anchor = cell_anchor
                    # Add image to worksheet
                    worksheet.add_image(excel_img)
                    logger.debug(f"Reused cached image: {img_path}")
                    success_count += 1
                    continue
                
                # Verify image file exists and is valid
                if not img_path or not os.path.exists(img_path):
                    # Log the issue
                    logger.warning(f"Image file not found: {img_path or cell.value}")
                    if isinstance(cell.value, dict) and 'url' in cell.value:
                        cell.value = cell.value.get('url')
                    else:
                        cell.value = ERROR_MESSAGES['file_not_found']
                    failed_count += 1
                    continue
                    
                try:
                    # Log the file type
                    file_ext = os.path.splitext(img_path)[1].lower()
                    logger.debug(f"Processing image: {img_path} (type: {file_ext})")
                    
                    # Generate a hash-based filename for the processed image to avoid name conflicts
                    img_hash = hashlib.md5(img_path.encode()).hexdigest()[:10]
                    temp_path = os.path.join(temp_dir, f"temp_{img_hash}.jpg")
                    
                    # Check if we've already processed this image in a previous run
                    if os.path.exists(temp_path) and os.path.getmtime(temp_path) > os.path.getmtime(img_path):
                        # Use cached processed image if it exists and is newer than source
                        img_path = temp_path
                        logger.debug(f"Using cached processed image: {temp_path}")
                    else:
                        # Open and validate image
                        with Image.open(img_path) as img:
                            # Get dimensions for logging
                            orig_width, orig_height = img.size
                            logger.debug(f"Original image dimensions: {orig_width}x{orig_height}")
                            
                            # Convert to RGB if needed
                            if img.mode in ('RGBA', 'LA'):
                                img = img.convert('RGB')
                            
                            # Resize if too large
                            if img.size[0] > IMAGE_MAX_SIZE[0] or img.size[1] > IMAGE_MAX_SIZE[1]:
                                img.thumbnail(IMAGE_MAX_SIZE, Image.Resampling.LANCZOS)
                                logger.debug(f"Resized image to: {img.size[0]}x{img.size[1]}")
                                
                            # Save as optimized JPG 
                            img.save(temp_path, 'JPEG', quality=IMAGE_QUALITY, optimize=True)
                            img_path = temp_path
                            logger.debug(f"Saved optimized image: {temp_path}")
                    
                    # Add to worksheet with proper positioning
                    excel_img = openpyxl.drawing.image.Image(img_path)
                    
                    # Adjust size proportionally based on standard size
                    with Image.open(img_path) as img:
                        width, height = img.size
                        ratio = min(IMAGE_STANDARD_SIZE[0] / width, IMAGE_STANDARD_SIZE[1] / height)
                        excel_img.width = int(width * ratio)
                        excel_img.height = int(height * ratio)
                    
                    # Calculate cell position for proper image placement
                    cell_anchor = f"{cell.coordinate}"
                    excel_img.anchor = cell_anchor
                    
                    # Store processed image info
                    processed_images[img_path] = {
                        'image': excel_img,
                        'temp_path': temp_path if temp_path != img_path else None
                    }
                    
                    # Add image to worksheet
                    worksheet.add_image(excel_img)
                    logger.debug(f"Successfully added image {img_path} to cell {cell.coordinate}")
                    success_count += 1
                            
                except Exception as img_e:
                    logger.error(f"Error processing image {img_path}: {img_e}")
                    cell.value = ERROR_MESSAGES['processing_error']
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing image in cell {cell.coordinate}: {e}")
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
                if value.strip() == '-' or any(err_msg in value for err_msg in ERROR_MESSAGE_VALUES):
                    continue
                
                # Format numeric values
                try:
                    # Skip empty values and placeholders
                    if value.lower().strip() in ['-', '', 'none', 'nan']:
                        df_prepared.at[idx, col_name] = '-'
                        continue
                    
                    # Clean numeric string and convert
                    clean_value = value.replace(',', '').replace('%', '')
                    numeric_value = float(clean_value)
                    
                    # Format based on column type
                    if is_price_col:
                        formatted_value = '-' if numeric_value == 0 else f"{numeric_value:,.0f}"
                    elif is_qty_col:
                        formatted_value = '-' if numeric_value == 0 else f"{int(numeric_value):,}"
                    elif is_pct_col:
                        formatted_value = f"{numeric_value:.1f}%"
                    
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

