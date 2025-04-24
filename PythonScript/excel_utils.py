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
from pathlib import Path

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
IMAGE_CELL_HEIGHT = 120  # Row height for image cells (increased from 90)
IMAGE_CELL_WIDTH = 22   # Column width for image cells (increased from 15)

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
            try:
                # Get the cell value (image path)
                cell = worksheet.cell(row=row_idx, column=col_idx)
                original_value = cell.value
                
                logger.info(f"Row {row_idx}, Column {col_name}:")
                logger.info(f"  Original value: '{original_value}'")
                logger.info(f"  Value type: {type(original_value)}")
                
                # Skip empty cells or error messages
                if not original_value or original_value == '-' or (isinstance(original_value, str) and 
                                                               any(err in original_value for err in ERROR_MESSAGE_VALUES)):
                    logger.info(f"  Skipping value: Empty or error message")
                    continue
                
                # Handle dictionary format - This is the preferred format for all image sources
                img_dict = None
                if isinstance(original_value, dict):
                    img_dict = original_value
                    logger.info(f"  Found image dictionary: {img_dict}")
                elif isinstance(original_value, str) and original_value.startswith('{') and original_value.endswith('}'):
                    # Try to parse dictionary from string
                    try:
                        import ast
                        img_dict = ast.literal_eval(original_value)
                        if not isinstance(img_dict, dict):
                            img_dict = None
                        logger.info(f"  Parsed dictionary from string: {img_dict}")
                    except (SyntaxError, ValueError) as e:
                        logger.warning(f"  Failed to parse dictionary-like string: {e}")
                        img_dict = None
                
                # Process image based on source type
                if img_dict and isinstance(img_dict, dict):
                    # Determine image path based on dictionary format
                    local_path = img_dict.get('local_path')
                    url = img_dict.get('url')
                    source = img_dict.get('source', '').lower()
                    
                    # Try local_path first if available
                    if local_path and os.path.isfile(local_path):
                        image_path = local_path
                        logger.info(f"  Using local path from dictionary: {local_path}")
                    # If no valid local path but URL exists, try to download
                    elif url:
                        logger.info(f"  No valid local path, attempting to download from URL: {url}")
                        
                        # Create a temporary directory for downloaded images if not exists
                        import tempfile
                        temp_dir = os.path.join(tempfile.gettempdir(), "rpa_image_cache")
                        os.makedirs(temp_dir, exist_ok=True)
                        
                        # Generate unique filename
                        url_hash = hashlib.md5(url.encode('utf-8', errors='ignore')).hexdigest()[:8]
                        file_ext = os.path.splitext(urlparse(url).path)[1].lower() or '.jpg'
                        if file_ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                            file_ext = '.jpg'
                            
                        # Create source-specific filename
                        if source in ['haoreum', 'kogift', 'naver']:
                            filename = f"{source}_{url_hash}{file_ext}"
                        else:
                            filename = f"image_{url_hash}{file_ext}"
                            
                        temp_file_path = os.path.join(temp_dir, filename)
                        
                        # Download the image if needed
                        if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                            try:
                                import requests
                                response = requests.get(url, timeout=10)
                                response.raise_for_status()
                                
                                with open(temp_file_path, "wb") as f:
                                    f.write(response.content)
                                logger.info(f"  Downloaded image to: {temp_file_path}")
                                image_path = temp_file_path
                            except Exception as download_err:
                                logger.error(f"  Failed to download image: {download_err}")
                                cell.value = "이미지 다운로드 실패"
                                continue
                        else:
                            logger.info(f"  Using cached image at: {temp_file_path}")
                            image_path = temp_file_path
                    else:
                        logger.warning(f"  No valid image path or URL in dictionary")
                        cell.value = "이미지 경로 없음"
                        continue
                # Handle string path directly
                elif isinstance(original_value, str):
                    # Check if it's a valid file path
                    if os.path.isfile(original_value):
                        image_path = original_value
                        logger.info(f"  Using direct file path: {image_path}")
                    # Check if it's a URL
                    elif original_value.startswith(('http://', 'https://')):
                        url = original_value
                        logger.info(f"  Found URL string, attempting to download: {url}")
                        
                        # Create a temporary directory for downloaded images if not exists
                        import tempfile
                        temp_dir = os.path.join(tempfile.gettempdir(), "rpa_image_cache")
                        os.makedirs(temp_dir, exist_ok=True)
                        
                        # Generate unique filename
                        url_hash = hashlib.md5(url.encode('utf-8', errors='ignore')).hexdigest()[:8]
                        file_ext = os.path.splitext(urlparse(url).path)[1].lower() or '.jpg'
                        if file_ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                            file_ext = '.jpg'
                            
                        # Determine source from column name
                        if '본사' in col_name:
                            source = 'haoreum'
                        elif '고려' in col_name:
                            source = 'kogift'
                        elif '네이버' in col_name:
                            source = 'naver'
                        else:
                            source = 'other'
                            
                        filename = f"{source}_{url_hash}{file_ext}"
                        temp_file_path = os.path.join(temp_dir, filename)
                        
                        # Download the image if needed
                        if not os.path.exists(temp_file_path) or os.path.getsize(temp_file_path) == 0:
                            try:
                                import requests
                                response = requests.get(url, timeout=10)
                                response.raise_for_status()
                                
                                with open(temp_file_path, "wb") as f:
                                    f.write(response.content)
                                logger.info(f"  Downloaded image to: {temp_file_path}")
                                image_path = temp_file_path
                            except Exception as download_err:
                                logger.error(f"  Failed to download image: {download_err}")
                                cell.value = "이미지 다운로드 실패"
                                continue
                        else:
                            logger.info(f"  Using cached image at: {temp_file_path}")
                            image_path = temp_file_path
                    else:
                        logger.warning(f"  Invalid image path: {original_value}")
                        cell.value = "이미지 파일 없음"
                        continue
                
                # Add image to worksheet
                try:
                    # Verify the image file
                    with Image.open(image_path) as img_check:
                        img_size = img_check.size
                        logger.info(f"  Image size: {img_size}")
                        
                        # Skip very small images
                        if img_size[0] < 20 or img_size[1] < 20:
                            logger.warning(f"  Image too small: {img_size}")
                            cell.value = "이미지 크기가 너무 작음"
                            continue
                    
                    # Add the image to the worksheet
                    img = openpyxl.drawing.image.Image(image_path)
                    img.width = img_width
                    img.height = img_height
                    
                    # Calculate cell position
                    cell_address = f"{get_column_letter(col_idx)}{row_idx}"
                    
                    # Set image anchor
                    img.anchor = cell_address
                    worksheet.add_image(img)
                    
                    # Clear the cell value since we're showing the image
                    cell.value = ""
                    
                    logger.info(f"  Successfully added image to cell {cell_address}")
                except Exception as img_err:
                    logger.error(f"  Failed to process image {image_path}: {img_err}")
                    cell.value = "이미지 처리 오류"
            
            except Exception as e:
                logger.error(f"Error processing image in row {row_idx}, column {col_name}: {e}")
                try:
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.value = "이미지 처리 오류"
                except:
                    pass

    logger.debug("Finished processing image columns")

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
                        Path('C:/RPA/Image/Main/Haoreum'),
                        Path('C:/RPA/Image/Target/Haoreum'),
                        Path('C:/RPA/Image/Haoreum'),
                        Path('C:/RPA/Image')
                    ]
                elif source == 'kogift':
                    base_paths = [
                        Path('C:/RPA/Image/Main/Kogift'),
                        Path('C:/RPA/Image/Target/Kogift'),
                        Path('C:/RPA/Image/Kogift'),
                        Path('C:/RPA/Image')
                    ]
                elif source == 'naver':
                    base_paths = [
                        Path('C:/RPA/Image/Main/Naver'),
                        Path('C:/RPA/Image/Target/Naver'),
                        Path('C:/RPA/Image/Naver'),
                        Path('C:/RPA/Image')
                    ]
                else:
                    base_paths = [
                        Path('C:/RPA/Image/Main'),
                        Path('C:/RPA/Image/Target'),
                        Path('C:/RPA/Image')
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
                            # Common Haoreum image patterns
                            f"C:/RPA/Image/Main/Haoreum/haoreum_{os.path.basename(img_value)}",
                            f"C:/RPA/Image/Main/Haoreum/haoreum_{img_value}",
                            f"C:/RPA/Image/Main/Haoreum/{os.path.basename(img_value)}"
                        ]
                        
                        for std_path in standard_paths:
                            if os.path.exists(std_path) and os.path.getsize(std_path) > 0:
                                std_path_str = std_path.replace('\\', '/')
                                placeholder_url = f"file:///{std_path_str}"
                                return {
                                    'url': placeholder_url, 
                                    'local_path': std_path, 
                                    'original_path': img_value, 
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
    Prepares the DataFrame for Excel output.
    
    Args:
        df (pd.DataFrame): The DataFrame to prepare
        skip_images (bool): If True, skip image columns for upload file
        
    Returns:
        pd.DataFrame: Prepared DataFrame
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure all required columns exist
    for col in FINAL_COLUMN_ORDER:
        if col not in df.columns:
            df[col] = ""
    
    # Select and reorder columns based on FINAL_COLUMN_ORDER
    df = df[FINAL_COLUMN_ORDER]
    
    # For upload file, remove image data columns
    if skip_images:
        image_columns = [col for col in df.columns if '이미지' in col]
        for col in image_columns:
            df[col] = df[col].apply(lambda x: '' if isinstance(x, dict) and 'path' in x else x)
    
    # Format numeric columns
    for col in df.columns:
        if any(keyword in col for keyword in ['단가', '가격']):
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna('')
            except Exception as e:
                logging.warning(f"Error formatting numeric column {col}: {str(e)}")
    
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
    upload_path = f"{base_name}_upload{ext}"
    
    # Log the paths that will be created
    logging.info(f"Result file path (with images): {output_path}")
    logging.info(f"Upload file path (links only): {upload_path}")
    
    # Check if either file is locked
    if os.path.exists(output_path):
        try:
            with open(output_path, 'a'):
                pass
        except PermissionError:
            logging.error(f"Result file is locked: {output_path}")
            return False, False, output_path, upload_path
    
    if os.path.exists(upload_path):
        try:
            with open(upload_path, 'a'):
                pass
        except PermissionError:
            logging.error(f"Upload file is locked: {upload_path}")
            return False, False, output_path, upload_path
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Copy the DataFrame to avoid modifying the original
    df_result = df.copy()
    df_upload = df.copy()
    
    # Process data for both files
    result_success = False
    upload_success = False
    
    try:
        # 1. Create the result file (with images)
        result_success = create_final_output_excel(df_result, output_path)
        
        # 2. Create the upload file (links only)
        if result_success:
            # Create the upload file without images
            upload_success = _create_upload_excel(df_upload, upload_path)
    
    except Exception as e:
        logging.error(f"Error creating split Excel outputs: {str(e)}", exc_info=True)
        return False, False, output_path, upload_path
    
    # Return the results
    if result_success and upload_success:
        logging.info(f"Successfully created both result and upload Excel files")
    else:
        if not result_success:
            logging.error(f"Failed to create result Excel file")
        if not upload_success:
            logging.error(f"Failed to create upload Excel file")
    
    return result_success, upload_success, output_path, upload_path

def _create_upload_excel(df: pd.DataFrame, output_path: str) -> bool:
    """
    Internal function to create the upload Excel file (with links only, no images)
    
    Args:
        df (pd.DataFrame): The DataFrame to save to Excel
        output_path (str): Path where to save the Excel file
        
    Returns:
        bool: True if successful, False otherwise
    """
    logging.info(f"Creating upload Excel file (links only): {output_path}")
    
    try:
        # Prepare data for Excel (similar to create_final_output_excel but without images)
        df = _prepare_data_for_excel(df, skip_images=True)
        
        # Create a Pandas Excel writer
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        
        # Convert the DataFrame to an Excel object
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        
        # Get workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # Apply basic formatting (headers, column widths, etc.)
        _apply_excel_formatting(workbook, worksheet, df, include_images=False)
        
        # Save the Excel file
        writer.close()
        logging.info(f"Successfully created upload Excel file at: {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error creating upload Excel file: {str(e)}", exc_info=True)
        return False

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
                df_prepared = df_temp[FINAL_COLUMN_ORDER].fillna('-').astype(str)
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
            df_prepared.to_excel(writer, index=False, sheet_name='Results', na_rep='-')
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
                    df_prepared = df_temp[FINAL_COLUMN_ORDER].fillna('-').astype(str)
                else:
                    raise

            df_prepared.to_excel(alternative_path, index=False, engine='openpyxl', sheet_name='Results', na_rep='-')
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

