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
import ast
import requests
from io import BytesIO

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
LINK_COLUMNS = [
    '본사상품링크', '고려기프트 상품링크', '네이버 쇼핑 링크', '공급사 상품링크'
]
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
        elif col_name_str in LINK_COLUMNS or '링크' in col_name_str:
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
    """
    Process image columns and insert images into Excel cells.
    """
    logger.info("Processing image columns...")
    
    def safe_load_image(path, max_height=150, max_width=150):
        """Safely load and resize an image."""
        try:
            if not path or path == '-':
                return None
                
            # If path is a URL, download the image
            if path.startswith(('http://', 'https://')):
                try:
                    response = requests.get(path, timeout=10)
                    if response.status_code == 200:
                        img = Image.open(BytesIO(response.content))
                    else:
                        logger.warning(f"Failed to download image from URL: {path}")
                        return None
                except Exception as e:
                    logger.error(f"Error downloading image from URL {path}: {e}")
                    return None
            else:
                # Local file path
                if not os.path.exists(path):
                    logger.warning(f"Image file not found: {path}")
                    return None
                img = Image.open(path)
            
            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                background.paste(img, mask=img.split()[-1])
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Resize image while maintaining aspect ratio
            width, height = img.size
            if width > max_width or height > max_height:
                ratio = min(max_width/width, max_height/height)
                new_size = (int(width * ratio), int(height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
            
            return img
        except Exception as e:
            logger.error(f"Error processing image {path}: {e}")
            return None

    # Process each image column
    for col_idx, col_name in enumerate(df.columns, 1):
        if col_name in IMAGE_COLUMNS:
            logger.info(f"Processing image column: {col_name}")
            
            # Get the column letter
            col_letter = get_column_letter(col_idx)
            
            # Process each cell in the column
            for row_idx, value in enumerate(df[col_name], 2):  # Start from row 2 (after header)
                if pd.isna(value) or value == '-':
                    continue
                
                # Load and process the image
                img = safe_load_image(value)
                if img is None:
                    continue
                
                # Convert PIL Image to openpyxl Image
                img_byte_arr = BytesIO()
                img.save(img_byte_arr, format='PNG')
                img_byte_arr.seek(0)
                
                # Create openpyxl Image object
                excel_img = openpyxl.drawing.image.Image(img_byte_arr)
                
                # Calculate cell dimensions
                cell = worksheet[f"{col_letter}{row_idx}"]
                cell_width = worksheet.column_dimensions[col_letter].width
                cell_height = worksheet.row_dimensions[row_idx].height
                
                # Set image dimensions
                excel_img.width = min(cell_width * 7, 150)  # Convert column width to pixels
                excel_img.height = min(cell_height * 1.2, 150)  # Convert row height to pixels
                
                # Add image to cell
                worksheet.add_image(excel_img, f"{col_letter}{row_idx}")
                
                # Adjust row height if needed
                if cell_height < excel_img.height / 1.2:
                    worksheet.row_dimensions[row_idx].height = excel_img.height / 1.2
    
    logger.info("Finished processing image columns.")

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
    Simplified version that just formats URL cells without hyperlinks.
    """
    try:
        # Define columns that should contain URLs
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
                        # Cell to apply formatting
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        cell.value = url
                        
                        # Style for URL
                        cell.font = Font(color="0563C1", underline="single")
                        
                        total_urls_processed += 1
                    except Exception as hyperlink_err:
                        logger.warning(f"Error formatting URL in row {row_idx}, col {col}: {hyperlink_err}")
                        # Keep original text if formatting fails
                        cell.value = url
                        
        logger.info(f"Processed {total_urls_processed} URLs across link columns.")
    except Exception as e:
        logger.warning(f"Error processing URLs: {e}")
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
        if pd.isna(img_value) or img_value in ['-', '', None]:
            return '-'

        # Handle dictionary format
        if isinstance(img_value, dict):
            # Ensure the dictionary has required keys
            if 'url' in img_value and img_value['url']:
                # For Naver images, ensure we have a valid URL
                if 'naver' in img_col_name.lower() and img_value['url'].startswith(('http://', 'https://')):
                    # Validate Naver URL format
                    if 'naver.com' in img_value['url'] or 'naver.net' in img_value['url']:
                        return img_value
                    else:
                        logging.warning(f"Invalid Naver URL format: {img_value['url']}")
                        return '-'
                return img_value
            return '-'

        # Handle string values
        if isinstance(img_value, str):
            img_value = img_value.strip()
            
            # Check if this is a string representation of a dictionary
            if img_value.startswith('{') and img_value.endswith('}'):
                try:
                    img_dict = ast.literal_eval(img_value)
                    if isinstance(img_dict, dict) and 'url' in img_dict and img_dict['url']:
                        return img_dict
                except (SyntaxError, ValueError):
                    pass

            # For URL strings
            if img_value.startswith(('http://', 'https://')):
                source = img_col_name.split()[0].lower()
                # Special handling for Naver URLs
                if 'naver' in img_col_name.lower():
                    if 'naver.com' in img_value or 'naver.net' in img_value:
                        return {'url': img_value, 'source': source}
                    else:
                        logging.warning(f"Invalid Naver URL format: {img_value}")
                        return '-'
                return {'url': img_value, 'source': source}

            # For file paths
            if os.path.exists(img_value) and os.path.getsize(img_value) > 0:
                source = img_col_name.split()[0].lower()
                img_value_str = img_value.replace(os.sep, '/')
                placeholder_url = f"file:///{img_value_str}"
                return {
                    'url': placeholder_url,
                    'local_path': img_value,
                    'original_path': img_value,
                    'source': source
                }

            # Try to resolve relative paths
            try:
                base_paths = [
                    Path('C:/RPA/Image/Main'),
                    Path('C:/RPA/Image/Target'),
                    Path('C:/RPA/Image')
                ]
                
                for base_path in base_paths:
                    if base_path.exists():
                        abs_path = (base_path / img_value).resolve()
                        if abs_path.exists() and abs_path.stat().st_size > 0:
                            source = img_col_name.split()[0].lower()
                            abs_path_str = str(abs_path).replace('\\', '/')
                            placeholder_url = f"file:///{abs_path_str}"
                            return {
                                'url': placeholder_url,
                                'local_path': str(abs_path),
                                'original_path': str(abs_path),
                                'source': source
                            }
            except Exception as e:
                logging.debug(f"Failed to resolve relative path {img_value}: {e}")

        return '-'
    except Exception as e:
        logging.warning(f"Error verifying image data '{str(img_value)[:100]}...' for column {img_col_name}: {e}")
        return '-'

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
    Create two Excel files: one with images and one with URL links only.
    
    Args:
        df_finalized: Finalized DataFrame to save
        output_path_base: Base path for output files
        
    Returns:
        tuple: (result_success, upload_success, result_path, upload_path)
    """
    try:
        logging.info(f"Starting creation of split Excel outputs from finalized DataFrame (Shape: {df_finalized.shape})")
        
        # Ensure DataFrame columns are in the correct order
        if not all(col in df_finalized.columns for col in FINAL_COLUMN_ORDER):
            logging.warning("Input DataFrame columns are not in the exact FINAL_COLUMN_ORDER. Reordering again.")
            df_finalized = df_finalized.reindex(columns=FINAL_COLUMN_ORDER)
        
        # Generate file paths
        base_name = os.path.splitext(output_path_base)[0]
        # Remove any existing timestamp from the base name
        base_name = re.sub(r'_\d{8}_\d{6}$', '', base_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create result file path (with images)
        result_path = f"{base_name}_result_{timestamp}.xlsx"
        logging.info(f"Result file path (with images): {result_path}")
        
        # Create upload file path (links only)
        upload_path = f"{base_name}_upload_{timestamp}.xlsx"
        logging.info(f"Upload file path (links only): {upload_path}")
        
        # Write result file with images
        logging.info(f"Attempting to write result file: {result_path} with {len(df_finalized)} rows.")
        try:
            # Create a copy of the DataFrame for the result file
            result_df = df_finalized.copy()
            
            # Write result data to Excel
            logging.info("Writing result data to Excel sheet...")
            result_df.to_excel(result_path, index=False, engine='openpyxl')
            
            # Load the workbook for formatting
            workbook = openpyxl.load_workbook(result_path)
            worksheet = workbook.active
            
            # Apply formatting
            _apply_column_widths(worksheet, result_df)
            _apply_cell_styles_and_alignment(worksheet, result_df)
            _add_header_footer(worksheet)
            _apply_table_format(worksheet)
            _apply_conditional_formatting(worksheet, result_df)
            _setup_page_layout(worksheet)
            
            # Add images
            logging.info("Adding images to result file...")
            _process_image_columns(worksheet, result_df)
            
            # Save the workbook
            workbook.save(result_path)
            logging.info(f"Successfully created result file: {result_path}")
            
        except Exception as e:
            logging.error(f"Error creating result file: {e}", exc_info=True)
            return False, False, None, None
        
        # Create upload file with links only
        logging.info(f"Preparing data for upload file: {upload_path}")
        try:
            # Create a copy of the DataFrame for the upload file
            upload_df = df_finalized.copy()
            
            # Extract image URLs for each image column
            for img_col in IMAGE_COLUMNS:
                if img_col in upload_df.columns:
                    logging.info(f"Extracting image URLs from {img_col} column...")
                    upload_df = extract_naver_image_urls(upload_df, img_col)
            
            # Write upload data to Excel
            logging.info(f"Writing upload file (with image links): {upload_path} with {len(upload_df)} rows.")
            upload_df.to_excel(upload_path, index=False, engine='openpyxl')
            
            # Load the workbook for formatting
            workbook = openpyxl.load_workbook(upload_path)
            worksheet = workbook.active
            
            # Apply upload-specific formatting
            _apply_upload_file_formatting(worksheet, upload_df.columns)
            
            # Add hyperlinks to image URLs
            logging.info("Adding hyperlinks to image URLs in upload file...")
            _add_hyperlinks_to_worksheet(worksheet, upload_df, hyperlinks_as_formulas=True)
            
            # Save the workbook
            workbook.save(upload_path)
            logging.info(f"Successfully created upload file (with image links): {upload_path}")
            
        except Exception as e:
            logging.error(f"Error creating upload file: {e}", exc_info=True)
            return True, False, result_path, None
        
        return True, True, result_path, upload_path
        
    except Exception as e:
        logging.error(f"Error in create_split_excel_outputs: {e}", exc_info=True)
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
    
    # Step 1: Create a new DataFrame to avoid modifying the original
    df_final = df.copy()
    
    # Step 2: Process image columns to extract URLs from nested dictionaries
    image_columns = ['본사 이미지', '고려기프트 이미지', '네이버 이미지', '해오름 이미지 URL']
    for col in image_columns:
        if col in df_final.columns:
            df_final[col] = df_final[col].apply(lambda x: 
                x.get('url', {}).get('url', '-') if isinstance(x, dict) and 'url' in x 
                else x.get('url', '-') if isinstance(x, dict) 
                else str(x) if pd.notna(x) else '-'
            )
    
    # Step 3: Rename columns to the target names
    df_final = df_final.rename(columns=COLUMN_RENAME_MAP, errors='ignore')
    logger.debug(f"Columns after rename: {df_final.columns.tolist()}")
    
    # Step 4: Create an output DataFrame with columns in the proper order
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

    # Step 5: Add missing columns with empty values
    for col in FINAL_COLUMN_ORDER:
        if col not in output_df.columns:
            output_df[col] = None # Add missing column with None values
            logger.debug(f"Added missing column '{col}' with None values")
    
    # Step 6: Format numeric columns
    image_cols = [col for col in output_df.columns if col in IMAGE_COLUMNS]
    
    logger.info("Applying numeric formatting to relevant columns...")
    for col in output_df.columns:
        # Skip image columns
        if col in image_cols:
            continue

        # Check if column should be numeric
        is_numeric_col = (
            col in PRICE_COLUMNS or
            col in QUANTITY_COLUMNS or
            col in PERCENTAGE_COLUMNS or
            col in ['가격차이(2)', '가격차이(3)']
        )

        if is_numeric_col:
            try:
                # Store original data before attempting conversion
                original_data = output_df[col].copy()
                
                # Attempt conversion to numeric, coercing errors to NaN
                output_df[col] = pd.to_numeric(output_df[col], errors='coerce')
                
                # If conversion resulted in all NaNs, revert
                if output_df[col].isna().all():
                    output_df[col] = original_data
            except Exception as e:
                logger.warning(f"Error converting column '{col}' to numeric: {e}. Keeping original data.")

    # Step 7: Replace NaN/NaT with None for Excel compatibility
    output_df = output_df.replace({pd.NA: None, np.nan: None, pd.NaT: None})

    # Step 8: Set default values for empty cells
    logger.info("Setting default values for empty cells ('-')...")
    for col in output_df.columns:
        if col not in image_cols:
            output_df[col] = output_df[col].apply(
                lambda x: '-' if (x is None or x == '') else x
            )

    # Final verification
    logger.info(f"DataFrame finalized. Output shape: {output_df.shape}")
    logger.debug(f"Final columns: {output_df.columns.tolist()}")

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
    Create two Excel files: one with images and one with URL links only.
    
    Args:
        df_finalized: Finalized DataFrame to save
        output_path_base: Base path for output files
        
    Returns:
        tuple: (result_success, upload_success, result_path, upload_path)
    """
    try:
        logging.info(f"Starting creation of split Excel outputs from finalized DataFrame (Shape: {df_finalized.shape})")
        
        # Ensure DataFrame columns are in the correct order
        if not all(col in df_finalized.columns for col in FINAL_COLUMN_ORDER):
            logging.warning("Input DataFrame columns are not in the exact FINAL_COLUMN_ORDER. Reordering again.")
            df_finalized = df_finalized.reindex(columns=FINAL_COLUMN_ORDER)
        
        # Generate file paths
        base_name = os.path.splitext(output_path_base)[0]
        # Remove any existing timestamp from the base name
        base_name = re.sub(r'_\d{8}_\d{6}$', '', base_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create result file path (with images)
        result_path = f"{base_name}_result_{timestamp}.xlsx"
        logging.info(f"Result file path (with images): {result_path}")
        
        # Create upload file path (links only)
        upload_path = f"{base_name}_upload_{timestamp}.xlsx"
        logging.info(f"Upload file path (links only): {upload_path}")
        
        # Write result file with images
        logging.info(f"Attempting to write result file: {result_path} with {len(df_finalized)} rows.")
        try:
            # Create a copy of the DataFrame for the result file
            result_df = df_finalized.copy()
            
            # Write result data to Excel
            logging.info("Writing result data to Excel sheet...")
            result_df.to_excel(result_path, index=False, engine='openpyxl')
            
            # Load the workbook for formatting
            workbook = openpyxl.load_workbook(result_path)
            worksheet = workbook.active
            
            # Apply formatting
            _apply_column_widths(worksheet, result_df)
            _apply_cell_styles_and_alignment(worksheet, result_df)
            _add_header_footer(worksheet)
            _apply_table_format(worksheet)
            _apply_conditional_formatting(worksheet, result_df)
            _setup_page_layout(worksheet)
            
            # Add images
            logging.info("Adding images to result file...")
            _process_image_columns(worksheet, result_df)
            
            # Save the workbook
            workbook.save(result_path)
            logging.info(f"Successfully created result file: {result_path}")
            
        except Exception as e:
            logging.error(f"Error creating result file: {e}", exc_info=True)
            return False, False, None, None
        
        # Create upload file with links only
        logging.info(f"Preparing data for upload file: {upload_path}")
        try:
            # Create a copy of the DataFrame for the upload file
            upload_df = df_finalized.copy()
            
            # Extract image URLs for each image column
            for img_col in IMAGE_COLUMNS:
                if img_col in upload_df.columns:
                    logging.info(f"Extracting image URLs from {img_col} column...")
                    upload_df = extract_naver_image_urls(upload_df, img_col)
            
            # Write upload data to Excel
            logging.info(f"Writing upload file (with image links): {upload_path} with {len(upload_df)} rows.")
            upload_df.to_excel(upload_path, index=False, engine='openpyxl')
            
            # Load the workbook for formatting
            workbook = openpyxl.load_workbook(upload_path)
            worksheet = workbook.active
            
            # Apply upload-specific formatting
            _apply_upload_file_formatting(worksheet, upload_df.columns)
            
            # Add hyperlinks to image URLs
            logging.info("Adding hyperlinks to image URLs in upload file...")
            _add_hyperlinks_to_worksheet(worksheet, upload_df, hyperlinks_as_formulas=True)
            
            # Save the workbook
            workbook.save(upload_path)
            logging.info(f"Successfully created upload file (with image links): {upload_path}")
            
        except Exception as e:
            logging.error(f"Error creating upload file: {e}", exc_info=True)
            return True, False, result_path, None
        
        return True, True, result_path, upload_path
        
    except Exception as e:
        logging.error(f"Error in create_split_excel_outputs: {e}", exc_info=True)
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

