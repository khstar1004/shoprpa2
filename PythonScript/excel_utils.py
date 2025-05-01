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
# Updated to map TO the desired "엑셀 골든" column names
COLUMN_RENAME_MAP = {
    # 구분 -> 구분(승인관리:A/가격관리:P)
    '구분': '구분(승인관리:A/가격관리:P)',
    '구분(승인관리:A/가격관리:P)': '구분(승인관리:A/가격관리:P)', # Keep self-map
    # 담당자 -> 담당자
    '담당자': '담당자',
    # 업체명 -> 공급사명
    '업체명': '공급사명',
    '공급사명': '공급사명', # Keep self-map (for first supplier)
    # 업체코드 -> 공급처코드
    '업체코드': '공급처코드',
    '공급처코드': '공급처코드', # Keep self-map
    # Code -> 상품코드
    'Code': '상품코드',
    '상품코드': '상품코드', # Keep self-map
    # 중분류카테고리 -> 카테고리(중분류)
    '중분류카테고리': '카테고리(중분류)',
    '카테고리(중분류)': '카테고리(중분류)', # Keep self-map
    # 상품명 -> 상품명
    '상품명': '상품명',
    'name': '상품명',
    # 기본수량(1) -> 본사 기본수량
    '기본수량(1)': '본사 기본수량',
    '본사 기본수량': '본사 기본수량', # Keep self-map
    # 판매단가(V포함) -> 판매단가1(VAT포함)
    '판매단가(V포함)': '판매단가1(VAT포함)',
    '판매단가1(VAT포함)': '판매단가1(VAT포함)', # Keep self-map
    # 본사상품링크 -> 본사링크
    '본사상품링크': '본사링크',
    '본사링크': '본사링크', # Keep self-map
    # 기본수량(2) -> 고려 기본수량
    '기본수량(2)': '고려 기본수량', # Corrected Target
    '고려 기본수량': '고려 기본수량', # Keep self-map
    # 판매가(V포함)(2) -> 판매단가2(VAT포함)
    '판매가(V포함)(2)': '판매단가2(VAT포함)', # Corrected Target
    '판매단가(V포함)(2)': '판매단가2(VAT포함)', # Handle potential input variation
    '판매단가2(VAT포함)': '판매단가2(VAT포함)', # Keep self-map
    # 가격차이(2) -> 고려 가격차이
    '가격차이(2)': '고려 가격차이', # Corrected Target
    '고려 가격차이': '고려 가격차이', # Keep self-map
    # 가격차이(2)(%) -> 고려 가격차이(%)
    '가격차이(2)(%)': '고려 가격차이(%)', # Corrected Target
    '고려 가격차이(%)': '고려 가격차이(%)', # Keep self-map
    # 고려기프트 상품링크 -> 고려 링크
    '고려기프트 상품링크': '고려 링크', # Corrected Target
    '고려 링크': '고려 링크', # Keep self-map
    # 기본수량(3) -> 네이버 기본수량
    '기본수량(3)': '네이버 기본수량', # Corrected Target
    '네이버 기본수량': '네이버 기본수량', # Keep self-map
    # 판매단가(V포함)(3) -> 판매단가3 (VAT포함)
    '판매단가(V포함)(3)': '판매단가3 (VAT포함)', # Corrected Target
    '판매단가3 (VAT포함)': '판매단가3 (VAT포함)', # Keep self-map
    # 가격차이(3) -> 네이버 가격차이
    '가격차이(3)': '네이버 가격차이', # Corrected Target
    '네이버 가격차이': '네이버 가격차이', # Keep self-map
    # 가격차이(3)(%) -> 네이버가격차이(%)
    '가격차이(3)(%)': '네이버가격차이(%)', # Corrected Target
    '네이버가격차이(%)': '네이버가격차이(%)', # Keep self-map
    # 네이버 공급사명 -> 네이버 공급사명 (Target column name)
    '네이버 공급사명': '네이버 공급사명', # Corrected: Map specific Naver source to the target '네이버 공급사명'
    # Avoid mapping a generic second '공급사명' if input isn't guaranteed unique
    # 네이버 쇼핑 링크 -> 네이버 링크
    '네이버 쇼핑 링크': '네이버 링크', # Corrected Target
    '네이버 링크': '네이버 링크', # Keep self-map
    # 공급사 상품링크 -> 공급사 상품링크 (This column isn't in FINAL_COLUMN_ORDER, can remove mapping or keep if needed for intermediate steps)
    # '공급사 상품링크': '공급사 상품링크',
    # '네이버 링크': '공급사 상품링크', # Avoid ambiguous mapping
    # 본사 이미지 -> 해오름(이미지링크)
    '본사 이미지': '해오름(이미지링크)', # Corrected Target
    '해오름이미지경로': '해오름(이미지링크)', # Corrected Target
    '해오름(이미지링크)': '해오름(이미지링크)', # Keep self-map
    # 고려기프트 이미지 -> 고려기프트(이미지링크)
    '고려기프트 이미지': '고려기프트(이미지링크)', # Corrected Target
    '고려기프트(이미지링크)': '고려기프트(이미지링크)', # Keep self-map
    # 네이버 이미지 -> 네이버쇼핑(이미지링크)
    '네이버 이미지': '네이버쇼핑(이미지링크)', # Corrected Target
    '네이버쇼핑(이미지링크)': '네이버쇼핑(이미지링크)' # Keep self-map
}

# Final Target Column Order (Based on "엑셀 골든" sample)
# THIS IS THE STRICT ORDER AND NAMING FOR THE OUTPUT FILE
FINAL_COLUMN_ORDER = [
    '구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드', '카테고리(중분류)', '상품명',
    '본사 기본수량', '판매단가1(VAT포함)', '본사링크',
    '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)', '고려 링크', # Corrected: Removed leading space
    '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)', '네이버 공급사명', # Corrected: Removed trailing space
    '네이버 링크',
    '해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'
]

# Columns that must be present in the input file for processing
# Update this based on the new FINAL_COLUMN_ORDER if necessary,
# focusing on the absolutely essential input fields needed.
# Let's assume the core identifier columns remain crucial, using the NEW names
REQUIRED_INPUT_COLUMNS = [
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리',
    '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
]

# --- Column Type Definitions for Formatting ---
# Update these lists based on the NEW FINAL_COLUMN_ORDER names ("엑셀 골든")
PRICE_COLUMNS = [
    '판매단가1(VAT포함)', '판매단가2(VAT포함)', '판매단가3 (VAT포함)',
    '고려 가격차이', '네이버 가격차이'
]
QUANTITY_COLUMNS = ['본사 기본수량', '고려 기본수량', '네이버 기본수량']
PERCENTAGE_COLUMNS = ['고려 가격차이(%)', '네이버가격차이(%)']
TEXT_COLUMNS = ['구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드', '카테고리(중분류)', '상품명', '네이버 공급사명']
LINK_COLUMNS_FOR_HYPERLINK = {
    # Map final column names used for links
    '본사링크': '본사링크',
    '고려 링크': '고려 링크',
    '네이버 링크': '네이버 링크'
    # Image columns are handled separately but have '링크' in name
}
# Define IMAGE_COLUMNS based on FINAL_COLUMN_ORDER
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
    """Processes image columns and embeds images into the worksheet."""
    logger.debug("Processing image columns...")
    logger.debug(f"DataFrame columns passed to _process_image_columns: {df.columns.tolist()}") # Log columns
    
    # Get indices of image columns using final names from IMAGE_COLUMNS constant
    image_column_indices = {}
    
    # Create a mapping of column name to DataFrame index (0-based) and Excel column index (1-based)
    for col_name in IMAGE_COLUMNS:
        if col_name in df.columns:
            try:
                # Get the 0-based index from DataFrame columns for data access
                df_col_idx_0based = df.columns.get_loc(col_name)
                
                # Handle multiple columns with the same name
                if isinstance(df_col_idx_0based, (np.ndarray, pd.Series)) and df_col_idx_0based.dtype == bool:
                    # Find the first True value in the boolean mask
                    true_indices = np.where(df_col_idx_0based)[0]
                    if len(true_indices) > 0:
                        df_col_idx_0based = true_indices[0]
                        logger.warning(f"Found column '{col_name}' at multiple positions, using first: {df_col_idx_0based}")
                    else:
                        logger.error(f"Boolean array for '{col_name}' doesn't contain any True values.")
                        continue
                
                # Ensure it's an integer now
                df_col_idx_0based = int(df_col_idx_0based)

                # Get the 1-based index for openpyxl cell access
                excel_col_idx_1based = df_col_idx_0based + 1
                image_column_indices[col_name] = excel_col_idx_1based
                logger.debug(f"Found image column: '{col_name}' at Excel index {excel_col_idx_1based}")
            except Exception as e:
                logger.error(f"Error getting index for column '{col_name}': {e}")
        else:
            logger.warning(f"Expected image column not found in DataFrame: '{col_name}'")

    if not image_column_indices:
        logger.debug("No image columns found in DataFrame using final names")
        return

    # Image size settings
    img_width = 150  # Increased from 120
    img_height = 150  # Increased from 120
    
    # Log the number of rows we'll be processing
    df_row_count = len(df)
    worksheet_row_count = worksheet.max_row
    logger.debug(f"Processing images for DataFrame with {df_row_count} rows, Worksheet has {worksheet_row_count} rows")

    # Process each row in the DataFrame (offset by 1 for Excel header row)
    for df_idx in range(df_row_count):
        row_idx = df_idx + 2  # Excel is 1-indexed and we have a header row
        
        # Process each image column
        for col_name, col_idx in image_column_indices.items():
            # Verify cell access will be valid
            try:
                current_row_idx = int(row_idx)
                current_col_idx = int(col_idx)
                
                if current_row_idx < 1 or current_col_idx < 1:
                    logger.warning(f"Invalid index for cell: R{current_row_idx}C{current_col_idx}. Skipping.")
                    continue
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid index conversion: {e}")
                continue

            # Get the image data from the DataFrame directly to avoid issues with cell value conversion
            try:
                # Extract the cell value directly from DataFrame
                img_data = df.iloc[df_idx][col_name]
                
                # Skip empty cells, None, NaN, or placeholder values
                if img_data is None or pd.isna(img_data) or img_data == '' or img_data == '-':
                    continue
                
                # Get the corresponding Excel cell
                cell = worksheet.cell(row=current_row_idx, column=current_col_idx)
                cell_coordinate = cell.coordinate
                
                # Handle different image data formats
                image_path_to_embed = None
                
                # Case 1: Dictionary format with local_path
                if isinstance(img_data, dict) and 'local_path' in img_data:
                    local_path = img_data['local_path']
                    if isinstance(local_path, str) and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        image_path_to_embed = local_path
                        logger.debug(f"Using local_path from dict for cell {cell_coordinate}: {image_path_to_embed}")
                
                # Case 2: String that is a file path
                elif isinstance(img_data, str) and os.path.exists(img_data) and os.path.isfile(img_data):
                    image_path_to_embed = img_data
                    logger.debug(f"Using direct file path for cell {cell_coordinate}: {image_path_to_embed}")
                
                # Case 3: Dictionary-like string that needs parsing
                elif isinstance(img_data, str) and img_data.startswith('{') and img_data.endswith('}'):
                    try:
                        import ast
                        parsed_dict = ast.literal_eval(img_data)
                        if isinstance(parsed_dict, dict) and 'local_path' in parsed_dict:
                            path = parsed_dict['local_path']
                            if os.path.exists(path) and os.path.isfile(path):
                                image_path_to_embed = path
                                logger.debug(f"Using local_path from parsed string for cell {cell_coordinate}: {image_path_to_embed}")
                    except Exception as e:
                        logger.warning(f"Failed to parse dictionary-like string for cell {cell_coordinate}: {e}")
                
                # Skip if no valid image path found
                if not image_path_to_embed:
                    logger.warning(f"No valid image path found for cell {cell_coordinate}")
                    # For URL-only data, display the URL
                    if isinstance(img_data, dict) and 'url' in img_data:
                        cell.value = img_data['url']
                    continue
                
                # Add image to worksheet
                try:
                    logger.info(f"Adding image to Excel: Path='{image_path_to_embed}', Cell='{cell_coordinate}'")
                    img = openpyxl.drawing.image.Image(image_path_to_embed)
                    img.width = img_width
                    img.height = img_height
                    img.anchor = cell_coordinate
                    worksheet.add_image(img)
                    
                    # Clear cell value after adding image
                    cell.value = ""
                    
                    # Apply styling
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    
                except Exception as e:
                    logger.error(f"Error adding image to cell {cell_coordinate}: {e}")
                    # Fallback to URL if available
                    if isinstance(img_data, dict) and 'url' in img_data:
                        cell.value = img_data['url']
                    else:
                        cell.value = f"이미지 추가 실패"
            
            except Exception as e:
                logger.error(f"Error processing image for DataFrame row {df_idx}, column {col_name}: {e}")
                # Try to update the cell with an error message
                try:
                    worksheet.cell(row=current_row_idx, column=current_col_idx).value = "이미지 처리 오류"
                except:
                    pass  # Silently fail if we can't even set the error message
    
    # Log completion
    logger.info(f"Finished processing image columns. Added images to worksheet.")
    
    # Check if images were added successfully
    image_count = len(worksheet._images) if hasattr(worksheet, '_images') else 0
    logger.info(f"Total images added to worksheet: {image_count}")

def _apply_conditional_formatting(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Applies conditional formatting (e.g., yellow fill for price difference < -1)."""
    logger.debug("Applying conditional formatting.")

    # Find price difference columns (non-percentage) using new names
    price_diff_cols = [
        col for col in df.columns
        if col in ['가격차이(2)', '가격차이(3)'] # Use new names
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

def create_split_excel_outputs(df_finalized: pd.DataFrame, output_path_base: str) -> tuple:
    """
    Creates two Excel outputs:
    1. Result file (with images embedded)
    2. Upload file (with image links only)
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
            return None, False, None, False
        
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
        try:
            if '구분(승인관리:A/가격관리:P)' in df_finalized.columns:
                # Get the most common value to use in naming
                source_val = df_finalized['구분(승인관리:A/가격관리:P)'].iloc[0]
                if source_val == 'A':
                    source_info = "승인관리"
                elif source_val == 'P':
                    source_info = "가격관리"
                else:
                    source_info = str(source_val)
        except Exception as e:
            logger.warning(f"Error getting source name: {e}")
            source_info = "Mixed"
        
        # Create timestamped filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_part = datetime.now().strftime("%Y%m%d")
        prev_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        
        # Format: {source}-{date_range}_{timestamp}_{type}.xlsx
        result_filename = f"{source_info}-{prev_date}_{date_part}_{timestamp}_result.xlsx"
        upload_filename = f"{source_info}-{prev_date}_{date_part}_{timestamp}_upload.xlsx"
        
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
                            df_for_excel.at[idx, col] = value['url']
                        else:
                            # Just convert to string representation if no URL
                            df_for_excel.at[idx, col] = str(value)
                    elif isinstance(value, pd.Series):
                        # For Series objects, convert to string
                        for item in value:
                            if pd.notna(item) and item not in ['-', '']:
                                if isinstance(item, dict) and 'url' in item:
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
            
            # Save the result file (without images for now)
            workbook_result.save(result_path)
            result_success = True
            
            # Now embed images
            # Skip this step for now - images will be processed separately if needed
            
            logger.info(f"Successfully created result file: {result_path}")
            
            # Potentially add a verification step here
            
        except Exception as e:
            logger.error(f"Error creating result file: {e}")
            logger.debug(traceback.format_exc())
            result_success = False
        
        # -----------------------------------------
        # 2. Create Upload File (with links only)
        # -----------------------------------------
        try:
            logger.info(f"Preparing data for upload file: {upload_path}")
            
            # Convert image data to URL strings for the upload file
            df_upload = df_finalized.copy()
            
            # Process image URLs: Extract URLs from image dictionaries
            image_columns = [col for col in df_upload.columns if '이미지' in col or 'image' in col.lower()]
            for col in image_columns:
                try:
                    # Convert dictionaries to URL strings in each cell
                    for idx in df_upload.index:
                        value = df_upload.loc[idx, col]
                        
                        # Handle different types of image data
                        if isinstance(value, dict) and 'url' in value:
                            # Extract URL from dictionary
                            df_upload.at[idx, col] = value['url']
                        elif isinstance(value, str) and value != '-' and ('http' in value or 'file:/' in value):
                            # Keep URL strings as-is
                            pass
                        elif isinstance(value, pd.Series):
                            # For Series objects, find first non-empty value
                            for item in value:
                                if pd.notna(item) and item not in ['-', '']:
                                    if isinstance(item, dict) and 'url' in item:
                                        df_upload.at[idx, col] = item['url']
                                    elif isinstance(item, str) and ('http' in item or 'file:/' in item):
                                        df_upload.at[idx, col] = item
                                    else:
                                        df_upload.at[idx, col] = str(item)
                                    break
                            else:
                                df_upload.at[idx, col] = '-'
                        else:
                            # For other types, convert to string
                            df_upload.at[idx, col] = str(value) if pd.notna(value) else '-'
                except Exception as e:
                    logger.error(f"Error processing image URLs for column '{col}': {e}")
            
            # Create new workbook for upload file
            workbook_upload = openpyxl.Workbook()
            worksheet_upload = workbook_upload.active
            worksheet_upload.title = "제품 가격 비교 (업로드용)"
            
            logger.info(f"Attempting to write upload file (links only): {upload_path} with {len(df_upload)} rows.")
            
            # Write header
            for col_idx, col_name in enumerate(df_upload.columns, 1):
                worksheet_upload.cell(row=1, column=col_idx, value=col_name)
            
            logger.info("Writing upload data to Excel sheet...")
            
            # Write data
            for row_idx, row in enumerate(df_upload.itertuples(), 2):
                for col_idx, value in enumerate(row[1:], 1):  # Skip the index
                    worksheet_upload.cell(row=row_idx, column=col_idx, value=value if not pd.isna(value) else "")
            
            logger.info(f"Upload data ({len(df_upload)} rows) written to Excel sheet. Applying minimal formatting...")
            
            # Apply minimal formatting
            _apply_basic_excel_formatting(worksheet_upload, df_upload.columns.tolist())
            _add_hyperlinks_to_worksheet(worksheet_upload, df_upload, hyperlinks_as_formulas=True)
            _add_header_footer(worksheet_upload)
            
            # Save the upload file
            workbook_upload.save(upload_path)
            upload_success = True
            
            logger.info(f"Successfully created upload file: {upload_path}")
            
        except Exception as e:
            logger.error(f"Error creating upload file: {e}")
            logger.debug(traceback.format_exc())
            upload_success = False
        
        # Return results
        if result_success and upload_success:
            logger.info("Successfully created both result and upload files.")
        elif upload_success:
            logger.warning("Created upload file but failed to create result file.")
        elif result_success:
            logger.warning("Created result file but failed to create upload file.")
        else:
            logger.error("Failed to create both result and upload files.")
            
        # Verify result file (if successful)
        if result_success:
            try:
                # Code to verify the result file
                pass
            except Exception as verify_err:
                logger.error(f"Error verifying result file: {verify_err}")
        
        return result_path, result_success, upload_path, upload_success
        
    except Exception as main_error:
        logger.error(f"Unexpected error in create_split_excel_outputs: {main_error}")
        logger.debug(traceback.format_exc())
        return None, False, None, False

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
        renamed_cols = {}
        
        for col in df.columns:
            if col in unique_cols:
                # Skip this column as we already have it
                continue
            unique_cols.append(col)
        
        # Create new DataFrame with only unique columns
        df = df[unique_cols]
        logger.info(f"Removed duplicate columns. New shape: {df.shape}")
    
    # Step 1: Ensure all required columns exist with custom mapping
    column_mapping = {
        # Map common input columns to standardized output columns
        '공급처': '공급사명',
        '공급업체명': '공급사명',
        '공급처명': '공급사명',
        '상품 코드': '상품코드',
        '상품코드(A SKU)': '상품코드',
        '품번': '상품코드',
        '중분류': '카테고리(중분류)',
        '카테고리': '카테고리(중분류)',
        '기본수량': '본사 기본수량',
        '수량': '본사 기본수량',
        '단가': '판매단가1(VAT포함)',
        '판매단가': '판매단가1(VAT포함)',
        '판매가': '판매단가1(VAT포함)',
        '링크': '본사링크',
        '상품링크': '본사링크',
        # Keep any standardized columns as-is
        # Mapping for image columns can be direct or URL/filename based
    }
    
    # Step 2: Rename columns using our mapping
    df = df.rename(columns=column_mapping, errors='ignore')
    logger.info(f"Columns AFTER renaming (errors ignored): {df.columns.tolist()}")
    
    # Step 3: Add any missing columns in FINAL_COLUMN_ORDER that don't exist yet
    missing_columns = [col for col in FINAL_COLUMN_ORDER if col not in df.columns]
    if missing_columns:
        logger.warning(f"Added missing columns expected in FINAL_COLUMN_ORDER: {missing_columns}")
        # Add with None values
        for col in missing_columns:
            df[col] = None
    logger.info(f"Columns AFTER adding missing: {df.columns.tolist()}")
    
    # Step 4: Ensure proper column order by creating a new DataFrame with only the needed columns
    final_df = pd.DataFrame()
    for col in FINAL_COLUMN_ORDER:
        if col in df.columns:
            final_df[col] = df[col]
    
    # Log the reordered columns
    logger.info(f"Columns after enforcing FINAL_COLUMN_ORDER: {final_df.columns.tolist()}")
    
    # Step 5: Basic type formatting - convert numeric columns where appropriate
    numeric_columns = [
        '공급처코드', '상품코드', '본사 기본수량', '판매단가1(VAT포함)',
        '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)',
        '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)'
    ]
    
    # Try to convert numeric columns to appropriate types
    for col in numeric_columns:
        if col in final_df.columns:
            try:
                # Only process non-image columns
                if 'image' not in col.lower() and '이미지' not in col.lower():
                    # First convert to string to handle mixed types
                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Error during numeric conversion attempt for column '{col}': {e}. Keeping original values.")
    
    # Step 6: Process image columns and ensure they are in proper format for Excel
    image_columns = [col for col in final_df.columns if '이미지' in col or 'image' in col.lower()]
    for col in image_columns:
        try:
            # Fix Series objects in image columns
            for idx in final_df.index:
                value = final_df.loc[idx, col]
                
                # Handle Series objects (from duplicate columns)
                if isinstance(value, pd.Series):
                    # Take the first non-empty value in the series
                    for item in value:
                        if pd.notna(item) and item not in ['-', '']:
                            final_df.at[idx, col] = item
                            break
                    else:
                        # If no valid value found, use empty string
                        final_df.at[idx, col] = '-'
                    
        except Exception as e:
            logger.warning(f"Could not process image column '{col}': {e}")
    
    logger.info(f"DataFrame finalized. Output shape: {final_df.shape}")
    
    return final_df

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
        
        # 5. Auto-filter for all columns
        worksheet.auto_filter.ref = f"A1:{get_column_letter(len(column_list))}{max_row}"
        
        logger.debug(f"Applied basic Excel formatting to worksheet (header + {max_row-1} data rows)")
        
    except Exception as e:
        logger.warning(f"Error applying basic Excel formatting: {e}")
        logger.debug(traceback.format_exc())

