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
    '가격차이(2)(%)': '가격차이(2)(%)',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '고 려기프트 상품링크': '고려기프트 상품링크',  # Add variant with space
    '고려 링크': '고려기프트 상품링크',
    '고 려 링크': '고려기프트 상품링크',  # Add variant with spaces
    '기본수량(3)': '기본수량(3)',
    '네이버 기본수량': '기본수량(3)',
    '판매단가(V포함)(3)': '판매단가(V포함)(3)',
    '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
    '가격차이(3)': '가격차이(3)',
    '가격차이(3)(%)': '가격차이(3)(%)',
    '가격차이 비율(3)': '가격차이(3)(%)', # Map both to the same output column
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
    '기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
    '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크',
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
PERCENTAGE_COLUMNS = ['가격차이(2)(%)', '가격차이(3)(%)']
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
    'file_not_found': '이미지 파일을 로컬 경로에서 찾을 수 없음',
    'invalid_image': '유효하지 않은 이미지 형식',
    'processing_error': '이미지 처리 중 오류가 발생했습니다',
    'too_small': '이미지 크기가 너무 작음 (저해상도)',
    'format_error': '지원하지 않는 이미지 형식',
    'download_failed': '이미지 다운로드 실패'
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
    """Processes image columns, handling local images and URLs with IMAGE function."""
    logger.debug("Processing image columns...")
    
    # Get image column indices
    image_cols = {
        '본사 이미지': None,
        '고려기프트 이미지': None,
        '네이버 이미지': None
    }
    
    for col_idx, col_name in enumerate(df.columns, 1):
        if col_name in image_cols:
            image_cols[col_name] = col_idx
    
    if not any(image_cols.values()):
        logger.debug("No image columns found in DataFrame")
        return
    
    # Process each row
    for row_idx in range(2, worksheet.max_row + 1):
        for col_name, col_idx in image_cols.items():
            if col_idx is None:
                continue
                
            cell = worksheet.cell(row=row_idx, column=col_idx)
            if not cell.value or cell.value == '-':
                continue
                
            try:
                img_path = str(cell.value)
                
                # Check if it's a URL
                if img_path.startswith(('http://', 'https://')):
                    # Use IMAGE function for URLs
                    cell.value = f'=IMAGE("{img_path}")'
                    cell.font = LINK_FONT
                else:
                    # Handle local image path
                    if os.path.exists(img_path):
                        img = Image(img_path)
                        img.width = 100
                        img.height = 100
                        worksheet.add_image(img, cell.coordinate)
                    else:
                        logger.warning(f"Local image not found: {img_path}")
                        cell.value = '이미지 파일을 로컬 경로에서 찾을 수 없음'
            except Exception as e:
                logger.error(f"Error processing image in cell {cell.coordinate}: {e}")
                cell.value = '이미지 처리 중 오류가 발생했습니다'
    
    logger.debug("Finished image processing.")

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
    if worksheet.max_row <= 1:
        logger.debug("Skipping table format: No data rows.")
        return

    table_range = f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"
    table_name = "PriceComparisonData"
    # Check if table already exists
    if table_name in worksheet.tables:
         logger.warning(f"Table '{table_name}' already exists. Skipping table creation.")
         # Optionally update table range if needed
         # worksheet.tables[table_name].ref = table_range
         return
    try:
        table = Table(displayName=table_name, ref=table_range)
        # Choose a professional looking style
        style = TableStyleInfo(
            name="TableStyleMedium2", # A common, clean style
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False
        )
        table.tableStyleInfo = style
        worksheet.add_table(table)
        logger.debug(f"Applied Excel table format 'TableStyleMedium2' to range {table_range}")
    except Exception as e:
        logger.error(f"Failed to apply table formatting to range {table_range}: {e}")

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
    for col in FINAL_COLUMN_ORDER:
        if col not in df_prepared.columns:
            logger.warning(f"Column '{col}' missing in input data for preparation, adding with default '-'.")
            df_prepared[col] = '-'

    # 2. Select and Reorder columns STRICTLY according to FINAL_COLUMN_ORDER
    # Only keep columns defined in FINAL_COLUMN_ORDER, discard others
    try:
        df_prepared = df_prepared[FINAL_COLUMN_ORDER]
        logger.debug(f"Columns reordered and selected. Final columns: {df_prepared.columns.tolist()}")
    except KeyError as ke:
        logger.error(f"KeyError during column selection/reordering. Missing columns likely: {ke}. DataFrame columns: {df.columns.tolist()}")
        # Return DataFrame with available columns from FINAL_COLUMN_ORDER if error
        available_final_cols = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
        df_prepared = df[available_final_cols]
        # Add truly missing ones back with '-'
        for col in FINAL_COLUMN_ORDER:
             if col not in df_prepared.columns:
                  df_prepared[col] = '-'
        df_prepared = df_prepared[FINAL_COLUMN_ORDER] # Try reordering again

    # 3. Format Numeric Data (Carefully preserving non-numeric error messages)
    logger.debug("Formatting numeric columns...")
    for col_name in df_prepared.columns:
        is_price_col = col_name in PRICE_COLUMNS
        is_qty_col = col_name in QUANTITY_COLUMNS
        is_pct_col = col_name in PERCENTAGE_COLUMNS

        if is_price_col or is_qty_col or is_pct_col:
            def format_value(value):
                original_value = value # Keep original for fallback
                formatted_value = '-' # Default formatted value

                if pd.isna(value) or str(value).strip().lower() in ['-', '', 'none', 'nan']:
                    return '-'
                elif isinstance(value, str) and any(err_msg in value for err_msg in ERROR_MESSAGE_VALUES):
                    return value # Preserve error messages

                try:
                    # Remove commas and % for numeric conversion
                    cleaned_value_str = str(value).replace(',', '').replace('%','').strip()
                    
                    # Skip conversion if it's an error message or placeholder
                    if cleaned_value_str == '-' or any(err_msg in cleaned_value_str for err_msg in ERROR_MESSAGE_VALUES):
                        return cleaned_value_str

                    numeric_value = float(cleaned_value_str)

                    # Format based on column type
                    if is_price_col:
                        if numeric_value == 0:
                            return '-'
                        return f"{numeric_value:,.0f}" # Comma separated integer
                    elif is_qty_col:
                        if numeric_value == 0:
                            return '-'
                        return f"{int(numeric_value):,}" # Comma separated integer
                    elif is_pct_col:
                        return f"{numeric_value:.1f}%" # One decimal place percentage
                    else:
                        return str(original_value).strip()

                except (ValueError, TypeError):
                    # If conversion fails, try to clean the string
                    cleaned_str = str(original_value).strip()
                    if cleaned_str in ['', '-', 'nan', 'None', 'none']:
                        return '-'
                    return cleaned_str

            # Apply the formatting function to numeric columns
            df_prepared[col_name] = df_prepared[col_name].apply(format_value)
            
    logger.debug("Finished numeric formatting.")

    # 4. Clean Text Data (Strip whitespace, handle NaN)
    logger.debug("Cleaning text columns...")
    for col_name in TEXT_COLUMNS:
        if col_name in df_prepared.columns:
            # Ensure column is treated as string, fill NA with '-', then strip
            df_prepared[col_name] = df_prepared[col_name].astype(str).fillna('-').str.strip()
            # Replace empty strings resulting from fillna/strip back to '-'
            df_prepared[col_name] = df_prepared[col_name].replace({'': '-'})

    # 5. Fill any remaining NaN/NaT values in other columns with '-' for consistent output
    # This handles columns not explicitly formatted above (like links, image paths before processing)
    df_prepared.fillna('-', inplace=True)
    logger.debug("Filled remaining NaN values with '-'.")

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
        # Pass a copy to avoid modifying the original DataFrame if called externally
        df_prepared = _prepare_data_for_excel(df.copy())

        if df_prepared.empty and not df.empty:
             logger.error("Data preparation resulted in an empty DataFrame. Cannot save Excel.")
             return False
        elif df_prepared.empty and df.empty:
             logger.warning("Input DataFrame was empty, saving an Excel file with only headers.")
             # Create empty DF with correct columns for header generation
             df_prepared = pd.DataFrame(columns=FINAL_COLUMN_ORDER)

        # 2. Save prepared data to Excel using openpyxl engine
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Use na_rep='-' during initial write for consistency
            df_prepared.to_excel(writer, index=False, sheet_name='Results', na_rep='-')
            worksheet = writer.sheets['Results']
            logger.debug(f"DataFrame written to sheet 'Results'. Max Row: {worksheet.max_row}, Max Col: {worksheet.max_column}")

            # --- Apply Formatting AFTER data is written ---
            # 3. Apply Column Widths and Cell Styles (Font, Border, Alignment)
            _apply_column_widths(worksheet, df_prepared)
            _apply_cell_styles_and_alignment(worksheet, df_prepared)

            # 4. Apply Conditional Formatting
            _apply_conditional_formatting(worksheet, df_prepared)

            # 5. Handle Images (Embedding)
            # Pass df_prepared containing the paths/URLs used for embedding
            _process_image_columns(worksheet, df_prepared)

            # 6. Add Hyperlinks
            # Pass df_prepared containing the link text
            _add_hyperlinks_to_worksheet(worksheet, df_prepared)

            # 7. Page Setup and Header/Footer
            _setup_page_layout(worksheet)
            _add_header_footer(worksheet)

            # 8. Apply Table Format (Apply last after other formatting)
            _apply_table_format(worksheet)

        logger.info(f"Successfully created and formatted Excel file: {output_path}")
        return True

    except PermissionError as pe:
         logger.error(f"Permission denied when trying to save Excel file: {output_path}. Check if the file is open. Error: {pe}")
         # Consider adding a retry mechanism here or in the decorator
         return False
    except Exception as e:
        logger.error(f"Failed to create final Excel output '{output_path}': {e}", exc_info=True)
        # Attempt to delete potentially corrupted file
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
                logger.info(f"Removed potentially corrupted output file: {output_path}")
            except OSError as del_err:
                logger.error(f"Could not remove potentially corrupted file {output_path}: {del_err}")
        return False

def filter_dataframe(df: pd.DataFrame, config: Optional[configparser.ConfigParser] = None) -> pd.DataFrame:
    """
    Filter the DataFrame based on configuration settings.
    
    Args:
        df: Input DataFrame to filter
        config: Optional ConfigParser instance with filter settings
        
    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df
        
    # Default filter settings
    price_diff_threshold = 0.1
    quality_threshold = 0.50
    
    # Get settings from config if provided
    if config:
        try:
            price_diff_threshold = config.getfloat('PriceDifference', 'threshold', fallback=price_diff_threshold)
            quality_threshold = config.getfloat('MatchQualityThresholds', 'low_quality', fallback=quality_threshold)
        except Exception as e:
            logger.warning(f"Error reading filter settings from config: {e}")
    
    # Filter by price difference
    if '가격차이(2)' in df.columns:
        df = df[df['가격차이(2)'].abs() <= price_diff_threshold]
        
    if '가격차이(3)' in df.columns:
        df = df[df['가격차이(3)'].abs() <= price_diff_threshold]
    
    # Filter by match quality
    if '매칭품질' in df.columns:
        df = df[df['매칭품질'].isin(['high', 'medium', 'low'])]
    
    return df

# Deprecated functions below (kept for reference or potential reuse if logic changes)

# def filter_dataframe(...) -> Removed, functionality likely elsewhere or combined
# def add_hyperlinks(...) -> Integrated into _add_hyperlinks_to_worksheet
# def apply_excel_styles(...) -> Integrated into _apply_cell_styles_and_alignment

def apply_excel_styles(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """
    Apply Excel styles to the worksheet.
    This is a wrapper around _apply_cell_styles_and_alignment for backward compatibility.
    """
    _apply_cell_styles_and_alignment(worksheet, df)

