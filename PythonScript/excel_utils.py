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

# --- Constants ---
PROMO_KEYWORDS = ['판촉', '기프트', '답례품', '기념품', '인쇄', '각인', '제작', '호갱', '몽키', '홍보']

# Column Rename Mapping to match the exact sample format
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

# 숫자 데이터로 변환해야 할 컬럼 목록
NUMERIC_COLUMNS = [
    '기본수량(1)', '기본수량(2)', '기본수량(3)',
    '판매단가(V포함)', '판매단가(V포함)(2)', '판매단가(V포함)(3)',
    '판매가(V포함)(2)',
    '가격차이(2)', '가격차이(3)',
    '가격차이(2)(%)', '가격차이(3)(%)', '가격차이 비율(3)',
    '판매단가1(VAT포함)', '표준소비자가(VAT포함)', '해오름단가', 
    '코기프트단가', '네이버단가', '코기프트최소주문수량', '네이버최소주문수량',
    '텍스트유사도(코기프트)', '이미지유사도(코기프트)', '텍스트유사도(네이버)', '이미지유사도(네이버)'
]

# Columns related to Goryeo and Naver for clearing/dropping logic
GORYEO_COLS = ['기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크', '고려기프트 이미지']
NAVER_COLS = ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크', '네이버 이미지']

# Columns to check for hyperlinking after renaming
LINK_COLUMN_MAP = {
    '본사상품링크': '본사상품링크',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '공급사 상품링크': '공급사 상품링크',
    '네이버 쇼핑 링크': '네이버 쇼핑 링크',
    '본사 이미지': '본사 이미지',
    '고려기프트 이미지': '고려기프트 이미지',
    '네이버 이미지': '네이버 이미지'
}

# Error Messages Constants
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

# --- 텍스트를 숫자로 변환하는 함수 ---
def convert_text_to_numbers(df):
    """
    텍스트로 저장된 숫자 데이터를 실제 숫자 타입으로 변환합니다.
    
    Args:
        df (pandas.DataFrame): 변환할 DataFrame
        
    Returns:
        pandas.DataFrame: 숫자 타입으로 변환된 DataFrame
    """
    logging.info("텍스트를 숫자 데이터로 변환 시작")
    
    # DataFrame 복사하여 원본 보존
    df_converted = df.copy()
    
    # 변환할 숫자 컬럼 찾기
    numeric_cols = [col for col in NUMERIC_COLUMNS if col in df_converted.columns]
    logging.info(f"변환 대상 컬럼: {numeric_cols}")
    
    # 각 컬럼에 대해 숫자 변환 적용
    for col in numeric_cols:
        try:
            # 원본 데이터 타입 및 값 형태 기록
            sample_values = df_converted[col].head(3).tolist()
            original_dtype = df_converted[col].dtype
            logging.debug(f"컬럼 '{col}' 변환 전: 타입={original_dtype}, 샘플값={sample_values}")
            
            # 텍스트 데이터 전처리 및 숫자로 변환
            df_converted[col] = df_converted[col].apply(
                lambda x: str(x).replace(',', '').replace(' ', '').strip() if pd.notna(x) else x
            )
            
            # '%' 기호 제거 (퍼센트 컬럼인 경우)
            if '%' in col or '유사도' in col:
                df_converted[col] = df_converted[col].apply(
                    lambda x: str(x).replace('%', '').strip() if pd.notna(x) and isinstance(x, str) else x
                )
            
            # 실제 숫자 타입으로 변환
            df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce')
            
            # 변환 결과 기록
            new_dtype = df_converted[col].dtype
            converted_sample = df_converted[col].head(3).tolist()
            logging.debug(f"컬럼 '{col}' 변환 후: 타입={new_dtype}, 샘플값={converted_sample}")
            
        except Exception as e:
            logging.warning(f"컬럼 '{col}' 숫자 변환 중 오류 발생: {e}")
    
    # 결과 요약
    successful_cols = sum(1 for col in numeric_cols if pd.api.types.is_numeric_dtype(df_converted[col].dtype))
    logging.info(f"전체 {len(numeric_cols)}개 컬럼 중 {successful_cols}개 컬럼 숫자 변환 완료")
    
    return df_converted

# Add the function to count empty rows after convert_text_to_numbers function and before _contains_keywords
def count_empty_rows(df):
    """
    Count rows in a DataFrame that contain only default/empty values like '-', '', 'nan'.
    
    Args:
        df (pandas.DataFrame): The DataFrame to check
        
    Returns:
        int: Number of empty rows found
    """
    try:
        # First convert all values to strings for consistency
        str_df = df.astype(str)
        
        # Check each row to see if all values are empty indicators
        empty_indicators = ['-', '', 'nan', 'None', 'NaN']
        
        # Count rows where all values match empty indicators
        empty_count = 0
        for _, row in str_df.iterrows():
            if all(val in empty_indicators for val in row):
                empty_count += 1
                
        return empty_count
    except Exception as e:
        logging.warning(f"Error counting empty rows: {e}")
        return 0

# --- Filtering Logic ---

def _contains_keywords(text, keywords):
    """Helper function to check if text contains any keywords (case-insensitive)."""
    if pd.isna(text):
        return False
    text_lower = str(text).lower()
    # Ensure keywords are strings for comparison
    return any(str(keyword).lower() in text_lower for keyword in keywords)

def filter_dataframe(df, config):
    """
    Filters the combined results DataFrame based on rules from requirement documents.
    Handles data cleaning, numeric conversion, conditional clearing, and final row drops.
    Returns the filtered DataFrame *before* final column renaming.
    """
    if df.empty:
        logging.warning("DataFrame is empty before filtering.")
        return df

    initial_rows = len(df)
    logging.info(f"Starting filtering of {initial_rows} matched results...")

    # IMPORTANT: Save the original input index to ensure we don't lose any products
    original_indices = df.index.tolist()
    logging.info(f"Preserved {len(original_indices)} original product indices")

    df_filtered = df.copy() # Work on a copy

    # --- 1. Data Cleaning and Numeric Conversion ---
    # Define columns expected to be numeric (use original names before potential rename)
    numeric_cols = ['가격차이(2)', '가격차이(3)', '가격차이(2)(%)', '가격차이(3)(%)', '가격차이 비율(3)',
                    '판매단가(V포함)', '판매단가(V포함)(2)', '판매단가(V포함)(3)']
    # Add similarity scores if they exist and should be numeric
    numeric_cols.extend([col for col in df_filtered.columns if '_Sim' in col or '_Combined' in col])


    # Clean percentage strings first (handle both '%', ' %', and potential extra spaces)
    percent_cols = ['가격차이(2)(%)', '가격차이(3)(%)', '가격차이 비율(3)']
    for col in percent_cols:
        if col in df_filtered.columns:
            df_filtered[col] = df_filtered[col].astype(str).str.replace(r'\s*%\s*$', '', regex=True).str.strip()

    # Clean price difference strings (remove commas)
    price_diff_cols = ['가격차이(2)', '가격차이(3)']
    for col in price_diff_cols:
        if col in df_filtered.columns:
            df_filtered[col] = df_filtered[col].astype(str).str.replace(r',', '', regex=True).str.strip()

    # Convert to numeric, coercing errors
    for col in numeric_cols:
        if col in df_filtered.columns:
            # Replace potential placeholders like '-' before conversion
            df_filtered[col] = df_filtered[col].replace(['-', ''], np.nan, regex=False)
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')
            logging.debug(f"Converted column '{col}' to numeric.")

    # --- 2. Initial Price Difference Filter ---
    # MODIFICAÇÃO: Remover o filtro inicial de diferença de preço negativo
    # Em vez disso, sinalizar preços negativos para destaque visual mantendo todas as linhas
    if '가격차이(2)' in df_filtered.columns:
        negative_price2 = (df_filtered['가격차이(2)'] < 0)
        logging.info(f"Identificados {negative_price2.sum()} registros com preço Kogift menor")
        
    if '가격차이(3)' in df_filtered.columns:
        negative_price3 = (df_filtered['가격차이(3)'] < 0)
        logging.info(f"Identificados {negative_price3.sum()} registros com preço Naver menor")

    # --- 3. Conditional Clearing / Removal of Data ---
    # Define columns for Goryeo and Naver processing
    original_goryeo_cols = ['기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', 
                          '고려기프트 상품링크', '고려기프트 이미지']
    original_naver_cols = ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '가격차이 비율(3)',
                         '공급사명', '공급사 상품링크', '네이버 쇼핑 링크', '네이버 이미지']

    # Get existing columns to avoid errors
    existing_goryeo_clear = [col for col in original_goryeo_cols if col in df_filtered.columns]
    existing_naver_clear = [col for col in original_naver_cols if col in df_filtered.columns]

    # 3a. Clear Goryeo Data if Price Diff >= 0 OR Price Diff % > -1%
    # BUT DON'T REMOVE ROWS - just clear the data cells for filtering display
    goryeo_cleared_count = 0
    goryeo_clear_cond = pd.Series(False, index=df_filtered.index) # Initialize with False
    if '가격차이(2)' in df_filtered.columns:
        goryeo_clear_cond |= (df_filtered['가격차이(2)'] >= 0)
    if '가격차이(2)(%)' in df_filtered.columns:
        goryeo_clear_cond |= (df_filtered['가격차이(2)(%)'] > -1.0) # Rule: > -1%

    rows_to_clear_goryeo = goryeo_clear_cond.fillna(False) # Ensure NaNs in condition are False
    if rows_to_clear_goryeo.any() and existing_goryeo_clear:
        # Mark rows that will be cleared but preserve the data
        df_filtered.loc[rows_to_clear_goryeo, existing_goryeo_clear] = np.nan 
        goryeo_cleared_count = rows_to_clear_goryeo.sum()
        logging.debug(f"Cleared Goryeo data for {goryeo_cleared_count} rows based on price diff >= 0 or % > -1.")

    # 3b. Clear Naver Data if Price Diff >= 0 OR Price Diff % > -1%
    # BUT DON'T REMOVE ROWS - just clear the data cells for filtering display
    naver_cleared_count1 = 0
    naver_clear_cond1 = pd.Series(False, index=df_filtered.index)
    if '가격차이(3)' in df_filtered.columns:
        naver_clear_cond1 |= (df_filtered['가격차이(3)'] >= 0)
    # Check both potential percentage columns
    if '가격차이 비율(3)' in df_filtered.columns:
         naver_clear_cond1 |= (df_filtered['가격차이 비율(3)'] > -1.0) # Rule: > -1%
    elif '가격차이(3)(%)' in df_filtered.columns:
         naver_clear_cond1 |= (df_filtered['가격차이(3)(%)'] > -1.0) # Rule: > -1%

    rows_to_clear_naver1 = naver_clear_cond1.fillna(False)
    if rows_to_clear_naver1.any() and existing_naver_clear:
        # Mark rows that will be cleared but preserve the data 
        df_filtered.loc[rows_to_clear_naver1, existing_naver_clear] = np.nan
        naver_cleared_count1 = rows_to_clear_naver1.sum()
        logging.debug(f"Cleared Naver data for {naver_cleared_count1} rows based on price diff >= 0 or % > -1.")

    # 3c. Clear Naver Data based on Qty, Price Diff %, and NON-Promo Keywords (Rule 2 from 2차 작업)
    # BUT DON'T REMOVE ROWS - just clear the data cells for filtering display
    naver_cleared_count2 = 0
    naver_clear_cond2 = pd.Series(False, index=df_filtered.index)
    
    # Check if required columns exist
    if '기본수량(3)' in df_filtered.columns and '공급사명' in df_filtered.columns and existing_naver_clear:
        # Identify rows missing quantity
        qty_missing = df_filtered['기본수량(3)'].isna() | (df_filtered['기본수량(3)'].astype(str).str.strip().isin(['', '-']))

        # Identify rows where price diff is not significant (> -10%)
        price_not_sig = pd.Series(False, index=df_filtered.index)
        if '가격차이 비율(3)' in df_filtered.columns:
             price_not_sig |= (df_filtered['가격차이 비율(3)'] > -10.0)
        elif '가격차이(3)(%)' in df_filtered.columns:
             price_not_sig |= (df_filtered['가격차이(3)(%)'] > -10.0)
        else:
             # If no % column, maybe apply based on absolute diff? Or skip? Skipping for now.
             logging.warning("Naver condition 3c requires a price diff % column ('가격차이 비율(3)' or '가격차이(3)(%)'). Skipping this part of the condition.")
             price_not_sig = pd.Series(False, index=df_filtered.index) # Cannot apply

        # Identify rows where supplier is NOT a promo site
        not_promo = ~df_filtered['공급사명'].apply(lambda x: _contains_keywords(x, PROMO_KEYWORDS))

        # Combine conditions
        naver_clear_cond2 = qty_missing & price_not_sig.fillna(False) & not_promo.fillna(True) # Treat missing supplier name as non-promo

        rows_to_clear_naver2 = naver_clear_cond2.fillna(False)
        if rows_to_clear_naver2.any():
            # Mark rows that will be cleared but preserve the data
            df_filtered.loc[rows_to_clear_naver2, existing_naver_clear] = np.nan
            naver_cleared_count2 = rows_to_clear_naver2.sum()
            logging.debug(f"Cleared Naver data for {naver_cleared_count2} additional rows based on missing qty / price % > -10 / non-promo supplier condition.")

    # --- 4. IMPORTANT: DO NOT drop any rows, even if they have no comparison data ---
    # Instead, just log how many would have been removed in the original logic
    all_comparison_cols_original = list(set(existing_goryeo_clear + existing_naver_clear))
    if all_comparison_cols_original:
        # Count how many rows have all comparison data empty, but don't remove them
        empty_comparison_mask = df_filtered[all_comparison_cols_original].isna().all(axis=1)
        empty_rows_count = empty_comparison_mask.sum()
        logging.info(f"Encontrados {empty_rows_count} produtos sem dados de comparação (seriam removidos no filtro original)")
    else:
        logging.warning("Skipping final empty row filtering - no comparison columns found.")

    # --- 5. Final Formatting before Renaming ---
    # Reapply string formatting for percentages and price differences
    # Use original column names here as renaming hasn't happened yet
    percent_cols_to_format = ['가격차이(2)(%)', '가격차이 비율(3)', '가격차이(3)(%)']
    for key in percent_cols_to_format:
        if key in df_filtered.columns:
            # Convert back to numeric temporarily for formatting check
            numeric_series = pd.to_numeric(df_filtered[key], errors='coerce')
            mask = numeric_series.notna()
            # Format valid numbers, leave others as is (might be NaN already)
            df_filtered.loc[mask, key] = numeric_series[mask].apply(lambda x: f"{x:.1f} %")

    price_diff_cols_to_format = ['가격차이(2)', '가격차이(3)']
    for key in price_diff_cols_to_format:
        if key in df_filtered.columns:
            numeric_series = pd.to_numeric(df_filtered[key], errors='coerce')
            mask = numeric_series.notna()
            df_filtered.loc[mask, key] = numeric_series[mask].apply(lambda x: f"{x:,.0f}")

    # IMPORTANT: Check if we have all original rows preserved
    final_rows = len(df_filtered)
    if final_rows != initial_rows:
        logging.error(f"Row count mismatch! Started with {initial_rows} rows but now have {final_rows} rows. Attempting to restore missing rows.")
        
        # This should not happen with our changes, but if it does, try to recover
        current_indices = df_filtered.index.tolist()
        missing_indices = [idx for idx in original_indices if idx not in current_indices]
        
        if missing_indices:
            logging.warning(f"Found {len(missing_indices)} missing rows. Restoring original rows.")
            missing_rows = df.loc[missing_indices].copy()
            df_filtered = pd.concat([df_filtered, missing_rows])
            logging.info(f"Restored missing rows. New row count: {len(df_filtered)}")

    logging.info(f"Finished filtering. {len(df_filtered)}/{initial_rows} rows maintained (no rows dropped).")
    return df_filtered


# --- Additional Function for Error Recovery ---
def safe_excel_operation(func):
    """Decorator for safely handling Excel operations with error recovery."""
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except PermissionError as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Permission error in {func.__name__}, retrying ({attempt+1}/{max_retries}): {e}")
                    time.sleep(1)  # Wait before retry
                else:
                    logging.error(f"Failed to {func.__name__} after {max_retries} attempts: {e}")
                    raise
            except Exception as e:
                logging.error(f"Error in {func.__name__}: {e}\n{traceback.format_exc()}")
                raise
    return wrapper

def apply_excel_styles(file_path, headers: List[str]):
    """
    Apply Excel styling including cell formatting, borders, column widths, 
    and conditional formatting for price differentials.
    
    Args:
        file_path: Path to the Excel file to style
        headers: List of column headers used for identification
    
    Returns:
        True if successful, False if there was an error
    """
    try:
        if not os.path.exists(file_path):
            logging.error(f"Excel file not found for styling: {file_path}")
            return False
            
        workbook = load_workbook(file_path)
        sheet = workbook.active
        
        # Find header row (could be row 1 or row 4 if there are explanation headers)
        header_row = 1
        if sheet.cell(row=1, column=1).value == "[알림]":
            header_row = 4
            
        # Get actual header values and their positions
        header_positions = {}
        data_start_row = header_row + 1
        for col in range(1, sheet.max_column + 1):
            header = sheet.cell(row=header_row, column=col).value
            if header:
                header_positions[header] = col  # Ensure column index is an integer
        
        logging.info(f"Headers found for styling: {list(header_positions.keys())}")
        
        # --- 1. Apply basic formatting for all data ---
        # Add basic borders to all cells with data
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        # Get the range that contains data
        max_row = sheet.max_row
        max_col = sheet.max_column
        
        logging.info(f"Applying styles to {max_row} rows and {max_col} columns")
        
        # Apply borders and alignment to all data cells
        for row in range(data_start_row, max_row + 1):
            for col in range(1, max_col + 1):
                cell = sheet.cell(row=row, column=col)
                cell.border = thin_border
                cell.alignment = Alignment(vertical='center')
                
                # Center specific columns
                header = sheet.cell(row=header_row, column=col).value
                if header in ['구분', '담당자', '기본수량(1)', '기본수량(2)', '기본수량(3)']:
                    cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # --- 2. Format headers ---
        # Make headers bold with background color
        header_fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
        header_font = Font(bold=True, size=11)
        
        for col in range(1, max_col + 1):
            header_cell = sheet.cell(row=header_row, column=col)
            header_cell.fill = header_fill
            header_cell.font = header_font
            header_cell.border = thin_border
            header_cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            
        # --- 3. Apply conditional formatting for price differences ---
        # Find price difference columns
        price_diff_cols = []
        price_diff_pct_cols = []
        
        for header, col_idx in header_positions.items():
            # Ensure col_idx is an integer
            if isinstance(col_idx, str):
                try:
                    col_idx = int(col_idx)
                    header_positions[header] = col_idx  # Update the dictionary
                except (ValueError, TypeError):
                    logging.warning(f"Invalid column index for header {header}: {col_idx}. Skipping.")
                    continue
                    
            if '가격차이' in header and '%' not in header:
                price_diff_cols.append(col_idx)
            elif '가격차이' in header and '%' in header:
                price_diff_pct_cols.append(col_idx)
                
        # Apply conditional formatting - negative price differences are good (highlighted in yellow)
        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        
        for col_idx in price_diff_cols:
            try:
                # Ensure col_idx is an integer
                col_idx = int(col_idx) if not isinstance(col_idx, int) else col_idx
                col_letter = get_column_letter(col_idx)
                
                # Check each cell in the column
                for row in range(data_start_row, max_row + 1):
                    cell = sheet.cell(row=row, column=col_idx)
                    # Try to convert to number for comparison
                    try:
                        if cell.value and cell.value != '-':
                            value = float(str(cell.value).replace(',', ''))
                            if value < 0:
                                # Negative price difference - highlight in yellow (good)
                                cell.fill = yellow_fill
                            elif value > 0:
                                # Positive price difference - no highlight
                                pass
                    except (ValueError, TypeError):
                        pass  # Ignore cells that can't be converted to numbers
            except Exception as e:
                logging.warning(f"Error processing price difference column {col_idx}: {e}")
                    
        # Format percentage difference columns
        for col_idx in price_diff_pct_cols:
            try:
                # Ensure col_idx is an integer
                col_idx = int(col_idx) if not isinstance(col_idx, int) else col_idx
                col_letter = get_column_letter(col_idx)
                
                # Check each cell in the column
                for row in range(data_start_row, max_row + 1):
                    cell = sheet.cell(row=row, column=col_idx)
                    # Try to convert to number for comparison
                    try:
                        if cell.value and cell.value != '-':
                            value = float(str(cell.value).replace(',', '').replace('%', ''))
                            if value < 0:
                                # Negative price difference - highlight in yellow (good)
                                cell.fill = yellow_fill
                            elif value > 0:
                                # Positive price difference - no highlight
                                pass
                    except (ValueError, TypeError):
                        pass  # Ignore cells that can't be converted to numbers
            except Exception as e:
                logging.warning(f"Error processing percentage column {col_idx}: {e}")
        
        # --- 4. Set column widths for better readability ---
        # Apply appropriate column widths based on content
        for header, col_idx in header_positions.items():
            try:
                # Ensure col_idx is an integer
                col_idx = int(col_idx) if not isinstance(col_idx, int) else col_idx
                col_letter = get_column_letter(col_idx)
                
                # Set a minimum width for all columns
                min_width = 12
                
                # Wider columns for specific types
                if '상품명' in header:
                    sheet.column_dimensions[col_letter].width = 40  # Product names can be long
                elif '상품링크' in header or '링크' in header:
                    sheet.column_dimensions[col_letter].width = 30  # Links can be long
                elif '이미지' in header:
                    sheet.column_dimensions[col_letter].width = 25  # Image columns
                    # Also set row height for image rows
                    for row in range(data_start_row, max_row + 1):
                        sheet.row_dimensions[row].height = 120  # Taller rows for images
                else:
                    # Set width based on content
                    max_length = 0
                    for row in range(header_row, min(header_row + 20, max_row + 1)):
                        cell = sheet.cell(row=row, column=col_idx)
                        if cell.value:
                            try:
                                max_length = max(max_length, len(str(cell.value)))
                            except:
                                continue
                
                    adjusted_width = max(min_width, max_length + 2)
                    sheet.column_dimensions[col_letter].width = min(adjusted_width, 50)  # Cap at 50
            except Exception as e:
                logging.warning(f"Error setting column width for header {header} (col_idx={col_idx}): {e}")
        
        # --- 5. Save the styled file ---
        workbook.save(file_path)
        logging.info(f"Excel styling successfully applied to {file_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error applying Excel styles to {file_path}: {e}", exc_info=True)
        return False


def process_image_cells(worksheet, image_columns=None):
    """
    Process image cells in an Excel worksheet to display images.
    
    Args:
        worksheet (openpyxl.worksheet.worksheet.Worksheet): The Excel worksheet to process
        image_columns (list, optional): List of column names that contain images. 
                                       If None, will detect columns with 'image' or '이미지' in the name.
    
    Returns:
        bool: True if processing was successful, False otherwise
    """
    logging.info("Processing image cells in Excel worksheet")
    
    try:
        # Find header row (usually row 1)
        header_row = 1
        header_cells = {}
        
        # Map column indices to column names
        for col_idx, cell in enumerate(worksheet[header_row], 1):
            if cell.value:
                header_cells[col_idx] = cell.value
        
        # If no image columns specified, detect them based on column name
        if image_columns is None:
            image_columns = []
            for idx, name in header_cells.items():
                if name and any(keyword in str(name) for keyword in ['이미지', 'image', 'Image']):
                    image_columns.append(name)
        
        logging.info(f"Processing image columns: {image_columns}")
        
        # Get column indices for image columns
        image_col_indices = [idx for idx, name in header_cells.items() if name in image_columns]
        
        if not image_col_indices:
            logging.warning("No image columns found in worksheet")
            return False
        
        processed_cells = 0
        
        # Process each data row
        for row_idx in range(2, worksheet.max_row + 1):  # Skip header row
            for col_idx in image_col_indices:
                cell = worksheet.cell(row=row_idx, column=col_idx)
                
                # Skip empty cells
                if not cell.value or cell.value == '-' or cell.value == 'nan':
                    continue
                
                try:
                    image_path = str(cell.value).strip()
                    
                    # Handle URL images
                    if image_path.startswith(('http://', 'https://')):
                        logging.debug(f"Processing URL image: {image_path}")
                        
                        # For URLs, we'll make them clickable
                        cell.hyperlink = image_path
                        cell.value = "View Image"
                        cell.font = openpyxl.styles.Font(color="0563C1", underline="single")
                        processed_cells += 1
                        
                    # Handle local file paths
                    else:
                        logging.debug(f"Processing local image: {image_path}")
                        
                        # Check if file exists
                        if not os.path.exists(image_path):
                            logging.warning(f"Image file does not exist: {image_path}")
                            # Try to search for the file in common image locations if not found
                            image_found = False
                            possible_locations = [
                                os.path.join('C:\\RPA\\Image\\Main', os.path.basename(image_path)),
                                os.path.join('C:\\RPA\\Image\\Target', os.path.basename(image_path))
                            ]
                            
                            for possible_path in possible_locations:
                                if os.path.exists(possible_path):
                                    image_path = possible_path
                                    image_found = True
                                    logging.info(f"Found image in alternate location: {image_path}")
                                    break
                                    
                            if not image_found:
                                cell.value = "Missing Image"
                                continue
                        
                        # Create IMAGE formula for Excel
                        # First, escape the path properly
                        safe_path = image_path.replace("\\", "\\\\")
                        
                        # Create the formula - most reliable is to use the 4 option (fit with size)
                        image_formula = f'=IMAGE("{safe_path}",4)'
                        
                        # Apply the formula to the cell
                        cell.value = image_formula
                        processed_cells += 1
                        
                except Exception as e:
                    logging.error(f"Error processing image cell at row {row_idx}, column {col_idx}: {e}")
        
        logging.info(f"Successfully processed {processed_cells} image cells")
        return True
        
    except Exception as e:
        logging.error(f"Error in process_image_cells: {e}")
        return False


def add_header_footer(sheet):
    """Add header and footer to the worksheet."""
    try:
        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        
        # Add header with company name
        sheet.oddHeader.left.text = "해오름 RPA 시스템"
        sheet.oddHeader.center.text = "가격비교 결과"
        sheet.oddHeader.right.text = f"생성일: {current_date}"
        
        # Add footer with page numbers
        sheet.oddFooter.left.text = "해오름 RPA 가격 비교"
        sheet.oddFooter.right.text = "페이지 &P / &N"
        
        logging.debug("Added header and footer to worksheet")
    except Exception as e:
        logging.warning(f"Could not set header/footer: {e}")


def apply_table_format(sheet, max_row, max_col):
    """Apply Excel table formatting to data range."""
    if max_row <= 1:
        return  # No data to format
        
    # Define table range
    table_range = f"A1:{get_column_letter(max_col)}{max_row}"
    
    # Create a new Table
    table = Table(displayName="PriceComparisonTable", ref=table_range)
    
    # Add a default style
    style = TableStyleInfo(
        name="TableStyleMedium2", 
        showFirstColumn=False,
        showLastColumn=False, 
        showRowStripes=True, 
        showColumnStripes=False
    )
    table.tableStyleInfo = style
    
    # Add the table to the sheet
    sheet.add_table(table)
    logging.debug(f"Applied table formatting to range {table_range}")


# --- Hyperlink Logic ---

def add_hyperlinks(file_path, link_column_map):
    """
    Add hyperlinks to cells based on link_column_map.
    
    Args:
        file_path: Path to the Excel file
        link_column_map: Dict mapping display column names to link column names
    
    Returns:
        True if successful, False otherwise
    """
    try:
        if not os.path.exists(file_path):
            logging.error(f"Excel file not found for hyperlinking: {file_path}")
            return False
            
        logging.info(f"Adding hyperlinks to Excel file: {file_path}")
        
        workbook = load_workbook(file_path)
        sheet = workbook.active
        
        # Find header row (could be row 1 or row 4/5 if there are explanation headers)
        header_row = 1
        if sheet.cell(row=1, column=1).value == "[알림]":
            # Check if we have 3 or 4 header rows (depends on if we added the error message explanation)
            if sheet.cell(row=4, column=1).value:  # Something in the 4th row
                header_row = 5  # New format with error explanation
            else:
                header_row = 4  # Original format
        
        # Get column indices
        column_indices = {}
        max_col = sheet.max_column
        
        for col_idx in range(1, max_col + 1):
            header_cell = sheet.cell(row=header_row, column=col_idx)
            if header_cell.value in link_column_map:
                column_indices[header_cell.value] = col_idx
                
        if not column_indices:
            logging.warning(f"No link columns found in {file_path}")
            return False
            
        logging.info(f"Found link columns: {list(column_indices.keys())}")
        
        # Get all error messages that should not be processed as links
        error_messages = list(ERROR_MESSAGES.values())
        
        # Define font styles for different types of links
        link_font = Font(color="0000FF", underline="single")  # Blue underlined
        invalid_link_font = Font(color="FF0000")  # Red
        
        # Process each row
        link_added_count = 0
        invalid_link_count = 0
        
        for row_idx in range(header_row + 1, sheet.max_row + 1):
            for display_col_name, display_col_idx in column_indices.items():
                # Skip processing image columns (they're handled by process_image_cells)
                if display_col_name in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
                    continue
                    
                display_cell = sheet.cell(row=row_idx, column=display_col_idx)
                display_text = display_cell.value
                
                # Skip empty cells, placeholders, cells with formulas, or cells with error messages
                if (not display_text or 
                    str(display_text).strip() in ['-', '', 'N/A'] or 
                    str(display_text).startswith('=') or
                    any(msg in str(display_text) for msg in error_messages)):
                    continue
                
                try:
                    # Get link target
                    link_target = display_text  # Default to using the text as the link
                    
                    # Check if it's a URL and add hyperlink
                    if isinstance(link_target, str):
                        if link_target.startswith(('http://', 'https://')):
                            # Validate URL format with regex
                            url_pattern = re.compile(
                                r'^(?:http|https)://'  # http:// or https://
                                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain
                                r'localhost|'  # localhost
                                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or ipv4
                                r'(?::\d+)?'  # optional port
                                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
                            
                            if url_pattern.match(link_target):
                                # Add hyperlink
                                display_cell.hyperlink = link_target
                                
                                # Style the cell as a hyperlink
                                display_cell.font = link_font
                                
                                link_added_count += 1
                            else:
                                # Invalid URL format
                                logging.warning(f"Invalid URL format: {link_target}")
                                # Style as invalid but don't change the text
                                display_cell.font = invalid_link_font
                                invalid_link_count += 1
                        elif '://' in link_target:
                            # Non-HTTP URL scheme (like ftp://)
                            # Add hyperlink but mark differently
                            display_cell.hyperlink = link_target
                            display_cell.font = Font(color="800080", underline="single")  # Purple
                            link_added_count += 1
                        
                except Exception as cell_err:
                    logging.warning(f"Error processing cell R{row_idx}C{display_col_idx} for hyperlink: {cell_err}")
                    # Style as error but don't change the text
                    display_cell.font = invalid_link_font
                    invalid_link_count += 1
        
        workbook.save(file_path)
        logging.info(f"Added {link_added_count} hyperlinks to {file_path} ({invalid_link_count} invalid links detected)")
        return True
        
    except Exception as e:
        logging.error(f"Error adding hyperlinks to Excel file {file_path}: {e}", exc_info=True)
        return False


# --- Main Output Function ---

@safe_excel_operation
def create_final_output_excel(df, output_path):
    """
    Create the final Excel file with proper formatting and styling.
    
    Args:
        df: DataFrame containing the formatted data
        output_path: Path where the Excel file should be saved
        
    Returns:
        Path to the created Excel file
    """
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Create a Pandas Excel writer using XlsxWriter as the engine
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        
        # Convert the dataframe to an XlsxWriter Excel object
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1
        })
        
        # Format numeric columns
        numeric_columns = [
            '판매단가(V포함)', '판매단가(V포함)(2)', '판매단가(V포함)(3)',
            '가격차이(2)', '가격차이(2)(%)', '가격차이(3)', '가격차이(3)(%)'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                col_idx = df.columns.get_loc(col)
                worksheet.set_column(col_idx, col_idx, 15, workbook.add_format({'num_format': '#,##0'}))
        
        # Format text columns
        text_columns = [
            '상품명', '업체명', '공급사명', '고려기프트 상품링크',
            '네이버 쇼핑 링크', '공급사 상품링크'
        ]
        
        for col in text_columns:
            if col in df.columns:
                col_idx = df.columns.get_loc(col)
                worksheet.set_column(col_idx, col_idx, 30)
        
        # Format image columns
        image_columns = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
        for col in image_columns:
            if col in df.columns:
                col_idx = df.columns.get_loc(col)
                worksheet.set_column(col_idx, col_idx, 50)
        
        # Write the column headers with the defined format
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Auto-adjust columns' width
        for idx, col in enumerate(df):
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),
                len(str(series.name))
            )) + 1
            worksheet.set_column(idx, idx, max_len)
        
        # Save the Excel file
        writer.close()
        
        logging.info(f"Successfully created Excel file at: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error creating Excel file: {str(e)}")
        raise