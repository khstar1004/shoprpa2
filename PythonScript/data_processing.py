import os
import glob
import logging
import pandas as pd
from datetime import datetime
import configparser
from excel_utils import create_final_output_excel, FINAL_COLUMN_ORDER
import re
import time
from typing import Optional, Tuple, Dict, List
import numpy as np

def process_input_file(config: configparser.ConfigParser) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Processes the main input Excel file, reading config with ConfigParser."""
    try:
        input_dir = config.get('Paths', 'input_dir')
    except configparser.Error as e:
        logging.error(f"Error reading configuration for input processing: {e}. Cannot proceed.")
        return None, None
        
    logging.info(f"Checking for input file in {input_dir}")
    start_time = time.time()

    try:
        excel_files = glob.glob(os.path.join(input_dir, '*.xlsx'))
        excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~')]

        if not excel_files:
            logging.warning(f"No Excel (.xlsx) file found in {input_dir}.")
            return None, None

        # Process only the first found Excel file
        input_file = excel_files[0]
        input_filename = os.path.basename(input_file)
        logging.info(f"Processing input file: {input_file}")

        required_cols = ['Code', '상품명', '본사상품링크', '구분']

        # Read the entire Excel file at once
        df = pd.read_excel(input_file, sheet_name=0)
        logging.info(f"Read {len(df)} rows from '{input_filename}'")

        # Check for required columns
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Input file '{input_filename}' missing columns: {missing_cols}.")
            return None, input_filename

        read_time = time.time() - start_time
        logging.info(f"Read {len(df)} rows from '{input_filename}' in {read_time:.2f} sec.")
        return df, input_filename

    except FileNotFoundError:
        logging.error(f"Input file {input_file} not found during read attempt.")
        return None, None
    except Exception as e:
        logging.error(f"Error reading Excel '{input_file}': {e}", exc_info=True)
        return None, input_filename

def filter_results(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """결과 데이터프레임 필터링"""
    if df.empty:
        return df
        
    # 가격 차이 필터링
    price_diff_threshold = config.getfloat('PriceDifference', 'threshold', fallback=0.1)
    
    # 고려 가격 차이 필터링
    if '고려_가격차이' in df.columns:
        df = df[df['고려_가격차이'].abs() <= price_diff_threshold]
        
    # 네이버 가격 차이 필터링
    if '네이버_가격차이' in df.columns:
        df = df[df['네이버_가격차이'].abs() <= price_diff_threshold]
        
    # 매칭 품질 필터링
    quality_threshold = config.getfloat('MatchQualityThresholds', 'low_quality', fallback=0.50)
    
    # 고려 매칭 품질 필터링
    if '고려_매칭품질' in df.columns:
        df = df[df['고려_매칭품질'].isin(['high', 'medium', 'low'])]
        
    # 네이버 매칭 품질 필터링
    if '네이버_매칭품질' in df.columns:
        df = df[df['네이버_매칭품질'].isin(['high', 'medium', 'low'])]
        
    return df

# Note: save_and_format_output and format_output_file were likely replaced by
# create_final_output_excel in excel_utils.py. We remove them here to avoid duplication.
# If they are still needed, they should also be updated to use ConfigParser.

# def save_and_format_output(df, input_filename_base, config: configparser.ConfigParser, progress_queue=None):
#     """(DEPRECATED - Functionality moved to excel_utils.create_final_output_excel)
#        Saves the final DataFrame to an Excel file and applies formatting.
#     """
#     try:
#         output_dir = config.get('Paths', 'output_dir')
#     except configparser.Error as e:
#          logging.error(f"Cannot save output: Error reading output_dir from config: {e}")
#          return None
#          
#     # ... (rest of the saving logic, using config parser where needed) ...
#     # Make sure to call styling/hyperlink functions from excel_utils which should use openpyxl
#     pass

# def format_output_file(file_path, config: configparser.ConfigParser, progress_queue=None):
#     """(DEPRECATED - Functionality moved to excel_utils.create_final_output_excel)
#        Applies final formatting using external utility function.
#     """
#     pass 

def format_product_data_for_output(input_df: pd.DataFrame, 
                             kogift_results: Dict[str, List[Dict]] = None, 
                             naver_results: Dict[str, List[Dict]] = None) -> pd.DataFrame:
    """Format matched data for final output, ensuring all required columns and image URLs.
    
    Args:
        input_df: Input DataFrame with matched products
        kogift_results: Dictionary of Kogift results by product name
        naver_results: Dictionary of Naver results by product name
        
    Returns:
        Formatted DataFrame ready for Excel output
    """
    logging.info(f"Starting data formatting. Input rows: {len(input_df)}")
    
    # Deep copy to avoid modifying original
    df = input_df.copy()
    
    # 필수 컬럼 목록 - 최종 결과에 반드시 포함되어야 하는 컬럼
    required_columns = ['기본수량(1)', '판매단가(V포함)']
    
    # 필수 컬럼 추가 (누락된 경우)
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.warning(f"일부 필수 컬럼이 누락되어 있습니다: {missing_columns}")
        
        # 필수 컬럼 추가 - 누락된 컬럼을 기본값으로 추가
        for col in missing_columns:
            if col == '기본수량(1)':
                # 본사 기본수량 컬럼이 있으면 그 값을 사용, 없으면 1로 기본값 설정
                df['기본수량(1)'] = df.get('본사 기본수량', 1)
                logging.info("'기본수량(1)' 컬럼이 추가되었습니다.")
            elif col == '판매단가(V포함)':
                # 다른 가격 컬럼이 있으면 그 값 사용, 없으면 0으로 기본값 설정
                if '판매단가' in df.columns:
                    df['판매단가(V포함)'] = df['판매단가']
                    logging.info("'판매단가(V포함)' 컬럼이 '판매단가'에서 복사되었습니다.")
                else:
                    df['판매단가(V포함)'] = 0
                    logging.info("'판매단가(V포함)' 컬럼이 0값으로 추가되었습니다.")
    
    # 컬럼 확인 (추가 후 다시 확인)
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        # 여전히 누락된 컬럼이 있으면 에러 발생
        logging.error(f"Input DataFrame is missing required columns: {missing_columns}")
        raise ValueError(f"Input DataFrame is missing required columns: {missing_columns}")

    # --- Standardize column names if needed ---
    # Add mapping for common column name variations
    column_name_map = {
        '상품코드': '상품코드',
        'Code': '상품코드',
        '제품코드': '상품코드',
        '상품분류': '상품분류',
        '상품명': '상품명',
        '품명': '상품명',
        '제품명': '상품명'
    }
    
    # Rename columns based on mapping (only if target name doesn't already exist)
    for old_name, new_name in column_name_map.items():
        if old_name in df.columns and new_name not in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
    
    # --- Ensure all expected output columns exist ---
    # Define final columns structure with defaults
    expected_output_columns = {
        '상품코드': None,
        '상품분류': None,
        '상품명': None,
        '본사 이미지': None,
        '기본수량(1)': 1,  # Default to 1 if not present
        '판매단가(V포함)': 0,  # Default to 0 if not present
        '공급사명': None,
        '공급사 상품링크': None,
        '고려 기본수량': None, 
        '판매단가(V포함)(2)': None,
        '고려기프트 상품링크': None,
        '고려기프트 이미지': None,
        '가격차이(2)': None,
        '가격차이(2)(%)': None,
        '네이버 기본수량': None,
        '판매단가(V포함)(3)': None,
        '네이버 쇼핑 링크': None,
        '네이버 이미지': None,
        '가격차이(3)': None,
        '가격차이(3)(%)': None
    }
    
    # Add missing columns with defaults
    for col, default_value in expected_output_columns.items():
        if col not in df.columns:
            df[col] = default_value
    
    # --- Column mapping for different data sources ---
    column_mappings = {
        # Map various internal column names to standardized output names
        '고려 링크': '고려기프트 상품링크',
        '고려기프트(이미지링크)': '고려기프트 이미지',
        '고려 기본수량': '고려 기본수량',
        '판매단가2(VAT포함)': '판매단가(V포함)(2)',
        
        '네이버 공급사명': '공급사명',
        '네이버 링크': '공급사 상품링크',
        '네이버쇼핑(이미지링크)': '네이버 이미지',
        '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
    }
    
    # Apply mappings
    for src_col, dst_col in column_mappings.items():
        if src_col in df.columns and dst_col not in df.columns:
            df[dst_col] = df[src_col]
            
    # --- Process and add images ---
    # Ensure image columns exist and add from crawl results if missing
    if '본사 이미지' not in df.columns:
        df['본사 이미지'] = df.get('해오름이미지경로', None)
        logging.info("Added '본사 이미지' column from downloaded Haeoreum image paths")
    
    # Add Kogift images from crawl results if available
    if kogift_results and '고려기프트 이미지' in df.columns:
        kogift_img_count = 0
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if pd.isna(row['고려기프트 이미지']) and product_name in kogift_results:
                # Get first image from Kogift results
                kogift_data = kogift_results[product_name]
                if kogift_data and len(kogift_data) > 0:
                    for item in kogift_data:
                        if 'image_path' in item and item['image_path']:
                            df.at[idx, '고려기프트 이미지'] = item['image_path']
                            kogift_img_count += 1
                            break
        logging.info(f"Added {kogift_img_count} missing Kogift images from crawl results")
                            
    # Add Naver images from crawl results if available
    if naver_results and '네이버 이미지' in df.columns:
        naver_img_count = 0
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if pd.isna(row['네이버 이미지']) and product_name in naver_results:
                # Get first image from Naver results
                naver_data = naver_results[product_name]
                if naver_data and len(naver_data) > 0:
                    for item in naver_data:
                        if 'image_path' in item and item['image_path']:
                            df.at[idx, '네이버 이미지'] = item['image_path']
                            naver_img_count += 1
                            break
                        elif 'image_url' in item and item['image_url']:
                            df.at[idx, '네이버 이미지'] = item['image_url']
                            naver_img_count += 1
                            break
        logging.info(f"Added {naver_img_count} missing Naver images from crawl results")
    
    # Add additional logic to ensure Haereum images are included
    if '본사 이미지' in df.columns and '해오름이미지경로' in df.columns:
        # Use the Haereum image URL if the original image is missing
        haoreum_img_missing = (df['본사 이미지'].isnull()) | (df['본사 이미지'] == '') | (df['본사 이미지'] == '-')
        # 해오름이미지경로가 존재하는 경우를 체크
        haoreum_path_present = ~(df['해오름이미지경로'].isnull() | (df['해오름이미지경로'] == ''))

        # 본사 이미지가 비어있고 해오름이미지경로가 존재하는 경우 업데이트 마스크
        update_mask = haoreum_img_missing & haoreum_path_present
        if update_mask.any():
            # '본사 이미지' 컬럼에 '해오름이미지경로' 컬럼의 값을 할당
            df.loc[update_mask, '본사 이미지'] = df.loc[update_mask, '해오름이미지경로'].astype(str)
            logging.info(f"Updated {update_mask.sum()} missing '본사 이미지' with downloaded paths from '해오름이미지경로'")
    
    # --- Calculate additional fields ---
    # Calculate price differences if base price exists
    if '판매단가(V포함)' in df.columns:
        # Kogift price difference
        if '판매단가(V포함)(2)' in df.columns:
            df['가격차이(2)'] = df.apply(
                lambda x: pd.to_numeric(x['판매단가(V포함)(2)'], errors='coerce') - 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce') 
                if pd.notna(x['판매단가(V포함)(2)']) and pd.notna(x['판매단가(V포함)']) else None, 
                axis=1
            )
            # Calculate percentage difference
            df['가격차이(2)(%)'] = df.apply(
                lambda x: (pd.to_numeric(x['가격차이(2)'], errors='coerce') / 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce')) * 100 
                if pd.notna(x['가격차이(2)']) and pd.notna(x['판매단가(V포함)']) and 
                   pd.to_numeric(x['판매단가(V포함)'], errors='coerce') != 0 else None, 
                axis=1
            )
            
        # Naver price difference
        if '판매단가(V포함)(3)' in df.columns:
            df['가격차이(3)'] = df.apply(
                lambda x: pd.to_numeric(x['판매단가(V포함)(3)'], errors='coerce') - 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce') 
                if pd.notna(x['판매단가(V포함)(3)']) and pd.notna(x['판매단가(V포함)']) else None, 
                axis=1
            )
            # Calculate percentage difference
            df['가격차이(3)(%)'] = df.apply(
                lambda x: (pd.to_numeric(x['가격차이(3)'], errors='coerce') / 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce')) * 100 
                if pd.notna(x['가격차이(3)']) and pd.notna(x['판매단가(V포함)']) and 
                   pd.to_numeric(x['판매단가(V포함)'], errors='coerce') != 0 else None, 
                axis=1
            )
    
    # --- Final formatting and cleanup ---
    # Convert NaN values to None/empty for cleaner Excel output
    df = df.replace({pd.NA: None, np.nan: None})
    
    logging.info(f"Data formatting complete. Output rows: {len(df)}")
    return df

def process_input_data(df: pd.DataFrame, config: Optional[configparser.ConfigParser] = None) -> pd.DataFrame:
    """
    Process input DataFrame with necessary data processing steps.
    """
    if config is None:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
        config.read(config_path)
    
    try:
        # Apply initial filtering
        filtered_df = filter_results(df, config)
        if filtered_df is None:
            logging.error("Failed to filter results")
            return df
            
        # Format data for output
        kogift_results = {}  # This would normally come from kogift processing
        naver_results = {}   # This would normally come from naver processing
        
        formatted_df = format_product_data_for_output(filtered_df, kogift_results, naver_results)
        
        # Create output directory if it doesn't exist
        output_dir = config.get('Paths', 'output_dir')
        os.makedirs(output_dir, exist_ok=True)
        
        return formatted_df
        
    except Exception as e:
        logging.error(f"Error in process_input_data: {e}", exc_info=True)
        return df 