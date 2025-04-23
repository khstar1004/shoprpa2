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
    """Format matched data for final output, ensuring all required columns and image URLs."""
    
    # Deep copy to avoid modifying original
    df = input_df.copy()
    
    # Store original columns to check what existed in input
    original_columns = df.columns.tolist()
    
    # 필수 컬럼 목록 - 최종 결과에 반드시 포함되어야 하는 컬럼
    required_columns = ['기본수량(1)', '판매단가(V포함)']
    
    # 필수 컬럼 확인 및 추가 로직 강화
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.warning(f"Initial check found missing required columns: {missing_columns}. Attempting to add them.")
        for col in missing_columns:
            if col == '기본수량(1)':
                if '본사 기본수량' in df.columns:
                    df['기본수량(1)'] = df['본사 기본수량']
                    logging.info(f"Added missing column '{col}' by copying from '본사 기본수량'.")
                else:
                    df[col] = '-' # Default to '-' if fallback is missing
                    logging.warning(f"Added missing column '{col}' with default value '-' as '본사 기본수량' was also missing.")
            elif col == '판매단가(V포함)':
                if '판매단가' in df.columns:
                    df['판매단가(V포함)'] = df['판매단가']
                    logging.info(f"Added missing column '{col}' by copying from '판매단가'.")
                else:
                    df[col] = '-' # Default to '-' if fallback is missing
                    logging.warning(f"Added missing column '{col}' with default value '-' as '판매단가' was also missing.")

    # 최종 확인: 필수 컬럼이 여전히 누락되었는지 확인 (추가 시도 후)
    missing_columns_after_add = [col for col in required_columns if col not in df.columns]
    if missing_columns_after_add:
        # 에러 대신 경고 로깅하고 기본값으로 채우기 시도
        logging.warning(f"Input DataFrame is STILL missing required columns after attempting to add them: {missing_columns_after_add}. Filling with '-' or NaN.")
        for col in missing_columns_after_add:
             # Ensure column exists, fill with default value
             df[col] = df.get(col, pd.NA) # Use pd.NA for missing values, or '-' if appropriate

    # --- Standardize column names if needed ---
    # Add mapping for common column name variations
    column_name_map = {
        # 'Code': '상품코드', # 최종 출력에 Code 컬럼이 필요하므로 주석 처리
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
        '구분': None,
        '담당자': None,
        '업체명': None,
        '업체코드': None,
        'Code': None,
        '중분류카테고리': None,
        '상품명': None,
        '기본수량(1)': None,
        '판매단가(V포함)': None,
        '본사상품링크': None,
        '기본수량(2)': None,
        '판매가(V포함)(2)': None,
        '판매단가(V포함)(2)': None,
        '가격차이(2)': None,
        '가격차이(2)(%)': None,
        '고려기프트 상품링크': None,
        '기본수량(3)': None,
        '판매단가(V포함)(3)': None,
        '가격차이(3)': None,
        '가격차이(3)(%)': None,
        '공급사명': None,
        '네이버 쇼핑 링크': None,
        '공급사 상품링크': None,
        '본사 이미지': None,
        '고려기프트 이미지': None,
        '네이버 이미지': None
    }
    
    # 보호해야 할 초기 입력 컬럼 목록 (표준화된 이름 기준)
    protected_initial_columns = {
        'Code', # 상품코드 대신 Code 보호
        '상품명', '기본수량(1)', '판매단가(V포함)',
        # 사용자가 언급한 다른 초기 컬럼들도 추가 (만약 expected_output_columns에 포함될 경우 대비)
        '구분', '담당자', '업체명', '업체코드', '중분류카테고리', '본사상품링크' 
    }

    # Add missing columns with defaults, ONLY if they were not in the original input
    # AND are not protected initial columns
    for col, default_value in expected_output_columns.items():
        if col not in df.columns:
            # 보호해야 할 컬럼인 경우, 기본값 할당 로직 건너뛰기
            if col in protected_initial_columns:
                logging.warning(f"Protected initial column '{col}' was missing. Skipping default assignment.")
                # 필요하다면 None으로 추가할 수는 있음 (현재는 그냥 건너뛰기)
                # df[col] = None 
                continue 

            # Check if the column was present in the original input DataFrame
            if col not in original_columns:
                logging.warning(f"Column '{col}' was missing from original input and current df. Adding with default: {default_value}")
                df[col] = default_value
            else:
                # Column existed originally but is now missing. This indicates a potential issue.
                # Log a warning, but don't add a default to avoid overwriting potentially recoverable data.
                # Consider adding it back with None/NaN or a specific placeholder if necessary.
                logging.warning(f"Column '{col}' existed in original input but is now missing. Not applying default.")
                df[col] = None # Or pd.NA, or '-' depending on desired handling

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
    
    # Add Kogift data from crawl results if available
    if kogift_results:
        kogift_update_count = 0
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if product_name in kogift_results:
                # Get first matching result from Kogift
                kogift_data = kogift_results[product_name]
                if kogift_data and len(kogift_data) > 0:
                    item = kogift_data[0]  # Use the first match
                    
                    # Update Kogift related columns
                    if '기본수량(2)' in df.columns and 'quantity' in item:
                        df.at[idx, '기본수량(2)'] = item['quantity']
                    if '판매단가(V포함)(2)' in df.columns and 'price' in item:
                        df.at[idx, '판매단가(V포함)(2)'] = item['price']
                    if '고려기프트 상품링크' in df.columns and 'link' in item:
                        df.at[idx, '고려기프트 상품링크'] = item['link']
                    if '고려기프트 이미지' in df.columns and 'image_path' in item:
                        df.at[idx, '고려기프트 이미지'] = item['image_path']
                    
                    kogift_update_count += 1
        
        logging.info(f"Updated {kogift_update_count} rows with Kogift data")
                            
    # Add Naver data from crawl results if available
    if naver_results:
        naver_update_count = 0
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if product_name in naver_results:
                # Get first matching result from Naver
                naver_data = naver_results[product_name]
                if naver_data and len(naver_data) > 0:
                    item = naver_data[0]  # Use the first match
                    
                    # Update Naver related columns
                    if '기본수량(3)' in df.columns and 'quantity' in item:
                        df.at[idx, '기본수량(3)'] = item['quantity']
                    if '판매단가(V포함)(3)' in df.columns and 'price' in item:
                        df.at[idx, '판매단가(V포함)(3)'] = item['price']
                    if '네이버 쇼핑 링크' in df.columns and 'link' in item:
                        df.at[idx, '네이버 쇼핑 링크'] = item['link']
                    if '공급사 상품링크' in df.columns and 'seller_link' in item:
                        df.at[idx, '공급사 상품링크'] = item['seller_link']
                    if '공급사명' in df.columns and 'seller_name' in item:
                        df.at[idx, '공급사명'] = item['seller_name']
                    if '네이버 이미지' in df.columns and 'image_path' in item:
                        df.at[idx, '네이버 이미지'] = item['image_path']
                    
                    naver_update_count += 1
        
        logging.info(f"Updated {naver_update_count} rows with Naver data")
    
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
    
    # Ensure image paths exist and are valid
    for img_col in ['네이버 이미지', '고려기프트 이미지', '본사 이미지']:
        if img_col in df.columns:
            df[img_col] = df[img_col].apply(
                lambda x: str(x) if pd.notna(x) and os.path.exists(str(x)) 
                and os.path.getsize(str(x)) > 0 else '-'
            )
    
    # --- Final formatting and cleanup ---
    # Convert NaN values to None/empty for cleaner Excel output
    df = df.replace({pd.NA: None, np.nan: None})
    
    logging.info(f"Data formatting complete. Output rows: {len(df)}")
    return df

def process_input_data(df: pd.DataFrame, config: Optional[configparser.ConfigParser] = None, 
                    kogift_results: Optional[Dict[str, List[Dict]]] = None,
                    naver_results: Optional[Dict[str, List[Dict]]] = None) -> pd.DataFrame:
    """
    Process input DataFrame with necessary data processing steps.
    
    Args:
        df: Input DataFrame to process
        config: Configuration object
        kogift_results: Dictionary mapping product names to Kogift crawl results
        naver_results: Dictionary mapping product names to Naver crawl results
    
    Returns:
        Processed DataFrame
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
            
        # Format data for output using provided crawl results
        formatted_df = format_product_data_for_output(
            filtered_df,
            kogift_results=kogift_results or {},
            naver_results=naver_results or {}
        )
        
        # Create output directory if it doesn't exist
        output_dir = config.get('Paths', 'output_dir')
        os.makedirs(output_dir, exist_ok=True)
        
        return formatted_df
        
    except Exception as e:
        logging.error(f"Error in process_input_data: {e}", exc_info=True)
        return df 