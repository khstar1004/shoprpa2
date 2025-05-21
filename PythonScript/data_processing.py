import os
import glob
import logging
import pandas as pd
from datetime import datetime
import configparser
from excel_utils import create_final_output_excel, FINAL_COLUMN_ORDER, REQUIRED_INPUT_COLUMNS
import re
import time
from typing import Optional, Tuple, Dict, List, Any
import numpy as np
from pathlib import Path
import ast

def process_input_file(config: configparser.ConfigParser) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Processes the main input Excel file, reading config with ConfigParser."""
    try:
        # Get input directory from config
        input_dir = config.get('Paths', 'input_dir')
        
        # Check if a specific input file is provided in the config
        specific_input_file = config.get('Paths', 'input_file', fallback=None)
        
        if specific_input_file and os.path.exists(specific_input_file):
            # Use the specific input file provided in config
            logging.info(f"Using specific input file from config: {specific_input_file}")
            input_file = specific_input_file
            input_filename = os.path.basename(input_file)
        else:
            # No specific file provided, search in the input directory
            logging.info(f"Checking for input file in {input_dir}")
            excel_files = glob.glob(os.path.join(input_dir, '*.xlsx'))
            excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~')]

            if not excel_files:
                logging.warning(f"No Excel (.xlsx) file found in {input_dir}.")
                return None, None

            # Process only the first found Excel file
            input_file = excel_files[0]
            input_filename = os.path.basename(input_file)
            logging.info(f"Processing input file: {input_file}")
    except configparser.Error as e:
        logging.error(f"Error reading configuration for input processing: {e}. Cannot proceed.")
        return None, None

    start_time = time.time()
    try:
        # Read the entire Excel file at once
        df = pd.read_excel(input_file, sheet_name=0)
        logging.info(f"Read {len(df)} rows from '{input_filename}'")
        
        # Clean column names
        original_columns = df.columns.tolist()
        df.columns = [col.strip().replace('\xa0', '') for col in df.columns]
        cleaned_columns = df.columns.tolist()
        if original_columns != cleaned_columns:
            logging.info(f"Cleaned column names. Original: {original_columns}, Cleaned: {cleaned_columns}")
        logging.info(f"Columns after cleaning: {df.columns.tolist()}")

        # Check for required columns using the imported list
        missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in df.columns]
        if missing_cols:
            logging.error(f"Input file '{input_filename}' missing required columns (defined in excel_utils): {missing_cols}.")
            logging.error(f"Required columns are: {REQUIRED_INPUT_COLUMNS}")
            logging.error(f"Columns found in file: {cleaned_columns}")
            return None, input_filename
        else:
            logging.info(f"All required columns found: {REQUIRED_INPUT_COLUMNS}")

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

def verify_image_data(img_value, img_col_name):
    """Helper function to verify and format image data for the Excel output."""
    try:
        # Handle dictionary format (expected for Naver images)
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
            
        # Handle string dictionary representation (common in pandas serialization)
        elif isinstance(img_value, str) and img_value and img_value != '-':
            # Check if this is a string representation of a dictionary
            img_value = img_value.strip()
            if img_value.startswith('{') and img_value.endswith('}'):
                try:
                    # Try to parse the string as a dictionary
                    img_dict = ast.literal_eval(img_value)
                    if isinstance(img_dict, dict):
                        # If it has a URL, return the parsed dictionary
                        if 'url' in img_dict and img_dict['url']:
                            return img_dict
                except (SyntaxError, ValueError):
                    pass  # If parsing fails, continue with normal string processing
            
            # For URL strings (not file paths)
            if img_value.startswith(('http:', 'https:')):
                # Return a dictionary format for consistency
                source = img_col_name.split()[0].lower()
                return {'url': img_value, 'source': source}

            # For file path strings (absolute paths preferred)
            elif os.path.isabs(img_value) and os.path.exists(img_value) and os.path.getsize(img_value) > 0:
                 # Convert file path to dictionary format for consistency
                 source = img_col_name.split()[0].lower()
                 # Try to construct a placeholder URL if none provided (might be inaccurate)
                 img_value_str = img_value.replace(os.sep, '/')
                 placeholder_url = f"file:///{img_value_str}"
                 return {'url': placeholder_url, 'local_path': img_value, 'original_path': img_value, 'source': source}
            # Handle relative paths (less ideal, try to resolve)
            elif not os.path.isabs(img_value):
                 try:
                     # Attempt to resolve relative to a base path (e.g., project root or specific image dir)
                     # This is a guess - adjust base_path as needed
                     base_path = Path('C:/RPA/Image/Main') # Example base path
                     abs_path = (base_path / img_value).resolve()
                     if abs_path.exists() and abs_path.stat().st_size > 0:
                         source = img_col_name.split()[0].lower()
                         abs_path_str = str(abs_path).replace('\\', '/')
                         placeholder_url = f"file:///{abs_path_str}"
                         return {'url': placeholder_url, 'local_path': str(abs_path), 'original_path': str(abs_path), 'source': source}
                 except Exception:
                     pass # Path resolution failed

            return '-'  # Invalid path or URL string

        return '-'  # None, NaN, empty string, etc.
    except Exception as e:
        logging.warning(f"Error verifying image data '{str(img_value)[:100]}...' for column {img_col_name}: {e}")
        return '-'  # Return placeholder on error

def format_product_data_for_output(input_df: pd.DataFrame, 
                             kogift_results: Dict[str, List[Dict]] = None, 
                             naver_results: Dict[str, List[Dict]] = None,
                             input_file_image_map: Dict[str, Any] = None,
                             haereum_image_url_map: Dict[str, str] = None) -> pd.DataFrame:
    """Format matched data for final output, ensuring all required columns and image URLs/dicts."""
    
    # Create a copy to avoid modifying the input
    df = input_df.copy()
    
    # Ensure required columns exist
    for col in ['상품명', '판매단가(V포함)']:
        if col not in df.columns:
            df[col] = None
            logging.warning(f"Adding missing required column: {col}")
    
    # Add columns for Kogift results if they don't exist yet
    # FIXED: Ensure 판매단가(V포함)(2) column exists explicitly for kogift prices
    for col in ['기본수량(2)', '판매단가(V포함)(2)', '판매가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크', '고려기프트 이미지']:
        if col not in df.columns:
            df[col] = None
            logging.debug(f"Adding column for Kogift data: {col}")
    
    # Add columns for Naver results if they don't exist yet
    for col in ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '네이버 쇼핑 링크', '공급사명', '공급사 상품링크', '네이버 이미지']:
        if col not in df.columns:
            df[col] = None
            logging.debug(f"Adding column for Naver data: {col}")
    
    # Initialize the image columns with proper dictionary format where applicable
    for img_col in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
        if img_col in df.columns:
            # Ensure we have a valid column (not None)
            df[img_col] = df[img_col].apply(lambda x: {} if pd.isna(x) or x is None else x)
            
    # Process input file image map (해오름 이미지)
    if (input_file_image_map or haereum_image_url_map) and '본사 이미지' in df.columns:
        haoreum_img_count = 0
        for idx, row in df.iterrows():
            product_code = row.get('Code')
            product_name = row.get('상품명')
            
            img_path = None
            web_url = None
            
            # 1. Try getting the URL from the crawled map first (most reliable source)
            if haereum_image_url_map and product_name in haereum_image_url_map and haereum_image_url_map[product_name]:
                web_url = haereum_image_url_map[product_name]
                logging.debug(f"Row {idx} ('{product_name}'): Using crawled Haoreum URL: {web_url}")
            
            # 2. Get local path from input file map (if available)
            if input_file_image_map and product_code and product_code in input_file_image_map:
                img_path = input_file_image_map[product_code]
            
            # Only proceed if we have either a valid URL or a valid path
            if web_url or (img_path and os.path.exists(img_path)):
                # 이미지 정보가 있는 경우에만 처리
                # 로컬 경로 및 URL 정보를 포함하는 딕셔너리 생성
                img_data = {
                    'source': 'haereum' # CHANGED source name for consistency
                }
                
                if img_path and os.path.exists(img_path):
                    img_data['local_path'] = img_path
                    img_data['original_path'] = img_path # Add original path
                
                # Use the reliably crawled URL if found
                if web_url:
                    img_data['url'] = web_url
                # If no web_url found via crawling, try to get it from the row (less reliable)
                elif '해오름이미지URL' in row and isinstance(row['해오름이미지URL'], str) and row['해오름이미지URL'].startswith(('http://', 'https://')):
                     img_data['url'] = row['해오름이미지URL']
                     logging.debug(f"Row {idx} ('{product_name}'): Using Haoreum URL from existing DF column as fallback.")
                # If still no URL, leave it blank in the dictionary
                elif 'url' not in img_data:
                    img_data['url'] = '' 
                
                df.at[idx, '본사 이미지'] = img_data
                haoreum_img_count += 1
        
        logging.info(f"Added {haoreum_img_count} 해오름 images from input file image map")

    # Add Kogift data from crawl results if available
    if kogift_results:
        kogift_update_count = 0
        kogift_img_count = 0
        kogift_price_count = 0
        
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if product_name in kogift_results:
                # Get first matching result from Kogift
                kogift_data = kogift_results[product_name]
                if kogift_data and len(kogift_data) > 0:
                    item = kogift_data[0]  # Use the first match
                    
                    # Update Kogift related columns
                    # 기본수량(2) should match 기본수량(1) for direct price comparison
                    if '기본수량(2)' in df.columns:
                        # Copy the value from 기본수량(1)
                        if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                            df.at[idx, '기본수량(2)'] = row['기본수량(1)']
                        # If quantity exists in the item, only use it as a fallback
                        elif 'quantity' in item:
                            df.at[idx, '기본수량(2)'] = item['quantity']
                    
                    # 기본수량 칼럼도 동일하게 기본수량(1)에서 복사 (요구사항)
                    if '기본수량' in df.columns:
                        if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                            df.at[idx, '기본수량'] = row['기본수량(1)']
                    
                    # Kogift 제품 URL 업데이트
                    if '고려기프트 상품링크' in df.columns:
                        if 'link' in item:
                            df.at[idx, '고려기프트 상품링크'] = item['link']
                        elif 'href' in item:
                            df.at[idx, '고려기프트 상품링크'] = item['href']
                    
                    # 판매가 정보 업데이트
                    if '판매단가(V포함)(2)' in df.columns:
                        # 부가세 포함 가격이 먼저 있는지 확인
                        if 'price_with_vat' in item:
                            df.at[idx, '판매단가(V포함)(2)'] = item['price_with_vat']
                            kogift_price_count += 1
                        # 없으면 일반 가격에 1.1 곱해서 부가세 계산
                        elif 'price' in item:
                            df.at[idx, '판매단가(V포함)(2)'] = round(item['price'] * 1.1)
                            kogift_price_count += 1
                    
                    # 동일한 가격 정보를 판매가(V포함)(2)에도 복사
                    if '판매가(V포함)(2)' in df.columns and '판매단가(V포함)(2)' in df.columns and pd.notna(df.at[idx, '판매단가(V포함)(2)']):
                        df.at[idx, '판매가(V포함)(2)'] = df.at[idx, '판매단가(V포함)(2)']
                        
                    # 이미지 URL 업데이트
                    # 이미지 URL 파악 (우선순위 순서대로 시도)
                    image_url = None
                    if 'image_url' in item and item['image_url']:
                        image_url = item['image_url']
                    elif 'image_path' in item and isinstance(item['image_path'], str) and item['image_path']:
                        image_url = item['image_path']
                    elif 'src' in item and item['src']:
                        image_url = item['src']
                    
                    # FIX: Check for 'local_image_path' which is stored by the Kogift image downloader
                    local_image_path = None
                    if 'local_image_path' in item and item['local_image_path']:
                        local_image_path = item['local_image_path']
                    
                    # Add Kogift image information if URL is available
                    if (image_url or local_image_path) and '고려기프트 이미지' in df.columns:
                        # 이미지 데이터 사전 생성
                        if image_url:
                            # 이미지 URL 확인 (확장자 검증)
                            is_valid_img = any(ext in image_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp'])
                            
                            if is_valid_img:
                                # 로컬 경로 추정 (다운로드된 이미지가 있을 경우)
                                filename = os.path.basename(image_url)
                                base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                                
                                # 가능한 로컬 경로 목록 생성
                                possible_local_paths = [
                                    os.path.join(base_img_dir, 'Main', 'Kogift', filename),
                                    os.path.join(base_img_dir, 'Main', 'Kogift', f"kogift_{filename}"),
                                    os.path.join(base_img_dir, 'Main', 'kogift', filename),
                                    os.path.join(base_img_dir, 'Main', 'kogift', f"kogift_{filename}"),
                                    os.path.join(base_img_dir, 'Kogift', filename),
                                    os.path.join(base_img_dir, 'Kogift', f"kogift_{filename}")
                                ]
                                
                                # 존재하는 로컬 파일 확인
                                local_path = local_image_path  # FIX: Use the downloaded path if available
                                
                                # If local_image_path wasn't available, search for the file 
                                if not local_path:
                                    for path in possible_local_paths:
                                        if os.path.exists(path):
                                            local_path = path
                                            break
                                
                                # 이미지 데이터 사전 생성
                                img_dict = {
                                    'url': image_url,
                                    'original_url': image_url,  # 추가: 원본 URL 보관
                                    'source': 'kogift',
                                    'product_name': product_name
                                }
                                
                                # 로컬 경로가 있으면 추가
                                if local_path:
                                    img_dict['local_path'] = local_path
                                    img_dict['original_path'] = local_path  # Add original_path for better compatibility
                                
                                df.at[idx, '고려기프트 이미지'] = img_dict
                                kogift_img_count += 1
                                logging.debug(f"Kogift 이미지 URL 추가: '{product_name}': {image_url[:50]}...")
                            else:
                                logging.warning(f"유효하지 않은 Kogift 이미지 URL: {image_url[:50]}...")
                        elif 'image_path' in item and not isinstance(item['image_path'], str) and item['image_path']:
                            # 이미지 경로가 문자열이 아닌 객체인 경우 (이미 사전 형태일 수 있음)
                            img_dict = item['image_path']
                            if isinstance(img_dict, dict):
                                if 'source' not in img_dict:
                                    img_dict['source'] = 'kogift'
                                if 'product_name' not in img_dict:
                                    img_dict['product_name'] = product_name
                                
                                # FIX: Explicitly add original_path for better compatibility
                                if 'local_path' in img_dict and 'original_path' not in img_dict:
                                    img_dict['original_path'] = img_dict['local_path']
                                    
                                df.at[idx, '고려기프트 이미지'] = img_dict
                                kogift_img_count += 1
                    
                    # Store actual price tiers from Kogift crawl
                    if 'product_actual_price_tiers' in item and item['product_actual_price_tiers']:
                        if '고려기프트_실제가격티어' not in df.columns:
                            df['고려기프트_실제가격티어'] = None # Initialize with a type that allows mixed content or strings
                        df.at[idx, '고려기프트_실제가격티어'] = str(item['product_actual_price_tiers'])
                    
                    kogift_update_count += 1
        
        logging.info(f"업데이트된 행 수: {kogift_update_count} (Kogift 데이터)")
        logging.info(f"Kogift 이미지 추가: {kogift_img_count}개")
        logging.info(f"Kogift 가격 추가: {kogift_price_count}개")

        # === DEBUG LOGGING START ===
        if kogift_update_count > 0 and '판매단가(V포함)(2)' in df.columns:
            try:
                sample_indices = df[df['판매단가(V포함)(2)'].notna()].index[:3] # Get first 3 rows with Kogift price
                if not sample_indices.empty:
                    logging.info("[DEBUG] Sample Kogift Price Data after processing:")
                    for idx in sample_indices:
                        product_name = df.at[idx, '상품명']
                        kogift_price = df.at[idx, '판매단가(V포함)(2)']
                        kogift_img_data = df.at[idx, '고려기프트 이미지'] if '고려기프트 이미지' in df.columns else 'N/A'
                        
                        # 이미지 데이터 로깅 형식 개선
                        img_info = "이미지 없음"
                        if isinstance(kogift_img_data, dict):
                            img_info = f"이미지 정보: {{url: '{kogift_img_data.get('url', '없음')[:30]}...'"
                            if 'local_path' in kogift_img_data:
                                img_info += f", local_path: '{kogift_img_data.get('local_path', '없음')[-30:]}...'"
                            img_info += "}"
                        
                        logging.info(f"  - 상품: '{product_name}', Kogift 가격: {kogift_price}, {img_info}")
                else:
                    logging.info("[DEBUG] 처리 후에도 Kogift 가격 데이터가 없습니다 (kogift_update_count > 0 에도 불구하고).")
            except Exception as log_err:
                logging.warning(f"[DEBUG] Kogift 샘플 데이터 로깅 중 오류: {log_err}")
        # === DEBUG LOGGING END ===

    # Add Naver data from crawl results if available
    if naver_results:
        naver_update_count = 0
        naver_img_count = 0
        naver_price_count = 0
        
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if product_name in naver_results:
                # Get first matching result from Naver
                naver_data = naver_results[product_name]
                
                if naver_data and len(naver_data) > 0:
                    # Use the first match
                    item = naver_data[0]  
                    
                    # Update Naver link columns
                    if '네이버 쇼핑 링크' in df.columns:
                        if 'link' in item:
                            df.at[idx, '네이버 쇼핑 링크'] = item['link']
                        elif 'href' in item:
                            df.at[idx, '네이버 쇼핑 링크'] = item['href']
                            
                    # Update Naver seller/mall information
                    if '공급사명' in df.columns:
                        if 'mall_name' in item:
                            df.at[idx, '공급사명'] = item['mall_name']
                        elif 'mallName' in item:
                            df.at[idx, '공급사명'] = item['mallName']
                        elif 'seller_name' in item:
                            df.at[idx, '공급사명'] = item['seller_name']
                        elif 'api_seller_name' in item:
                            df.at[idx, '공급사명'] = item['api_seller_name']
                            
                    if '공급사 상품링크' in df.columns:
                        if 'mall_link' in item:
                            df.at[idx, '공급사 상품링크'] = item['mall_link']
                        elif 'mallProductUrl' in item:
                            df.at[idx, '공급사 상품링크'] = item['mallProductUrl']
                        elif 'seller_link' in item:
                            df.at[idx, '공급사 상품링크'] = item['seller_link']
                    
                    # Update Naver price information (V포함)(3)
                    if '판매단가(V포함)(3)' in df.columns:
                        # Price with VAT
                        if 'price_with_vat' in item:
                            df.at[idx, '판매단가(V포함)(3)'] = item['price_with_vat']
                            naver_price_count += 1
                        # Calculate VAT manually if not included
                        elif 'price' in item:
                            df.at[idx, '판매단가(V포함)(3)'] = round(item['price'] * 1.1)
                            naver_price_count += 1
                    
                    # Update Naver quantity - 기본수량(3) should match 기본수량(1) for direct comparison
                    if '기본수량(3)' in df.columns:
                        # Copy the value from 기본수량(1) for consistent comparison
                        if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                            df.at[idx, '기본수량(3)'] = row['기본수량(1)']
                        # If quantity exists in the item, use it as a fallback
                        elif 'quantity' in item:
                            df.at[idx, '기본수량(3)'] = item['quantity']
                    
                    # Update Naver Image information
                    # Find image URL in the item data
                    image_url = None
                    if 'image_url' in item and item['image_url']:
                        image_url = item['image_url']
                    elif 'image_path' in item and isinstance(item['image_path'], str) and item['image_path']:
                        if item['image_path'].startswith(('http://', 'https://')):
                            image_url = item['image_path']
                            
                    # Get the local path if available
                    local_image_path = None
                    if 'image_path' in item and item['image_path'] and isinstance(item['image_path'], str):
                        if not item['image_path'].startswith(('http://', 'https://')) and os.path.exists(item['image_path']):
                            local_image_path = item['image_path']
                    
                    # Add a fallback to image_data structure if available
                    if not local_image_path and 'image_data' in item and isinstance(item['image_data'], dict):
                        img_data_dict = item['image_data']
                        if 'local_path' in img_data_dict and img_data_dict['local_path']:
                            if isinstance(img_data_dict['local_path'], str) and os.path.exists(img_data_dict['local_path']):
                                local_image_path = img_data_dict['local_path']
                        
                        # Also try to get image URL from image_data if needed
                        if not image_url and 'url' in img_data_dict and img_data_dict['url']:
                            image_url = img_data_dict['url']
                    
                    # Add Naver image information if URL is available
                    if (image_url or local_image_path) and '네이버 이미지' in df.columns:
                        # Create image data dictionary
                        img_dict = {
                            'source': 'naver',
                            'product_name': product_name,
                        }
                        
                        # Add URL if available
                        if image_url:
                            img_dict['url'] = image_url
                            img_dict['original_url'] = image_url  # 추가: 원본 URL 보관
                        
                        # Add local path if available
                        if local_image_path:
                            img_dict['local_path'] = local_image_path
                            img_dict['original_path'] = local_image_path
                        
                        df.at[idx, '네이버 이미지'] = img_dict
                        naver_img_count += 1
                        
                    naver_update_count += 1
        
        logging.info(f"Added data from {naver_update_count} Naver results: {naver_img_count} images, {naver_price_count} price points")
    
    # --- Calculate additional fields ---
    # Calculate price differences if base price exists
    if '판매단가(V포함)' in df.columns:
        # Kogift price difference
        if '판매단가(V포함)(2)' in df.columns:
            # FIXED: Ensure we convert to numeric before calculation
            df['가격차이(2)'] = df.apply(
                lambda x: pd.to_numeric(x['판매단가(V포함)(2)'], errors='coerce') - 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce') 
                if pd.notna(x['판매단가(V포함)(2)']) and pd.notna(x['판매단가(V포함)']) else None, 
                axis=1
            )
            # Calculate percentage difference
            df['가격차이(2)(%)'] = df.apply(
                lambda x: int((pd.to_numeric(x['가격차이(2)'], errors='coerce') / 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce')) * 100)
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
                lambda x: int((pd.to_numeric(x['가격차이(3)'], errors='coerce') / 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce')) * 100)
                if pd.notna(x['가격차이(3)']) and pd.notna(x['판매단가(V포함)']) and 
                   pd.to_numeric(x['판매단가(V포함)'], errors='coerce') != 0 else None, 
                axis=1
            )
    
    # Ensure all image columns have proper dictionary format
    for img_col in ['네이버 이미지', '고려기프트 이미지', '본사 이미지']:
        if img_col in df.columns:
            # Process the image column consistently with structured data
            df[img_col] = df[img_col].apply(
                lambda x: verify_image_data(x, img_col)
            )
    
    # --- Final formatting and cleanup ---
    # Convert NaN values to None/empty for cleaner Excel output
    # Important: Do not replace '-' here as it's used as a valid placeholder
    df = df.replace({pd.NA: None, np.nan: None}) # Keep NaN -> None for general cleaning
    # Explicitly replace None with '-' only for specific columns where needed BEFORE final output
    # This step is usually handled in finalize_dataframe_for_excel
    
    # Count image URLs per column for logging
    img_url_counts = {
        col: (df[col].map(lambda x: 0 if x == '-' or pd.isna(x) else 1).sum()) 
        for col in ['본사 이미지', '고려기프트 이미지', '네이버 이미지'] 
        if col in df.columns
    }
    
    # FIXED: Log column values to verify data is properly formatted
    logging.info(f"Formatted data for output: {len(df)} rows with image URLs: {img_url_counts}")
    logging.debug(f"Columns in formatted data: {df.columns.tolist()}")
    
    # Verify price data is present in the output DataFrame
    kogift_price_count = df['판매단가(V포함)(2)'].notnull().sum()
    logging.info(f"Kogift price data count: {kogift_price_count} rows have valid price values")
    
    return df

def process_input_data(df: pd.DataFrame, config: Optional[configparser.ConfigParser] = None, 
                    kogift_results: Optional[Dict[str, List[Dict]]] = None,
                    naver_results: Optional[Dict[str, List[Dict]]] = None,
                    input_file_image_map: Optional[Dict[str, Any]] = None,
                    haereum_image_url_map: Dict[str, str] = None) -> pd.DataFrame:
    """
    Process input DataFrame with necessary data processing steps.
    
    Args:
        df: Input DataFrame to process
        config: Configuration object
        kogift_results: Dictionary mapping product names to Kogift crawl results
        naver_results: Dictionary mapping product names to Naver crawl results
        input_file_image_map: Dictionary mapping product codes to image paths
        haereum_image_url_map: Dictionary mapping product names to Haoreum image URLs
    
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
            naver_results=naver_results or {},
            input_file_image_map=input_file_image_map or {},
            haereum_image_url_map=haereum_image_url_map or {}
        )
        
        # Create output directory if it doesn't exist
        output_dir = config.get('Paths', 'output_dir')
        os.makedirs(output_dir, exist_ok=True)
        
        return formatted_df
        
    except Exception as e:
        logging.error(f"Error in process_input_data: {e}", exc_info=True)
        return df 