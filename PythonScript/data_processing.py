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
    
    # NEW: Add column for promotional site indicator
    for col in ['판촉물사이트여부', '수량별가격여부']:
        if col not in df.columns:
            df[col] = None
            logging.debug(f"Adding column for promotional site data: {col}")
    
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
                        # Check for quantity-specific prices first (new structure)
                        if 'quantity_prices' in item and item['quantity_prices']:
                            # Get the target quantity from 기본수량(1) or fallback to a default
                            target_qty = None
                            if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                                try:
                                    target_qty = int(row['기본수량(1)'])
                                except (ValueError, TypeError):
                                    target_qty = None
                            
                            # Default to the first quantity if target not found
                            if target_qty is None:
                                target_qty = min(item['quantity_prices'].keys()) if item['quantity_prices'] else None
                            
                            # 수량-가격 테이블에서 적절한 가격 찾기
                            if target_qty and item['quantity_prices']:
                                # 테이블의 최소/최대 수량 확인
                                quantities = sorted(item['quantity_prices'].keys())
                                min_qty = min(quantities)
                                
                                # 1. 정확히 일치하는 수량이 있는 경우
                                if target_qty in item['quantity_prices']:
                                    price_info = item['quantity_prices'][target_qty]
                                    df.at[idx, '판매단가(V포함)(2)'] = price_info['price_with_vat']
                                    kogift_price_count += 1
                                    logging.info(f"Kogift 정확한 수량({target_qty})에 대한 가격 데이터 사용: {price_info['price_with_vat']}")
                                
                                # 2. 최소 수량보다 작은 경우 -> 최소 수량의 가격 사용
                                elif target_qty < min_qty:
                                    price_info = item['quantity_prices'][min_qty]
                                    df.at[idx, '판매단가(V포함)(2)'] = price_info['price_with_vat']
                                    kogift_price_count += 1
                                    logging.info(f"Kogift 최소 수량({min_qty}) 가격 적용 (요청 수량: {target_qty}): {price_info['price_with_vat']}")
                                
                                # 3. 구간 가격 찾기
                                else:
                                    # 주문 수량보다 작거나 같은 최대 수량 찾기
                                    lower_quantities = [q for q in quantities if q <= target_qty]
                                    if lower_quantities:
                                        max_lower_qty = max(lower_quantities)
                                        price_info = item['quantity_prices'][max_lower_qty]
                                        df.at[idx, '판매단가(V포함)(2)'] = price_info['price_with_vat']
                                        kogift_price_count += 1
                                        logging.info(f"Kogift 구간 가격 적용 (구간: {max_lower_qty}, 요청 수량: {target_qty}): {price_info['price_with_vat']}")
                                    else:
                                        # 모든 수량보다 큰 경우 -> 가장 큰 수량의 가격 사용
                                        max_qty = max(quantities)
                                        price_info = item['quantity_prices'][max_qty]
                                        df.at[idx, '판매단가(V포함)(2)'] = price_info['price_with_vat']
                                        kogift_price_count += 1
                                        logging.info(f"Kogift 최대 수량({max_qty}) 가격 적용 (요청 수량: {target_qty}): {price_info['price_with_vat']}")
                        
                        # 부가세 포함 가격이 먼저 있는지 확인 (기존 방식)
                        elif 'price_with_vat' in item:
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
        naver_matched_count = 0
        naver_promo_site_count = 0  # NEW: Count promotional sites
        naver_qty_pricing_count = 0  # NEW: Count items with quantity pricing
        
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            
            if product_name in naver_results:
                naver_data = naver_results[product_name]
                if naver_data and len(naver_data) > 0:
                    # Use first match for now
                    item = naver_data[0]
                    
                    # Process if item is valid and has required data
                    if isinstance(item, dict):
                        naver_matched_count += 1
                        
                        # NEW: Check if it's a promotional site
                        is_promotional = False
                        has_quantity_pricing = False
                        
                        # Check from newer structure
                        if 'is_promotional_site' in item:
                            is_promotional = item['is_promotional_site']
                            if is_promotional:
                                naver_promo_site_count += 1
                        
                        # Check quantity pricing flag
                        if 'has_quantity_pricing' in item:
                            has_quantity_pricing = item['has_quantity_pricing']
                            if has_quantity_pricing:
                                naver_qty_pricing_count += 1
                        
                        # Update promotional site indicator columns
                        if '판촉물사이트여부' in df.columns:
                            df.at[idx, '판촉물사이트여부'] = 'Y' if is_promotional else 'N'
                        
                        if '수량별가격여부' in df.columns:
                            df.at[idx, '수량별가격여부'] = 'Y' if has_quantity_pricing else 'N'
                        
                        # Update Naver related columns
                        # 기본수량(3) - 요청에 따라 수량정보는 생략 (항상 기본수량(1)과 동일하게 설정)
                        if '기본수량(3)' in df.columns:
                            # 기본수량(1)의 값을 그대로 복사 (직접 가격 비교를 위해)
                            if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                                df.at[idx, '기본수량(3)'] = row['기본수량(1)']
                            else:
                                df.at[idx, '기본수량(3)'] = 1  # 기본값
                        
                        # 판매단가 정보 업데이트
                        if '판매단가(V포함)(3)' in df.columns:
                            # NEW: Check for quantity pricing first
                            if has_quantity_pricing and 'quantity_prices' in item and item['quantity_prices']:
                                # Get the target quantity from 기본수량(1) or fallback
                                target_qty = None
                                if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                                    try:
                                        target_qty = int(row['기본수량(1)'])
                                    except (ValueError, TypeError):
                                        target_qty = None
                                
                                # Default if no target found
                                if target_qty is None:
                                    target_qty = 300  # Default quantity
                                
                                # Find the price for this quantity
                                if str(target_qty) in item['quantity_prices']:  # Check string key
                                    price_info = item['quantity_prices'][str(target_qty)]
                                    df.at[idx, '판매단가(V포함)(3)'] = price_info['price_with_vat']
                                    logging.info(f"Using quantity-based price for {product_name}: {price_info['price_with_vat']} (qty: {target_qty})")
                                elif target_qty in item['quantity_prices']:  # Check int key
                                    price_info = item['quantity_prices'][target_qty]
                                    df.at[idx, '판매단가(V포함)(3)'] = price_info['price_with_vat']
                                    logging.info(f"Using quantity-based price for {product_name}: {price_info['price_with_vat']} (qty: {target_qty})")
                                else:
                                    # Find closest match
                                    available_qtys = []
                                    for qty_key in item['quantity_prices'].keys():
                                        try:
                                            # Convert keys to int (they could be strings)
                                            available_qtys.append(int(qty_key))
                                        except (ValueError, TypeError):
                                            pass
                                    
                                    if available_qtys:
                                        # Find closest lower quantity
                                        lower_qtys = [q for q in available_qtys if q <= target_qty]
                                        if lower_qtys:
                                            closest_qty = max(lower_qtys)
                                            price_info = item['quantity_prices'].get(
                                                closest_qty,
                                                item['quantity_prices'].get(str(closest_qty))
                                            )
                                            if price_info:
                                                df.at[idx, '판매단가(V포함)(3)'] = price_info['price_with_vat']
                                                logging.info(f"Using closest quantity price for {product_name}: {price_info['price_with_vat']} (qty: {closest_qty}, target: {target_qty})")
                                        else:
                                            # Use minimum quantity price
                                            min_qty = min(available_qtys)
                                            price_info = item['quantity_prices'].get(
                                                min_qty,
                                                item['quantity_prices'].get(str(min_qty))
                                            )
                                            if price_info:
                                                df.at[idx, '판매단가(V포함)(3)'] = price_info['price_with_vat']
                                                logging.info(f"Using minimum quantity price for {product_name}: {price_info['price_with_vat']} (qty: {min_qty}, target: {target_qty})")
                            # Check for price_with_vat
                            elif 'price_with_vat' in item:
                                df.at[idx, '판매단가(V포함)(3)'] = item['price_with_vat']
                            # Check for regular price and apply VAT
                            elif 'price' in item:
                                # If it's a promotional site, add VAT if not already included
                                if is_promotional and not item.get('vat_included', False):
                                    df.at[idx, '판매단가(V포함)(3)'] = round(item['price'] * 1.1)
                                    logging.info(f"Added VAT to price for promotional site {product_name}: {item['price']} -> {round(item['price'] * 1.1)}")
                                else:
                                    df.at[idx, '판매단가(V포함)(3)'] = item['price']
                        
                        # 링크 정보 업데이트
                        if '네이버 쇼핑 링크' in df.columns and 'link' in item:
                            df.at[idx, '네이버 쇼핑 링크'] = item['link']
                        if '공급사 상품링크' in df.columns and 'seller_link' in item:
                            df.at[idx, '공급사 상품링크'] = item['seller_link']
                        if '공급사명' in df.columns and 'seller_name' in item:
                            df.at[idx, '공급사명'] = item['seller_name']
                        
                        # 이미지 URL 추가
                        if '네이버 이미지' in df.columns:
                            # 이미지 경로 정보가 있으면 추가
                            img_path = None
                            img_url = None
                            
                            # IMPROVED: 실제 이미지 URL 찾기 (순서대로 시도)
                            if 'image_url' in item and item['image_url'] and item['image_url'].startswith(('http://', 'https://')):
                                img_url = item['image_url']
                            elif 'image_path' in item and isinstance(item['image_path'], str) and item['image_path'].startswith(('http://', 'https://')):
                                img_url = item['image_path']
                            elif 'image_path' in item:
                                img_path = item['image_path']
                            
                            if img_path or img_url:
                                # 이미지 URL을 포함하는 사전 생성
                                if img_url:
                                    img_dict = {
                                        'url': img_url,
                                        'source': '네이버'
                                    }
                                    if img_path and not isinstance(img_path, dict) and not img_path.startswith(('http://', 'https://')):
                                        img_dict['local_path'] = img_path
                                    
                                    df.at[idx, '네이버 이미지'] = img_dict
                                    logging.debug(f"Found Naver image URL: {img_url[:50]}...")
                                elif isinstance(img_path, dict):
                                    # 이미 사전 형태인 경우
                                    img_dict = img_path.copy()
                                    if 'source' not in img_dict:
                                        img_dict['source'] = '네이버'
                                    df.at[idx, '네이버 이미지'] = img_dict
                                else:
                                    # 로컬 경로만 있는 경우
                                    df.at[idx, '네이버 이미지'] = {
                                        'local_path': img_path,
                                        'source': '네이버'
                                    }
        
        logging.info(f"Updated {naver_matched_count} rows with Naver data")
        logging.info(f"Detected {naver_promo_site_count} promotional sites and {naver_qty_pricing_count} sites with quantity pricing")
    
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
    kogift_price_count = df['판매단가(V포함)(2)'].notnull().sum() if '판매단가(V포함)(2)' in df.columns else 0
    naver_price_count = df['판매단가(V포함)(3)'].notnull().sum() if '판매단가(V포함)(3)' in df.columns else 0
    logging.info(f"Price data counts: Kogift: {kogift_price_count}, Naver: {naver_price_count} rows have valid price values")
    
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