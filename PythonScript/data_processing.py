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
from urllib.parse import urlparse

def process_input_file(config: configparser.ConfigParser) -> Tuple[pd.DataFrame, str]:
    """Process the input Excel file and return a DataFrame and filename."""
    try:
        input_file = config.get('Paths', 'input_file')
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
            
        # Read the Excel file
        df = pd.read_excel(input_file)
        
        # Get the filename without path
        input_filename = os.path.basename(input_file)
        
        # Handle Haereum image URLs
        if '본사 이미지' not in df.columns and '본사상품링크' in df.columns:
            # Try to extract image URLs from product links
            df['본사 이미지'] = df['본사상품링크'].apply(lambda x: extract_image_url_from_link(x) if pd.notna(x) else None)
            logging.info("Extracted image URLs from product links")
        
        # Validate URLs
        if '본사 이미지' in df.columns:
            df['본사 이미지'] = df['본사 이미지'].apply(lambda x: validate_and_fix_url(x) if pd.notna(x) else None)
        
        return df, input_filename
    except Exception as e:
        logging.error(f"Error processing input file: {e}")
        raise

def extract_image_url_from_link(link: str) -> Optional[str]:
    """Try to extract image URL from product link."""
    if not isinstance(link, str):
        return None
    
    try:
        # Common patterns for image URLs in product pages
        img_patterns = [
            r'https?://[^\s<>"]+?\.(?:jpg|jpeg|png|gif)',
            r'src=[\'"](https?://[^\s<>"]+?\.(?:jpg|jpeg|png|gif))[\'"]',
            r'data-original=[\'"](https?://[^\s<>"]+?\.(?:jpg|jpeg|png|gif))[\'"]'
        ]
        
        for pattern in img_patterns:
            match = re.search(pattern, link, re.IGNORECASE)
            if match:
                return match.group(1) if 'src=' in pattern or 'data-original=' in pattern else match.group(0)
        
        return None
    except Exception as e:
        logging.warning(f"Error extracting image URL from link: {e}")
        return None

def validate_and_fix_url(url: str) -> Optional[str]:
    """Validate and fix common URL issues."""
    if not isinstance(url, str):
        return None
        
    try:
        # Remove whitespace
        url = url.strip()
        
        # Add http:// if missing
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
            
        # Validate URL format
        parsed = urlparse(url)
        if not all([parsed.scheme, parsed.netloc]):
            return None
            
        return url
    except Exception as e:
        logging.warning(f"Error validating URL: {e}")
        return None

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
    kogift_columns = [
        '기본수량(2)', '판매단가(V포함)(2)', '판매가(V포함)(2)', 
        '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크', 
        '고려기프트 이미지', '_temp_kogift_quantity_prices'  # Added temporary column
    ]
    for col in kogift_columns:
        if col not in df.columns:
            df[col] = None
            logging.debug(f"Adding column for Kogift data: {col}")
    
    # Add columns for Naver results if they don't exist yet
    # UPDATED: Added temporary columns for internal processing (will be removed before final output)
    standard_columns = ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', 
                       '네이버 쇼핑 링크', '공급사명', '공급사 상품링크', '네이버 이미지']
    temp_columns = ['_temp_판촉물여부', '_temp_수량별가격여부', '_temp_수량별가격정보', '_temp_VAT포함여부']
    
    # Add standard columns that will remain in final output
    for col in standard_columns:
        if col not in df.columns:
            df[col] = None
            logging.debug(f"Adding standard Naver column: {col}")
    
    # Add temporary columns for internal processing
    for col in temp_columns:
        if col not in df.columns:
            df[col] = None
            logging.debug(f"Adding temporary column for processing: {col}")

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

    # Process Kogift data
    if kogift_results:
        kogift_update_count = 0
        kogift_img_count = 0
        kogift_price_count = 0
        
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            if product_name in kogift_results:
                kogift_data = kogift_results[product_name]
                if kogift_data and len(kogift_data) > 0:
                    item = kogift_data[0]  # Use the first match
                    
                    # Store quantity prices in temporary column
                    if 'quantity_prices' in item:
                        df.at[idx, '_temp_kogift_quantity_prices'] = item['quantity_prices']
                    
                    # Get base quantity from input
                    base_quantity = None
                    if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                        base_quantity = int(row['기본수량(1)'])
                    elif '기본수량' in df.columns and pd.notna(row['기본수량']):
                        base_quantity = int(row['기본수량'])
                    
                    # Update quantity and price information
                    if base_quantity and '_temp_kogift_quantity_prices' in df.columns:
                        qty_prices = df.at[idx, '_temp_kogift_quantity_prices']
                        if isinstance(qty_prices, dict):
                            # Convert string keys to int for proper comparison
                            qty_prices = {int(k): v for k, v in qty_prices.items()}
                            
                            # Sort quantities for proper tier comparison
                            available_qtys = sorted(qty_prices.keys())
                            
                            # Find the closest quantity price
                            available_qtys = sorted([int(q) for q in qty_prices.keys()])
                            closest_qty = min(available_qtys, key=lambda x: abs(x - base_quantity))
                            
                            # Enhanced error handling for quantity price lookup
                            try:
                                # First try with string key
                                if str(closest_qty) in qty_prices:
                                    price_info = qty_prices[str(closest_qty)]
                                # Then try with integer key
                                elif closest_qty in qty_prices:
                                    price_info = qty_prices[closest_qty]
                                # Try looking for a similar key (for minor quantity differences)
                                else:
                                    # Log all available quantity keys for debugging
                                    available_keys = list(qty_prices.keys())
                                    logging.debug(f"Available quantity price keys: {available_keys}")
                                    logging.warning(f"Could not find exact match for quantity {closest_qty} in keys {available_keys}")
                                    
                                    # Look for the nearest quantity tier in the available keys
                                    closest_available = min(available_keys, key=lambda x: abs(int(str(x)) - closest_qty), default=None)
                                    if closest_available:
                                        logging.info(f"Using closest available quantity tier: {closest_available} instead of {closest_qty}")
                                        price_info = qty_prices[closest_available]
                                    else:
                                        # If all fails, create a default price info
                                        logging.warning(f"No suitable quantity tier found. Using default price.")
                                        price_info = {'price': item.get('price', 0), 'price_with_vat': item.get('price', 0)}
                            except Exception as e:
                                # Handle any errors during price lookup
                                logging.error(f"Error finding quantity price for {closest_qty}: {e}")
                                price_info = {'price': item.get('price', 0), 'price_with_vat': item.get('price', 0)}
                            
                            # Fix: Use closest_qty instead of undefined selected_qty
                            if closest_qty is not None:
                                # Use the price from the appropriate tier
                                df.at[idx, '판매단가(V포함)(2)'] = price_info.get('price_with_vat', price_info.get('price', 0))
                                df.at[idx, '기본수량(2)'] = base_quantity
                                kogift_price_count += 1
                                logging.debug(f"Using price tier {closest_qty} for quantity {base_quantity}")
                            elif available_qtys:  # If quantity is less than minimum tier
                                # Use the minimum tier price
                                min_qty = min(available_qtys)
                                price_info = qty_prices[min_qty]
                                df.at[idx, '판매단가(V포함)(2)'] = price_info.get('price_with_vat', price_info.get('price', 0))
                                df.at[idx, '기본수량(2)'] = base_quantity
                                kogift_price_count += 1
                                logging.debug(f"Using minimum tier price ({min_qty}) for quantity {base_quantity}")
                    
                    # If no quantity prices found or processing failed, use default price
                    if pd.isna(df.at[idx, '판매단가(V포함)(2)']):
                        if 'price_with_vat' in item:
                            df.at[idx, '판매단가(V포함)(2)'] = item['price_with_vat']
                            df.at[idx, '기본수량(2)'] = base_quantity if base_quantity else 1
                            kogift_price_count += 1
                        elif 'price' in item:
                            df.at[idx, '판매단가(V포함)(2)'] = item['price']
                            df.at[idx, '기본수량(2)'] = base_quantity if base_quantity else 1
                            kogift_price_count += 1
                    
                    # Copy price to 판매가(V포함)(2) and calculate price differences
                    if pd.notna(df.at[idx, '판매단가(V포함)(2)']):
                        # Copy to 판매가(V포함)(2)
                        df.at[idx, '판매가(V포함)(2)'] = df.at[idx, '판매단가(V포함)(2)']
                        
                        # Calculate price differences if base price exists
                        if '판매단가(V포함)' in df.columns and pd.notna(row['판매단가(V포함)']):
                            try:
                                base_price = float(row['판매단가(V포함)'])
                                kogift_price = float(df.at[idx, '판매단가(V포함)(2)'])
                                
                                # Calculate absolute difference
                                price_diff = kogift_price - base_price
                                df.at[idx, '가격차이(2)'] = price_diff
                                
                                # Calculate percentage difference
                                if base_price != 0:
                                    pct_diff = (price_diff / base_price) * 100
                                    # Fix: Use correct column name with consistent naming
                                    df.at[idx, '가격차이(2)(%)'] = round(pct_diff, 1)
                            except (ValueError, TypeError) as e:
                                logging.warning(f"Error calculating price differences for row {idx}: {e}")
                    
                    # Update Kogift product URL
                    if 'link' in item:
                        df.at[idx, '고려기프트 상품링크'] = item['link']
                    elif 'href' in item:
                        df.at[idx, '고려기프트 상품링크'] = item['href']
                    
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
        
        logging.info(f"Updated {kogift_update_count} rows with Kogift data")
        logging.info(f"Added {kogift_price_count} Kogift prices")
        logging.info(f"Added {kogift_img_count} Kogift images")

    # Update Naver data processing section to use temporary columns
    if naver_results:
        naver_matched_count = 0
        for idx, row in df.iterrows():
            product_name = row.get('상품명')
            
            if product_name in naver_results:
                naver_data = naver_results[product_name]
                if naver_data and len(naver_data) > 0:
                    item = naver_data[0]
                    
                    if isinstance(item, dict):
                        naver_matched_count += 1
                        
                        # Update standard columns as before
                        if '기본수량(3)' in df.columns:
                            if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']):
                                df.at[idx, '기본수량(3)'] = row['기본수량(1)']
                            else:
                                df.at[idx, '기본수량(3)'] = 1
                        
                        # Update price information
                        if '판매단가(V포함)(3)' in df.columns:
                            # If it's a promotional site with quantity pricing, use the price for the base quantity
                            is_promo = item.get('is_promotional_site', False)
                            has_qty_pricing = item.get('has_quantity_pricing', False)
                            qty_prices = item.get('quantity_prices', {})
                            
                            if is_promo and has_qty_pricing and qty_prices:
                                # Store promotional site info in temporary columns
                                df.at[idx, '_temp_판촉물여부'] = True
                                df.at[idx, '_temp_수량별가격여부'] = True
                                df.at[idx, '_temp_VAT포함여부'] = item.get('vat_included', False)
                                
                                # Store quantity pricing info for internal use
                                import json
                                df.at[idx, '_temp_수량별가격정보'] = json.dumps(qty_prices)
                                
                                # Get base quantity price (using 기본수량(1) if available)
                                base_qty = row['기본수량(1)'] if '기본수량(1)' in df.columns and pd.notna(row['기본수량(1)']) else 1
                                
                                # Find the closest quantity price
                                available_qtys = sorted([int(q) for q in qty_prices.keys()])
                                closest_qty = min(available_qtys, key=lambda x: abs(x - base_qty))
                                
                                # Enhanced error handling for quantity price lookup
                                try:
                                    # First try with string key
                                    if str(closest_qty) in qty_prices:
                                        price_info = qty_prices[str(closest_qty)]
                                    # Then try with integer key
                                    elif closest_qty in qty_prices:
                                        price_info = qty_prices[closest_qty]
                                    # Try looking for a similar key (for minor quantity differences)
                                    else:
                                        # Log all available quantity keys for debugging
                                        available_keys = list(qty_prices.keys())
                                        logging.debug(f"Available quantity price keys: {available_keys}")
                                        logging.warning(f"Could not find exact match for quantity {closest_qty} in keys {available_keys}")
                                        
                                        # Look for the nearest quantity tier in the available keys
                                        closest_available = min(available_keys, key=lambda x: abs(int(str(x)) - closest_qty), default=None)
                                        if closest_available:
                                            logging.info(f"Using closest available quantity tier: {closest_available} instead of {closest_qty}")
                                            price_info = qty_prices[closest_available]
                                        else:
                                            # If all fails, create a default price info
                                            logging.warning(f"No suitable quantity tier found. Using default price.")
                                            price_info = {'price': item.get('price', 0), 'price_with_vat': item.get('price', 0)}
                                except Exception as e:
                                    # Handle any errors during price lookup
                                    logging.error(f"Error finding quantity price for {closest_qty}: {e}")
                                    price_info = {'price': item.get('price', 0), 'price_with_vat': item.get('price', 0)}
                                
                                # Use VAT-included price
                                df.at[idx, '판매단가(V포함)(3)'] = price_info.get('price_with_vat', item.get('price', 0))
                            else:
                                # Use regular price for non-promotional items
                                df.at[idx, '판매단가(V포함)(3)'] = item.get('price', 0)
                        
                        # Update other standard columns
                        if '네이버 쇼핑 링크' in df.columns and 'link' in item:
                            df.at[idx, '네이버 쇼핑 링크'] = item['link']
                        if '공급사 상품링크' in df.columns and 'seller_link' in item:
                            df.at[idx, '공급사 상품링크'] = item['seller_link']
                        if '공급사명' in df.columns and 'seller_name' in item:
                            df.at[idx, '공급사명'] = item['seller_name']
                            
                        # Process image information as before
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
                lambda x: round((pd.to_numeric(x['가격차이(2)'], errors='coerce') / 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce')) * 100, 1)
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
                lambda x: round((pd.to_numeric(x['가격차이(3)'], errors='coerce') / 
                           pd.to_numeric(x['판매단가(V포함)'], errors='coerce')) * 100, 1)
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
    naver_price_count = df['판매단가(V포함)(3)'].notnull().sum()
    
    # Fix: Use the temporary columns for these counts before they're removed
    # These columns might not exist after previous cleanup
    promo_site_count = df['_temp_판촉물여부'].sum() if '_temp_판촉물여부' in df.columns else 0
    qty_pricing_count = df['_temp_수량별가격여부'].sum() if '_temp_수량별가격여부' in df.columns else 0
    
    logging.info(f"Data summary:")
    logging.info(f"- Kogift price data count: {kogift_price_count} rows")
    logging.info(f"- Naver price data count: {naver_price_count} rows")
    logging.info(f"- Promotional sites detected: {promo_site_count} rows")
    logging.info(f"- Quantity pricing available: {qty_pricing_count} rows")
    
    # Remove temporary columns before returning
    # Fix: Define temp_columns list (was being referenced before assignment)
    all_temp_columns = ['_temp_kogift_quantity_prices', '_temp_판촉물여부', '_temp_수량별가격여부', 
                        '_temp_수량별가격정보', '_temp_VAT포함여부']
    
    for col in all_temp_columns:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            logging.debug(f"Removed temporary column: {col}")
    
    # Verify final columns match expected format
    current_cols = set(df.columns)
    expected_cols = set(FINAL_COLUMN_ORDER)
    extra_cols = current_cols - expected_cols
    missing_cols = expected_cols - current_cols
    
    if extra_cols:
        logging.warning(f"Found unexpected columns that will be removed: {extra_cols}")
        df = df[[col for col in df.columns if col in FINAL_COLUMN_ORDER]]
    
    if missing_cols:
        logging.warning(f"Missing expected columns: {missing_cols}")
        for col in missing_cols:
            df[col] = None
    
    # Ensure final column order matches exactly
    df = df[FINAL_COLUMN_ORDER]
    
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