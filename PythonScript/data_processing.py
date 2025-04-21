import os
import glob
import logging
import pandas as pd
from datetime import datetime
import configparser # Import configparser
from excel_utils import filter_dataframe, apply_excel_styles, add_hyperlinks # Import necessary utils
import re
import time
from typing import Optional, Tuple

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

        required_cols = ['Code', '상품명', '판매단가(V포함)', '본사상품링크', '본사 이미지', '구분']

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

def filter_results(df: Optional[pd.DataFrame], config: configparser.ConfigParser, progress_queue=None) -> Optional[pd.DataFrame]:
    """Filters the combined results DataFrame using external utility function.
       Accepts ConfigParser object.
       
       Nota: A função filter_dataframe foi modificada para manter todas as linhas de entrada,
       incluindo produtos sem correspondências, para garantir que a saída sempre contenha 
       informações mesmo quando não há correspondências perfeitas.
    """
    if df is None or df.empty:
        logging.warning("Input DataFrame for filtering is None or empty. Skipping filter.")
        return df

    if progress_queue: progress_queue.put(("filter", 0, 1))

    # Pass ConfigParser to filter_dataframe (assuming it's updated)
    filtered_df = filter_dataframe(df, config)

    if progress_queue: progress_queue.put(("filter", 1, 1))

    return filtered_df

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

def format_product_data_for_output(input_df, kogift_results, naver_results):
    """
    Format crawled product data into the necessary format for Excel output.
    Ensures all image URLs and product data are properly included.
    
    Args:
        input_df: Original input DataFrame
        kogift_results: Dictionary mapping product name to KoGift results
        naver_results: Dictionary mapping product name to Naver results
        
    Returns:
        DataFrame with properly formatted data for Excel output
    """
    output_df = input_df.copy()
    
    # Log original column names for debugging
    logging.info(f"Original columns in input_df: {output_df.columns.tolist()}")
    
    # Add image columns if they don't exist
    image_columns = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
    for col in image_columns:
        if col not in output_df:
            output_df[col] = ''
    
    # Add result columns if they don't exist
    kogift_cols = [
        '기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', 
        '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크'
    ]
    
    naver_cols = [
        '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', 
        '공급사명', '네이버 쇼핑 링크', '공급사 상품링크'
    ]
    
    for col in kogift_cols + naver_cols:
        if col not in output_df:
            output_df[col] = '-'
    
    # Preserve these important fields to ensure they're in the output
    essential_fields = ['구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명']
    for field in essential_fields:
        if field in input_df.columns and field not in output_df.columns:
            output_df[field] = input_df[field]
    
    # Process each row
    for idx, row in output_df.iterrows():
        product_name = row.get('상품명')
        if not product_name:
            continue
            
        # Get Haoreum price, handling potential missing column or value
        haoreum_price = 0
        for price_col in ['판매단가(V포함)', '판매단가1(VAT포함)']:
            if price_col in row:
                try:
                    price_val = row[price_col]
                    if pd.notna(price_val):
                        haoreum_price = float(price_val)
                        break
                except (ValueError, TypeError):
                    continue
            
        # Process KoGift data
        kogift_data = kogift_results.get(product_name, [])
        if kogift_data:
            best_match = kogift_data[0]  # Use first match as best match
            
            # Image URL - Try all possible image URL field names
            image_url = None
            for field in ['image_url', 'image_path', 'src', 'img_url', 'image']:
                if field in best_match and best_match[field] and isinstance(best_match[field], str):
                    image_url = best_match[field]
                    break
                    
            if image_url:
                output_df.loc[idx, '고려기프트 이미지'] = str(image_url)  # Use loc instead of at
                logging.debug(f"Added Kogift image URL for '{product_name}': {image_url[:50]}...")
            
            # Link
            for link_field in ['link', 'href', 'url']:
                if link_field in best_match and best_match[link_field]:
                    output_df.loc[idx, '고려기프트 상품링크'] = str(best_match[link_field])  # Use loc instead of at
                    break
            
            # Price
            kogift_price = best_match.get('price', 0)
            if isinstance(kogift_price, (int, float)) and kogift_price > 0:
                output_df.loc[idx, '판매단가(V포함)(2)'] = float(kogift_price)  # Use loc instead of at
                
                # Calculate price difference only if haoreum_price is valid
                if haoreum_price > 0:
                    price_diff = kogift_price - haoreum_price
                    output_df.loc[idx, '가격차이(2)'] = float(price_diff)  # Use loc instead of at
                    
                    # Calculate percentage
                    price_diff_pct = (price_diff / haoreum_price) * 100
                    output_df.loc[idx, '가격차이(2)(%)'] = float(round(price_diff_pct, 1))  # Use loc instead of at
            
            # Quantity
            quantity = best_match.get('quantity', '-')
            output_df.loc[idx, '기본수량(2)'] = str(quantity)  # Use loc instead of at
            
        # Process Naver data
        naver_data = naver_results.get(product_name, [])
        if naver_data:
            best_match = naver_data[0]  # Use first match as best match
            
            # Image URL - Try all possible image URL field names
            image_url = None
            for field in ['image_url', 'image_path', 'src', 'img_url', 'image']:
                if field in best_match and best_match[field] and isinstance(best_match[field], str):
                    image_url = best_match[field]
                    break
                    
            if image_url:
                output_df.loc[idx, '네이버 이미지'] = str(image_url)  # Use loc instead of at
                logging.debug(f"Added Naver image URL for '{product_name}': {image_url[:50]}...")
            
            # Links
            for link_field in ['link', 'href', 'url']:
                if link_field in best_match and best_match[link_field]:
                    output_df.loc[idx, '네이버 쇼핑 링크'] = str(best_match[link_field])  # Use loc instead of at
                    break
            
            # Price
            naver_price = best_match.get('price', 0)
            if isinstance(naver_price, (int, float)) and naver_price > 0:
                output_df.loc[idx, '판매단가(V포함)(3)'] = float(naver_price)  # Use loc instead of at
                
                # Calculate price difference only if haoreum_price is valid
                if haoreum_price > 0:
                    price_diff = naver_price - haoreum_price
                    output_df.loc[idx, '가격차이(3)'] = float(price_diff)  # Use loc instead of at
                    
                    # Calculate percentage
                    price_diff_pct = (price_diff / haoreum_price) * 100
                    output_df.loc[idx, '가격차이(3)(%)'] = float(round(price_diff_pct, 1))  # Use loc instead of at
                
            # Seller name
            seller = best_match.get('seller', '-')
            output_df.loc[idx, '공급사명'] = str(seller)  # Use loc instead of at
            
            # Quantity
            quantity = best_match.get('quantity', '-')
            output_df.loc[idx, '기본수량(3)'] = str(quantity)  # Use loc instead of at
    
    # Log summary of image URLs
    image_count = {}
    for col in image_columns:
        image_count[col] = (output_df[col].notna() & (output_df[col] != '') & (output_df[col] != '-')).sum()
    
    logging.info(f"Formatted data for output: {len(output_df)} rows with image URLs: {image_count}")
    return output_df 