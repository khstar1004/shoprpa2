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

def format_product_data_for_output(matched_df: pd.DataFrame) -> pd.DataFrame:
    """Format matched data for final output, ensuring all required columns and image URLs/dicts.
       Operates on the DataFrame returned by the matching process, which should contain
       _temp_haoreum_image_data, _temp_kogift_image_data, _temp_naver_image_data columns.
    """
    
    # Create a copy to avoid modifying the input
    df = matched_df.copy()
    
    # --- Define expected image columns and their temporary source columns --- 
    image_column_map = {
        '본사 이미지': '_temp_haoreum_image_data',
        '고려기프트 이미지': '_temp_kogift_image_data',
        '네이버 이미지': '_temp_naver_image_data'
    }

    # --- Ensure final image columns exist --- 
    for final_col in image_column_map.keys():
        if final_col not in df.columns:
            df[final_col] = None # Initialize as None, will hold dicts or '-'
            logging.debug(f"Adding missing final image column: {final_col}")

    # --- Process and verify image data from temporary columns --- 
    logging.info("Formatting image data from temporary columns...")
    image_processed_counts = {key: 0 for key in image_column_map.keys()}

    for final_col, temp_col in image_column_map.items():
        if temp_col in df.columns:
            logging.debug(f"Processing temporary column '{temp_col}' into '{final_col}'")
            # Apply verify_image_data to the temporary column
            df[final_col] = df[temp_col].apply(lambda x: verify_image_data(x, final_col))
            # Count how many valid entries were processed (not '-')
            image_processed_counts[final_col] = (df[final_col] != '-').sum()
            # Optionally drop the temporary column after processing
            # df = df.drop(columns=[temp_col]) 
        else:
            logging.warning(f"Temporary image data column '{temp_col}' not found in DataFrame.")
            # Ensure the final column exists even if the temp one is missing
            if final_col not in df.columns:
                 df[final_col] = '-' # Initialize with placeholder

    logging.info(f"Image data formatting complete. Processed counts: {image_processed_counts}")

    # --- Ensure required columns from original input still exist --- 
    # These should have been carried over from the input df during matching
    for col in ['상품명', '판매단가(V포함)']:
        if col not in df.columns:
            df[col] = None # Or pd.NA
            logging.warning(f"Adding missing required column during formatting: {col}")

    # --- Ensure columns populated by matching logic exist --- 
    # (These might not be present if matching failed for all rows, but format should handle)
    kogift_cols = ['기본수량(2)', '판매단가(V포함)(2)', '판매가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크']
    naver_cols = ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '네이버 쇼핑 링크', '공급사명', '공급사 상품링크']
    
    for col in kogift_cols + naver_cols:
         if col not in df.columns:
              df[col] = None # Or pd.NA
              logging.debug(f"Adding missing matching result column: {col}")

    # --- Calculate Price Differences --- 
    # This logic remains the same as it reads directly from df columns
    if '판매단가(V포함)' in df.columns:
        # Convert base price safely
        base_price = pd.to_numeric(df['판매단가(V포함)'], errors='coerce')
        
        # Kogift price difference
        if '판매단가(V포함)(2)' in df.columns:
            kogift_price = pd.to_numeric(df['판매단가(V포함)(2)'], errors='coerce')
            mask = base_price.notna() & kogift_price.notna() & (base_price != 0)
            
            df['가격차이(2)'] = pd.NA
            df['가격차이(2)(%)'] = pd.NA
            if mask.any():
                 diff = kogift_price.where(mask) - base_price.where(mask)
                 df.loc[mask, '가격차이(2)'] = diff[mask]
                 df.loc[mask, '가격차이(2)(%)'] = np.rint((diff[mask] / base_price[mask]) * 100).astype(pd.Int64Dtype())

        # Naver price difference
        if '판매단가(V포함)(3)' in df.columns:
            naver_price = pd.to_numeric(df['판매단가(V포함)(3)'], errors='coerce')
            mask = base_price.notna() & naver_price.notna() & (base_price != 0)

            df['가격차이(3)'] = pd.NA
            df['가격차이(3)(%)'] = pd.NA
            if mask.any():
                 diff = naver_price.where(mask) - base_price.where(mask)
                 df.loc[mask, '가격차이(3)'] = diff[mask]
                 df.loc[mask, '가격차이(3)(%)'] = np.rint((diff[mask] / base_price[mask]) * 100).astype(pd.Int64Dtype())
    else:
         logging.warning("Base price column '판매단가(V포함)' not found. Cannot calculate price differences.")
         # Ensure columns exist even if calculation skipped
         if '가격차이(2)' not in df.columns: df['가격차이(2)'] = pd.NA
         if '가격차이(2)(%)' not in df.columns: df['가격차이(2)(%)'] = pd.NA
         if '가격차이(3)' not in df.columns: df['가격차이(3)'] = pd.NA
         if '가격차이(3)(%)' not in df.columns: df['가격차이(3)(%)'] = pd.NA

    # --- Final formatting and cleanup --- 
    # Convert specific NaN/NA types to None/empty string for cleaner Excel output
    # Using fillna might be safer if dtypes are mixed
    # df = df.fillna('') # Too broad, might convert numbers to strings
    
    # Replace specific types of missing values
    # Use pd.NA for nullable types, None for object types if needed by Excel writer
    df = df.replace({pd.NA: None, np.nan: None})
    
    # Count image URLs per final column for logging
    final_img_url_counts = {
        col: (df[col].apply(lambda x: isinstance(x, dict) and x.get('url')).sum()) 
        for col in image_column_map.keys() 
        if col in df.columns
    }
    final_img_path_counts = {
        col: (df[col].apply(lambda x: isinstance(x, dict) and x.get('local_path')).sum()) 
        for col in image_column_map.keys() 
        if col in df.columns
    }
    
    logging.info(f"Formatted data for output: {len(df)} rows.")
    logging.info(f"Final image URL counts: {final_img_url_counts}")
    logging.info(f"Final image local_path counts: {final_img_path_counts}")
    logging.debug(f"Columns in formatted data: {df.columns.tolist()}")
    
    # Verify price data is present in the output DataFrame
    kogift_price_count_final = df['판매단가(V포함)(2)'].notnull().sum() if '판매단가(V포함)(2)' in df.columns else 0
    naver_price_count_final = df['판매단가(V포함)(3)'].notnull().sum() if '판매단가(V포함)(3)' in df.columns else 0
    logging.info(f"Kogift price data count in final formatted df: {kogift_price_count_final}")
    logging.info(f"Naver price data count in final formatted df: {naver_price_count_final}")
    
    return df

def process_input_data(df: pd.DataFrame, config: Optional[configparser.ConfigParser] = None, 
                    kogift_results: Optional[Dict[str, List[Dict]]] = None,
                    naver_results: Optional[Dict[str, List[Dict]]] = None,
                    input_file_image_map: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Process input DataFrame with necessary data processing steps.
    
    Args:
        df: Input DataFrame to process
        config: Configuration object
        kogift_results: Dictionary mapping product names to Kogift crawl results
        naver_results: Dictionary mapping product names to Naver crawl results
        input_file_image_map: Dictionary mapping product codes to image paths
    
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
        formatted_df = format_product_data_for_output(filtered_df)
        
        # Create output directory if it doesn't exist
        output_dir = config.get('Paths', 'output_dir')
        os.makedirs(output_dir, exist_ok=True)
        
        return formatted_df
        
    except Exception as e:
        logging.error(f"Error in process_input_data: {e}", exc_info=True)
        return df 