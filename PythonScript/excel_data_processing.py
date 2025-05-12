import os
import logging
import pandas as pd
import numpy as np
import re
import functools
import json
from typing import Optional, Dict, Any, List, Union, Tuple
from pathlib import Path

from excel_constants import (
    COLUMN_RENAME_MAP, FINAL_COLUMN_ORDER, 
    PRICE_COLUMNS, QUANTITY_COLUMNS, PERCENTAGE_COLUMNS,
    IMAGE_COLUMNS, ERROR_MESSAGE_VALUES, ERROR_MESSAGES,
    IMAGE_DIRS
)

# Initialize logger
logger = logging.getLogger(__name__)

# Add promotional site indicator columns for internal processing only
if 'PROMOTIONAL_SITE_COLUMNS' not in globals():
    PROMOTIONAL_SITE_COLUMNS = ['판촉물사이트여부', '수량별가격여부']
    INTERNAL_PROCESSING_COLUMNS = PROMOTIONAL_SITE_COLUMNS  # Keep track of columns that should not appear in final output

def retry_on_failure(max_retries: int = 3, delay: int = 1):
    """
    Decorator for retrying functions on failure with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        logger.error(f"Function {func.__name__} failed after {max_retries} attempts: {str(e)}")
                        raise last_exception
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}")
                    import time
                    # Exponential backoff
                    time.sleep(delay * (2 ** attempt))
            return None
        return wrapper
    return decorator

@retry_on_failure()
def find_excel_file(directory: str, extension: str = '.xlsx') -> Optional[str]:
    """
    Find the first Excel file with the specified extension in the directory.
    
    Args:
        directory: Directory to search in
        extension: File extension to look for
        
    Returns:
        Optional[str]: Filename if found, None otherwise
    
    Raises:
        FileNotFoundError: If directory doesn't exist
        PermissionError: If directory access is denied
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Directory does not exist: {directory}")
        
    try:
        # Ignore temporary Excel files starting with ~$
        files = [f for f in os.listdir(directory) 
                if f.lower().endswith(extension) and not f.startswith('~$')]
        return files[0] if files else None
    except PermissionError as e:
        logger.error(f"Permission denied accessing directory '{directory}': {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error finding Excel file in '{directory}': {str(e)}")
        raise

def preprocess_product_name(name: str) -> str:
    """
    Preprocess product name for better matching.
    
    Args:
        name: Product name to process
        
    Returns:
        str: Processed product name
    """
    if not isinstance(name, str):
        return str(name)
    
    # Remove brackets and their contents
    name = re.sub(r'[\(\[\{].*?[\)\]\}]', '', name)
    
    # Remove special characters but keep Korean, English, numbers
    name = re.sub(r'[^\w\s가-힣]', ' ', name)
    
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name.strip()

def flatten_nested_image_dicts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten any nested dictionaries in image data structures.
    
    Args:
        df: DataFrame with image data
        
    Returns:
        DataFrame with flattened image data
    """
    if df is None or df.empty:
        return df

    df_result = df.copy()
    
    # Get all image-related columns
    image_cols = [col for col in df.columns 
                 if col in IMAGE_COLUMNS or '이미지' in col]
    image_cols = list(dict.fromkeys(image_cols))  # Remove duplicates
    
    if not image_cols:
        return df_result
    
    def extract_url_from_value(value):
        """Helper function to extract URL from various data structures"""
        if pd.isna(value) or value == '-':
            return '-'
            
        # Handle dictionary format
        if isinstance(value, dict):
            # Try to extract URL from nested structures
            if 'url' in value:
                if isinstance(value['url'], dict) and 'url' in value['url']:
                    return value['url']['url']
                elif isinstance(value['url'], str):
                    return value['url']
            elif 'local_path' in value:
                return value['local_path']
            return '-'
            
        # Handle string format
        if isinstance(value, str):
            if value.startswith('{') and value.endswith('}'):
                try:
                    import json
                    json_value = json.loads(value.replace("'", '"'))
                    if isinstance(json_value, dict):
                        if 'url' in json_value:
                            if isinstance(json_value['url'], dict):
                                return json_value['url'].get('url', '-')
                            return json_value['url']
                        elif 'local_path' in json_value:
                            return json_value['local_path']
                except json.JSONDecodeError:
                    pass
            if value.startswith(('http://', 'https://', 'file://')):
                return value
            return '-'
            
        # Handle other types
        return '-'
    
    # Process each image column
    for col in image_cols:
        df_result[col] = df_result[col].apply(extract_url_from_value)
    
    return df_result

def prepare_naver_image_urls_for_upload(df_with_image_urls: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare Naver image URLs for the upload file by prioritizing product links over image URLs.
    
    Args:
        df_with_image_urls: DataFrame with extracted image URLs
        
    Returns:
        DataFrame with processed Naver image URLs
    """
    if df_with_image_urls.empty:
        return df_with_image_urls
        
    # Naver image column in upload format
    naver_img_col = '네이버쇼핑(이미지링크)'
    # Naver link column in upload format 
    naver_link_col = '네이버 링크'
    
    # Check if necessary columns exist
    if naver_img_col not in df_with_image_urls.columns:
        logger.warning(f"Naver image column '{naver_img_col}' not found in DataFrame. Skipping preparation.")
        return df_with_image_urls
    
    if naver_link_col not in df_with_image_urls.columns:
        logger.warning(f"Naver link column '{naver_link_col}' not found in DataFrame. Cannot replace with product links.")
        return df_with_image_urls
    
    # Track processed items
    replaced_count = 0
    processed_count = 0
    
    # Create a copy of the DataFrame
    result_df = df_with_image_urls.copy()
    
    # Process each row
    for idx in df_with_image_urls.index:
        try:
            # Get the image URL value
            img_url = df_with_image_urls.at[idx, naver_img_col]
            # Get the product link value
            product_link = df_with_image_urls.at[idx, naver_link_col]
            
            processed_count += 1
            
            # Skip if no product link exists or if image URL is already empty
            if pd.isna(product_link) or product_link in ['', '-', 'None', None] or pd.isna(img_url) or img_url == '':
                continue
            
            # Replace image URL with product link
            if product_link and isinstance(product_link, str) and product_link.startswith(('http://', 'https://')):
                # Only replace if the current image URL is from pstatic.net
                if isinstance(img_url, str) and ('pstatic.net' in img_url or not img_url.strip()):
                    result_df.at[idx, naver_img_col] = product_link
                    replaced_count += 1
                    logger.debug(f"Row {idx}: Replaced Naver image URL with product link in upload file: {product_link[:50]}...")
        except Exception as e:
            logger.error(f"Error processing row {idx} in prepare_naver_image_urls_for_upload: {e}")
    
    logger.info(f"Prepared {replaced_count}/{processed_count} Naver image URLs for upload file")
    return result_df

def is_unreliable_naver_url(url: str) -> bool:
    """
    Checks if a URL is an unreliable Naver 'front' URL.
    
    Args:
        url: The URL to check
        
    Returns:
        True if it's an unreliable front URL, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    # Check for the problematic pattern
    if "pstatic.net/front/" in url:
        logger.warning(f"Detected unreliable 'front' URL: {url}")
        return True
    
    return False

def _prepare_data_for_excel(df: pd.DataFrame, skip_images=False) -> pd.DataFrame:
    """
    Prepare DataFrame for Excel output by cleaning and formatting data.
    Removes internal processing columns and ensures only expected columns are present.
    """
    try:
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Remove internal processing columns that shouldn't appear in final output
        for col in INTERNAL_PROCESSING_COLUMNS:
            if col in df.columns:
                df = df.drop(columns=[col])

        # 1) Rename columns EARLY so that original names are preserved before we drop/reorder columns
        df.rename(columns=COLUMN_RENAME_MAP, inplace=True, errors='ignore')

        # 2) Ensure all required columns from FINAL_COLUMN_ORDER exist
        for col in FINAL_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = ""
                logger.debug(f"Added missing column '{col}' to DataFrame before ordering.")
            
        # 3) Add promotional site columns if they're not in FINAL_COLUMN_ORDER
        # (This keeps them in the output even if they're not officially part of the final column order)
        for col in PROMOTIONAL_SITE_COLUMNS:
            if col in df.columns and col not in FINAL_COLUMN_ORDER:
                # Keep these columns in the dataframe even if they're not in the final column order
                logger.debug(f"Keeping promotional site column '{col}' in output DataFrame")

        # 4) Re-order columns based on FINAL_COLUMN_ORDER, but keep promotional site columns
        order_cols = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
        extra_cols = [col for col in df.columns if col in PROMOTIONAL_SITE_COLUMNS and col not in order_cols]
        
        # Use all main columns first, then add any extra promotional columns
        df = df[order_cols + extra_cols]
        logger.debug(f"Ordered {len(order_cols)} standard columns and kept {len(extra_cols)} extra columns")

        # For upload file, modify image column values to be web URLs or empty
        if skip_images:
            # Image columns now use new names from FINAL_COLUMN_ORDER / IMAGE_COLUMNS constant
            image_columns = [col for col in df.columns if col in IMAGE_COLUMNS]  # Use the constant

            for col in image_columns:
                # Replace image dict/path with web URL or empty string for upload file
                df[col] = df[col].apply(
                    lambda x:
                        # Case 1: Input is a dictionary with 'url' key
                        x['url'] if isinstance(x, dict) and 'url' in x and isinstance(x['url'], str) and x['url'].startswith(('http://', 'https://'))
                        # Case 2: Input is a string that is already a web URL
                        else (x if isinstance(x, str) and x.startswith(('http://', 'https://'))
                        # Case 3: Anything else (dict without web URL, local path, file://, other types, None)
                        else '')
                    if pd.notna(x) else ''
                )
            logger.debug(f"Processed image columns for upload file, keeping only web URLs: {image_columns}")

        # Format numeric columns (prices, quantities) using new names
        for col in df.columns:
            if any(keyword in col for keyword in ['단가', '가격', '수량']):
                try:
                    # 수량 컬럼 특별 처리
                    if '수량' in col:
                        # 수량은 정수형으로 변환
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        df[col] = df[col].fillna(pd.NA)  # 빈 값은 NA로 처리
                    # 가격 컬럼 처리
                    elif any(keyword in col for keyword in ['단가', '가격']):
                        # 가격은 실수형으로 변환 후 반올림
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        # VAT 포함 가격은 정수로 반올림
                        if 'V포함' in col or 'VAT' in col:
                            df[col] = df[col].round().astype('Int64')
                        else:
                            df[col] = df[col].round(2)  # VAT 미포함 가격은 소수점 2자리까지
                        df[col] = df[col].fillna(pd.NA)
                    
                    # 빈 값 처리
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].replace({pd.NA: '', np.nan: ''})
                    else:
                        df[col] = df[col].fillna('')
                except Exception as e:
                    logging.warning(f"Error formatting numeric column '{col}': {str(e)}")
                    df[col] = df[col].fillna('')  # Ensure NaNs are handled even on error
        
        # Format the promotional site indicator columns (Y/N format)
        for col in PROMOTIONAL_SITE_COLUMNS:
            if col in df.columns:
                # Ensure Y/N format
                df[col] = df[col].apply(
                    lambda x: 'Y' if isinstance(x, str) and x.upper() in ['Y', 'YES', 'TRUE', '1'] 
                    else ('N' if isinstance(x, str) and x.upper() in ['N', 'NO', 'FALSE', '0'] 
                    else ('Y' if x == True else ('N' if x == False else x)))
                    )
        
        logger.debug(f"Final columns for Excel output: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error preparing DataFrame for Excel: {e}")
        return pd.DataFrame()

def finalize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform final cleanup and formatting of DataFrame before Excel output.
    Ensures only expected columns are present in the correct order.
    """
    try:
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Remove any internal processing columns that might have slipped through
        for col in INTERNAL_PROCESSING_COLUMNS:
            if col in df.columns:
                df = df.drop(columns=[col])
                
        # Ensure only expected columns are present and in the correct order
        df = df[FINAL_COLUMN_ORDER]
        
        logger.info(f"Finalizing DataFrame for Excel. Input shape: {df.shape}")
        
        try:
            # Remove duplicate columns
            duplicate_cols = df.columns[df.columns.duplicated()].tolist()
            if duplicate_cols:
                logger.warning(f"Removing {len(duplicate_cols)} duplicate columns: {duplicate_cols}")
                df = df.loc[:, ~df.columns.duplicated()]
            
            # Create working copy
            output_df = df.copy()
            
            # Rename columns
            output_df.rename(columns=COLUMN_RENAME_MAP, inplace=True, errors='ignore')
            
            # Ensure all required columns exist
            for col in FINAL_COLUMN_ORDER:
                if col not in output_df.columns:
                    output_df[col] = None
                    logger.debug(f"Added missing column: {col}")
            
            # Add promotional site columns if they exist in the original
            for col in PROMOTIONAL_SITE_COLUMNS:
                if col in df.columns and col not in output_df.columns:
                    output_df[col] = df[col]
                    logger.debug(f"Preserved promotional site column: {col}")
            
            # Format numeric columns
            numeric_cols = set(PRICE_COLUMNS + QUANTITY_COLUMNS + PERCENTAGE_COLUMNS)
            for col in numeric_cols:
                if col in output_df.columns:
                    try:
                        output_df[col] = pd.to_numeric(output_df[col], errors='coerce')
                    except Exception as e:
                        logger.warning(f"Error converting column {col} to numeric: {e}")
            
            # Replace NaN/None values
            output_df = output_df.replace({pd.NA: None, np.nan: None})
            
            # Set default values for empty cells
            non_image_cols = [col for col in output_df.columns if col not in IMAGE_COLUMNS]
            for col in non_image_cols:
                output_df[col] = output_df[col].apply(
                    lambda x: '-' if pd.isna(x) or x == '' else x
                )
            
            # Format promotional site indicator columns (Y/N format)
            for col in PROMOTIONAL_SITE_COLUMNS:
                if col in output_df.columns:
                    output_df[col] = output_df[col].apply(
                        lambda x: 'Y' if isinstance(x, str) and x.upper() in ['Y', 'YES', 'TRUE', '1'] 
                        else ('N' if isinstance(x, str) and x.upper() in ['N', 'NO', 'FALSE', '0'] 
                        else ('Y' if x == True else ('N' if x == False else x)))
                        )
            
            # Reorder columns, keeping promotional site columns
            standard_cols = [col for col in FINAL_COLUMN_ORDER if col in output_df.columns]
            promo_cols = [col for col in PROMOTIONAL_SITE_COLUMNS if col in output_df.columns]
            extra_cols = [col for col in output_df.columns if col not in standard_cols and col not in promo_cols]
            
            # Order: standard columns, then promotional columns, then any other columns
            final_column_order = standard_cols + promo_cols + extra_cols
            output_df = output_df[final_column_order]
            
            logger.info(f"DataFrame finalized successfully. Output shape: {output_df.shape}")
            return output_df
        
        except Exception as e:
            logger.error(f"Error finalizing DataFrame: {e}")
            # Return original DataFrame if processing fails
            return df 
    except Exception as e:
        logger.error(f"Error finalizing DataFrame: {e}")
        # Return original DataFrame if processing fails
        return df 