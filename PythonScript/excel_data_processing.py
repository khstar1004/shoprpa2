import os
import logging
import pandas as pd
import numpy as np
import re
import functools
import json
from typing import Optional, Dict, Any, List, Union, Tuple
from pathlib import Path
import traceback

from excel_constants import (
    COLUMN_RENAME_MAP, FINAL_COLUMN_ORDER, 
    PRICE_COLUMNS, QUANTITY_COLUMNS, PERCENTAGE_COLUMNS,
    IMAGE_COLUMNS, ERROR_MESSAGE_VALUES, ERROR_MESSAGES,
    IMAGE_DIRS, REQUIRED_INPUT_COLUMNS,
    UPLOAD_COLUMN_MAPPING,
    UPLOAD_COLUMN_ORDER
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
                 if col in IMAGE_COLUMNS or '이미지' in col.lower()]
    image_cols = list(dict.fromkeys(image_cols))  # Remove duplicates
    
    if not image_cols:
        logger.debug("No image columns found in DataFrame to flatten")
        return df_result
    
    logger.info(f"Flattening nested dictionaries in {len(image_cols)} image columns")
    
    # Apply the extraction to all image columns
    for col in image_cols:
        if col in df_result.columns:
            try:
                # Use the improved extract_url_from_value function
                df_result[col] = df_result[col].apply(extract_url_from_value)
                logger.debug(f"Successfully flattened column: {col}")
            except Exception as e:
                logger.error(f"Error flattening image data in column {col}: {e}")
                # Don't modify the column if there's an error
    
    # Double check we didn't miss any dict or complex structures
    for col in image_cols:
        if col in df_result.columns:
            # Check for any remaining dictionary values
            has_dict = df_result[col].apply(lambda x: isinstance(x, dict)).any()
            if has_dict:
                logger.warning(f"Column {col} still contains dictionary values after flattening")
                # Apply a second pass with stringification for any dictionaries
                df_result[col] = df_result[col].apply(
                    lambda x: str(x) if isinstance(x, dict) else x
                )
    
    return df_result

def extract_url_from_value(value) -> str:
    """
    Extract URL or meaningful string representation from complex data structures.
    
    This function handles various nested dictionary formats that contain image URLs.
    
    Args:
        value: The value to extract URL from (dict, str, list, etc.)
        
    Returns:
        str: Extracted URL or meaningful string representation
    """
    # Handle None/NaN values
    if pd.isna(value) or value is None:
        return ""
    
    # Handle strings
    if isinstance(value, str):
        # Check if it's an error message we want to preserve
        if any(error_msg in value for error_msg in ERROR_MESSAGE_VALUES):
            return value
        
        # Handle simple strings
        if value.startswith(('http://', 'https://', 'file://')):
            return value
        elif value.strip() == "" or value == "-":
            return "-"
        return value
    
    # Handle numbers
    if isinstance(value, (int, float)):
        return str(value)
    
    # Handle dictionary values - most common case for image data
    if isinstance(value, dict):
        # Case 1: Nested URL structure {'url': {'url': 'actual_url', ...}}
        if 'url' in value and isinstance(value['url'], dict) and 'url' in value['url']:
            return value['url']['url']
        
        # Case 2: Direct URL {'url': 'actual_url'}
        elif 'url' in value and isinstance(value['url'], str):
            return value['url']
        
        # Case 3: Look for local path
        for path_field in ['local_path', 'path', 'file_path']:
            if path_field in value and value[path_field]:
                return str(value[path_field])
        
        # Case 4: Look for other common URL fields
        for url_field in ['image_url', 'product_url', 'src', 'link', 'href']:
            if url_field in value and isinstance(value[url_field], str):
                return value[url_field]
        
        # Case 5: Product name as fallback
        if 'product_name' in value:
            return f"Product: {value['product_name']}"
        
        # If no useful field found, convert to simple string with length limit
        dict_str = str(value)
        if len(dict_str) > 255:  # Excel cell has size limitations
            return f"Complex data (Dict with {len(value)} keys)"
        return dict_str
    
    # Handle list/tuple values
    if isinstance(value, (list, tuple)):
        # Try to extract URL from first item
        if len(value) > 0:
            first_item = extract_url_from_value(value[0])
            if first_item and first_item != '-':
                return first_item
        
        # If no URL in items, use a generic representation
        return f"List with {len(value)} items"
    
    # Default case - convert to string safely
    try:
        return str(value)
    except:
        return "Complex data (unconvertible)"

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
    Ensures all required columns are present in the correct order.
    """
    try:
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Add missing required columns with default values
        missing_cols = [col for col in REQUIRED_INPUT_COLUMNS if col not in df.columns]
        if missing_cols:
            logger.warning(f"Adding missing required columns with default values: {missing_cols}")
            for col in missing_cols:
                if col == '구분':
                    df[col] = 'A'  # Default to 승인관리
                elif col in ['업체명', '업체코드', 'Code', '중분류카테고리']:
                    df[col] = '-'  # Default placeholder for text columns
                elif col in ['기본수량(1)']:
                    df[col] = 1    # Default quantity
                elif col in ['판매단가(V포함)']:
                    df[col] = 0    # Default price
                elif col in ['본사상품링크']:
                    df[col] = ''   # Empty string for links
                else:
                    df[col] = ''   # Empty string for other columns

        # Handle NaN values
        df = df.fillna('')
        
        # Convert numeric columns to appropriate types
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0)
            
            # Format percentage columns
            if '%' in col:
                df[col] = df[col].apply(lambda x: f"{x:.1f}%" if x != 0 else '')
            else:
                df[col] = df[col].apply(lambda x: f"{int(x):,}" if x != 0 else '')
        
        # Process image columns
        for col in IMAGE_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: {} if pd.isna(x) or x == '' else x)
        
        return df
        
    except Exception as e:
        logger.error(f"Error in finalize_dataframe_for_excel: {e}")
        logger.debug(traceback.format_exc())
        raise

def prepare_upload_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for upload file format.
    """
    try:
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Rename columns according to upload format
        df = df.rename(columns=UPLOAD_COLUMN_MAPPING)
        
        # Reorder columns according to upload format
        existing_columns = [col for col in UPLOAD_COLUMN_ORDER if col in df.columns]
        extra_columns = [col for col in df.columns if col not in UPLOAD_COLUMN_ORDER]
        df = df[existing_columns + extra_columns]
        
        # Add empty rows at the end as per example
        df.loc[len(df)] = ''  # Add empty row
        df.loc[len(df)] = ['\\'] + [''] * (len(df.columns) - 1)  # Add row with backslash
        
        return df
        
    except Exception as e:
        logger.error(f"Error in prepare_upload_dataframe: {e}")
        logger.debug(traceback.format_exc())
        raise

def format_product_data_for_output(input_df: pd.DataFrame, 
                             kogift_results: Dict[str, List[Dict]] = None, 
                             naver_results: Dict[str, List[Dict]] = None,
                             input_file_image_map: Dict[str, Any] = None,
                             haereum_image_url_map: Dict[str, str] = None) -> pd.DataFrame:
    """Format matched data for final output, ensuring all required columns and image URLs/dicts."""
    
    # 필요한 컬럼 리스트 정의
    required_columns = [
        '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명',
        '기본수량(1)', '판매단가(V포함)', '본사상품링크',
        '기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
        '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크',
        '본사 이미지', '고려기프트 이미지', '네이버 이미지'
    ]
    
    # 입력 데이터프레임 복사
    df = input_df.copy()
    
    # ... (기존 코드) ...
    
    # 마지막에 필요한 컬럼만 선택하여 반환
    final_df = df[required_columns]
    return final_df 

def validate_price_data(row_data, base_qty, price):
    """
    가격 데이터를 검증하는 함수
    
    Args:
        row_data: 행 데이터
        base_qty: 기본 수량
        price: 검증할 가격
        
    Returns:
        list: 경고 메시지 리스트
    """
    warnings = []
    
    # None 값 체크
    if price is None:
        return warnings
        
    try:
        # 숫자로 변환 시도
        price = float(price) if price != '' else 0
        base_qty = int(base_qty) if base_qty is not None and base_qty != '' else 0
        
        # 수량 검증
        if base_qty < 100:  # 일반적인 최소 주문 수량보다 작은 경우
            warnings.append(f"주문 수량({base_qty})이 일반적인 최소 주문 수량보다 작습니다")
            
        # 가격 검증
        if price <= 0:
            warnings.append("가격이 0 이하입니다")
        elif price > 1000000:  # 비정상적으로 높은 가격
            warnings.append(f"비정상적으로 높은 가격({price:,}원)입니다")
            
        # 가격과 수량의 관계 검증
        if base_qty > 0 and price > 0:
            price_per_unit = price / base_qty
            if price_per_unit < 10:  # 단가가 너무 낮은 경우
                warnings.append(f"단가가 너무 낮습니다 ({price_per_unit:,.1f}원/개)")
            elif price_per_unit > 100000:  # 단가가 너무 높은 경우
                warnings.append(f"단가가 너무 높습니다 ({price_per_unit:,.1f}원/개)")
                
    except (ValueError, TypeError, ZeroDivisionError):
        pass  # 변환 실패시 경고 추가하지 않음
        
    return warnings 