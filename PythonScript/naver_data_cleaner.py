import pandas as pd
import logging
import os
import re
import glob

logger = logging.getLogger(__name__)

def clean_naver_data(df):
    """
    Clean DataFrame by removing all Naver data (images, prices, quantities) for invalid matches
    
    Args:
        df (pd.DataFrame): Input DataFrame containing product data
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with invalid Naver data removed
    """
    if df.empty:
        return df
        
    def _check_invalid_naver_image(cell_value):
        """Check if a cell contains invalid Naver image data"""
        if not isinstance(cell_value, dict):
            return False
            
        is_naver_source = cell_value.get('source') == 'naver'
        if not is_naver_source:
            return False

        has_valid_local_path = False
        local_path = cell_value.get('local_path')
        if isinstance(local_path, str) and local_path.strip():
            # Be more permissive - don't require file to exist if path is valid format
            if os.path.exists(local_path):
                has_valid_local_path = True
            else:
                # Consider it valid if it looks like a path even if file doesn't exist
                if '\\' in local_path or '/' in local_path:
                    has_valid_local_path = True

        has_valid_url = False
        url = cell_value.get('url')
        if isinstance(url, str) and url.strip().startswith(('http://', 'https://')):
            # Accept all pstatic.net URLs, not just reject front ones
            if "pstatic.net" in url:
                has_valid_url = True
            else:
                has_valid_url = True
        
        is_fallback = cell_value.get('fallback', False)

        # Only consider invalid if we have neither valid local path nor URL and it's a fallback
        if not has_valid_local_path and not has_valid_url and is_fallback:
            logger.debug(f"Invalid Naver image: No valid local path AND no valid URL AND marked as fallback. Data: {cell_value}")
            return True
            
        return False

    # 네이버 관련 모든 컬럼 정의
    naver_columns = {
        'image': ['네이버 이미지', '네이버쇼핑(이미지링크)'],
        'data': [
            '네이버 쇼핑 링크',
            '공급사 상품링크',
            '기본수량(3)',
            '판매단가(V포함)(3)',
            '가격차이(3)',
            '가격차이(3)(%)',
            '공급사명'
        ]
    }
    
    # 네이버 이미지 컬럼 찾기
    naver_image_col = None
    for col in df.columns:
        if col in naver_columns['image']:
            naver_image_col = col
            break
    
    if not naver_image_col:
        logger.warning("No Naver image column found in DataFrame")
        return df
    
    # 이미지가 유효하지 않은 경우 모든 네이버 데이터 삭제
    rows_modified = 0
    for idx, row in df.iterrows():
        cell_value = row[naver_image_col]
        if _check_invalid_naver_image(cell_value):
            logger.info(f"Clearing all Naver data in row {idx} for product: {row.get('상품명', 'Unknown product')}")
            # 이미지 컬럼 클리어
            df.at[idx, naver_image_col] = None
            
            # 관련 데이터 컬럼들도 클리어
            for col in naver_columns['data']:
                if col in df.columns:
                    df.at[idx, col] = None
            
            rows_modified += 1
    
    if rows_modified:
        logger.info(f"Cleared all Naver data for {rows_modified} products with invalid images")
        
    return df

def get_invalid_naver_rows(df):
    """
    Get indices of rows that have invalid Naver data (missing URL but using fallback)
    Useful for debugging or reporting purposes.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        list: List of row indices with invalid Naver data
    """
    invalid_rows = []
    
    for idx, row in df.iterrows():
        for col in df.columns:
            cell_value = row[col]
            if isinstance(cell_value, dict):
                if (cell_value.get('source') == 'naver' and 
                    cell_value.get('url') is None and 
                    cell_value.get('fallback', False)):
                    invalid_rows.append({
                        'index': idx,
                        'column': col,
                        'product_name': row.get('상품명', 'Unknown'),
                        'image_info': cell_value
                    })
                    
    return invalid_rows 

def fix_missing_naver_images(df, result_file=True):
    """
    Fix Naver images that have URLs but are not displaying in Excel by finding 
    matching local images in the Naver image directory.
    
    This function scans the DataFrame for Naver image cells, checks if they have valid local paths,
    and if not, tries to find a matching local file based on URL patterns.
    
    Args:
        df (pd.DataFrame): DataFrame containing product data with Naver images
        result_file (bool): True if this is the result file (with images), False for upload file
        
    Returns:
        pd.DataFrame: DataFrame with fixed Naver image paths
    """
    if df.empty:
        return df
    
    # Skip processing for upload file (links only)
    if not result_file:
        logger.info("Skipping Naver image fix for upload file (links only)")
        return df
    
    # Define Naver image directory path
    naver_image_dir = os.path.join('C:\\RPA\\Image\\Main', 'Naver')
    if not os.path.exists(naver_image_dir):
        logger.warning(f"Naver image directory not found: {naver_image_dir}")
        return df
    
    # Get list of all available Naver image files
    naver_image_files = glob.glob(os.path.join(naver_image_dir, "naver_*.jpg")) + glob.glob(os.path.join(naver_image_dir, "naver_*.png"))
    if not naver_image_files:
        logger.warning(f"No Naver image files found in directory: {naver_image_dir}")
        return df
    
    logger.info(f"Found {len(naver_image_files)} Naver image files in {naver_image_dir}")
    
    # Create a lookup dictionary by hash patterns in filenames
    image_lookup = {}
    hash_pattern = re.compile(r'naver_([a-f0-9]+)_([a-f0-9]+)')
    
    for image_path in naver_image_files:
        filename = os.path.basename(image_path)
        match = hash_pattern.search(filename)
        if match:
            # Extract hash values from filename
            hash1, hash2 = match.groups()
            # Create different lookup keys
            keys = [
                f"{hash1}_{hash2}",  # Complete hash key
                hash1,              # First hash only
                hash2               # Second hash only
            ]
            
            for key in keys:
                if key not in image_lookup:
                    image_lookup[key] = []
                image_lookup[key].append(image_path)
    
    # Define possible Naver image column names
    naver_image_columns = ['네이버 이미지', '네이버쇼핑(이미지링크)']
    
    # Track statistics
    fixed_count = 0
    missing_count = 0
    already_valid_count = 0
    
    # Scan DataFrame for Naver image cells
    for col in df.columns:
        if col not in naver_image_columns:
            continue
            
        for idx, row in df.iterrows():
            cell_value = row[col]
            
            # Skip non-dictionary cells or non-Naver images
            if not isinstance(cell_value, dict) or cell_value.get('source') != 'naver':
                continue
                
            # Skip if local path already exists and is valid
            local_path = cell_value.get('local_path')
            if isinstance(local_path, str) and os.path.exists(local_path):
                already_valid_count += 1
                continue
                
            # Get URL to check for hash patterns
            url = cell_value.get('url')
            if not isinstance(url, str) or not url.strip():
                missing_count += 1
                continue
                
            # Try to extract hash patterns from URL
            hash_match = re.search(r'([a-f0-9]{16})[^a-f0-9]?([a-f0-9]{8})', url)
            found_local_path = None
            
            if hash_match:
                hash1, hash2 = hash_match.groups()
                keys_to_try = [
                    f"{hash1}_{hash2}",  # Complete match
                    hash1,              # First hash only
                    hash2               # Second hash only
                ]
                
                # Try each key to find a matching image
                for key in keys_to_try:
                    if key in image_lookup:
                        # Prefer _nobg.png files if available (background removed)
                        nobg_files = [f for f in image_lookup[key] if '_nobg.png' in f]
                        if nobg_files:
                            found_local_path = nobg_files[0]
                            break
                        # Otherwise use any available file
                        found_local_path = image_lookup[key][0]
                        break
            
            # If no match by hash, try a simpler approach with filename pattern
            if not found_local_path:
                # Extract any hash-like segments from URL
                simple_hash_match = re.findall(r'[a-f0-9]{8,}', url)
                if simple_hash_match:
                    for hash_segment in simple_hash_match:
                        # Look for any file containing this hash segment
                        matching_files = [f for f in naver_image_files if hash_segment in f]
                        if matching_files:
                            # Prefer _nobg.png files
                            nobg_files = [f for f in matching_files if '_nobg.png' in f]
                            if nobg_files:
                                found_local_path = nobg_files[0]
                                break
                            found_local_path = matching_files[0]
                            break
            
            # Update the cell with the found local path
            if found_local_path:
                logger.info(f"Fixed Naver image path for row {idx}: {found_local_path}")
                cell_value['local_path'] = found_local_path
                cell_value['original_path'] = found_local_path
                df.at[idx, col] = cell_value
                fixed_count += 1
            else:
                logger.debug(f"Could not find matching local Naver image for URL: {url}")
                missing_count += 1
    
    logger.info(f"Naver image fix complete: {fixed_count} images fixed, {already_valid_count} already valid, {missing_count} could not be fixed")
    return df 