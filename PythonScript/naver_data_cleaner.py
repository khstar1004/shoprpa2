import pandas as pd
import logging
import os

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