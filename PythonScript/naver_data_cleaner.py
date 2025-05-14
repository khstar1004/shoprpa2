import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_naver_data(df):
    """
    Clean DataFrame by removing Naver product rows where image URL is missing
    but local fallback images are used.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing product data
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with invalid Naver entries removed
    """
    if df.empty:
        return df
        
    def _check_invalid_naver_image(cell_value):
        """Check if a cell contains invalid Naver image data"""
        if not isinstance(cell_value, dict):
            return False
            
        is_naver = cell_value.get('source') == 'naver'
        has_no_url = cell_value.get('url') is None
        is_fallback = cell_value.get('fallback', False)
        
        return is_naver and has_no_url and is_fallback

    # Find columns containing image data
    image_columns = []
    for col in df.columns:
        if isinstance(df[col].iloc[0], dict) and 'local_path' in df[col].iloc[0]:
            image_columns.append(col)
    
    rows_to_drop = []
    
    for idx, row in df.iterrows():
        for col in image_columns:
            cell_value = row[col]
            if _check_invalid_naver_image(cell_value):
                logger.warning(f"Dropping row {idx} due to invalid Naver image in column {col}")
                logger.warning(f"Product info: {row.get('상품명', 'Unknown product')}")
                rows_to_drop.append(idx)
                break
    
    if rows_to_drop:
        df = df.drop(rows_to_drop)
        logger.info(f"Removed {len(rows_to_drop)} rows with invalid Naver images")
        
    return df.reset_index(drop=True)

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