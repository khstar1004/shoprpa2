import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_naver_data(df):
    """
    Clean DataFrame by removing Naver product rows where image URL is missing
    but local fallback images are used. (Modified: Clears Naver data instead of dropping rows)
    
    Args:
        df (pd.DataFrame): Input DataFrame containing product data
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with invalid Naver entries modified
    """
    if df.empty:
        return df.copy() # Return a copy to avoid modifying the original empty DataFrame
        
    def _check_invalid_naver_image(cell_value):
        """Check if a cell contains invalid Naver image data"""
        if not isinstance(cell_value, dict):
            return False
            
        is_naver = cell_value.get('source') == 'naver'
        # An image is problematic if its URL is None or an empty string, AND it's a fallback
        has_no_valid_url = cell_value.get('url') is None or cell_value.get('url') == ''
        is_fallback = cell_value.get('fallback', False)
        
        return is_naver and has_no_valid_url and is_fallback

    # Find columns that might contain image data dictionaries
    # Iterate through a sample of rows (e.g., first 5 non-null) or rely on known names
    image_columns = []
    potential_image_cols = df.select_dtypes(include=['object']).columns
    
    # Heuristic to find image columns: check first few non-NA cells
    for col in potential_image_cols:
        try:
            # Find the first non-NA value in the column to check its type
            first_valid_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_valid_value, dict) and ('local_path' in first_valid_value or 'url' in first_valid_value):
                image_columns.append(col)
        except IndexError:
            # Column might be all NA, skip
            continue
        except Exception as e:
            logger.debug(f"Could not determine if column {col} is an image column: {e}")
            continue
            
    if not image_columns:
        logger.debug("No image columns identified for Naver data cleaning.")
        return df.copy() # Return a copy as no changes will be made

    # Define Naver-specific columns to clear
    # These are typically associated with Naver data entries
    NAVER_DATA_COLUMNS_TO_CLEAR = [
        '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)',
        '공급사명', ' 네이버 쇼핑 링크', '공급사 상품링크', '네이버쇼핑(이미지링크)', '네이버 이미지' 
        # Ensure all relevant Naver columns are listed. '네이버 이미지' is often the primary one.
        # Also include the column found by 'col' if it's a known Naver image column not in this list.
    ]
    
    modified_rows_indices = set()
    
    for idx, row in df.iterrows():
        for col in image_columns: # Iterate identified potential image columns
            cell_value = row[col]
            if _check_invalid_naver_image(cell_value): # Check if it's specifically a problematic Naver image
                product_name_log = row.get('상품명', 'Unknown product')
                logger.warning(
                    f"Row {idx} ('{product_name_log}'): Found invalid Naver image data in column '{col}'. "
                    f"Clearing this cell and related Naver data columns instead of dropping the row."
                )
                
                # Clear the problematic Naver image cell itself
                # Check if 'col' is the main '네이버 이미지' or similar, ensure it's cleared
                df.at[idx, col] = None # Using None, adjust if '-' or pd.NA is standard

                # Clear all other Naver-specific data columns for this row
                for naver_data_col in NAVER_DATA_COLUMNS_TO_CLEAR:
                    if naver_data_col in df.columns:
                        df.at[idx, naver_data_col] = None # Using None, adjust as needed
                
                modified_rows_indices.add(idx)
                # Once problematic Naver data is found and cleared for the row,
                # no need to check other image_columns for the same Naver issue in this row.
                break 
    
    if modified_rows_indices:
        logger.info(f"Cleared Naver-specific data for {len(modified_rows_indices)} rows due to invalid Naver images (instead of dropping them).")
    
    # The original function always returned df.reset_index(drop=True).
    # If rows were dropped, this is essential. If rows are only modified,
    # reset_index might not be strictly necessary but is kept for consistency
    # with the original function's output contract (always a fresh 0-based index).
    # However, modifying in place with .at does not invalidate the index.
    # Returning a copy if no changes, or the modified df.
    if not modified_rows_indices and df.index.is_monotonic_increasing and df.index.is_unique and df.index.min() == 0:
         # If no changes and index is already a standard RangeIndex, return a copy to be safe.
         # Or, if confident, return df directly if no modifications.
         # For safety and consistency with original reset_index, we can do it, or return copy if no change.
         pass # Let it fall through to reset_index if there's doubt

    # If modifications happened, or if original df wasn't a fresh index, reset it.
    # The original code *always* did this.
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