import pandas as pd
import logging
import configparser
import os
import openpyxl # Required for reading/writing .xlsx

# Define expected column names from the final Excel output
# Adjust these if the actual column names in the generated Excel differ
KOREAGIFT_LINK_COL = '고려 링크'
NAVER_LINK_COL = '네이버 링크'
# Price difference columns to filter
KOREAGIFT_PRICE_DIFF_COL = '고려 가격차이'
NAVER_PRICE_DIFF_COL = '네이버 가격차이'
# Optional: Add other columns to check if links alone are insufficient
# KOREAGIFT_PRICE_COL = '판매단가2(VAT포함)'
# NAVER_PRICE_COL = '판매단가3 (VAT포함)'

def _is_data_missing(series: pd.Series) -> pd.Series:
    """Checks if data in a pandas Series is missing.
    Considers '-', '', None, NaN as missing.
    """
    # Using vectorized operations for efficiency
    is_missing = series.isna() | series.isin(['-', ''])
    return is_missing

def filter_upload_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters DataFrame based on two criteria:
    1. Removes rows where both Koreagift and Naver data are missing
    2. Removes rows where price difference is >= -1 for either Koreagift or Naver
    """
    if df.empty:
        logging.warning("Input DataFrame for upload filtering is empty. Returning empty DataFrame.")
        return df.copy() # Return a copy

    # Check if necessary link columns exist
    required_cols = [KOREAGIFT_LINK_COL, NAVER_LINK_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Required columns for upload filtering missing: {missing_cols}. Cannot filter.")
        return df.copy() # Return a copy of the original if columns are missing

    # --- 1. Filter based on missing links ---
    # Determine missing status based on link columns
    is_kogift_missing = _is_data_missing(df[KOREAGIFT_LINK_COL])
    is_naver_missing = _is_data_missing(df[NAVER_LINK_COL])
    
    # Identify rows where BOTH sources are missing
    missing_links_mask = is_kogift_missing & is_naver_missing
    
    # --- 2. Filter based on price differences ---
    # Initialize masks for price difference conditions
    kogift_price_diff_mask = pd.Series(False, index=df.index)
    naver_price_diff_mask = pd.Series(False, index=df.index)
    
    # Check Koreagift price difference (if column exists)
    if KOREAGIFT_PRICE_DIFF_COL in df.columns:
        # Convert to numeric, coercing errors to NaN
        kogift_price_diff = pd.to_numeric(df[KOREAGIFT_PRICE_DIFF_COL], errors='coerce')
        # Find rows where price difference is >= -1 and the link is not missing
        kogift_price_diff_mask = (kogift_price_diff >= -1) & (~is_kogift_missing)
        
    # Check Naver price difference (if column exists)
    if NAVER_PRICE_DIFF_COL in df.columns:
        # Convert to numeric, coercing errors to NaN
        naver_price_diff = pd.to_numeric(df[NAVER_PRICE_DIFF_COL], errors='coerce')
        # Find rows where price difference is >= -1 and the link is not missing
        naver_price_diff_mask = (naver_price_diff >= -1) & (~is_naver_missing)
    
    # Combine all conditions: remove rows with missing links OR positive price differences
    rows_to_remove_mask = missing_links_mask | kogift_price_diff_mask | naver_price_diff_mask

    # Apply the filter using boolean indexing, keeping rows where the condition is FALSE
    initial_rows = len(df)
    filtered_df = df[~rows_to_remove_mask].copy()
    removed_count = initial_rows - len(filtered_df)
    
    # Log the filtering results
    if removed_count > 0:
        logging.info(f"Upload filter: Removed {removed_count} rows total:")
        missing_links_count = missing_links_mask.sum()
        kogift_price_count = kogift_price_diff_mask.sum()
        naver_price_count = naver_price_diff_mask.sum()
        logging.info(f"  - {missing_links_count} rows with both Koreagift and Naver links missing")
        logging.info(f"  - {kogift_price_count} rows with Koreagift price difference >= -1")
        logging.info(f"  - {naver_price_count} rows with Naver price difference >= -1")
    else:
        logging.info("Upload filter: No rows removed after applying all filtering criteria.")

    return filtered_df

def apply_filter_to_upload_excel(upload_file_path: str, config: configparser.ConfigParser) -> bool:
    """Reads the upload Excel, applies filtering, and saves it back, overwriting the original.

    Args:
        upload_file_path: Path to the upload .xlsx file.
        config: The configuration object (unused currently, but passed for potential future use).

    Returns:
        True if filtering was applied successfully, False otherwise.
    """
    if not upload_file_path or not isinstance(upload_file_path, str):
        logging.error("Upload file path is invalid. Skipping filtering.")
        return False
    if not os.path.exists(upload_file_path):
        logging.error(f"Upload file does not exist: {upload_file_path}. Skipping filtering.")
        return False

    logging.info(f"Applying post-creation filter to upload file: {upload_file_path}")
    try:
        # Read the Excel file. Assumes data is in the first sheet.
        # Ensure openpyxl is installed and used as the engine.
        df = pd.read_excel(upload_file_path, engine='openpyxl')

        # Apply the filtering logic
        filtered_df = filter_upload_data(df)

        # Save the filtered data back to the same file path, overwriting it.
        # Use 'openpyxl' engine to ensure compatibility with .xlsx format features.
        filtered_df.to_excel(upload_file_path, index=False, engine='openpyxl')

        logging.info(f"Successfully filtered and overwrote upload file: {upload_file_path}")
        return True
    except FileNotFoundError:
         logging.error(f"Upload file not found during filtering read: {upload_file_path}")
         return False
    except ImportError:
         logging.error("`openpyxl` is required to read/write Excel .xlsx files. Please install it (`pip install openpyxl`).")
         return False
    except Exception as e:
        logging.error(f"Error filtering or saving upload Excel file {upload_file_path}: {e}", exc_info=True)
        return False 