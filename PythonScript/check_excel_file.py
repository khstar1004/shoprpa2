import os
import logging
import pandas as pd
from excel_utils import (
    find_excel_file, REQUIRED_COLUMNS
)

# Configure logging (consider moving to a central setup if used elsewhere)
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'excel_check.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'INPUT')

def validate_input_excel() -> bool:
    """Finds and validates the first Excel file in the INPUT directory."""
    logger.info(f"Starting input Excel validation in directory: {INPUT_DIR}")
    try:
        # Find the first Excel file
        excel_filename = find_excel_file(INPUT_DIR, extension='.xlsx')

        if not excel_filename:
            logger.error(f"Validation Failed: No Excel file (.xlsx) found in the input directory: {INPUT_DIR}")
            return False

        input_file_path = os.path.join(INPUT_DIR, excel_filename)
        logger.info(f"Validating input file: {input_file_path}")

        # Basic check: File exists (already implied by find_excel_file)
        if not os.path.exists(input_file_path):
            logger.error(f"Validation Failed: File not found at path: {input_file_path} (this should not happen if find_excel_file worked)")
            return False

        # Read the Excel file to check columns
        try:
            df = pd.read_excel(input_file_path, sheet_name=0) # Read first sheet
            logger.info(f"Successfully read '{excel_filename}'. Shape: {df.shape}")
        except Exception as read_err:
            logger.error(f"Validation Failed: Could not read Excel file '{input_file_path}'. Error: {read_err}", exc_info=True)
            return False

        # Clean column names (strip whitespace)
        original_columns = list(df.columns)
        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
        cleaned_columns = list(df.columns)
        if original_columns != cleaned_columns:
             logger.warning(f"Column names were stripped of leading/trailing whitespace.")
             logger.debug(f"Original columns: {original_columns}")
             logger.debug(f"Cleaned columns: {cleaned_columns}")

        # Check for required columns (case-sensitive)
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

        if missing_columns:
            logger.error(f"Validation Failed: Input file '{excel_filename}' is missing required columns: {missing_columns}")
            logger.error(f"Required columns are: {REQUIRED_COLUMNS}")
            logger.error(f"Columns found in file: {cleaned_columns}")
            return False
        else:
            logger.info(f"All required columns ({len(REQUIRED_COLUMNS)}) found in '{excel_filename}'.")
            logger.debug(f"Found columns: {cleaned_columns}")
            logger.info("Input Excel validation successful.")
            return True

    except Exception as e:
        logger.error(f"An unexpected error occurred during Excel validation: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    print("Running input Excel file check...")
    is_valid = validate_input_excel()
    if is_valid:
        print("\nInput Excel file validation successful.")
    else:
        print("\nInput Excel file validation failed. Please check the logs and the file in the INPUT directory.")