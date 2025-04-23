import os
import sys
import pandas as pd
import logging
import configparser
from utils import load_config
from excel_utils import find_excel_file, REQUIRED_INPUT_COLUMNS

# Use standard Python logger
logger = logging.getLogger(__name__)

def check_input_file(config: configparser.ConfigParser):
    """Check the input Excel file and report any issues. Reads config for path."""
    try:
        # Get INPUT_DIR from config
        input_dir = config.get('Paths', 'input_dir', fallback='./INPUT')
        logger.info(f"Starting input Excel validation in configured directory: {input_dir}")

        # Find the first Excel file using utility function
        excel_filename = find_excel_file(input_dir, extension='.xlsx')

        if not excel_filename:
            logger.error(f"Validation Failed: No Excel file (.xlsx) found in the input directory: {input_dir}")
            return False

        input_file_path = os.path.join(input_dir, excel_filename)
        logger.info(f"Validating input file: {input_file_path}")

        # Check file existence and size
        if not os.path.exists(input_file_path):
            logger.error(f"Validation Failed: File not found at path: {input_file_path}")
            return False
            
        file_size = os.path.getsize(input_file_path)
        logger.info(f"Input file exists. Size: {file_size} bytes")

        # Try to read the Excel file
        try:
            df = pd.read_excel(input_file_path, sheet_name=0)
            logger.info(f"Excel file read successfully. Shape: {df.shape}")
        except Exception as excel_error:
            logger.error(f"Error reading Excel file: {excel_error}")
            return False

        # Clean column names (strip whitespace and remove \xa0)
        original_columns = list(df.columns)
        df.columns = [col.replace('\xa0', ' ').strip() if isinstance(col, str) else col for col in df.columns]
        cleaned_columns = list(df.columns)
        if original_columns != cleaned_columns:
             logger.warning(f"Column names were cleaned of \xa0 characters and whitespace.")
             logger.debug(f"Original columns: {original_columns}")
             logger.debug(f"Cleaned columns: {cleaned_columns}")
        logger.info(f"Columns after cleaning: {cleaned_columns}")

        # Use REQUIRED_INPUT_COLUMNS from excel_utils
        required_columns_from_utils = REQUIRED_INPUT_COLUMNS 

        # Check for missing required columns
        missing_columns = [col for col in required_columns_from_utils if col not in df.columns]
        if missing_columns:
            logger.error(f"Input file is missing required columns: {missing_columns}")
            logger.error(f"Required columns defined in excel_utils.py: {required_columns_from_utils}")
            return False
        else:
            logger.info(f"All required columns found: {required_columns_from_utils}")

        # Check content of required columns
        empty_columns = []
        for col in required_columns_from_utils:
            # Check for empty values (NaN, None, empty string, or whitespace, or '-')
            empty_mask = (
                df[col].isna() |
                (df[col].astype(str).str.strip() == '') |
                (df[col].astype(str).str.strip() == '-')
            )
            empty_count = empty_mask.sum()

            if empty_count > 0:
                empty_columns.append((col, empty_count))
                logger.warning(f"Required column '{col}' has {empty_count} empty/placeholder values")

        if empty_columns:
            logger.error("Some required columns have empty or placeholder ('-') values:")
            for col, count in empty_columns:
                logger.error(f"- {col}: {count} empty/placeholder values")
            return False

        # Check data types and validate numeric columns
        logger.info("Validating data types for key numeric columns...")
        try:
            # Validate only the core numeric columns needed early on
            numeric_columns_to_validate = ['기본수량(1)', '판매단가(V포함)'] 
            validation_failed = False
            for col in numeric_columns_to_validate:
                if col in df.columns:
                    # Try to convert to numeric, allowing non-numeric values to become NaN
                    numeric_values = pd.to_numeric(df[col], errors='coerce')
                    non_numeric_count = numeric_values.isna().sum()

                    if non_numeric_count > 0:
                        logger.error(f"Column '{col}' has {non_numeric_count} non-numeric values that could not be converted.")
                        validation_failed = True

            if validation_failed:
                return False
        except Exception as e:
            logger.error(f"Error validating numeric columns: {e}")
            return False

        # Print a sample of the data
        logger.info("Sample data from input file (after cleaning columns):")
        logger.info(df.head(3).to_string())

        logger.info("Input Excel validation successful.")
        return True

    except Exception as e:
        logger.error(f"Error checking input file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    print("Running input Excel file check...")
    # Load config to pass to the function
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
    if not os.path.exists(config_path):
         print(f"Config file not found at {config_path}. Cannot run validation.")
         sys.exit(1)
         
    config = load_config(config_path)
    if not config:
         print("Failed to load configuration. Cannot run validation.")
         sys.exit(1)
         
    # Set up basic logging if run standalone
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    if check_input_file(config):
        print("\nInput file validation completed successfully.")
    else:
        print("\nInput file validation failed.")
        sys.exit(1) 