import os
import sys
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("input_check.log"),
        logging.StreamHandler()
    ]
)

# Path to input directory
INPUT_DIR = 'C:\\RPA\\Input'

def check_input_file():
    """Check the input Excel file and report any issues"""
    try:
        # Find the first Excel file in the input directory
        xlsx_files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith('.xlsx')]
        
        if not xlsx_files:
            logging.error("No Excel files found in the input directory.")
            return False
        
        # Read the first Excel file
        input_file = os.path.join(INPUT_DIR, xlsx_files[0])
        logging.info(f"Checking input file: {input_file}")
        
        # Check file existence and size
        if os.path.exists(input_file):
            file_size = os.path.getsize(input_file)
            logging.info(f"Input file exists. Size: {file_size} bytes")
        else:
            logging.error(f"Input file does not exist: {input_file}")
            return False
        
        # Try to read the Excel file
        try:
            df = pd.read_excel(input_file)
            logging.info(f"Excel file read successfully. Shape: {df.shape}")
            logging.info(f"Columns found: {list(df.columns)}")
        except Exception as excel_error:
            logging.error(f"Error reading Excel file: {excel_error}")
            return False
        
        # Clean column names
        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
        
        # Required columns that must exist and have content
        required_columns = [
            '구분', '담당자', '업체명', '업체코드', 'Code', 
            '중분류카테고리', '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
        ]
        
        # Optional columns that should exist but can be empty
        optional_columns = [
            '기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', 
            '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
            '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', 
            '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크'
        ]
        
        # Check for missing required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Input file is missing required columns: {missing_columns}")
            return False
            
        # Check for missing optional columns
        missing_optional = [col for col in optional_columns if col not in df.columns]
        if missing_optional:
            logging.warning(f"Input file is missing optional columns: {missing_optional}")
            # Add missing optional columns with empty values
            for col in missing_optional:
                df[col] = '-'
        
        # Check content of required columns
        empty_columns = []
        for col in required_columns:
            # Check for empty values (NaN, None, empty string, or whitespace)
            empty_mask = (
                df[col].isna() | 
                (df[col].astype(str).str.strip() == '') |
                (df[col].astype(str).str.strip() == '-')
            )
            empty_count = empty_mask.sum()
            
            if empty_count > 0:
                empty_columns.append((col, empty_count))
                logging.warning(f"Column '{col}' has {empty_count} empty values")
        
        if empty_columns:
            logging.error("Some required columns have empty values:")
            for col, count in empty_columns:
                logging.error(f"- {col}: {count} empty values")
            return False
        
        # Check data types and validate
        logging.info("Validating data types...")
        
        # Check numeric columns
        try:
            numeric_columns = ['기본수량(1)', '판매단가(V포함)']
            for col in numeric_columns:
                if col in df.columns:
                    # Try to convert to numeric, allowing non-numeric values to become NaN
                    numeric_values = pd.to_numeric(df[col], errors='coerce')
                    non_numeric_count = numeric_values.isna().sum()
                    
                    if non_numeric_count > 0:
                        logging.error(f"Column '{col}' has {non_numeric_count} non-numeric values")
                        return False
                    
                    # Check for zero or negative values
                    invalid_count = (numeric_values <= 0).sum()
                    if invalid_count > 0:
                        logging.error(f"Column '{col}' has {invalid_count} zero or negative values")
                        return False
        except Exception as e:
            logging.error(f"Error validating numeric columns: {e}")
            return False
        
        # Print a sample of the data
        logging.info("Sample data from input file:")
        logging.info(df.head(3).to_string())
        
        return True
        
    except Exception as e:
        logging.error(f"Error checking input file: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    if check_input_file():
        logging.info("Input file validation completed successfully.")
    else:
        logging.error("Input file validation failed.")
        sys.exit(1) 