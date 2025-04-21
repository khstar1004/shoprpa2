import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("check_excel.log"),
        logging.StreamHandler()
    ]
)

def check_excel_file():
    """
    Check the input Excel file and add any missing columns required for processing.
    Returns the file path if successful.
    """
    input_directory = 'C:\\RPA\\Input'
    
    # Find Excel files in the input directory
    xlsx_files = [f for f in os.listdir(input_directory) if f.lower().endswith('.xlsx')]
    
    if not xlsx_files:
        logging.error("No Excel files found in the input directory.")
        return None
    
    file_path = os.path.join(input_directory, xlsx_files[0])
    logging.info(f"Processing input file: {file_path}")
    
    try:
        df = pd.read_excel(file_path)
        logging.info(f"Successfully read Excel file. Shape: {df.shape}")
        
        # Clean column names - remove whitespace
        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
        
        # All required columns based on the sample format
        required_columns = [
            '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명',
            '기본수량(1)', '판매단가(V포함)', '본사상품링크',
            '기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크', 
            '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크',
            '본사 이미지', '고려기프트 이미지', '네이버 이미지'
        ]
        
        # Image columns that need special handling
        image_columns = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
        
        # Check for column name variations
        column_aliases = {
            '담 당자': '담당자',
            '상품코드': 'Code',
            '카테고리(중분류)': '중분류카테고리',
            'name': '상품명',
            '본사 기본수량': '기본수량(1)',
            '판매단가1(VAT포함)': '판매단가(V포함)',
            '본사링크': '본사상품링크',
            '고려 기본수량': '기본수량(2)',
            '판매단가2(VAT포함)': '판매단가(V포함)(2)',
            '고려 링크': '고려기프트 상품링크',
            '네이버 기본수량': '기본수량(3)',
            '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
            '네이버 공급사명': '공급사명',
            '네이버 링크': '네이버 쇼핑 링크',
            '해오름이미지경로': '본사 이미지',
            '고려기프트(이미지링크)': '고려기프트 이미지',
            '네이버쇼핑(이미지링크)': '네이버 이미지'
        }
        
        # First, rename any aliased columns to their standard names
        rename_dict = {}
        for alias, standard in column_aliases.items():
            if alias in df.columns and standard not in df.columns:
                rename_dict[alias] = standard
        
        if rename_dict:
            df = df.rename(columns=rename_dict)
            logging.info(f"Renamed columns: {rename_dict}")
        
        # Now check for missing required columns and add them
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            for col in missing_columns:
                if col in image_columns:
                    # For image columns, initialize with empty string
                    df[col] = ''
                else:
                    # For regular columns, use dash as placeholder
                    df[col] = '-'
            logging.info(f"Added missing columns: {missing_columns}")
            
            # Save the updated file
            df.to_excel(file_path, index=False)
            logging.info(f"Updated Excel file saved with additional columns: {file_path}")
        else:
            logging.info("All required columns already exist in the file.")
            
        # Make sure image columns are properly formatted - convert existing image data to URL form
        modified = False
        for img_col in image_columns:
            if img_col in df.columns:
                # Check if any cell needs to be formatted as an image URL
                for i in range(len(df)):
                    cell_value = df.at[i, img_col]
                    if isinstance(cell_value, str) and cell_value and cell_value.strip() != '-' and not cell_value.startswith(('http://', 'https://', '=IMAGE(')):
                        # If it's a local path or non-URL format, ensure it's properly formatted
                        # For now, just ensure it's a string
                        df.at[i, img_col] = str(cell_value)
                        modified = True
        
        if modified:
            df.to_excel(file_path, index=False)
            logging.info(f"Updated image column formats in Excel file: {file_path}")
        
        return file_path
        
    except Exception as e:
        logging.error(f"Error processing Excel file: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    result = check_excel_file()
    if result:
        print(f"Excel file check completed successfully: {result}")
    else:
        print("Excel file check failed.")