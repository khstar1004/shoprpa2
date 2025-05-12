import os
import logging
import pandas as pd
from excel_utils import (
    find_excel_file, validate_excel_file, create_final_output_excel,
    preprocess_product_name, excel_generator,
    FINAL_COLUMN_ORDER, REQUIRED_INPUT_COLUMNS
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_convert.log'),
        logging.StreamHandler()
    ]
)

# Constants
INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'INPUT')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'OUTPUT')

def convert_xlsx():
    """Convert .xls files to .xlsx format with preprocessing."""
    try:
        # 입력 디렉토리에서 Excel 파일 찾기
        excel_file = find_excel_file(INPUT_DIR, extension='.xls')
        
        if not excel_file:
            logging.error("No .xls file found in INPUT directory")
            return False
            
        file_path = os.path.join(INPUT_DIR, excel_file)
        logging.info(f"Found .xls file: {file_path}")
        
        # Excel 파일 유효성 검사
        if not validate_excel_file(file_path):
            return False
            
        # Excel 파일 읽기
        df = pd.read_excel(file_path)
        logging.info(f"Excel file read successfully. Shape: {df.shape}")
        
        # 네이버 관련 컬럼 확인
        naver_cols = ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', 
                     '공급사명', '네이버 쇼핑 링크', '공급사 상품링크', '네이버 이미지']
        for col in naver_cols:
            if col in df.columns:
                logging.info(f"Naver column {col} has {df[col].notna().sum()} non-empty values")
        
        # 상품명 전처리
        if '상품명' in df.columns:
            df['상품명'] = df['상품명'].apply(preprocess_product_name)
            logging.info("Product names preprocessed successfully")
        
        # 결과 파일 생성
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, f"{os.path.splitext(excel_file)[0]}.xlsx")
        
        if excel_generator.create_excel_output(df, output_path)[0]:
            logging.info(f"Successfully converted to .xlsx: {output_path}")
            return True
        else:
            logging.error("Failed to create output Excel file")
            return False
        
    except Exception as e:
        logging.error(f"Error in convert_xlsx: {str(e)}")
        return False

if __name__ == "__main__":
    result = convert_xlsx()
    if result:
        print("Excel conversion completed successfully.")
    else:
        print("Excel conversion failed.")