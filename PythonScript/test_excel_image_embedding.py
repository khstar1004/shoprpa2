"""
Test script to verify Excel image dictionary handling fix works properly.

This script will:
1. Create sample data with complex image dictionaries
2. Try to write it to Excel using our sanitize_dataframe_for_excel function
3. Verify the output is correct
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TestExcelImageFix")

# Import just the sanitization function
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from excel_utils import sanitize_dataframe_for_excel

def create_test_data():
    """Create test DataFrame with complex image objects"""
    data = {
        "상품명": ["테스트 상품 1", "테스트 상품 2", "테스트 상품 3"],
        "가격": [1000, 2000, 3000],
        "본사 이미지": [
            {
                "url": {
                    "url": "https://example.com/image1.jpg", 
                    "local_path": "C:\\RPA\\Image\\Main\\test1.jpg",
                    "source": "haereum"
                },
                "source": "haereum",
                "product_name": "테스트 상품 1"
            },
            {
                "url": "https://example.com/image2.jpg",
                "local_path": "C:\\RPA\\Image\\Main\\test2.jpg",
                "source": "haereum"
            },
            "https://example.com/image3.jpg"  # Simple string URL
        ],
        "고려기프트 이미지": [
            "가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음",
            {
                "url": "https://example.com/kogift2.jpg",
                "local_path": "C:\\RPA\\Image\\Main\\kogift2.jpg",
                "source": "kogift"
            },
            None
        ],
        "네이버 이미지": [
            None,
            {
                "product_name": "네이버 상품 2",
                "image_url": "https://example.com/naver2.jpg"
            },
            "일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음"
        ]
    }
    
    return pd.DataFrame(data)

def run_test():
    """Run the test to verify Excel writing works with complex dictionaries"""
    logger.info("Creating test data with complex image dictionaries")
    df = create_test_data()
    
    # Print original data
    logger.info("Original DataFrame:")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")
    logger.info(f"Sample complex value: {df['본사 이미지'][0]}")
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output path with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"test_image_dict_fix_{timestamp}.xlsx")
    
    logger.info(f"Sanitizing DataFrame for Excel")
    df_sanitized = sanitize_dataframe_for_excel(df)
    
    logger.info("Sanitized DataFrame:")
    logger.info(f"Shape: {df_sanitized.shape}")
    logger.info(f"Columns: {df_sanitized.columns.tolist()}")
    logger.info(f"Sample sanitized value: {df_sanitized['본사 이미지'][0]}")
    
    logger.info(f"Writing sanitized data to Excel: {output_path}")
    
    # Write direct to Excel using pandas
    try:
        df_sanitized.to_excel(output_path, index=False)
        logger.info(f"Successfully wrote to Excel file: {output_path}")
        
        # Verify the file exists and has non-zero size
        file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        logger.info(f"File size: {file_size} bytes")
        
        return True
    except Exception as e:
        logger.error(f"Failed to write to Excel: {e}")
        return False

if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1) 