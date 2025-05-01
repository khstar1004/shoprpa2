import os
import sys
import logging
import pandas as pd
import configparser
from pathlib import Path

# Add the project path to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from PythonScript.image_integration import integrate_and_filter_images
from PythonScript.data_processing import format_product_data_for_output
from PythonScript.excel_utils import _apply_conditional_formatting, create_final_output_excel
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_image_matching_fix.log', mode='w')
    ]
)

def test_image_matching_fix():
    """Test the fixed image matching functionality."""
    logging.info("Starting image matching fix test...")
    
    # Load configuration
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    if not config.has_section('Paths'):
        config.add_section('Paths')
    
    config.set('Paths', 'image_main_dir', 'C:\\RPA\\Image\\Main')
    config.set('Paths', 'output_dir', 'C:\\RPA\\Output')
    
    # Create test DataFrame with product names
    test_df = pd.DataFrame({
        '구분': ['A'] * 5,
        '담당자': ['테스트'] * 5,
        '업체명': ['해오름'] * 5,
        '업체코드': ['1234'] * 5,
        'Code': ['C1', 'C2', 'C3', 'C4', 'C5'],
        '중분류카테고리': ['가방', '우산', '네일', '안대', '우산'],
        '상품명': [
            '고급 3단 자동 양우산 10k',
            '목쿠션 메모리폼 목베개 여행용목베개',
            '손톱깎이 세트 선물세트 네일세트 12p',
            '양면 수면안대 눈안대 인쇄주문안대',
            '플라워 양우산 UV자외선 차단 파우치'
        ],
        '기본수량(1)': [100, 100, 100, 100, 100],
        '판매단가(V포함)': [5000, 4000, 3000, 2000, 5500],
        '본사상품링크': ['http://example.com'] * 5
    })
    
    # Test image integration
    logging.info("Testing image integration with fixed matching...")
    result_df = integrate_and_filter_images(test_df, config)
    
    # Verify image matching results
    logging.info("Verifying image matching results...")
    image_counts = {
        '본사 이미지': result_df['본사 이미지'].apply(lambda x: isinstance(x, dict)).sum(),
        '고려기프트 이미지': result_df['고려기프트 이미지'].apply(lambda x: isinstance(x, dict)).sum(),
        '네이버 이미지': result_df['네이버 이미지'].apply(lambda x: isinstance(x, dict)).sum()
    }
    
    logging.info(f"Image matching results: {image_counts}")
    
    # Check that images contain product names
    for idx, row in result_df.iterrows():
        product_name = row['상품명']
        
        for img_col in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
            if isinstance(row[img_col], dict) and 'product_name' in row[img_col]:
                if row[img_col]['product_name'] == product_name:
                    logging.info(f"Row {idx}: {img_col} correctly matches product name '{product_name}'")
                else:
                    logging.warning(f"Row {idx}: {img_col} has mismatched product name. Expected: '{product_name}', Got: '{row[img_col]['product_name']}'")
    
    # Test Kogift data formatting
    logging.info("Testing Kogift data formatting...")
    kogift_results = {
        '고급 3단 자동 양우산 10k': [{
            'price_with_vat': 4800,
            'quantity': 100,
            'link': 'http://kogift.example.com/1'
        }],
        '목쿠션 메모리폼 목베개 여행용목베개': [{
            'price_with_vat': 3800,
            'quantity': 100,
            'link': 'http://kogift.example.com/2'
        }]
    }
    
    # Add price differences for testing yellow highlighting
    for idx, row in result_df.iterrows():
        if idx == 0:
            result_df.at[idx, '판매단가(V포함)(2)'] = 4800
            result_df.at[idx, '가격차이(2)'] = -200  # Negative difference to trigger highlight
            result_df.at[idx, '가격차이(2)(%)'] = -4
        elif idx == 1:
            result_df.at[idx, '판매단가(V포함)(2)'] = 3800
            result_df.at[idx, '가격차이(2)'] = -200  # Negative difference to trigger highlight
            result_df.at[idx, '가격차이(2)(%)'] = -5
        else:
            result_df.at[idx, '판매단가(V포함)(2)'] = row['판매단가(V포함)']
            result_df.at[idx, '가격차이(2)'] = 0
            result_df.at[idx, '가격차이(2)(%)'] = 0
    
    # Test adding kogift data (should update 기본수량 and 판매가)
    result_df = format_product_data_for_output(result_df, kogift_results=kogift_results)
    
    # Verify that 기본수량 and 판매가 are filled for Kogift
    kogift_columns_check = ['기본수량', '판매가', '기본수량(2)', '판매단가(V포함)(2)']
    for col in kogift_columns_check:
        if col in result_df.columns:
            null_count = result_df[col].isnull().sum()
            logging.info(f"Column '{col}' has {null_count} null values out of {len(result_df)} rows")
    
    # Create a temporary Excel file to test conditional formatting
    test_excel_path = 'C:\\RPA\\Output\\test_image_matching_fix.xlsx'
    logging.info(f"Creating test Excel file: {test_excel_path}")
    
    # Create Excel file
    create_final_output_excel(result_df, test_excel_path)
    
    # Check if Excel file exists
    if os.path.exists(test_excel_path):
        logging.info(f"Excel file created successfully: {test_excel_path}")
        
        # Load the Excel file to verify conditional formatting
        try:
            wb = load_workbook(test_excel_path)
            ws = wb.active
            
            # Check for yellow highlighting
            yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
            highlighted_rows = []
            
            for row_idx in range(2, ws.max_row + 1):  # Skip header row
                if ws.cell(row=row_idx, column=1).fill.start_color.rgb == yellow_fill.start_color.rgb:
                    highlighted_rows.append(row_idx)
            
            logging.info(f"Found {len(highlighted_rows)} rows with yellow highlighting: {highlighted_rows}")
            
            # Rows 2 and 3 (index 0 and 1) should be highlighted as they have negative price differences
            expected_highlighted_rows = [2, 3]  # Excel row numbers (1-based)
            
            if set(highlighted_rows) == set(expected_highlighted_rows):
                logging.info("Yellow highlighting for price differences works correctly!")
            else:
                logging.warning(f"Yellow highlighting for price differences failed. Expected rows {expected_highlighted_rows}, got {highlighted_rows}")
                
        except Exception as e:
            logging.error(f"Error checking Excel formatting: {e}")
    else:
        logging.error(f"Failed to create Excel file: {test_excel_path}")
    
    logging.info("Test completed!")
    return result_df

if __name__ == "__main__":
    test_result = test_image_matching_fix()
    print("Test completed, check the output Excel file and log for results.") 