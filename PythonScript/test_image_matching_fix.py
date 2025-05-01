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
    
    # Create manual configuration instead of loading from file
    config = configparser.ConfigParser()
    
    # Set up required sections and values
    config.add_section('Paths')
    
    # Use a local test directory instead of C:\RPA path to avoid permission issues
    test_dir = Path("test_images")
    test_dir.mkdir(exist_ok=True)
    
    # Create subdirectories for images
    image_main_dir = test_dir / "Main"
    image_main_dir.mkdir(exist_ok=True)
    
    haereum_dir = image_main_dir / "Haereum"
    kogift_dir = image_main_dir / "Kogift"
    naver_dir = image_main_dir / "Naver"
    
    haereum_dir.mkdir(exist_ok=True)
    kogift_dir.mkdir(exist_ok=True)
    naver_dir.mkdir(exist_ok=True)
    
    # Create test output directory
    output_dir = test_dir / "Output"
    output_dir.mkdir(exist_ok=True)
    
    # Create dummy test images if they don't exist
    test_images = [
        (haereum_dir / "haereum_고급_3단_자동_양우산_10k_d4caa6a694.jpg"),
        (haereum_dir / "haereum_목쿠션_메모리폼_목베개_여행용목베개_bda60bd016.jpg"),
        (haereum_dir / "haereum_손톱깎이_세트_선물세트_네일세트_12p_06f5435e4e.jpg"),
        (haereum_dir / "haereum_양면_수면안대_눈안대_인쇄주문안대_e86c7c53ae.jpg"),
        (haereum_dir / "haereum_플라워_양우산_UV자외선_차단_파우치_541d22ca20.jpg"),
        
        (kogift_dir / "kogift_1912824fba_2061e0f04f.jpg"),
        (kogift_dir / "kogift_c85244abdf_e4f4b98d58.jpg"),
        (kogift_dir / "kogift_d05fe70853_db8e46e9e4.jpg"),
        
        (naver_dir / "naver_0d3dca10db841346_e17141bd.jpg"),
        (naver_dir / "naver_16f0dac0124fe5f4_4bb177a0.jpg")
    ]
    
    # Create empty image files for testing
    for img_path in test_images:
        if not img_path.exists():
            try:
                with open(img_path, 'wb') as f:
                    # Write minimal JPEG header
                    f.write(b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00\xff\xdb\x00C\x00\xff\xc0\x00\x11\x08\x00\x01\x00\x01\x03\x01\x11\x00\x02\x11\x01\x03\x11\x01\xff\xc4\x00\x14\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\n\xff\xc4\x00\x14\x10\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\n\xff\xda\x00\x08\x01\x01\x00\x00?\x00\x92\x00\xff\xd9')
                logging.info(f"Created test image: {img_path}")
            except Exception as e:
                logging.error(f"Failed to create test image {img_path}: {e}")
    
    # Update paths in config to use test directory
    config.set('Paths', 'image_main_dir', str(image_main_dir))
    config.set('Paths', 'output_dir', str(output_dir))
    
    # ImageMatching section
    config.add_section('ImageMatching')
    config.set('ImageMatching', 'use_enhanced_matcher', 'true')
    config.set('ImageMatching', 'minimum_match_confidence', '0.1')
    
    # Matching section
    config.add_section('Matching')
    config.set('Matching', 'image_threshold', '0.1')
    config.set('Matching', 'image_display_threshold', '0.05')
    
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
    test_excel_path = output_dir / "test_image_matching_fix.xlsx"
    logging.info(f"Creating test Excel file: {test_excel_path}")
    
    # Create Excel file
    create_final_output_excel(result_df, str(test_excel_path))
    
    # Check if Excel file exists
    if test_excel_path.exists():
        logging.info(f"Excel file created successfully: {test_excel_path}")
        
        # Load the Excel file to verify conditional formatting
        try:
            wb = load_workbook(str(test_excel_path))
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
    
    # Print a summary of test results
    print("\n============ 테스트 결과 요약 ============")
    print(f"테스트 이미지 디렉토리: {image_main_dir}")
    print(f"테스트 결과 파일: {test_excel_path}")
    print(f"검출된 이미지 수: 해오름({image_counts['본사 이미지']}), 고려기프트({image_counts['고려기프트 이미지']}), 네이버({image_counts['네이버 이미지']})")
    print(f"가격차이 강조 테스트: {'성공' if set(highlighted_rows) == set(expected_highlighted_rows) else '실패'}")
    if '기본수량' in result_df.columns:
        print(f"고려기프트 기본수량 및 판매가 컬럼 추가: 성공")
    else:
        print(f"고려기프트 기본수량 및 판매가 컬럼 추가: 실패")
    print("=========================================\n")
    
    return result_df

if __name__ == "__main__":
    test_result = test_image_matching_fix()
    print("Test completed, check the output Excel file and log for results.") 