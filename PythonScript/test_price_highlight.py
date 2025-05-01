import os
import pandas as pd
from datetime import datetime
from excel_utils import create_final_output_excel, create_split_excel_outputs, finalize_dataframe_for_excel

def test_price_highlighting():
    """Test to ensure rows are highlighted when price difference is < -1"""
    print("Testing price difference highlighting...")
    
    # Create test data that includes negative price differences
    data = {
        '구분': ['A', 'A', 'A', 'A', 'A'],
        '담당자': ['김균아', '김균아', '김균아', '김균아', '김균아'],
        '업체명': ['테스트1', '테스트2', '테스트3', '테스트4', '테스트5'],
        '업체코드': ['001', '002', '003', '004', '005'],
        'Code': ['C001', 'C002', 'C003', 'C004', 'C005'],
        '중분류카테고리': ['가방', '가방', '가방', '가방', '가방'],
        '상품명': ['상품1', '상품2', '상품3', '상품4', '상품5'],
        '기본수량(1)': [100, 200, 300, 400, 500],
        '판매단가(V포함)': [1000, 2000, 3000, 4000, 5000],
        '본사상품링크': ['http://example.com/1', 'http://example.com/2', 'http://example.com/3', 'http://example.com/4', 'http://example.com/5'],
        '기본수량(2)': [100, 200, 300, 400, 500],
        '판매가(V포함)(2)': [1100, 1900, 2700, 4200, 5100],
        '가격차이(2)': [100, -100, -300, 200, 100],  # Row 3 should be highlighted (-300 < -1)
        '가격차이(2)(%)': [10, -5, -10, 5, 2],  # Rows 2 and 3 should be highlighted (-5, -10 < -1)
        '고려기프트 상품링크': ['http://kogift.com/1', 'http://kogift.com/2', 'http://kogift.com/3', 'http://kogift.com/4', 'http://kogift.com/5'],
        '기본수량(3)': [100, 200, 300, 400, 500],
        '판매단가(V포함)(3)': [950, 2100, 3100, 3900, 4800],
        '가격차이(3)': [-50, 100, 100, -100, -200],  # Rows 1, 4, and 5 should be highlighted (-50, -100, -200 < -1)
        '가격차이(3)(%)': [-5, 5, 3.3, -2.5, -4],  # Rows 1, 4, and 5 should be highlighted (-5, -2.5, -4 < -1)
        '공급사명': ['공급사1', '공급사2', '공급사3', '공급사4', '공급사5'],
        '네이버 쇼핑 링크': ['http://shopping.naver.com/1', 'http://shopping.naver.com/2', 'http://shopping.naver.com/3', 'http://shopping.naver.com/4', 'http://shopping.naver.com/5'],
        # Add the missing columns
        '공급사 상품링크': ['http://supplier.com/1', 'http://supplier.com/2', 'http://supplier.com/3', 'http://supplier.com/4', 'http://supplier.com/5'],
        '본사 이미지': ['-', '-', '-', '-', '-'],
        '고려기프트 이미지': ['-', '-', '-', '-', '-'],
        '네이버 이미지': ['-', '-', '-', '-', '-'],
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Create output directories if they don't exist
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_output')
    os.makedirs(output_dir, exist_ok=True)
    
    # Format current timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output paths
    result_path = os.path.join(output_dir, f'test_price_highlight_result_{timestamp}.xlsx')
    split_base_path = os.path.join(output_dir, f'test_price_highlight_split_{timestamp}')
    
    print(f"Creating test Excel files with price highlighting...")
    
    # Test create_final_output_excel
    print(f"Creating single output file: {result_path}")
    success = create_final_output_excel(df, result_path)
    if success:
        print(f"✓ Successfully created single output file.")
    else:
        print(f"✗ Failed to create single output file.")
    
    # Test create_split_excel_outputs
    print(f"Creating split output files: {split_base_path}")
    df_final = finalize_dataframe_for_excel(df)
    result_success, upload_success, result_file_path, upload_file_path = create_split_excel_outputs(df_final, split_base_path)
    
    if result_success:
        print(f"✓ Successfully created result file: {result_file_path}")
    else:
        print(f"✗ Failed to create result file.")
        
    if upload_success:
        print(f"✓ Successfully created upload file: {upload_file_path}")
    else:
        print(f"✗ Failed to create upload file.")
    
    print("\nTest completed. Please check the output files to verify highlighting of rows with negative price differences (< -1).")
    print("Expected highlights:")
    print("- Row 1: 가격차이(3) = -50, 가격차이(3)(%) = -5")
    print("- Row 2: 가격차이(2) = -100, 가격차이(2)(%) = -5")
    print("- Row 3: 가격차이(2) = -300, 가격차이(2)(%) = -10")
    print("- Row 4: 가격차이(3) = -100, 가격차이(3)(%) = -2.5")
    print("- Row 5: 가격차이(3) = -200, 가격차이(3)(%) = -4")
    
    return result_path, result_file_path, upload_file_path

if __name__ == "__main__":
    test_price_highlighting() 