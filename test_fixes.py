import pandas as pd
import os
import logging
from PythonScript.excel_utils import create_final_output_excel

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to see detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def test_fixes():
    """
    Test the fixes for:
    1. Kogift images not being embedded
    2. Negative price differences (<-1) row highlighting
    """
    print("\n=== Testing Fixes for Kogift Images and Price Difference Highlighting ===")
    
    # Create test data with Kogift images and negative price differences
    data = {
        "구분": ["A", "P", "A"],
        "담당자": ["", "", ""],
        "업체명": ["테스트업체", "테스트업체", "테스트업체"],
        "업체코드": ["001", "002", "003"],
        "Code": ["ABC123", "DEF456", "GHI789"],
        "중분류카테고리": ["가방", "우산", "문구"],
        "상품명": ["에코백 샘플", "우산 샘플", "볼펜 샘플"],
        "기본수량(1)": [100, 50, 200],
        "판매단가(V포함)": [2000, 15000, 500],
        "본사상품링크": ["http://example.com/1", "http://example.com/2", "http://example.com/3"],
        "기본수량(2)": [100, 50, 200],
        "판매가(V포함)(2)": [1700, 14800, 450],
        "가격차이(2)": [-300, -200, -50],
        "가격차이(2)(%)": [-15, -1.3, -10],
        "고려기프트 상품링크": [
            "http://koreagift.com/example/1", 
            "http://koreagift.com/example/2", 
            "http://koreagift.com/example/3"
        ],
        "기본수량(3)": [100, 50, 200],
        "판매단가(V포함)(3)": [1800, 16000, 480],
        "가격차이(3)": [-200, 1000, -20],
        "가격차이(3)(%)": [-10, 6.7, -4],
        "공급사명": ["네이버", "네이버", "네이버"],
        "네이버 쇼핑 링크": [
            "https://search.shopping.naver.com/example/1",
            "https://search.shopping.naver.com/example/2",
            "https://search.shopping.naver.com/example/3"
        ],
        "공급사 상품링크": ["", "", ""],
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add image data in dictionary format (as it would appear in the real system)
    df["본사 이미지"] = [
        {"url": "http://example.com/img1.jpg", "source": "haoreum"},
        {"url": "http://example.com/img2.jpg", "source": "haoreum"},
        {"url": "http://example.com/img3.jpg", "source": "haoreum"}
    ]
    
    # Add Kogift image data in the format that was causing issues
    df["고려기프트 이미지"] = [
        {"url": "http://koreagift.com/img1.jpg", "source": "kogift"},
        {"url": "http://koreagift.com/img2.jpg", "source": "kogift"},
        {"url": "http://koreagift.com/img3.jpg", "source": "kogift"}
    ]
    
    # Add Naver image data
    df["네이버 이미지"] = [
        {"url": "https://shopping-phinf.pstatic.net/img1.jpg", "source": "naver"},
        {"url": "https://shopping-phinf.pstatic.net/img2.jpg", "source": "naver"},
        {"url": "https://shopping-phinf.pstatic.net/img3.jpg", "source": "naver"}
    ]
    
    # Create output directory if it doesn't exist
    output_dir = "test_fixes_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create output file path
    output_file = os.path.join(output_dir, "fix_test.xlsx")
    
    # Create Excel file
    print(f"Creating test Excel file: {output_file}")
    success = create_final_output_excel(df, output_file)
    
    if success:
        print(f"Test Excel file successfully created at: {output_file}")
        print(f"Check the file for:")
        print("1. Kogift images (they should be embedded)")
        print("2. Yellow highlighting on rows where price difference < -1")
    else:
        print("Failed to create test Excel file")

if __name__ == "__main__":
    test_fixes() 