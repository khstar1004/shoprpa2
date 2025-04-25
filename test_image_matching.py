import logging
import sys
import os
import configparser
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('image_matching_test.log', encoding='utf-8')
    ]
)

# Add the project directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

# Import the image integration function
from PythonScript.image_integration import integrate_and_filter_images

# Load configuration
config = configparser.ConfigParser()
config_path = os.path.join(script_dir, 'config.ini')
if not os.path.exists(config_path):
    raise FileNotFoundError(f"Config file not found at {config_path}")

config.read(config_path, encoding='utf-8')

def test_image_matching():
    """Test the image matching functionality with different test cases"""
    logging.info("Starting image matching test...")
    
    # Test with products that should match images in all three directories
    test_df = pd.DataFrame({
        '상품명': [
            '고급 3단 자동 양우산 10k',
            '목쿠션 메모리폼 목베개 여행용목베개',
            '손톱깎이 세트 선물세트 네일세트 12p',
            '양면 수면안대 눈안대 인쇄주문안대',
            '플라워 양우산 UV자외선 차단 파우치'
        ]
    })
    
    # Check image directories
    main_img_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'))
    logging.info(f"Using image main directory: {main_img_dir}")
    
    if not main_img_dir.exists():
        logging.error(f"Image main directory does not exist: {main_img_dir}")
        return
    
    # Process the test data
    try:
        logging.info("Calling integrate_and_filter_images function with improved matching logic...")
        result_df = integrate_and_filter_images(test_df, config, save_excel_output=True)
        
        # Display results
        logging.info(f"Result DataFrame columns: {result_df.columns.tolist()}")
        
        # Check image matching results
        for idx, row in result_df.iterrows():
            product_name = row['상품명']
            
            # Check if images are found - cp949 인코딩 문제를 피하기 위해 체크 표시 대신 'O'와 'X' 사용
            has_haereum = "O" if '본사 이미지' in result_df.columns and pd.notna(row['본사 이미지']) else "X"
            has_kogift = "O" if '고려기프트 이미지' in result_df.columns and pd.notna(row['고려기프트 이미지']) else "X"
            has_naver = "O" if '네이버 이미지' in result_df.columns and pd.notna(row['네이버 이미지']) else "X"
            
            logging.info(f"Product: '{product_name}' - Haereum: {has_haereum}, Kogift: {has_kogift}, Naver: {has_naver}")
            
            # Log image paths for verification
            if has_haereum == "O":
                haereum_path = row['본사 이미지']['local_path']
                logging.info(f"  Haereum image: {os.path.basename(haereum_path)}")
            
            if has_kogift == "O":
                kogift_path = row['고려기프트 이미지']['local_path']
                logging.info(f"  Kogift image: {os.path.basename(kogift_path)}")
            
            if has_naver == "O":
                naver_path = row['네이버 이미지']['local_path']
                logging.info(f"  Naver image: {os.path.basename(naver_path)}")
            
            logging.info("-" * 50)
    
    except Exception as e:
        logging.error(f"Error during image matching test: {e}", exc_info=True)

if __name__ == "__main__":
    test_image_matching() 