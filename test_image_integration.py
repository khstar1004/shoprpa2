import logging
import sys
import os
import configparser
import pandas as pd
from pathlib import Path

# Configure logging for both this script and the image_integration module
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Ensure the PythonScript logger is set to DEBUG level
logging.getLogger('PythonScript').setLevel(logging.DEBUG)

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

def test_image_integration():
    """Test the image integration functionality"""
    logging.info("Starting image integration test...")
    
    # Create a test DataFrame with product names that should match the image filenames
    test_df = pd.DataFrame({
        '상품명': [
            '고급 3단 자동 양우산 10k', 
            '목쿠션 메모리폼 목베개 여행용목베개',
            '손톱깎이 세트 선물세트 네일세트 12p',
            '양면 수면안대 눈안대 인쇄주문안대',
            '플라워 양우산 UV자외선 차단 파우치'
        ]
    })
    
    # Check if image main directory is properly configured
    main_img_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'))
    logging.info(f"Using image main directory: {main_img_dir}")
    
    if not main_img_dir.exists():
        logging.error(f"Image main directory does not exist: {main_img_dir}")
        return
    
    # Check image subdirectories
    haereum_dir = main_img_dir / 'Haereum'
    kogift_dir = main_img_dir / 'Kogift'  
    naver_dir = main_img_dir / 'Naver'
    
    logging.info(f"Haereum dir exists: {haereum_dir.exists()}")
    if haereum_dir.exists():
        haereum_images = list(haereum_dir.glob("*.jpg")) + list(haereum_dir.glob("*.png"))
        haereum_images = [img for img in haereum_images if "_nobg" not in img.name]
        logging.info(f"Found {len(haereum_images)} Haereum images")
        for img in haereum_images[:3]:  # Show first 3
            logging.info(f"Sample Haereum image: {img.name}")
    
    logging.info(f"Kogift dir exists: {kogift_dir.exists()}")
    if kogift_dir.exists():
        kogift_images = list(kogift_dir.glob("*.jpg")) + list(kogift_dir.glob("*.png"))
        kogift_images = [img for img in kogift_images if "_nobg" not in img.name]
        logging.info(f"Found {len(kogift_images)} Kogift images")
        for img in kogift_images[:3]:  # Show first 3
            logging.info(f"Sample Kogift image: {img.name}")
    
    logging.info(f"Naver dir exists: {naver_dir.exists()}")
    if naver_dir.exists():
        naver_images = list(naver_dir.glob("*.jpg")) + list(naver_dir.glob("*.png"))
        naver_images = [img for img in naver_images if "_nobg" not in img.name]
        logging.info(f"Found {len(naver_images)} Naver images")
        for img in naver_images[:3]:  # Show first 3
            logging.info(f"Sample Naver image: {img.name}")
    
    # Process the test data
    try:
        logging.info("Calling integrate_and_filter_images function...")
        result_df = integrate_and_filter_images(test_df, config, save_excel_output=False)
        
        # Display results
        logging.info(f"Result DataFrame columns: {result_df.columns.tolist()}")
        
        if '본사 이미지' in result_df.columns:
            logging.info(f"Haereum images found: {result_df['본사 이미지'].notna().sum()}")
        else:
            logging.warning("'본사 이미지' column not found in result DataFrame")
            
        if '고려기프트 이미지' in result_df.columns:
            logging.info(f"Kogift images found: {result_df['고려기프트 이미지'].notna().sum()}")
        else:
            logging.warning("'고려기프트 이미지' column not found in result DataFrame")
            
        if '네이버 이미지' in result_df.columns:
            logging.info(f"Naver images found: {result_df['네이버 이미지'].notna().sum()}")
        else:
            logging.warning("'네이버 이미지' column not found in result DataFrame")
        
        # Show what products had images matched
        for idx, row in result_df.iterrows():
            product_name = row['상품명']
            has_haereum = "✓" if '본사 이미지' in result_df.columns and pd.notna(row['본사 이미지']) else "✗"
            has_kogift = "✓" if '고려기프트 이미지' in result_df.columns and pd.notna(row['고려기프트 이미지']) else "✗"
            has_naver = "✓" if '네이버 이미지' in result_df.columns and pd.notna(row['네이버 이미지']) else "✗"
            logging.info(f"Product: '{product_name}' - Haereum: {has_haereum}, Kogift: {has_kogift}, Naver: {has_naver}")
            
    except Exception as e:
        logging.error(f"Error during image integration: {e}", exc_info=True)

if __name__ == "__main__":
    test_image_integration() 