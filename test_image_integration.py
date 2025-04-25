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
        logging.FileHandler('image_integration_test.log', encoding='utf-8')
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

def test_image_integration():
    """Test the image integration functionality with enhanced matching"""
    logging.info("Starting image integration test...")
    
    # Check if enhanced image matcher is available
    try:
        from PythonScript.enhanced_image_matcher import EnhancedImageMatcher
        logging.info("Enhanced image matcher module is available")
        enhanced_available = True
    except ImportError:
        logging.warning("Enhanced image matcher module is not available, will use text-based matching")
        enhanced_available = False
    
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
    
    # Check if directories exist
    haereum_dir = main_img_dir / 'Haereum'
    kogift_dir = main_img_dir / 'Kogift'
    naver_dir = main_img_dir / 'Naver'
    
    logging.info(f"Haereum dir exists: {haereum_dir.exists()}")
    if haereum_dir.exists():
        haereum_images = list(haereum_dir.glob("*.jpg"))
        logging.info(f"Found {len(haereum_images)} Haereum images")
        if haereum_images:
            logging.info(f"Sample Haereum image: {haereum_images[0].name}")
            
    logging.info(f"Kogift dir exists: {kogift_dir.exists()}")
    if kogift_dir.exists():
        kogift_images = list(kogift_dir.glob("*.jpg"))
        logging.info(f"Found {len(kogift_images)} Kogift images")
        if kogift_images:
            logging.info(f"Sample Kogift image: {kogift_images[0].name}")
            
    logging.info(f"Naver dir exists: {naver_dir.exists()}")
    if naver_dir.exists():
        naver_images = list(naver_dir.glob("*.jpg"))
        logging.info(f"Found {len(naver_images)} Naver images")
        if naver_images:
            logging.info(f"Sample Naver image: {naver_images[0].name}")
    
    # Process the test data
    try:
        logging.info("Calling integrate_and_filter_images function...")
        result_df = integrate_and_filter_images(test_df, config, save_excel_output=True)
        
        # Display results
        logging.info(f"Result DataFrame columns: {result_df.columns.tolist()}")
        
        # Calculate match statistics
        haereum_found = 0
        kogift_found = 0
        naver_found = 0
        
        for idx, row in result_df.iterrows():
            product_name = row['상품명']
            
            # Check if images are found
            has_haereum = '본사 이미지' in result_df.columns and pd.notna(row['본사 이미지'])
            has_kogift = '고려기프트 이미지' in result_df.columns and pd.notna(row['고려기프트 이미지'])
            has_naver = '네이버 이미지' in result_df.columns and pd.notna(row['네이버 이미지'])
            
            if has_haereum:
                haereum_found += 1
            if has_kogift:
                kogift_found += 1
            if has_naver:
                naver_found += 1
                
            logging.info(f"Product: '{product_name}' - Haereum: {'O' if has_haereum else 'X'}, Kogift: {'O' if has_kogift else 'X'}, Naver: {'O' if has_naver else 'X'}")
            
            # Log image paths for verification
            if has_haereum:
                haereum_path = row['본사 이미지']['local_path']
                logging.info(f"  Haereum image: {os.path.basename(haereum_path)}")
            
            if has_kogift:
                kogift_path = row['고려기프트 이미지']['local_path']
                logging.info(f"  Kogift image: {os.path.basename(kogift_path)}")
            
            if has_naver:
                naver_path = row['네이버 이미지']['local_path']
                logging.info(f"  Naver image: {os.path.basename(naver_path)}")
                
            logging.info("-" * 50)
        
        # Log match statistics
        logging.info(f"Haereum images found: {haereum_found}")
        logging.info(f"Kogift images found: {kogift_found}")
        logging.info(f"Naver images found: {naver_found}")
        
    except Exception as e:
        logging.error(f"Error during image integration test: {e}", exc_info=True)

if __name__ == "__main__":
    test_image_integration() 