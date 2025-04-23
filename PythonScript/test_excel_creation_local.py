import os
import pandas as pd
import logging
# from PIL import Image, ImageDraw, ImageFont # No longer needed for dummy images
import sys
from datetime import datetime
import random # Needed for random image selection
import configparser # Needed to read config for image paths
from typing import List  # Add List import for type hints

# --- Add PythonScript directory to path ---
# This allows importing modules from PythonScript when running this test directly
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir) # Assuming project root is one level up
if script_dir not in sys.path:
    sys.path.append(script_dir)
if project_root not in sys.path:
     sys.path.append(project_root)

# --- Imports from project modules ---
# Make sure excel_utils.py and utils.py are importable
try:
    from excel_utils import create_final_output_excel, FINAL_COLUMN_ORDER
    from utils import load_config # Need load_config to get image paths
except ImportError as e:
    print(f"Error importing project modules: {e}")
    print("Ensure the script is run from the project root or PythonScript is in PYTHONPATH.")
    sys.exit(1)

# --- Test Setup ---
CONFIG_PATH = os.path.join(project_root, 'config.ini')
OUTPUT_DIR = os.path.join(project_root, "OUTPUT") # Save output in main OUTPUT folder
TEST_OUTPUT_FILENAME = f"test_real_local_image_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
TEST_OUTPUT_PATH = os.path.join(OUTPUT_DIR, TEST_OUTPUT_FILENAME)

# Configure basic logging for the test
log_file_path = os.path.join(project_root, 'logs', 'test_excel_real_local.log')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG, # Use DEBUG to see detailed logs from excel_utils
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # Log to console as well
    ]
)
logger = logging.getLogger(__name__)

# Removed create_dummy_image function

def get_local_images(config: configparser.ConfigParser) -> List[str]:
    """Finds .jpg image files in Main and Target directories specified in config."""
    image_paths = []
    try:
        main_dir = config.get('Paths', 'image_main_dir', fallback=None)
        target_dir = config.get('Paths', 'image_target_dir', fallback=None)

        dirs_to_scan = [d for d in [main_dir, target_dir] if d and os.path.isdir(d)]
        if not dirs_to_scan:
             logger.warning(f"Image directories not found or not specified correctly in config. Searched: {main_dir}, {target_dir}")
             return []

        logger.info(f"Scanning for JPG images in: {dirs_to_scan}")
        for img_dir in dirs_to_scan:
            try:
                found_files = [os.path.join(img_dir, f) for f in os.listdir(img_dir)
                               if os.path.isfile(os.path.join(img_dir, f)) and f.lower().endswith('.jpg')]
                logger.info(f"Found {len(found_files)} JPG images in {img_dir}")
                image_paths.extend(found_files)
            except OSError as e:
                logger.error(f"Error scanning directory {img_dir}: {e}")

    except configparser.Error as e:
        logger.error(f"Error reading image paths from config: {e}")
    except Exception as e:
        logger.error(f"Unexpected error getting local images: {e}")

    if not image_paths:
         logger.warning("No local JPG images found in configured directories.")

    return image_paths

def run_excel_creation_test():
    """Runs the test for creating Excel using real local images based on '엑셀 골든' data."""
    logger.info("--- Starting Excel Creation Test with REAL Local Images ---")

    # 1. Load Config
    config = load_config(CONFIG_PATH)
    if not config.sections():
         logger.error(f"Failed to load or parse config file: {CONFIG_PATH}. Cannot proceed.")
         return

    # 2. Find Local Images
    local_image_files = get_local_images(config)
    logger.info(f"Total local JPG images found: {len(local_image_files)}")

    # 3. Create Directories
    # os.makedirs(TEST_IMAGE_DIR, exist_ok=True) # No longer needed
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"Ensured output directory exists: {OUTPUT_DIR}")

    # 4. Create Sample DataFrame based on "엑셀 골든" structure
    sample_data = []

    # --- Row 1 (Based on 엑셀 골든 Row 1) ---
    row1 = {col: '-' for col in FINAL_COLUMN_ORDER} # Initialize with '-'
    row1.update({
        '구분': 'A', '담당자': '김균아', '업체명': '엠제이(MJ)기획', '업체코드': '3941', 'Code': '437611',
        '중분류카테고리': '가방(에코백/면)', '상품명': '사랑이 엔젤하트 투포켓 에코백', '기본수량(1)': 200,
        '판매단가(V포함)': 2970, '본사상품링크': 'http://www.jclgift.com/product/product_view.asp?p_idx=437611',
        '고려기프트 상품링크': '가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음',
        '네이버 쇼핑 링크': '가격이 범위내에 없거나 검색된 상품이 없음',
        '본사 이미지': r"C:\RPA\Image\Main\hae_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '고려기프트 이미지': r"C:\RPA\Image\Main\kogift\kogift_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '네이버 이미지': r"C:\RPA\Image\Main\Naver\naver_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg"
    })
    sample_data.append(row1)

    # --- Row 2 (Based on 엑셀 골든 Row 2) ---
    row2 = {col: '-' for col in FINAL_COLUMN_ORDER}
    row2.update({
        '구분': 'A', '담당자': '김균아', '업체명': '엠제이(MJ)기획', '업체코드': '3941', 'Code': '437593',
        '중분류카테고리': '가방(에코백/면)', '상품명': '사랑이 큐피트화살 투포켓 에코백', '기본수량(1)': 200,
        '판매단가(V포함)': 2970, '본사상품링크': 'http://www.jclgift.com/product/product_view.asp?p_idx=437593',
        '기본수량(2)': 200, '판매가(V포함)(2)': '570,900', '판매단가(V포함)(2)': 2854.5, '가격차이(2)': -115.5, '가격차이(2)(%)': -3.9,
        '고려기프트 상품링크': 'http://koreagift.com/ez/mall.php?cat=003011001&query=view&no=170303',
        '네이버 쇼핑 링크': '일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음',
        '본사 이미지': r"C:\RPA\Image\Main\hae_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '고려기프트 이미지': r"C:\RPA\Image\Main\kogift\kogift_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '네이버 이미지': r"C:\RPA\Image\Main\Naver\naver_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg"
    })
    sample_data.append(row2)

    # --- Row 3 (Based on 엑셀 골든 Row 5 - 캐치티니핑) ---
    row3 = {col: '-' for col in FINAL_COLUMN_ORDER}
    row3.update({
        '구분': 'A', '담당자': '김균아', '업체명': '엠제이(MJ)기획', '업체코드': '3941', 'Code': '437551',
        '중분류카테고리': '어린이우산', '상품명': '캐치티니핑 53 스무디 입체리본 투명 아동우산', '기본수량(1)': 50,
        '판매단가(V포함)': 17820, '본사상품링크': 'http://www.jclgift.com/product/product_view.asp?p_idx=437551',
        '기본수량(2)': 50, '판매가(V포함)(2)': '842,600', '판매단가(V포함)(2)': 16852, '가격차이(2)': -968, '가격차이(2)(%)': -5.4,
        '고려기프트 상품링크': 'http://koreagift.com/ez/mall.php?cat=004002005&query=view&no=170277',
        '기본수량(3)': 1, '판매단가(V포함)(3)': 14490, '가격차이(3)': -3330, '가격차이(3)(%)': -18.6, '공급사명': '네이버',
        '네이버 쇼핑 링크': 'https://search.shopping.naver.com/catalog/53165134501', '공급사 상품링크': '-',
        '본사 이미지': r"C:\RPA\Image\Main\hae_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '고려기프트 이미지': r"C:\RPA\Image\Main\kogift\kogift_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '네이버 이미지': r"C:\RPA\Image\Main\Naver\naver_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg"
    })
    sample_data.append(row3)

    # --- Row 4 (Test invalid image path) ---
    row4 = {col: '-' for col in FINAL_COLUMN_ORDER}
    row4.update({
        '구분': 'D', '담당자': '테스터4', '업체명': '경로테스트', '업체코드': 'P001', 'Code': 'PathTest01',
        '상품명': '잘못된 경로 이미지 테스트', '기본수량(1)': 1, '판매단가(V포함)': 1000, '본사상품링크': '-',
        '본사 이미지': r"C:\RPA\Image\Main\hae_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '고려기프트 이미지': r"C:\RPA\Image\Main\kogift\kogift_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg",
        '네이버 이미지': r"C:\RPA\Image\Main\Naver\naver_마루는강쥐_클리어미니케이스_15ad2f8e91.jpg"
    })
    sample_data.append(row4)

    df_sample = pd.DataFrame(sample_data)
    # Ensure columns are in the exact final order before passing
    df_sample = df_sample[FINAL_COLUMN_ORDER]

    logger.info(f"Sample DataFrame created with {len(df_sample)} rows using real data structure.")
    logger.debug("Sample DataFrame Head:\n" + df_sample.head().to_string())

    # 5. Call the function to create the Excel file
    logger.info(f"Calling create_final_output_excel to generate: {TEST_OUTPUT_PATH}")
    success = False
    try:
        # Pass a copy to avoid modifying df_sample if create_final_output_excel modifies inplace
        success = create_final_output_excel(df_sample.copy(), TEST_OUTPUT_PATH)
    except Exception as e:
        logger.error(f"An error occurred during create_final_output_excel call: {e}", exc_info=True)
        success = False

    # 6. Print result
    if success:
        logger.info(f"Excel file creation test successful! Output saved to: {TEST_OUTPUT_PATH}")
        print(f"\n[SUCCESS] Excel file created successfully: {TEST_OUTPUT_PATH}")
        print("Please open the file and verify:")
        print("- Images from C:\RPA\Image\Main and C:\RPA\Image\Target should be embedded where specified.")
        print("- Rows with invalid paths or URLs in image columns should show error text.")
        print("- Rows with error messages in link/data columns should display the messages correctly.")
        print("- Formatting (numbers, alignment, borders, table style) should match '엑셀 골든' sample.")
    else:
        logger.error(f"Excel file creation test failed. Check logs at '{log_file_path}'.")
        print(f"\n[FAILURE] Excel file creation failed. Check logs at '{log_file_path}'.")

    logger.info("--- Finished Excel Creation Test with REAL Local Images ---")

if __name__ == "__main__":
    run_excel_creation_test() 