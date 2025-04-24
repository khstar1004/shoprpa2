import os
import pandas as pd
import logging
from PIL import Image, ImageDraw, ImageFont # No longer needed for dummy images
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

def verify_image_paths(image_paths):
    """Verify that image paths exist and are accessible."""
    for path in image_paths:
        if not os.path.isfile(path):
            logger.warning(f"Image file not found: {path}")
        else:
            try:
                with Image.open(path) as img:
                    logger.info(f"Successfully verified image: {path}")
            except Exception as e:
                logger.error(f"Failed to open image {path}: {e}")

def run_excel_creation_test():
    """Runs the test for creating Excel using real local images."""
    logger.info("--- Starting Excel Creation Test with REAL Local Images ---")

    # Define test image paths
    test_images = {
        'haereum': r"C:\RPA\Image\Main\Haereum\haereum__AGK_NORDIC__블랙_프리미엄_28_프라이팬_콤비_111b244db2.jpg",
        'kogift': r"C:\RPA\Image\Main\kogift_pre\kogift_pre_12c8c525c8_1c8b17f355.jpg",
        'naver': r"C:\RPA\Image\Main\Naver\naver_웨일리케어_올인원_샴푸바디워시_19cdadc75a.jpg"
    }

    # Verify test images exist
    logger.info("Verifying test image paths...")
    verify_image_paths(test_images.values())

    # Create sample DataFrame
    sample_data = []

    # --- Row 1 ---
    row1 = {col: '-' for col in FINAL_COLUMN_ORDER}
    row1.update({
        '구분': 'A', '담당자': '김균아', '업체명': '엠제이(MJ)기획', '업체코드': '3941', 'Code': '437611',
        '중분류카테고리': '가방(에코백/면)', '상품명': '사랑이 엔젤하트 투포켓 에코백', '기본수량(1)': 200,
        '판매단가(V포함)': 2970, '본사상품링크': 'http://www.jclgift.com/product/product_view.asp?p_idx=437611',
        '고려기프트 상품링크': '가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음',
        '네이버 쇼핑 링크': '가격이 범위내에 없거나 검색된 상품이 없음',
        '본사 이미지': test_images['haereum'],
        '고려기프트 이미지': test_images['kogift'],
        '네이버 이미지': test_images['naver']
    })
    sample_data.append(row1)

    # --- Row 2 ---
    row2 = {col: '-' for col in FINAL_COLUMN_ORDER}
    row2.update({
        '구분': 'A', '담당자': '김균아', '업체명': '엠제이(MJ)기획', '업체코드': '3941', 'Code': '437593',
        '중분류카테고리': '가방(에코백/면)', '상품명': '사랑이 큐피트화살 투포켓 에코백', '기본수량(1)': 200,
        '판매단가(V포함)': 2970, '본사상품링크': 'http://www.jclgift.com/product/product_view.asp?p_idx=437593',
        '기본수량(2)': 200, '판매가(V포함)(2)': '570,900', '판매단가(V포함)(2)': 2854.5,
        '가격차이(2)': -115.5, '가격차이(2)(%)': -3.9,
        '고려기프트 상품링크': 'http://koreagift.com/ez/mall.php?cat=003011001&query=view&no=170303',
        '네이버 쇼핑 링크': '일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음',
        '본사 이미지': test_images['haereum'],
        '고려기프트 이미지': test_images['kogift'],
        '네이버 이미지': test_images['naver']
    })
    sample_data.append(row2)

    # --- Row 3 ---
    row3 = {col: '-' for col in FINAL_COLUMN_ORDER}
    row3.update({
        '구분': 'A', '담당자': '김균아', '업체명': '엠제이(MJ)기획', '업체코드': '3941', 'Code': '437551',
        '중분류카테고리': '어린이우산', '상품명': '캐치티니핑 53 스무디 입체리본 투명 아동우산', '기본수량(1)': 50,
        '판매단가(V포함)': 17820, '본사상품링크': 'http://www.jclgift.com/product/product_view.asp?p_idx=437551',
        '기본수량(2)': 50, '판매가(V포함)(2)': '842,600', '판매단가(V포함)(2)': 16852,
        '가격차이(2)': -968, '가격차이(2)(%)': -5.4,
        '고려기프트 상품링크': 'http://koreagift.com/ez/mall.php?cat=004002005&query=view&no=170277',
        '기본수량(3)': 1, '판매단가(V포함)(3)': 14490, '가격차이(3)': -3330, '가격차이(3)(%)': -18.6,
        '공급사명': '네이버', '네이버 쇼핑 링크': 'https://search.shopping.naver.com/catalog/53165134501',
        '본사 이미지': test_images['haereum'],
        '고려기프트 이미지': test_images['kogift'],
        '네이버 이미지': test_images['naver']
    })
    sample_data.append(row3)

    # Create DataFrame
    df_sample = pd.DataFrame(sample_data)
    df_sample = df_sample[FINAL_COLUMN_ORDER]  # Ensure correct column order

    logger.info(f"Sample DataFrame created with {len(df_sample)} rows using real data structure.")
    logger.debug("Sample DataFrame Head:\n" + df_sample.head().to_string())

    # Create Excel file
    logger.info(f"Calling create_final_output_excel to generate: {TEST_OUTPUT_PATH}")
    success = False
    try:
        success = create_final_output_excel(df_sample.copy(), TEST_OUTPUT_PATH)
    except Exception as e:
        logger.error(f"An error occurred during create_final_output_excel call: {e}", exc_info=True)
        success = False

    if success:
        logger.info(f"Excel file creation test successful! Output saved to: {TEST_OUTPUT_PATH}")
        print(f"\n[SUCCESS] Excel file created successfully: {TEST_OUTPUT_PATH}")
        print("Please open the file and verify:")
        print("- Images should be embedded in their respective columns")
        print("- Formatting (numbers, alignment, borders) should be correct")
        print("- Hyperlinks should be working")
    else:
        logger.error(f"Excel file creation test failed. Check logs at '{log_file_path}'.")
        print(f"\n[FAILURE] Excel file creation failed. Check logs at '{log_file_path}'.")

    logger.info("--- Finished Excel Creation Test with REAL Local Images ---")

if __name__ == "__main__":
    run_excel_creation_test() 