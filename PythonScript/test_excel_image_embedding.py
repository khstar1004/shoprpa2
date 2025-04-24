import os
import pandas as pd
import logging
import sys
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Import our Excel utilities
from excel_utils import create_final_output_excel

def main():
    """Test script to verify image embedding in Excel files"""
    logging.info("Starting test for Excel image embedding...")
    
    # Create directories for test output
    output_dir = Path("C:/RPA/Output")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create test data
    test_data = []
    
    # Paths to image directories
    haereum_dir = Path("C:/RPA/Image/Main/Haereum")
    kogift_dir = Path("C:/RPA/Image/Main/kogift")
    naver_dir = Path("C:/RPA/Image/Main/naver")
    
    # Check if directories exist
    if not haereum_dir.exists() or not kogift_dir.exists() or not naver_dir.exists():
        logging.error(f"One or more image directories don't exist. Please check paths.")
        return
    
    # Find sample images (just taking first few from each directory)
    haereum_images = sorted([f for f in haereum_dir.glob("*.jpg") if not "_nobg" in f.name])[:5]
    kogift_images = sorted([f for f in kogift_dir.glob("*.jpg") if not "_nobg" in f.name])[:5]
    naver_images = sorted([f for f in naver_dir.glob("*.jpg") if not "_nobg" in f.name])[:5]
    
    logging.info(f"Found {len(haereum_images)} Haereum images, {len(kogift_images)} Kogift images, {len(naver_images)} Naver images")
    
    # Create test rows
    for i in range(max(len(haereum_images), len(kogift_images), len(naver_images))):
        haereum_img = str(haereum_images[i % len(haereum_images)]) if haereum_images else ""
        kogift_img = str(kogift_images[i % len(kogift_images)]) if kogift_images else ""
        naver_img = str(naver_images[i % len(naver_images)]) if naver_images else ""
        
        test_data.append({
            "구분": f"테스트-{i+1}",
            "담당자": "테스트담당자",
            "업체명": f"테스트업체-{i+1}",
            "업체코드": f"TEST{i+1:03d}",
            "Code": f"P{i+1:04d}",
            "중분류카테고리": "테스트카테고리",
            "상품명": f"테스트 상품 {i+1}",
            "기본수량(1)": 1,
            "판매단가(V포함)": 10000 + (i * 1000),
            "본사상품링크": "https://example.com/product1",
            "기본수량(2)": 1,
            "판매단가(V포함)(2)": 9500 + (i * 900),
            "가격차이(2)": 500 + (i * 100),
            "가격차이(2)(%)": 5.0,
            "고려기프트 상품링크": "https://kogift.com/product1",
            "기본수량(3)": 1,
            "판매단가(V포함)(3)": 9800 + (i * 950),
            "가격차이(3)": 200 + (i * 50),
            "가격차이(3)(%)": 2.0,
            "공급사명": "테스트공급사",
            "네이버 쇼핑 링크": "https://shopping.naver.com/product1",
            "공급사 상품링크": "https://supplier.com/product1",
            "본사 이미지": haereum_img,
            "고려기프트 이미지": kogift_img,
            "네이버 이미지": naver_img
        })
    
    # Create DataFrame
    df = pd.DataFrame(test_data)
    
    # Output Excel file
    output_file = output_dir / "image_embedding_test.xlsx"
    
    # Create Excel file with images
    logging.info(f"Creating Excel file with images: {output_file}")
    success = create_final_output_excel(df, str(output_file))
    
    if success:
        logging.info(f"Test successful! Excel file created at: {output_file}")
        logging.info(f"Please check the file to verify that images are displayed correctly.")
    else:
        logging.error("Failed to create Excel file.")

if __name__ == "__main__":
    main() 