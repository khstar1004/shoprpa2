"""
Column name patch module.
This module updates the column names in excel_utils.py to match the "엑셀 골든" format.
Import this module at the beginning of your script to ensure the correct column names are used.
"""

import logging
import sys
import os

def patch_column_names():
    """Patch column names in excel_utils.py to match '엑셀 골든' format."""
    try:
        # Import the module we want to patch
        import PythonScript.excel_utils as excel_utils
        
        # Store original values for verification
        original_columns = excel_utils.FINAL_COLUMN_ORDER.copy()
        original_images = excel_utils.IMAGE_COLUMNS.copy()
        
        # New values based on "엑셀 골든"
        new_columns = [
            '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명',
            '기본수량(1)', '판매단가(V포함)', '본사상품링크',
            '기본수량(2)', '판매가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
            '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', 
            '네이버 쇼핑 링크', '공급사 상품링크',
            '본사 이미지', '고려기프트 이미지', '네이버 이미지'
        ]
        
        new_images = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
        
        # Update the constants in the module
        excel_utils.FINAL_COLUMN_ORDER = new_columns
        excel_utils.IMAGE_COLUMNS = new_images
        
        # Also update related constants
        excel_utils.PRICE_COLUMNS = [
            '판매단가(V포함)', '판매가(V포함)(2)', '판매단가(V포함)(3)',
            '가격차이(2)', '가격차이(3)'
        ]
        excel_utils.QUANTITY_COLUMNS = ['기본수량(1)', '기본수량(2)', '기본수량(3)']
        excel_utils.PERCENTAGE_COLUMNS = ['가격차이(2)(%)', '가격차이(3)(%)']
        excel_utils.TEXT_COLUMNS = ['구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명', '공급사명']
        
        # Update the COLUMN_RENAME_MAP to map old names to new names
        excel_utils.COLUMN_RENAME_MAP.update({
            '구분(승인관리:A/가격관리:P)': '구분',
            '공급사명': '업체명',
            '공급처코드': '업체코드',
            '상품코드': 'Code',
            '카테고리(중분류)': '중분류카테고리',
            '본사 기본수량': '기본수량(1)',
            '판매단가1(VAT포함)': '판매단가(V포함)',
            '본사링크': '본사상품링크',
            '고려 기본수량': '기본수량(2)',
            '판매단가2(VAT포함)': '판매가(V포함)(2)',
            '고려 가격차이': '가격차이(2)',
            '고려 가격차이(%)': '가격차이(2)(%)',
            '고려 링크': '고려기프트 상품링크',
            '네이버 기본수량': '기본수량(3)',
            '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
            '네이버 가격차이': '가격차이(3)',
            '네이버가격차이(%)': '가격차이(3)(%)',
            '네이버 공급사명': '공급사명',
            '네이버 링크': '네이버 쇼핑 링크',
            '해오름(이미지링크)': '본사 이미지',
            '고려기프트(이미지링크)': '고려기프트 이미지',
            '네이버쇼핑(이미지링크)': '네이버 이미지'
        })
        
        # Also patch image_integration.py directly
        import PythonScript.image_integration as image_integration
        
        # Create mapping for old to new column names
        old_to_new_img = {
            '해오름(이미지링크)': '본사 이미지',
            '고려기프트(이미지링크)': '고려기프트 이미지', 
            '네이버쇼핑(이미지링크)': '네이버 이미지'
        }
        
        # Log successful patching
        logging.info(f"Patched column names in excel_utils:")
        logging.info(f"  - FINAL_COLUMN_ORDER: {len(new_columns)} columns")
        logging.info(f"  - IMAGE_COLUMNS: {len(new_images)} columns")
        
        # Verify the patch
        assert excel_utils.FINAL_COLUMN_ORDER != original_columns, "Failed to update FINAL_COLUMN_ORDER"
        assert excel_utils.IMAGE_COLUMNS != original_images, "Failed to update IMAGE_COLUMNS"
        
        return True
    except Exception as e:
        logging.error(f"Failed to patch column names: {e}")
        return False

# Auto-patch when module is imported
success = patch_column_names()
if not success:
    logging.warning("Column name patching failed. Excel output may not match the expected format.")
else:
    logging.info("Column names successfully patched to match '엑셀 골든' format.") 