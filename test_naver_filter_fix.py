#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to verify that when Naver images are filtered out due to low similarity,
related Naver data columns are also properly cleared.

This addresses the issue where Naver images were being filtered out but Naver price
and product link data remained in the DataFrame.
"""

import sys
import os
import pandas as pd
import configparser
import logging

# Add PythonScript directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'PythonScript'))

from PythonScript.image_integration import filter_images_by_similarity

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_test_dataframe():
    """Create a test DataFrame with Naver image and related data"""
    
    test_data = {
        '상품명': ['테스트 상품1', '테스트 상품2', '테스트 상품3'],
        '본사 이미지': [
            {'url': 'http://test1.jpg', 'similarity': 0.95, 'source': 'haereum'},
            {'url': 'http://test2.jpg', 'similarity': 0.90, 'source': 'haereum'},
            {'url': 'http://test3.jpg', 'similarity': 0.85, 'source': 'haereum'}
        ],
        '고려기프트 이미지': [
            {'url': 'http://kogift1.jpg', 'similarity': 0.75, 'source': 'kogift'},  # 높은 유사도 - 유지됨
            {'url': 'http://kogift2.jpg', 'similarity': 0.40, 'source': 'kogift'},  # 낮은 유사도 - 필터링됨 (0.65 미만)
            None
        ],
        '네이버 이미지': [
            {'url': 'http://naver1.jpg', 'similarity': 0.80, 'source': 'naver'},  # 높은 유사도 - 유지됨
            {'url': 'http://naver2.jpg', 'similarity': 0.50, 'source': 'naver'},  # 중간 유사도 - 필터링됨 (0.70 미만)
            {'url': 'http://naver3.jpg', 'similarity': 0.10, 'source': 'naver'}   # 낮은 유사도 - 필터링됨
        ],
        # Naver related columns
        '네이버 쇼핑 링크': [
            'https://shopping.naver.com/product1',
            'https://shopping.naver.com/product2',  # Should be cleared when image is filtered
            'https://shopping.naver.com/product3'   # Should be cleared when image is filtered
        ],
        '공급사 상품링크': [
            'https://supplier1.com/product1',
            'https://supplier2.com/product2',       # Should be cleared when image is filtered
            'https://supplier3.com/product3'        # Should be cleared when image is filtered
        ],
        '공급사명': [
            '공급사1',
            '공급사2',                              # Should be cleared when image is filtered
            '공급사3'                               # Should be cleared when image is filtered
        ],
        '판매단가(V포함)(3)': [
            15000,
            25000,                                  # Should be cleared when image is filtered
            35000                                   # Should be cleared when image is filtered
        ],
        '가격차이(3)': [
            1000,
            -2000,                                  # Should be cleared when image is filtered
            3000                                    # Should be cleared when image is filtered
        ],
        '가격차이(3)(%)': [
            5.0,
            -8.5,                                   # Should be cleared when image is filtered
            12.3                                    # Should be cleared when image is filtered
        ],
        '기본수량(3)': [
            10,
            20,                                     # Should be cleared when image is filtered
            30                                      # Should be cleared when image is filtered
        ],
        # Kogift related columns
        '고려기프트 상품링크': [
            'https://kogift1.com/product1',
            'https://kogift2.com/product2',         # Should be cleared when image is filtered
            '-'
        ],
        '판매가(V포함)(2)': [
            12000,
            22000,                                  # Should be cleared when image is filtered
            '-'
        ],
        '가격차이(2)': [
            800,
            -1500,                                  # Should be cleared when image is filtered
            '-'
        ]
    }
    
    return pd.DataFrame(test_data)

def create_test_config():
    """Create a test configuration for production-level strict filtering"""
    config = configparser.ConfigParser()
    config.add_section('ImageFiltering')
    config.set('ImageFiltering', 'similarity_threshold', '0.75')  # 프로덕션 수준 임계값
    config.set('ImageFiltering', 'naver_similarity_threshold', '0.70')  # 네이버 엄격한 기준
    config.set('ImageFiltering', 'kogift_similarity_threshold', '0.65')  # 고려기프트 엄격한 기준
    
    return config

def test_naver_filter_fix():
    """Test that Naver related data is cleared when Naver images are filtered"""
    
    logger.info("=== Testing Naver Filter Fix ===")
    
    # Create test data and config
    test_df = create_test_dataframe()
    config = create_test_config()
    
    logger.info("Original DataFrame:")
    print("\n--- BEFORE FILTERING ---")
    
    # Show only relevant columns for clarity
    relevant_cols = [
        '상품명', '네이버 이미지', '네이버 쇼핑 링크', '공급사명', 
        '판매단가(V포함)(3)', '고려기프트 이미지', '고려기프트 상품링크'
    ]
    
    for col in relevant_cols:
        if col in test_df.columns:
            print(f"{col}: {test_df[col].tolist()}")
    
    # Apply filtering
    filtered_df = filter_images_by_similarity(test_df, config)
    
    logger.info("Filtered DataFrame:")
    print("\n--- AFTER FILTERING ---")
    
    for col in relevant_cols:
        if col in filtered_df.columns:
            print(f"{col}: {filtered_df[col].tolist()}")
    
    # Verify results
    print("\n--- VERIFICATION ---")
    
    # Check row 0 (should keep Naver data - similarity 0.80 >= 0.70)
    naver_img_0 = filtered_df.at[0, '네이버 이미지']
    naver_link_0 = filtered_df.at[0, '네이버 쇼핑 링크']
    print(f"Row 0 - Naver image kept: {naver_img_0 is not None} (similarity 0.80 >= 0.70)")
    print(f"Row 0 - Naver link kept: {naver_link_0 != '-'}")
    
    # Check row 1 (should clear Naver data - similarity 0.50 < 0.70)
    naver_img_1 = filtered_df.at[1, '네이버 이미지']
    naver_link_1 = filtered_df.at[1, '네이버 쇼핑 링크']
    supplier_1 = filtered_df.at[1, '공급사명']
    price_1 = filtered_df.at[1, '판매단가(V포함)(3)']
    
    print(f"Row 1 - Naver image filtered: {naver_img_1 is None} (similarity 0.50 < 0.70)")
    print(f"Row 1 - Naver link cleared: {naver_link_1 == '-'}")
    print(f"Row 1 - Supplier cleared: {supplier_1 == '-'}")
    print(f"Row 1 - Price cleared: {price_1 == '-'}")
    
    # Check row 2 (should clear Naver data - similarity 0.10 < 0.70)
    naver_img_2 = filtered_df.at[2, '네이버 이미지']
    naver_link_2 = filtered_df.at[2, '네이버 쇼핑 링크']
    supplier_2 = filtered_df.at[2, '공급사명']
    
    print(f"Row 2 - Naver image filtered: {naver_img_2 is None} (similarity 0.10 < 0.70)")
    print(f"Row 2 - Naver link cleared: {naver_link_2 == '-'}")
    print(f"Row 2 - Supplier cleared: {supplier_2 == '-'}")
    
    # Check Kogift filtering (row 1 should be filtered - similarity 0.40 < 0.65)
    kogift_img_1 = filtered_df.at[1, '고려기프트 이미지']
    kogift_link_1 = filtered_df.at[1, '고려기프트 상품링크']
    
    print(f"Row 1 - Kogift image filtered: {kogift_img_1 is None} (similarity 0.40 < 0.65)")
    print(f"Row 1 - Kogift link cleared: {kogift_link_1 == '-'}")
    
    # Summary
    print("\n--- TEST SUMMARY ---")
    success_count = 0
    total_tests = 6
    
    if naver_img_0 is not None: success_count += 1
    else: print("❌ Row 0 Naver image should be kept")
    
    if naver_img_1 is None: success_count += 1  
    else: print("❌ Row 1 Naver image should be filtered")
    
    if naver_link_1 == '-': success_count += 1
    else: print("❌ Row 1 Naver link should be cleared")
    
    if naver_img_2 is None: success_count += 1
    else: print("❌ Row 2 Naver image should be filtered")
    
    if kogift_img_1 is None: success_count += 1
    else: print("❌ Row 1 Kogift image should be filtered")
    
    if kogift_link_1 == '-': success_count += 1
    else: print("❌ Row 1 Kogift link should be cleared")
    
    print(f"✅ Tests passed: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        logger.info("🎉 All tests passed! The fix is working correctly.")
        return True
    else:
        logger.error(f"❌ {total_tests - success_count} tests failed!")
        return False

if __name__ == "__main__":
    success = test_naver_filter_fix()
    sys.exit(0 if success else 1) 