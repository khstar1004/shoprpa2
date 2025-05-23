#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, 'PythonScript')

from utils import generate_product_name_hash, generate_consistent_filename

def test_hash_functions():
    """새로 추가한 해시 함수들을 테스트합니다."""
    
    print("=== 상품명 해시 생성 테스트 ===")
    
    # 테스트 상품명들
    test_product_names = [
        '테스트 상품명 ABC',
        '노트북 가방',
        '마우스패드 세트',
        '무선마우스',
        '블루투스 스피커',
        '고급 펜 세트'
    ]
    
    for product_name in test_product_names:
        hash_value = generate_product_name_hash(product_name)
        kogift_filename = generate_consistent_filename(product_name, 'kogift', '.jpg')
        haereum_filename = generate_consistent_filename(product_name, 'haereum', '.jpg')
        naver_filename = generate_consistent_filename(product_name, 'naver', '.png')
        
        print(f"\n상품명: {product_name}")
        print(f"  해시값 (16자): {hash_value}")
        print(f"  Kogift 파일명: {kogift_filename}")
        print(f"  Haereum 파일명: {haereum_filename}")
        print(f"  Naver 파일명: {naver_filename}")
    
    print("\n=== 동일 상품명 해시 일관성 테스트 ===")
    test_name = "일관성 테스트 상품"
    hash1 = generate_product_name_hash(test_name)
    hash2 = generate_product_name_hash(test_name)
    hash3 = generate_product_name_hash(test_name)
    
    print(f"상품명: {test_name}")
    print(f"해시1: {hash1}")
    print(f"해시2: {hash2}")
    print(f"해시3: {hash3}")
    print(f"모든 해시 동일? {hash1 == hash2 == hash3}")
    
    # 파일명도 해시 부분은 같은지 확인 (랜덤 부분은 다름)
    filename1 = generate_consistent_filename(test_name, 'kogift', '.jpg')
    filename2 = generate_consistent_filename(test_name, 'kogift', '.jpg')
    
    # 해시 부분만 추출해서 비교
    hash_part1 = filename1.split('_')[1]  # kogift_{hash}_{random}.jpg에서 hash 부분
    hash_part2 = filename2.split('_')[1]
    
    print(f"\n파일명1: {filename1}")
    print(f"파일명2: {filename2}")
    print(f"해시 부분 동일? {hash_part1 == hash_part2}")
    print(f"랜덤 부분 다름? {filename1.split('_')[2] != filename2.split('_')[2]}")

if __name__ == "__main__":
    test_hash_functions() 