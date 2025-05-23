#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# Add the PythonScript directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'PythonScript'))

from PythonScript.utils import generate_consistent_filename, generate_product_name_hash

def test_hash_functions():
    """새로 추가한 해시 함수들을 테스트합니다."""
    
    print("=== 상품명 해시 생성 테스트 ===")
    
    # Test cases
    test_cases = [
        ("테스트 상품명", "kogift"),
        ("Test Product Name", "haereum"),
        ("상품명 with spaces", "naver"),
        ("Product with numbers 123", "other")
    ]
    
    for product_name, prefix in test_cases:
        # Test generate_product_name_hash
        name_hash = generate_product_name_hash(product_name)
        print(f"\nProduct: {product_name}")
        print(f"Name hash: {name_hash}")
        
        # Test generate_consistent_filename
        filename = generate_consistent_filename(product_name, prefix)
        print(f"Generated filename: {filename}")
        
        # Verify filename format
        parts = filename.split('_')
        assert len(parts) == 3, f"Invalid filename format: {filename}"
        assert parts[0] == prefix, f"Invalid prefix in filename: {filename}"
        assert len(parts[1]) == 16, f"Invalid name hash length in filename: {filename}"
        assert len(parts[2].split('.')[0]) == 8, f"Invalid random hash length in filename: {filename}"
    
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