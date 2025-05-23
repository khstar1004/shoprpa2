#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
해시 생성 일관성 테스트 스크립트

네이버, 해오름, 고려기프트 간 해시 생성이 일관되는지 확인합니다.
"""

import sys
import os
import hashlib

# PythonScript 디렉토리를 Python 경로에 추가
sys.path.append(os.path.join(os.path.dirname(__file__), 'PythonScript'))

try:
    from PythonScript.utils import generate_product_name_hash
    print("✅ utils.generate_product_name_hash 함수를 성공적으로 임포트했습니다.")
except ImportError as e:
    print(f"❌ utils.generate_product_name_hash 임포트 실패: {e}")
    
    # Fallback 함수 정의
    def generate_product_name_hash(product_name: str) -> str:
        """
        상품명으로부터 16자리 해시값을 생성합니다 (Fallback 버전).
        """
        try:
            # 상품명 정규화 (공백 제거, 소문자 변환)
            normalized_name = ''.join(product_name.split()).lower()
            # MD5 해시 생성 후 첫 16자리 사용
            hash_obj = hashlib.md5(normalized_name.encode('utf-8'))
            return hash_obj.hexdigest()[:16]
        except Exception as e:
            print(f"Error generating hash for product name {product_name}: {e}")
            return ""

def test_hash_consistency():
    """해시 생성 일관성을 테스트합니다."""
    print("\n" + "="*60)
    print("고려기프트/네이버/해오름 해시 일관성 테스트")
    print("="*60)
    
    # 테스트할 상품명들
    test_products = [
        "대형 사무용 집게클립",
        "람프로스 아트콜라 자개 명함케이스 경주 첨성대",
        "로페리아 크로스오버 해변 바캉스 비치타올 230g",
        "모스니에 제로웨이스트 대나무칫솔",
        "하모니 심플칫솔세트 805"
    ]
    
    print(f"\n📋 테스트할 상품 수: {len(test_products)}개")
    print("-" * 60)
    
    for i, product_name in enumerate(test_products, 1):
        print(f"\n{i}. 상품명: '{product_name}'")
        
        # 해시 생성 (utils 함수 사용)
        name_hash = generate_product_name_hash(product_name)
        
        # 두 번째 해시 생성 (고려기프트 방식과 동일)
        normalized_name = ''.join(product_name.split()).lower()
        second_hash = hashlib.md5(normalized_name.encode('utf-8')).hexdigest()[16:24]
        
        print(f"   정규화된 이름: '{normalized_name}'")
        print(f"   첫 번째 해시 (16자): {name_hash}")
        print(f"   두 번째 해시 (8자): {second_hash}")
        
        # 파일명 생성 (각 플랫폼별로)
        platforms = ['kogift', 'naver', 'haereum']
        for platform in platforms:
            filename = f"{platform}_{name_hash}_{second_hash}.jpg"
            print(f"   {platform.upper()} 파일명: {filename}")

def test_normalize_function():
    """정규화 함수의 동작을 테스트합니다."""
    print("\n" + "="*60)
    print("상품명 정규화 함수 테스트")
    print("="*60)
    
    test_cases = [
        "대형 사무용   집게클립",  # 여러 공백
        " 람프로스 아트콜라 자개 명함케이스 ",  # 앞뒤 공백
        "Test\tProduct\n",  # 탭, 개행 문자
        "Product    With    Multiple    Spaces",  # 여러 연속 공백
        "MixedCase Product Name",  # 대소문자 혼합
    ]
    
    for i, original in enumerate(test_cases, 1):
        normalized = ''.join(original.split()).lower()
        print(f"{i}. 원본: '{original}'")
        print(f"   정규화: '{normalized}'")
        hash_value = hashlib.md5(normalized.encode('utf-8')).hexdigest()[:16]
        print(f"   해시: {hash_value}")
        print()

def check_existing_files():
    """기존 파일들의 해시 패턴을 확인합니다."""
    print("\n" + "="*60)
    print("기존 파일 해시 패턴 분석")
    print("="*60)
    
    image_dirs = [
        r"C:\RPA\Image\Main\kogift",
        r"C:\RPA\Image\Main\naver", 
        r"C:\RPA\Image\Main\haereum"
    ]
    
    for img_dir in image_dirs:
        platform = os.path.basename(img_dir)
        print(f"\n📁 {platform.upper()} 디렉토리: {img_dir}")
        
        if not os.path.exists(img_dir):
            print(f"   ❌ 디렉토리가 존재하지 않습니다.")
            continue
            
        files = [f for f in os.listdir(img_dir) if f.endswith(('.jpg', '.png', '.jpeg'))]
        print(f"   📊 이미지 파일 수: {len(files)}개")
        
        # 파일명 패턴 분석
        hash_patterns = {}
        for file in files[:10]:  # 처음 10개만 분석
            if '_' in file:
                parts = file.split('_')
                if len(parts) >= 3:
                    prefix = parts[0]
                    first_hash = parts[1] 
                    second_hash_with_ext = parts[2]
                    second_hash = second_hash_with_ext.split('.')[0]
                    
                    pattern = f"{len(first_hash)}자+{len(second_hash)}자"
                    if pattern not in hash_patterns:
                        hash_patterns[pattern] = []
                    hash_patterns[pattern].append(file)
        
        print(f"   🔍 해시 패턴:")
        for pattern, files_with_pattern in hash_patterns.items():
            print(f"      {pattern}: {len(files_with_pattern)}개 파일")
            if files_with_pattern:
                print(f"         예시: {files_with_pattern[0]}")

if __name__ == "__main__":
    test_hash_consistency()
    test_normalize_function()
    check_existing_files()
    
    print("\n" + "="*60)
    print("✅ 테스트 완료")
    print("="*60) 