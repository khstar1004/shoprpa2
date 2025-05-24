#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
네이버 이미지 필터링 수정 사항 테스트 스크립트

이 스크립트는 네이버 이미지 유사도 필터링 문제가 해결되었는지 확인합니다.
"""

import sys
import os
import pandas as pd
import configparser
import logging

# Add the PythonScript directory to the path
script_dir = os.path.dirname(os.path.abspath(__file__))
python_script_dir = os.path.join(script_dir, 'PythonScript')
sys.path.insert(0, python_script_dir)

try:
    from image_integration import filter_images_by_similarity
except ImportError as e:
    print(f"Import error: {e}")
    print("Please make sure you're running this from the correct directory")
    sys.exit(1)

def test_naver_filtering():
    """네이버 이미지 필터링 로직 테스트"""
    print("=== 네이버 이미지 필터링 수정 테스트 ===\n")
    
    # 테스트용 config 설정
    config = configparser.ConfigParser()
    config.add_section('ImageFiltering')
    config.set('ImageFiltering', 'similarity_threshold', '0.4')
    config.set('ImageFiltering', 'naver_similarity_threshold', '0.01')
    config.set('ImageFiltering', 'kogift_similarity_threshold', '0.4')
    config.set('ImageFiltering', 'haereum_similarity_threshold', '0.3')
    
    # 테스트용 DataFrame 생성
    test_data = {
        '상품명': [
            '도루코 PACE7 II FRESH 특판기획세트 1호 손잡이',
            '한국인삼공사 정관장 홍삼본력 40ml x 30포'
        ],
        '본사 이미지': [
            {
                'url': 'https://www.jclgift.com/test1.jpg',
                'local_path': '/fake/path/test1.jpg',
                'source': 'haereum',
                'similarity': 0.8
            },
            {
                'url': 'https://www.jclgift.com/test2.jpg',  
                'local_path': '/fake/path/test2.jpg',
                'source': 'haereum',
                'similarity': 0.7
            }
        ],
        '고려기프트 이미지': [
            {
                'url': 'https://koreagift.com/test1.jpg',
                'local_path': '/fake/path/kogift1.jpg',
                'source': 'kogift',
                'similarity': 0.6
            },
            None
        ],
        '네이버 이미지': [
            {
                'url': 'https://shopping-phinf.pstatic.net/main_1234/1234.jpg',
                'local_path': '/fake/path/naver1.jpg',
                'source': 'naver',
                'similarity': 0.0  # 문제가 되었던 0.0 유사도
            },
            {
                'url': 'https://shopping-phinf.pstatic.net/main_5678/5678.jpg',
                'local_path': '/fake/path/naver2.jpg', 
                'source': 'naver',
                'similarity': 0.0  # 문제가 되었던 0.0 유사도
            }
        ]
    }
    
    df = pd.DataFrame(test_data)
    
    print("=== 필터링 전 데이터 ===")
    for idx, row in df.iterrows():
        print(f"상품 {idx+1}: {row['상품명'][:30]}...")
        for col in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
            if isinstance(row[col], dict):
                similarity = row[col].get('similarity', 0)
                print(f"  {col}: 유사도 {similarity:.3f}")
            else:
                print(f"  {col}: None")
        print()
    
    # 필터링 적용
    print("=== 필터링 적용 중 ===")
    try:
        filtered_df = filter_images_by_similarity(df, config)
        print("필터링 성공!\n")
        
        print("=== 필터링 후 데이터 ===")
        naver_count_before = sum(1 for i in range(len(df)) if isinstance(df.at[i, '네이버 이미지'], dict))
        naver_count_after = sum(1 for i in range(len(filtered_df)) if isinstance(filtered_df.at[i, '네이버 이미지'], dict))
        
        for idx, row in filtered_df.iterrows():
            print(f"상품 {idx+1}: {row['상품명'][:30]}...")
            for col in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
                if isinstance(row[col], dict):
                    similarity = row[col].get('similarity', 0)
                    print(f"  {col}: 유사도 {similarity:.3f} (유지됨)")
                else:
                    print(f"  {col}: None (필터링됨)")
            print()
        
        print("=== 결과 요약 ===")
        print(f"네이버 이미지 - 필터링 전: {naver_count_before}개")
        print(f"네이버 이미지 - 필터링 후: {naver_count_after}개")
        
        if naver_count_after > 0:
            print("✅ 성공: 네이버 이미지가 필터링되지 않고 유지되었습니다!")
            print("   0.0 유사도를 가진 네이버 이미지도 보존되어 엑셀에 표시될 것입니다.")
        else:
            print("❌ 실패: 네이버 이미지가 여전히 필터링되고 있습니다.")
            
    except Exception as e:
        print(f"❌ 필터링 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()

def test_config_reading():
    """config.ini 설정 읽기 테스트"""
    print("\n=== Config 설정 확인 ===")
    
    config_path = os.path.join(script_dir, 'config.ini')
    if not os.path.exists(config_path):
        print(f"❌ config.ini 파일을 찾을 수 없습니다: {config_path}")
        return
        
    config = configparser.ConfigParser()
    try:
        config.read(config_path, encoding='utf-8')
        
        # ImageFiltering 섹션 확인
        if 'ImageFiltering' in config:
            print("✅ ImageFiltering 섹션이 존재합니다")
            
            # 각 설정값 확인
            settings = [
                ('similarity_threshold', '0.4'),
                ('naver_similarity_threshold', '0.01'),
                ('kogift_similarity_threshold', '0.4'), 
                ('haereum_similarity_threshold', '0.3')
            ]
            
            for key, expected in settings:
                if config.has_option('ImageFiltering', key):
                    value = config.get('ImageFiltering', key)
                    print(f"  {key}: {value}")
                    if key == 'naver_similarity_threshold' and float(value) <= 0.01:
                        print(f"    ✅ 네이버 임계값이 매우 낮게 설정됨 (필터링 방지)")
                else:
                    print(f"  ❌ {key} 설정이 없습니다")
        else:
            print("❌ ImageFiltering 섹션이 없습니다")
            
    except Exception as e:
        print(f"❌ config.ini 읽기 오류: {e}")

def main():
    """메인 테스트 실행"""
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s'
    )
    
    print("네이버 이미지 필터링 수정 사항 테스트\n")
    
    # Config 설정 테스트
    test_config_reading()
    
    # 필터링 로직 테스트  
    test_naver_filtering()
    
    print("\n=== 테스트 완료 ===")
    print("실제 RPA 실행 시 네이버 이미지가 엑셀에 표시되는지 확인하세요.")

if __name__ == "__main__":
    main() 