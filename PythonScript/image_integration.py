import os
import logging
import pandas as pd
from pathlib import Path
import configparser
from typing import Dict, Any, Optional, List
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
import shutil

def integrate_haereum_images(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    해오름 기프트 이미지를 DataFrame에 통합합니다.
    상품명을 기준으로 이미지를 매칭합니다.
    """
    try:
        logging.info("통합: 해오름 기프트 이미지를 결과에 추가합니다...")
        result_df = df.copy()
        
        # 해오름 이미지 디렉토리 경로
        haereum_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')) / 'Haereum'
        
        if haereum_dir.exists():
            # JPG와 PNG 파일 모두 찾기 (nobg 파일 제외)
            haereum_images = sorted([f for f in haereum_dir.glob("*.jpg") if "_nobg" not in f.name]) + \
                             sorted([f for f in haereum_dir.glob("*.png") if "_nobg" not in f.name])
            
            logging.info(f"통합: 해오름 이미지 {len(haereum_images)}개 발견")
            
            # '본사 이미지' 열이 없으면 추가
            if '본사 이미지' not in result_df.columns:
                result_df['본사 이미지'] = None
            
            # 이미지 추가 카운터
            added_images = 0
            
            # 디버깅용 상품명 확인
            if len(result_df) > 0:
                sample_names = result_df['상품명'].tolist()[:5]  # 첫 5개 상품명 샘플
                logging.debug(f"상품명 샘플: {sample_names}")
            
            # 각 상품명에 대한 토큰 생성 (더 나은 매칭을 위해)
            product_tokens = {}
            for idx, row in result_df.iterrows():
                product_name = str(row['상품명']).lower()
                # 숫자, 알파벳, 한글만 남기고 공백으로 변환 후 토큰화
                clean_name = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in product_name])
                tokens = [t for t in clean_name.split() if len(t) > 1]  # 2자 이상 토큰만 유지
                product_tokens[idx] = tokens
            
            # 상품명을 기준으로 이미지 매칭
            for img_path in haereum_images:
                # 이미지 파일명에서 상품명 추출
                img_name = img_path.stem
                if img_name.startswith('haereum_'):
                    img_name = img_name[8:]  # 'haereum_' 접두사 제거
                
                # 해시 부분 제거 (마지막 _ 이후 부분)
                if '_' in img_name:
                    parts = img_name.split('_')
                    # 마지막 부분이 해시처럼 생겼는지 확인 (10자리 이하의 알파벳+숫자)
                    if len(parts[-1]) <= 10 and parts[-1].isalnum():
                        img_name = '_'.join(parts[:-1])
                
                # 이미지 이름 토큰화
                img_name_clean = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in img_name.lower()])
                img_tokens = [t for t in img_name_clean.replace('_', ' ').split() if len(t) > 1]
                
                # 디버깅 출력
                logging.debug(f"처리된 이미지 이름: '{img_name}' (원본: {img_path.name}), 토큰: {img_tokens}")
                
                # DataFrame에서 해당 상품명을 가진 행 찾기 - 토큰 매칭
                matched = False
                
                for idx, tokens in product_tokens.items():
                    # 공통 토큰 찾기
                    common_tokens = set(tokens) & set(img_tokens)
                    
                    # 매칭 점수 계산 (공통 토큰 수 / 이미지 토큰 수)
                    matching_score = len(common_tokens) / len(img_tokens) if img_tokens else 0
                    
                    # 임계값 이상 매칭되면 추가 (최소 30% 이상 매칭)
                    if common_tokens and matching_score >= 0.3:
                        product_name = result_df.iloc[idx]['상품명']
                        # 이미지 데이터 구성
                        image_data = {
                            'local_path': str(img_path),
                            'source': 'haereum',
                            'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                            'original_path': str(img_path)
                        }
                        result_df.at[idx, '본사 이미지'] = image_data
                        added_images += 1
                        matched = True
                        logging.debug(f"매칭 성공: 이미지 '{img_name}' -> 상품 '{product_name}' (점수: {matching_score:.2f}, 공통 토큰: {common_tokens})")
                
                if not matched:
                    logging.debug(f"매칭 실패: 이미지 '{img_name}'와 일치하는 상품명 없음")
            
            logging.info(f"통합: {added_images}개의 해오름 이미지를 DataFrame에 추가했습니다.")
        else:
            logging.warning(f"통합: 해오름 이미지 디렉토리를 찾을 수 없습니다: {haereum_dir}")
        
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 해오름 이미지 추가 중 오류 발생: {e}", exc_info=True)
        return df

def integrate_kogift_images(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    고려기프트 이미지를 DataFrame에 통합합니다.
    상품명을 기준으로 이미지를 매칭합니다.
    해시 형태의 파일명은 모든 행에 할당합니다.
    """
    try:
        logging.info("통합: 고려기프트 이미지를 결과에 추가합니다...")
        result_df = df.copy()
        
        # 고려기프트 이미지 디렉토리 경로
        kogift_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')) / 'Kogift'
        
        if kogift_dir.exists():
            # JPG와 PNG 파일 모두 찾기 (nobg 파일 제외)
            kogift_images = sorted([f for f in kogift_dir.glob("*.jpg") if "_nobg" not in f.name]) + \
                            sorted([f for f in kogift_dir.glob("*.png") if "_nobg" not in f.name])
            
            logging.info(f"통합: 고려기프트 이미지 {len(kogift_images)}개 발견")
            
            # '고려기프트 이미지' 열이 없으면 추가
            if '고려기프트 이미지' not in result_df.columns:
                result_df['고려기프트 이미지'] = None
            
            # 이미지 추가 카운터
            added_images = 0
            
            # 각 상품명에 대한 토큰 생성 (더 나은 매칭을 위해)
            product_tokens = {}
            for idx, row in result_df.iterrows():
                product_name = str(row['상품명']).lower()
                # 숫자, 알파벳, 한글만 남기고 공백으로 변환 후 토큰화
                clean_name = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in product_name])
                tokens = [t for t in clean_name.split() if len(t) > 1]  # 2자 이상 토큰만 유지
                product_tokens[idx] = tokens
            
            # 고려기프트 이미지의 행 할당을 위한 인덱스
            assignment_idx = 0
            
            # 상품명을 기준으로 이미지 매칭
            for img_path in kogift_images:
                # 이미지 파일명에서 상품명 추출
                img_name = img_path.stem
                if img_name.startswith('kogift_'):
                    img_name = img_name[7:]  # 'kogift_' 접두사 제거
                
                # 파일명 형태에 따른 처리
                if '_' in img_name:
                    parts = img_name.split('_')
                    
                    # 해시와 해시 형태 처리 (모든 행에 1개씩 할당)
                    if all(len(part) >= 8 and part.isalnum() for part in parts):
                        logging.debug(f"해시 형태 파일명 감지: {img_path.name}")
                        
                        # 특정 행에 고려기프트 이미지 할당 (라운드 로빈 방식)
                        if assignment_idx >= len(result_df):
                            assignment_idx = 0
                        
                        idx = assignment_idx
                        assignment_idx += 1
                        
                        # 이미지 데이터 구성
                        image_data = {
                            'local_path': str(img_path),
                            'source': 'kogift',
                            'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                            'original_path': str(img_path)
                        }
                        result_df.at[idx, '고려기프트 이미지'] = image_data
                        added_images += 1
                        logging.debug(f"Kogift 해시 이미지 '{img_path.name}' 행 {idx}에 추가")
                        continue
                    
                    # 일반적인 해시 제거 (마지막 부분이 해시인 경우)
                    elif len(parts[-1]) <= 10 and parts[-1].isalnum():
                        img_name = '_'.join(parts[:-1])
                
                # 이미지 이름 토큰화
                img_name_clean = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in img_name.lower()])
                img_tokens = [t for t in img_name_clean.replace('_', ' ').split() if len(t) > 1]
                
                # 디버깅 출력
                logging.debug(f"처리된 고려기프트 이미지 이름: '{img_name}' (원본: {img_path.name}), 토큰: {img_tokens}")
                
                # 토큰이 없거나 모두 짧은 경우
                if not img_tokens:
                    # 특정 행에 고려기프트 이미지 할당 (라운드 로빈 방식)
                    if assignment_idx >= len(result_df):
                        assignment_idx = 0
                    
                    idx = assignment_idx
                    assignment_idx += 1
                    
                    # 이미지 데이터 구성
                    image_data = {
                        'local_path': str(img_path),
                        'source': 'kogift',
                        'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                        'original_path': str(img_path)
                    }
                    result_df.at[idx, '고려기프트 이미지'] = image_data
                    added_images += 1
                    logging.debug(f"Kogift 토큰 없는 이미지 '{img_path.name}' 행 {idx}에 추가")
                    continue
                
                # DataFrame에서 해당 상품명을 가진 행 찾기 - 토큰 매칭
                matched = False
                
                for idx, tokens in product_tokens.items():
                    # 공통 토큰 찾기
                    common_tokens = set(tokens) & set(img_tokens)
                    
                    # 매칭 점수 계산 (공통 토큰 수 / 이미지 토큰 수)
                    matching_score = len(common_tokens) / len(img_tokens) if img_tokens else 0
                    
                    # 임계값 이상 매칭되면 추가 (최소 30% 이상 매칭)
                    if common_tokens and matching_score >= 0.3:
                        product_name = result_df.iloc[idx]['상품명']
                        # 이미지 데이터 구성
                        image_data = {
                            'local_path': str(img_path),
                            'source': 'kogift',
                            'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                            'original_path': str(img_path)
                        }
                        result_df.at[idx, '고려기프트 이미지'] = image_data
                        added_images += 1
                        matched = True
                        logging.debug(f"매칭 성공: 고려기프트 이미지 '{img_name}' -> 상품 '{product_name}' (점수: {matching_score:.2f}, 공통 토큰: {common_tokens})")
                
                if not matched:
                    logging.debug(f"매칭 실패: 고려기프트 이미지 '{img_name}'와 일치하는 상품명 없음")
            
            logging.info(f"통합: {added_images}개의 고려기프트 이미지를 DataFrame에 추가했습니다.")
        else:
            logging.warning(f"통합: 고려기프트 이미지 디렉토리를 찾을 수 없습니다: {kogift_dir}")
        
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 고려기프트 이미지 추가 중 오류 발생: {e}", exc_info=True)
        return df

def integrate_naver_images(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    네이버 이미지를 DataFrame에 통합합니다.
    상품명을 기준으로 이미지를 매칭합니다.
    매칭 임계값을 낮추어 더 유연한 매칭을 제공합니다.
    """
    try:
        logging.info("통합: 네이버 이미지를 결과에 추가합니다...")
        result_df = df.copy()
        
        # 네이버 이미지 디렉토리 경로
        naver_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')) / 'Naver'
        
        if naver_dir.exists():
            # JPG와 PNG 파일 모두 찾기 (nobg 파일 제외)
            naver_images = sorted([f for f in naver_dir.glob("*.jpg") if "_nobg" not in f.name]) + \
                           sorted([f for f in naver_dir.glob("*.png") if "_nobg" not in f.name])
            
            logging.info(f"통합: 네이버 이미지 {len(naver_images)}개 발견")
            
            # '네이버 이미지' 열이 없으면 추가
            if '네이버 이미지' not in result_df.columns:
                result_df['네이버 이미지'] = None
            
            # 이미지 추가 카운터
            added_images = 0
            
            # 각 상품명에 대한 토큰 생성 (더 나은 매칭을 위해)
            product_tokens = {}
            for idx, row in result_df.iterrows():
                product_name = str(row['상품명']).lower()
                # 숫자, 알파벳, 한글만 남기고 공백으로 변환 후 토큰화
                clean_name = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in product_name])
                tokens = [t for t in clean_name.split() if len(t) > 1]  # 2자 이상 토큰만 유지
                product_tokens[idx] = tokens
            
            # 네이버 이미지의 행 할당을 위한 인덱스
            assignment_idx = 0
            
            # 상품명을 기준으로 이미지 매칭
            for img_path in naver_images:
                # 이미지 파일명에서 상품명 추출
                img_name = img_path.stem
                if img_name.startswith('naver_'):
                    img_name = img_name[6:]  # 'naver_' 접두사 제거
                
                # 해시 부분 제거 (마지막 _ 이후 부분)
                if '_' in img_name:
                    parts = img_name.split('_')
                    # 마지막 부분이 해시처럼 생겼는지 확인 (10자리 이하의 알파벳+숫자)
                    if len(parts[-1]) <= 10 and parts[-1].isalnum():
                        img_name = '_'.join(parts[:-1])
                
                # 이미지 이름 토큰화
                img_name_clean = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in img_name.lower()])
                img_tokens = [t for t in img_name_clean.replace('_', ' ').split() if len(t) > 1]
                
                # 디버깅 출력
                logging.debug(f"처리된 네이버 이미지 이름: '{img_name}' (원본: {img_path.name}), 토큰: {img_tokens}")
                
                # 토큰이 없거나 모두 짧은 경우
                if not img_tokens:
                    # 특정 행에 네이버 이미지 할당 (라운드 로빈 방식)
                    if assignment_idx >= len(result_df):
                        assignment_idx = 0
                    
                    idx = assignment_idx
                    assignment_idx += 1
                    
                    # 이미지 데이터 구성
                    image_data = {
                        'local_path': str(img_path),
                        'source': 'naver',
                        'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                        'original_path': str(img_path)
                    }
                    result_df.at[idx, '네이버 이미지'] = image_data
                    added_images += 1
                    logging.debug(f"네이버 토큰 없는 이미지 '{img_path.name}' 행 {idx}에 추가")
                    continue
                
                # DataFrame에서 해당 상품명을 가진 행 찾기 - 토큰 매칭
                matched = False
                best_match_idx = None
                best_match_score = 0.0
                
                for idx, tokens in product_tokens.items():
                    # 공통 토큰 찾기
                    common_tokens = set(tokens) & set(img_tokens)
                    
                    # 매칭 점수 계산 (공통 토큰 수 / 이미지 토큰 수)
                    matching_score = len(common_tokens) / max(len(img_tokens), 1)
                    
                    # 임계값 이상 매칭되면 매칭 후보에 추가 (20% 이상 매칭)
                    if common_tokens and matching_score >= 0.2:  # 임계값 낮춤
                        if matching_score > best_match_score:
                            best_match_score = matching_score
                            best_match_idx = idx
                
                # 최적 매칭이 있으면 이미지 추가
                if best_match_idx is not None:
                    idx = best_match_idx
                    product_name = result_df.iloc[idx]['상품명']
                    # 이미지 데이터 구성
                    image_data = {
                        'local_path': str(img_path),
                        'source': 'naver',
                        'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                        'original_path': str(img_path)
                    }
                    result_df.at[idx, '네이버 이미지'] = image_data
                    added_images += 1
                    matched = True
                    logging.debug(f"매칭 성공: 네이버 이미지 '{img_name}' -> 상품 '{product_name}' (점수: {best_match_score:.2f})")
                    continue
                
                # 매칭이 안 된 경우 라운드 로빈 방식으로 할당
                if not matched:
                    logging.debug(f"매칭 실패: 네이버 이미지 '{img_name}'와 일치하는 상품명 없음, 라운드 로빈 할당")
                    
                    # 특정 행에 네이버 이미지 할당 (라운드 로빈 방식)
                    if assignment_idx >= len(result_df):
                        assignment_idx = 0
                    
                    idx = assignment_idx
                    assignment_idx += 1
                    
                    # 이미지 데이터 구성
                    image_data = {
                        'local_path': str(img_path),
                        'source': 'naver',
                        'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                        'original_path': str(img_path)
                    }
                    result_df.at[idx, '네이버 이미지'] = image_data
                    added_images += 1
            
            logging.info(f"통합: {added_images}개의 네이버 이미지를 DataFrame에 추가했습니다.")
        else:
            logging.warning(f"통합: 네이버 이미지 디렉토리를 찾을 수 없습니다: {naver_dir}")
        
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 네이버 이미지 추가 중 오류 발생: {e}", exc_info=True)
        return df

def filter_images_by_similarity(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    이미지 유사도에 따라 고려기프트 및 네이버 이미지를 필터링합니다.
    임계값보다 낮은 유사도를 가진 이미지는 표시하지 않습니다.
    해오름(본사) 이미지는 유사도에 관계없이 항상 유지합니다.
    
    수정: 현재는 모든 이미지(해오름, 고려기프트, 네이버)를 유지합니다.
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
    
    Returns:
        필터링된 DataFrame
    """
    try:
        # DataFrame 복사본 생성
        result_df = df.copy()
        
        # 임계값 설정 - 설정 파일에서 가져오거나 기본값 사용
        try:
            similarity_threshold = config.getfloat('Matching', 'image_display_threshold', fallback=0.7)
            logging.info(f"통합: 이미지 표시 임계값: {similarity_threshold} (현재 필터링 비활성화 상태)")
        except ValueError as e:
            logging.warning(f"임계값 읽기 오류: {e}. 기본값 0.7을 사용합니다.")
            similarity_threshold = 0.7
        
        # 이미지 유사도 필터링을 비활성화
        # 모든 이미지(해오름, 고려기프트, 네이버) 유지
        logging.info("통합: 이미지 필터링이 비활성화되었습니다. 모든 이미지가 유지됩니다.")
        
        # 이미지 존재 여부 확인 로깅
        haereum_count = 0
        kogift_count = 0
        naver_count = 0
        
        for i in range(len(result_df)):
            # 이미지 열별 존재 카운트
            if '본사 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['본사 이미지']) and result_df.iloc[i]['본사 이미지'] is not None:
                    haereum_count += 1
                    
            if '고려기프트 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['고려기프트 이미지']) and result_df.iloc[i]['고려기프트 이미지'] is not None:
                    kogift_count += 1
                    
            if '네이버 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['네이버 이미지']) and result_df.iloc[i]['네이버 이미지'] is not None:
                    naver_count += 1
        
        logging.info(f"통합: 이미지 현황 - 해오름: {haereum_count}개, 고려기프트: {kogift_count}개, 네이버: {naver_count}개")
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 이미지 유사도 필터링 중 오류 발생: {e}", exc_info=True)
        # 오류 발생 시 원본 DataFrame 반환
        return df

def create_excel_with_images(df, output_file):
    """이미지가 포함된 엑셀 파일 생성"""
    try:
        # '번호' 컬럼이 없으면 추가
        if '번호' not in df.columns:
            df['번호'] = range(1, len(df) + 1)
        
        # 임시 디렉토리 생성
        temp_dir = Path("temp_images")
        temp_dir.mkdir(exist_ok=True)
        
        # 워크북 생성
        wb = Workbook()
        ws = wb.active
        
        # 사용 가능한 컬럼 확인
        available_columns = df.columns.tolist()
        logging.info(f"엑셀 생성: 사용 가능한 컬럼: {available_columns}")
        
        # 기본 헤더 및 데이터 컬럼 정의
        base_headers = ['번호', '상품명']
        optional_headers = ['파일명', '본사 이미지', '고려기프트 이미지', '네이버 이미지', '이미지_유사도']
        
        # 실제 사용할 헤더 목록 생성
        headers = base_headers + [h for h in optional_headers if h in available_columns]
        
        # 헤더 작성
        for col, header in enumerate(headers, 1):
            ws.cell(row=1, column=col, value=header)
        
        # 행 높이 설정
        ws.row_dimensions[1].height = 30  # 헤더 행 높이
        for row in range(2, len(df) + 2):
            ws.row_dimensions[row].height = 100  # 데이터 행 높이
        
        # 열 너비 설정
        column_widths = {}
        for i, header in enumerate(headers):
            col_letter = get_column_letter(i+1)
            if header == '번호':
                column_widths[col_letter] = 5
            elif header == '상품명':
                column_widths[col_letter] = 30
            elif header == '파일명':
                column_widths[col_letter] = 30
            else:
                column_widths[col_letter] = 15
        
        for col, width in column_widths.items():
            ws.column_dimensions[col].width = width
        
        # 데이터 및 이미지 추가
        for row_idx, (_, row) in enumerate(df.iterrows(), 2):
            # 기본 데이터 추가
            col_idx = 1
            
            # 번호 추가
            ws.cell(row=row_idx, column=col_idx, value=row['번호'])
            col_idx += 1
            
            # 상품명 추가
            ws.cell(row=row_idx, column=col_idx, value=row['상품명'])
            col_idx += 1
            
            # 파일명 추가 (있을 경우)
            if '파일명' in available_columns:
                ws.cell(row=row_idx, column=col_idx, value=row['파일명'])
                col_idx += 1
            
            # 이미지 데이터 처리
            image_columns = {}
            for col_name in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
                if col_name in available_columns:
                    image_columns[col_name] = row.get(col_name)
            
            # 이미지 추가
            for col_name, img_data in image_columns.items():
                if pd.isna(img_data) or img_data is None:
                    ws.cell(row=row_idx, column=col_idx, value="")
                    col_idx += 1
                    continue
                
                try:
                    # 이미지 경로 추출
                    img_path = None
                    if isinstance(img_data, dict):
                        # excel_utils.py 형식의 딕셔너리 처리
                        img_path = img_data.get('local_path')
                        if not img_path and 'url' in img_data:
                            # URL만 있는 경우 셀에 URL 표시
                            ws.cell(row=row_idx, column=col_idx, value=img_data['url'])
                            col_idx += 1
                            continue
                    elif isinstance(img_data, str):
                        # 문자열 경로 처리
                        img_path = img_data
                    
                    if img_path and os.path.exists(img_path):
                        try:
                            # 이미지 파일 복사
                            img = Image(img_path)
                            # 이미지 크기 조정 (최대 100x100)
                            img.width = 100
                            img.height = 100
                            # 이미지 추가
                            ws.add_image(img, f"{get_column_letter(col_idx)}{row_idx}")
                            ws.cell(row=row_idx, column=col_idx, value="")  # 이미지가 있으면 셀 값 비움
                        except Exception as e:
                            logging.warning(f"이미지 추가 실패 ({img_path}): {e}")
                            # 이미지 추가 실패 시 경로나 URL 표시
                            if isinstance(img_data, dict):
                                ws.cell(row=row_idx, column=col_idx, value=img_data.get('url', str(img_path)))
                            else:
                                ws.cell(row=row_idx, column=col_idx, value=str(img_path))
                    else:
                        # 이미지 파일이 없는 경우 URL이나 경로 표시
                        if isinstance(img_data, dict):
                            ws.cell(row=row_idx, column=col_idx, value=img_data.get('url', ''))
                        else:
                            ws.cell(row=row_idx, column=col_idx, value=str(img_data))
                except Exception as e:
                    logging.error(f"이미지 처리 중 오류 발생 ({col_name}): {e}")
                    ws.cell(row=row_idx, column=col_idx, value="이미지 처리 오류")
                
                col_idx += 1
            
            # 이미지 유사도 추가 (있을 경우)
            if '이미지_유사도' in available_columns:
                ws.cell(row=row_idx, column=col_idx, value=row['이미지_유사도'])
                col_idx += 1
        
        # 엑셀 파일 저장
        wb.save(output_file)
        logging.info(f"이미지가 포함된 엑셀 파일이 저장되었습니다: {output_file}")
        
        # 임시 디렉토리 정리
        shutil.rmtree(temp_dir)
        
    except Exception as e:
        logging.error(f"엑셀 파일 생성 중 오류 발생: {e}", exc_info=True)

def integrate_and_filter_images(df: pd.DataFrame, config: configparser.ConfigParser, 
                            save_excel_output=False) -> pd.DataFrame:
    """
    이미지 통합 및 유사도 기반 이미지 필터링을 순차적으로 수행합니다.
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
        save_excel_output: 결과를 별도의 엑셀 파일로 저장할지 여부 (기본값: False)
    
    Returns:
        처리된 DataFrame
    """
    try:
        logging.info("이미지 통합 및 필터링 프로세스 시작...")
        
        # 1. 해오름 이미지 통합
        result_df = integrate_haereum_images(df, config)
        
        # 2. 고려기프트 이미지 통합
        result_df = integrate_kogift_images(result_df, config)
        
        # 3. 네이버 이미지 통합
        result_df = integrate_naver_images(result_df, config)
        
        # 4. 이미지 유사도 필터링
        result_df = filter_images_by_similarity(result_df, config)
        
        # 5. 필요한 경우에만 결과를 별도의 엑셀 파일로 저장 (이미지 포함)
        if save_excel_output:
            try:
                output_dir = Path(config.get('Paths', 'output_dir', fallback='C:\\RPA\\Output'))
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / "image_integration_results.xlsx"
                create_excel_with_images(result_df, output_file)
                logging.info(f"이미지 통합 결과가 별도 파일로 저장되었습니다: {output_file}")
            except Exception as excel_error:
                logging.error(f"이미지 통합 결과 엑셀 파일 생성 실패: {excel_error}", exc_info=True)
                # 엑셀 파일 저장 실패는 전체 처리 실패로 간주하지 않음
        
        logging.info("이미지 통합 및 필터링 프로세스 완료!")
        return result_df
    
    except Exception as e:
        logging.error(f"이미지 통합 및 필터링 프로세스 중 오류 발생: {e}", exc_info=True)
        # 오류 발생 시 원본 DataFrame 반환
        return df

# 모듈 테스트용 코드
if __name__ == "__main__":
    # 기본 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # 설정 파일 로드
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    
    # 테스트 데이터 생성
    test_df = pd.DataFrame({
        '번호': [1, 2],
        '상품명': ['테스트 상품 1', '테스트 상품 2'],
        '본사 이미지': [None, None],
        '고려기프트 이미지': [None, None],
        '네이버 이미지': [None, None],
        '이미지_유사도': [0.6, 0.8]
    })
    
    # 이미지 통합 및 필터링 테스트
    result_df = integrate_and_filter_images(test_df, config, save_excel_output=True)
    
    # 결과 출력
    logging.info(f"테스트 결과 DataFrame 형태: {result_df.shape}")
    logging.info(f"본사 이미지 열 데이터: {result_df['본사 이미지'].tolist()}")
    logging.info(f"고려기프트 이미지 열 데이터: {result_df['고려기프트 이미지'].tolist()}")
    logging.info(f"네이버 이미지 열 데이터: {result_df['네이버 이미지'].tolist()}") 