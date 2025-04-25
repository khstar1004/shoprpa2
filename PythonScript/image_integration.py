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
            
            # 상품명을 기준으로 이미지 매칭
            for img_path in haereum_images:
                # 이미지 파일명에서 상품명 추출
                img_name = img_path.stem
                if img_name.startswith('haereum_'):
                    img_name = img_name[8:]  # 'haereum_' 접두사 제거
                img_name = img_name.replace('_', ' ')  # 언더스코어를 공백으로 변경
                
                # DataFrame에서 해당 상품명을 가진 행 찾기
                matching_rows = result_df[result_df['상품명'].str.contains(img_name, case=False, na=False)]
                
                if not matching_rows.empty:
                    for idx in matching_rows.index:
                        # 이미지 데이터 구성
                        image_data = {
                            'local_path': str(img_path),
                            'source': 'haereum',
                            'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                            'original_path': str(img_path)
                        }
                        result_df.at[idx, '본사 이미지'] = image_data
                        added_images += 1
            
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
            
            # 상품명을 기준으로 이미지 매칭
            for img_path in kogift_images:
                # 이미지 파일명에서 상품명 추출
                img_name = img_path.stem
                if img_name.startswith('kogift_'):
                    img_name = img_name[7:]  # 'kogift_' 접두사 제거
                img_name = img_name.replace('_', ' ')  # 언더스코어를 공백으로 변경
                
                # DataFrame에서 해당 상품명을 가진 행 찾기
                matching_rows = result_df[result_df['상품명'].str.contains(img_name, case=False, na=False)]
                
                if not matching_rows.empty:
                    for idx in matching_rows.index:
                        # 이미지 데이터 구성
                        image_data = {
                            'local_path': str(img_path),
                            'source': 'kogift',
                            'url': f"file:///{str(img_path).replace(os.sep, '/')}",
                            'original_path': str(img_path)
                        }
                        result_df.at[idx, '고려기프트 이미지'] = image_data
                        added_images += 1
            
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
            
            # 상품명을 기준으로 이미지 매칭
            for img_path in naver_images:
                # 이미지 파일명에서 상품명 추출
                img_name = img_path.stem
                if img_name.startswith('naver_'):
                    img_name = img_name[6:]  # 'naver_' 접두사 제거
                img_name = img_name.replace('_', ' ')  # 언더스코어를 공백으로 변경
                
                # DataFrame에서 해당 상품명을 가진 행 찾기
                matching_rows = result_df[result_df['상품명'].str.contains(img_name, case=False, na=False)]
                
                if not matching_rows.empty:
                    for idx in matching_rows.index:
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
            logging.info(f"통합: 이미지 표시 임계값: {similarity_threshold}")
        except ValueError as e:
            logging.warning(f"임계값 읽기 오류: {e}. 기본값 0.7을 사용합니다.")
            similarity_threshold = 0.7
        
        # 필터링 카운터
        filtered_kogift = 0
        filtered_naver = 0
        
        # 일정 임계값 이상의 유사도를 가진 이미지만 유지
        for i in range(len(result_df)):
            # 매칭 유사도 확인
            image_similarity = 0.0
            if '이미지_유사도' in result_df.columns:
                try:
                    similarity_value = result_df.iloc[i]['이미지_유사도']
                    image_similarity = float(similarity_value) if pd.notna(similarity_value) else 0.0
                except (ValueError, TypeError):
                    image_similarity = 0.0
            
            # 유사도가 임계값보다 낮으면, 이미지를 표시하지 않음
            if image_similarity < similarity_threshold:
                # 고려기프트 이미지 초기화
                if '고려기프트 이미지' in result_df.columns:
                    kogift_value = result_df.iloc[i]['고려기프트 이미지']
                    if pd.notna(kogift_value) and kogift_value is not None:
                        result_df.at[i, '고려기프트 이미지'] = None
                        filtered_kogift += 1
                
                # 네이버 이미지 초기화
                if '네이버 이미지' in result_df.columns:
                    naver_value = result_df.iloc[i]['네이버 이미지']
                    if pd.notna(naver_value) and naver_value is not None:
                        result_df.at[i, '네이버 이미지'] = None
                        filtered_naver += 1
        
        logging.info(f"통합: 이미지 유사도 필터링 결과 - 고려기프트: {filtered_kogift}개, 네이버: {filtered_naver}개 제거됨")
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
        
        # 헤더 작성
        headers = ['번호', '상품명', '파일명', '본사 이미지', '고려기프트 이미지', '네이버 이미지', '이미지_유사도']
        for col, header in enumerate(headers, 1):
            ws.cell(row=1, column=col, value=header)
        
        # 행 높이 설정
        ws.row_dimensions[1].height = 30  # 헤더 행 높이
        for row in range(2, len(df) + 2):
            ws.row_dimensions[row].height = 100  # 데이터 행 높이
        
        # 열 너비 설정
        column_widths = {'A': 5, 'B': 30, 'C': 30, 'D': 15, 'E': 15, 'F': 15, 'G': 15}
        for col, width in column_widths.items():
            ws.column_dimensions[col].width = width
        
        # 데이터 및 이미지 추가
        for row_idx, (_, row) in enumerate(df.iterrows(), 2):
            # 기본 데이터 추가
            ws.cell(row=row_idx, column=1, value=row['번호'])
            ws.cell(row=row_idx, column=2, value=row['상품명'])
            ws.cell(row=row_idx, column=3, value=row['파일명'])
            ws.cell(row=row_idx, column=7, value=row['이미지_유사도'])
            
            # 이미지 추가
            image_columns = {
                '본사 이미지': row['본사 이미지'],
                '고려기프트 이미지': row['고려기프트 이미지'],
                '네이버 이미지': row['네이버 이미지']
            }
            
            for col_idx, (col_name, img_data) in enumerate(image_columns.items(), 4):
                if pd.isna(img_data) or img_data is None:
                    ws.cell(row=row_idx, column=col_idx, value="")
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
        
        # 엑셀 파일 저장
        wb.save(output_file)
        logging.info(f"이미지가 포함된 엑셀 파일이 저장되었습니다: {output_file}")
        
        # 임시 디렉토리 정리
        shutil.rmtree(temp_dir)
        
    except Exception as e:
        logging.error(f"엑셀 파일 생성 중 오류 발생: {e}", exc_info=True)

def integrate_and_filter_images(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    이미지 통합 및 유사도 기반 이미지 필터링을 순차적으로 수행합니다.
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
    
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
        
        # 5. 결과를 엑셀 파일로 저장 (이미지 포함)
        output_dir = Path(config.get('Paths', 'output_dir', fallback='C:\\RPA\\Output'))
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "image_integration_results.xlsx"
        create_excel_with_images(result_df, output_file)
        
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
        '상품명': ['테스트 상품 1', '테스트 상품 2'],
        '본사 이미지': [None, None],
        '고려기프트 이미지': [None, None],
        '네이버 이미지': [None, None],
        '이미지_유사도': [0.6, 0.8]
    })
    
    # 이미지 통합 및 필터링 테스트
    result_df = integrate_and_filter_images(test_df, config)
    
    # 결과 출력
    logging.info(f"테스트 결과 DataFrame 형태: {result_df.shape}")
    logging.info(f"본사 이미지 열 데이터: {result_df['본사 이미지'].tolist()}")
    logging.info(f"고려기프트 이미지 열 데이터: {result_df['고려기프트 이미지'].tolist()}")
    logging.info(f"네이버 이미지 열 데이터: {result_df['네이버 이미지'].tolist()}") 