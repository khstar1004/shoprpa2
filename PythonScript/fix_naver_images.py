import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import logging
import os
import json

def fix_naver_data_in_excel(excel_path):
    """
    네이버 이미지 링크가 없는 경우 이미지와 관련 정보를 초기화하는 함수
    
    Args:
        excel_path (str): 처리할 엑셀 파일 경로
        
    Returns:
        str: 처리된 엑셀 파일 경로
    """
    try:
        # 엑셀 파일 읽기
        df = pd.read_excel(excel_path)
        
        # 네이버 이미지 링크 확인 및 처리
        naver_columns = [
            '기본수량(3)',
            '판매단가(V포함)(3)',
            '가격차이(3)',
            '가격차이(3)(%)',
            '공급사명',
            '네이버 쇼핑 링크',
            '공급사 상품링크'
        ]
        
        # 네이버 이미지 칼럼이 있는지 확인
        if '네이버 이미지' in df.columns:
            # 네이버 이미지 링크가 없는 행 찾기 ('-' 또는 빈 문자열)
            empty_image_mask = df['네이버 이미지'].isin(['-', '']) | df['네이버 이미지'].isna()
            
            # 해당 행들의 네이버 관련 칼럼 초기화
            for col in naver_columns:
                if col in df.columns:
                    df.loc[empty_image_mask, col] = '-'
            
            # 네이버 이미지 데이터 처리 (이미지 삭제 포함)
            for idx, row in df[empty_image_mask].iterrows():
                naver_image_data = row['네이버 이미지']
                
                # 이미지 데이터가 문자열로 저장된 딕셔너리인 경우 처리
                if isinstance(naver_image_data, str):
                    try:
                        image_dict = json.loads(naver_image_data)
                        if isinstance(image_dict, dict):
                            # 로컬 이미지 파일 삭제
                            local_path = image_dict.get('local_path')
                            if local_path and os.path.exists(local_path):
                                try:
                                    os.remove(local_path)
                                    logging.info(f"로컬 이미지 파일 삭제 완료: {local_path}")
                                except Exception as del_err:
                                    logging.error(f"이미지 파일 삭제 실패: {local_path}, 오류: {str(del_err)}")
                    except json.JSONDecodeError:
                        pass
                
                # 네이버 이미지 칼럼 초기화
                df.at[idx, '네이버 이미지'] = '-'
            
            # 변경된 내용 저장
            df.to_excel(excel_path, index=False)
            logging.info(f"네이버 이미지 데이터 수정 완료: {excel_path}")
            
            return excel_path
    except Exception as e:
        logging.error(f"네이버 이미지 데이터 수정 중 오류 발생: {str(e)}")
        return None
