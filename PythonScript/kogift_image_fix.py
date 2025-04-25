#!/usr/bin/env python3
"""
고려기프트 이미지 URL 처리 문제 해결 스크립트
"""

import os
import re
import shutil
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def backup_file(file_path):
    """파일 백업"""
    if not os.path.exists(file_path):
        logger.error(f"파일이 존재하지 않습니다: {file_path}")
        return False
        
    # 백업 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{file_path}.{timestamp}.bak"
    
    try:
        shutil.copy2(file_path, backup_path)
        logger.info(f"파일 백업 완료: {backup_path}")
        return True
    except Exception as e:
        logger.error(f"파일 백업 실패: {e}")
        return False

def modify_excel_utils(file_path):
    """excel_utils.py 파일 수정"""
    if not os.path.exists(file_path):
        logger.error(f"파일이 존재하지 않습니다: {file_path}")
        return False
        
    try:
        # 파일 읽기
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 수정할 부분 찾기 (if not content_type.startswith('image/') and not ('jclgift' in url or 'kogift' in url or 'pstatic' in url):)
        original_line = r"if not content_type\.startswith\('image/'\) and not \('jclgift' in url or 'kogift' in url or 'pstatic' in url\):"
        modified_line = r"# 고려기프트, adpanchok 사이트는 text/plain으로 이미지를 반환하므로 예외 처리\n                                        is_kogift_url = any(domain in url.lower() for domain in ['koreagift.com', 'adpanchok.co.kr', 'kogift'])\n                                        if not content_type.startswith('image/') and not is_kogift_url and not ('jclgift' in url or 'pstatic' in url):"
        
        # 정규식으로 수정
        modified_content = re.sub(original_line, modified_line, content)
        
        # 변경 확인
        if modified_content == content:
            logger.warning("변경할 부분을 찾지 못했습니다.")
            return False
        
        # 파일 저장
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(modified_content)
            
        logger.info("파일 수정 완료")
        return True
    
    except Exception as e:
        logger.error(f"파일 수정 중 오류 발생: {e}")
        return False

if __name__ == "__main__":
    # 현재 스크립트 위치 기준으로 excel_utils.py 경로 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    excel_utils_path = os.path.join(current_dir, "excel_utils.py")
    
    if not os.path.exists(excel_utils_path):
        logger.error(f"excel_utils.py 파일을 찾을 수 없습니다: {excel_utils_path}")
        exit(1)
    
    # 백업
    if not backup_file(excel_utils_path):
        logger.error("백업 실패로 인해 수정을 진행하지 않습니다.")
        exit(1)
    
    # 수정
    if modify_excel_utils(excel_utils_path):
        print("고려기프트 이미지 URL 문제가 해결되었습니다.")
    else:
        print("수정에 실패했습니다. 로그를 확인하세요.")
        exit(1) 