import os
import logging
from pathlib import Path
import configparser
from PIL import Image
from typing import Dict, List, Any

# Initialize logger
logger = logging.getLogger(__name__)

# --- Image Processing Constants ---
try:
    # Python 3.10+ with newer Pillow
    RESAMPLING_FILTER = Image.Resampling.LANCZOS
except (AttributeError, ImportError):
    try:
        # Older Pillow versions
        RESAMPLING_FILTER = Image.LANCZOS
    except (AttributeError, ImportError):
        # Very old Pillow versions
        RESAMPLING_FILTER = Image.ANTIALIAS

# --- Configuration Setup ---
CONFIG = configparser.ConfigParser()
config_ini_path = Path(__file__).resolve().parent.parent / 'config.ini'

try:
    if not CONFIG.read(config_ini_path, encoding='utf-8'):
        raise FileNotFoundError(f"Could not read config file: {config_ini_path}")
    logger.info(f"Successfully loaded configuration from {config_ini_path}")
    
    # Get main paths from config with validation
    IMAGE_MAIN_DIR = Path(CONFIG.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main'))
    if not IMAGE_MAIN_DIR.exists():
        logger.warning(f"Image directory does not exist: {IMAGE_MAIN_DIR}. Will create if needed.")
        IMAGE_MAIN_DIR.mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.error(f"Error loading config from {config_ini_path}: {e}, using default values")
    IMAGE_MAIN_DIR = Path('C:\\RPA\\Image\\Main')

# --- Directory Constants ---
IMAGE_DIRS = {
    'HAEREUM': 'Haereum',
    'KOGIFT': 'Kogift',
    'NAVER': 'Naver',
    'OTHER': 'Other'
}

# Directory name constants for direct import
HAEREUM_DIR_NAME = IMAGE_DIRS['HAEREUM']
KOGIFT_DIR_NAME = IMAGE_DIRS['KOGIFT']
NAVER_DIR_NAME = IMAGE_DIRS['NAVER']
OTHER_DIR_NAME = IMAGE_DIRS['OTHER']

# --- Promotion Keywords ---
PROMO_KEYWORDS: List[str] = ['판촉', '기프트', '답례품', '기념품', '인쇄', '각인', '제작', '호갱', '몽키', '홍보']

# --- Column Mappings ---
# Standard column renames - map FROM old names TO new names
COLUMN_RENAME_MAP = {
    # 기본 정보
    '구분(승인관리:A/가격관리:P)': '구분',
    '공급사명': '업체명',
    '공급처코드': '업체코드',
    '상품코드': 'Code',
    '카테고리(중분류)': '중분류카테고리',
    
    # 해오름(본사) 정보
    '본사 기본수량': '기본수량(1)',
    '판매단가1(VAT포함)': '판매단가(V포함)',
    '본사링크': '본사상품링크',
    
    # 고려기프트 정보
    '고려 기본수량': '기본수량(2)',
    '판매단가2(VAT포함)': '판매가(V포함)(2)',
    '고려 가격차이': '가격차이(2)',
    '고려 가격차이(%)': '가격차이(2)(%)',
    '고려 링크': '고려기프트 상품링크',
    
    # 네이버 정보
    '네이버 기본수량': '기본수량(3)',
    '판매단가3 (VAT포함)': '판매단가(V포함)(3)',
    '네이버 가격차이': '가격차이(3)',
    '네이버가격차이(%)': '가격차이(3)(%)',
    '네이버 공급사명': '공급사명',
    '네이버 링크': '네이버 링크',
    
    # 이미지 정보
    '해오름(이미지링크)': '해오름(이미지링크)',
    '고려기프트(이미지링크)': '고려기프트(이미지링크)',
    '네이버쇼핑(이미지링크)': '네이버쇼핑(이미지링크)'
}

# Upload file column mapping (result file -> upload file)
UPLOAD_COLUMN_MAPPING = {
    # 기본 정보
    '구분': '구분(승인관리:A/가격관리:P)',
    '담당자': '담당자',
    '업체명': '공급사명',
    '업체코드': '공급처코드',
    'Code': '상품코드',
    '중분류카테고리': '카테고리(중분류)',
    '상품명': '상품명',
    
    # 해오름(본사) 정보
    '기본수량(1)': '본사 기본수량',
    '판매단가(V포함)': '판매단가1(VAT포함)',
    '본사상품링크': '본사링크',
    
    # 고려기프트 정보
    '기본수량(2)': '고려 기본수량',
    '판매가(V포함)(2)': '판매단가2(VAT포함)',
    '가격차이(2)': '고려 가격차이',
    '가격차이(2)(%)': '고려 가격차이(%)',
    '고려기프트 상품링크': '고려 링크',
    
    # 네이버 정보
    '기본수량(3)': '네이버 기본수량',
    '판매단가(V포함)(3)': '판매단가3 (VAT포함)',
    '가격차이(3)': '네이버 가격차이',
    '가격차이(3)(%)': '네이버가격차이(%)',
    '공급사명': '네이버 공급사명',
    '네이버 쇼핑 링크': '네이버 링크',
    
    # 이미지 정보
    '해오름(이미지링크)': '해오름(이미지링크)',
    '고려기프트(이미지링크)': '고려기프트(이미지링크)',
    '네이버쇼핑(이미지링크)': '네이버쇼핑(이미지링크)'
}

# Final Target Column Order (Based on "엑셀 골든" sample)
FINAL_COLUMN_ORDER = [
    # 기본 정보
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명',
    
    # 해오름(본사) 정보
    '기본수량(1)', '판매단가(V포함)', '본사상품링크',
    
    # 고려기프트 정보
    '기본수량(2)', '판매가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', '고려기프트 상품링크',
    
    # 네이버 정보
    '기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '공급사명', '네이버 쇼핑 링크',
    
    # 추가 정보
    '공급사 상품링크',
    
    # 이미지 정보
    '해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'
]

# Columns that must be present in the input file for processing
REQUIRED_INPUT_COLUMNS = [
    '구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리',
    '상품명', '기본수량(1)', '판매단가(V포함)', '본사상품링크'
]

# Column Type Definitions for Formatting
PRICE_COLUMNS = [
    '판매단가(V포함)', '판매가(V포함)(2)', '판매단가(V포함)(3)',
    '가격차이(2)', '가격차이(3)'
]
QUANTITY_COLUMNS = ['기본수량(1)', '기본수량(2)', '기본수량(3)']
PERCENTAGE_COLUMNS = ['가격차이(2)(%)', '가격차이(3)(%)']
TEXT_COLUMNS = ['구분', '담당자', '업체명', '업체코드', 'Code', '중분류카테고리', '상품명', '공급사명']
LINK_COLUMNS_FOR_HYPERLINK = {
    # Map final column names used for links
    '본사상품링크': '본사상품링크',
    '고려기프트 상품링크': '고려기프트 상품링크',
    '네이버 쇼핑 링크': '네이버 쇼핑 링크',
    '공급사 상품링크': '공급사 상품링크'
}
# Image columns based on FINAL_COLUMN_ORDER
IMAGE_COLUMNS = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']

# Upload file columns (based on '엑셀골든_upload' notepad)
UPLOAD_COLUMN_ORDER = [
    # 기본 정보
    '구분(승인관리:A/가격관리:P)', '담당자', '공급사명', '공급처코드', '상품코드', '카테고리(중분류)', '상품명',
    
    # 해오름(본사) 정보
    '본사 기본수량', '판매단가1(VAT포함)', '본사링크',
    
    # 고려기프트 정보
    '고려 기본수량', '판매단가2(VAT포함)', '고려 가격차이', '고려 가격차이(%)', '고려 링크',
    
    # 네이버 정보
    '네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)', '네이버 공급사명', '네이버 링크',
    
    # 이미지 정보
    '해오름(이미지링크)', '고려기프트(이미지링크)', '네이버쇼핑(이미지링크)'
]

# Error Messages Constants
ERROR_MESSAGES: Dict[str, str] = {
    'no_match': '가격 범위내에 없거나 텍스트 유사율을 가진 상품이 없음',
    'no_price_match': '가격이 범위내에 없거나 검색된 상품이 없음',
    'low_similarity': '일정 정확도 이상의 텍스트 유사율을 가진 상품이 없음',
    'no_results': '검색 결과 0',
    'no_image': '이미지를 찾을 수 없음',
    'file_not_found': '-이미지 없음-',
    'invalid_image': '유효하지 않은 이미지 형식',
    'processing_error': '-처리 오류-',
    'too_small': '이미지 크기가 너무 작음 (저해상도)',
    'format_error': '지원하지 않는 이미지 형식',
    'download_failed': '이미지 다운로드 실패',
    'excel_limit': '이미지 크기가 Excel 제한을 초과함',
    'invalid_data': '유효하지 않은 데이터',
    'missing_required': '필수 데이터 누락'
}
ERROR_MESSAGE_VALUES: List[str] = list(ERROR_MESSAGES.values())  # Cache list for faster checking

# Image Processing Constants
IMAGE_SETTINGS = {
    'MAX_SIZE': (2000, 2000),
    'STANDARD_SIZE': (600, 600),
    'QUALITY': 85,
    'SUPPORTED_FORMATS': ['.jpg', '.jpeg', '.png', '.gif', '.bmp'],
    'CELL_HEIGHT': 420,
    'CELL_WIDTH': 60
}

# Image size constants for direct import
IMAGE_MAX_SIZE = IMAGE_SETTINGS['MAX_SIZE']
IMAGE_STANDARD_SIZE = IMAGE_SETTINGS['STANDARD_SIZE']

# --- Export all constants ---
__all__ = [
    'RESAMPLING_FILTER',
    'IMAGE_MAIN_DIR',
    'IMAGE_DIRS',
    'HAEREUM_DIR_NAME',
    'KOGIFT_DIR_NAME',
    'NAVER_DIR_NAME',
    'OTHER_DIR_NAME',
    'IMAGE_MAX_SIZE',
    'IMAGE_STANDARD_SIZE',
    'PROMO_KEYWORDS',
    'COLUMN_RENAME_MAP',
    'FINAL_COLUMN_ORDER',
    'REQUIRED_INPUT_COLUMNS',
    'PRICE_COLUMNS',
    'QUANTITY_COLUMNS',
    'PERCENTAGE_COLUMNS',
    'TEXT_COLUMNS',
    'LINK_COLUMNS_FOR_HYPERLINK',
    'IMAGE_COLUMNS',
    'UPLOAD_COLUMN_ORDER',
    'UPLOAD_COLUMN_MAPPING',
    'ERROR_MESSAGES',
    'ERROR_MESSAGE_VALUES',
    'IMAGE_SETTINGS'
] 