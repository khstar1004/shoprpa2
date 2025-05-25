import os
import logging
import pandas as pd
from pathlib import Path
import configparser
from typing import Dict, Any, Optional, List, Tuple, Set, Union
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
import shutil
import sys
import re
import hashlib
from datetime import datetime
import glob
import time
import json
import numpy as np
import tqdm
import traceback
import openpyxl
import PIL

# Add the parent directory to sys.path to allow imports from PythonScript
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Import common utilities first
try:
    from .utils import generate_product_name_hash, extract_product_hash_from_filename
    from .tokenize_product_names import tokenize_product_name, extract_meaningful_keywords
    logging.info("✅ 공통 유틸리티 함수들을 성공적으로 import했습니다.")
except ImportError:
    try:
        from utils import generate_product_name_hash, extract_product_hash_from_filename
        from tokenize_product_names import tokenize_product_name, extract_meaningful_keywords
        logging.info("✅ 공통 유틸리티 함수들을 직접 import했습니다.")
    except ImportError as e:
        logging.error(f"❌ 공통 유틸리티 함수 import 실패: {e}")
        # Fallback implementations
        def generate_product_name_hash(product_name: str) -> str:
            """Fallback hash generation"""
            try:
                normalized = ''.join(product_name.split()).lower()
                return hashlib.md5(normalized.encode('utf-8')).hexdigest()[:16]
            except Exception:
                return ""
        
        def extract_product_hash_from_filename(filename: str) -> Optional[str]:
            """Fallback hash extraction"""
            try:
                name = os.path.splitext(os.path.basename(filename))[0]
                parts = name.split('_')
                if len(parts) >= 2 and len(parts[1]) == 16:
                    return parts[1].lower()
                return None
            except Exception:
                return None

# Initialize logger
logger = logging.getLogger(__name__)

# Import enhanced image matcher with improved error handling
try:
    from .enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
    ENHANCED_MATCHER_AVAILABLE = True
    logging.info("✅ 고급 이미지 매처를 성공적으로 import했습니다.")
except ImportError:
    try:
        from enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
        ENHANCED_MATCHER_AVAILABLE = True
        logging.info("✅ 고급 이미지 매처를 직접 import했습니다.")
    except ImportError:
        ENHANCED_MATCHER_AVAILABLE = False
        logging.warning("⚠️ 고급 이미지 매처를 사용할 수 없습니다. 기본 텍스트 기반 매칭을 사용합니다.")

def prepare_image_metadata(image_dir: Path, prefix: str, prefer_original: bool = True, prefer_jpg: bool = True) -> Dict[str, Dict]:
    """
    이미지 디렉토리에서 메타데이터를 추출해 인덱스를 생성합니다.
    
    Args:
        image_dir: 이미지 디렉토리 경로
        prefix: 이미지 소스 구분용 접두사 (예: 'haereum', 'kogift', 'naver')
        prefer_original: _nobg가 아닌 원본 이미지를 우선시할지 여부
        prefer_jpg: PNG보다 JPG 파일을 우선시할지 여부
        
    Returns:
        이미지 경로를 키로, 이미지 메타데이터를 값으로 하는 딕셔너리
    """
    image_info = {}
    
    # Normalize path to use forward slashes
    abs_image_dir = os.path.abspath(str(image_dir)).replace('\\', '/')
    logging.info(f"Preparing image metadata from directory: {abs_image_dir} (prefix: {prefix}, prefer_original: {prefer_original}, prefer_jpg: {prefer_jpg})")
    
    # Handle case where directory doesn't exist
    if not os.path.exists(abs_image_dir):
        logging.warning(f"Image directory does not exist: {abs_image_dir}")
        return {}
    
    # First look for image files in the directory
    all_image_files = []
    valid_extensions = ('.jpg', '.jpeg', '.png', '.gif')
    
    try:
        # Get all image files with normalized paths
        for root, _, files in os.walk(abs_image_dir):
            root = root.replace('\\', '/')  # Normalize path
            for file in files:
                if file.lower().endswith(valid_extensions):
                    full_path = os.path.join(root, file).replace('\\', '/')
                    all_image_files.append(full_path)
        
        logging.info(f"Found {len(all_image_files)} total images in {abs_image_dir}")
        
        # Group images by base name (without _nobg suffix)
        image_groups = {}
        
        for img_path in all_image_files:
            filename = os.path.basename(img_path)
            file_root, file_ext = os.path.splitext(filename)
            
            # Handle _nobg suffix
            base_name = file_root
            is_nobg = False
            if file_root.endswith('_nobg'):
                base_name = file_root[:-5]  # Remove _nobg suffix
                is_nobg = True
                
            # Group by base name
            if base_name not in image_groups:
                image_groups[base_name] = {'original_jpg': None, 'original_png': None, 'nobg_png': None, 'nobg_jpg': None}
                
            # Store the path based on type and extension
            if is_nobg:
                if file_ext.lower() in ['.jpg', '.jpeg']:
                    image_groups[base_name]['nobg_jpg'] = img_path
                else:
                    image_groups[base_name]['nobg_png'] = img_path
            else:
                if file_ext.lower() in ['.jpg', '.jpeg']:
                    image_groups[base_name]['original_jpg'] = img_path
                else:
                    image_groups[base_name]['original_png'] = img_path
        
        # Process each group and prioritize files according to preferences
        for base_name, paths in image_groups.items():
            try:
                # Select the best path based on preferences
                img_path = None
                
                # Priority order based on preferences:
                if prefer_original and prefer_jpg:
                    # 1. Original JPG
                    # 2. Original PNG
                    # 3. Nobg JPG
                    # 4. Nobg PNG
                    if paths['original_jpg']:
                        img_path = paths['original_jpg']
                    elif paths['original_png']:
                        img_path = paths['original_png']
                    elif paths['nobg_jpg']:
                        img_path = paths['nobg_jpg']
                    elif paths['nobg_png']:
                        img_path = paths['nobg_png']
                elif prefer_original and not prefer_jpg:
                    # 1. Original PNG
                    # 2. Original JPG
                    # 3. Nobg PNG
                    # 4. Nobg JPG
                    if paths['original_png']:
                        img_path = paths['original_png']
                    elif paths['original_jpg']:
                        img_path = paths['original_jpg']
                    elif paths['nobg_png']:
                        img_path = paths['nobg_png']
                    elif paths['nobg_jpg']:
                        img_path = paths['nobg_jpg']
                elif not prefer_original and prefer_jpg:
                    # 1. Nobg JPG
                    # 2. Original JPG
                    # 3. Nobg PNG
                    # 4. Original PNG
                    if paths['nobg_jpg']:
                        img_path = paths['nobg_jpg']
                    elif paths['original_jpg']:
                        img_path = paths['original_jpg']
                    elif paths['nobg_png']:
                        img_path = paths['nobg_png']
                    elif paths['original_png']:
                        img_path = paths['original_png']
                else:
                    # not prefer_original and not prefer_jpg
                    # 1. Nobg PNG
                    # 2. Original PNG
                    # 3. Nobg JPG
                    # 4. Original JPG
                    if paths['nobg_png']:
                        img_path = paths['nobg_png']
                    elif paths['original_png']:
                        img_path = paths['original_png']
                    elif paths['nobg_jpg']:
                        img_path = paths['nobg_jpg']
                    elif paths['original_jpg']:
                        img_path = paths['original_jpg']
                
                # Skip if no image found
                if not img_path:
                    continue
                
                # Always store all available paths for reference
                original_jpg_path = paths['original_jpg']
                original_png_path = paths['original_png']
                nobg_png_path = paths['nobg_png']
                nobg_jpg_path = paths['nobg_jpg']
                
                # Extract metadata
                filename = os.path.basename(img_path)
                file_root, file_ext = os.path.splitext(filename)
                
                # Extract product hash from filename
                product_hash = extract_product_hash_from_filename(filename)
                if product_hash:
                    logging.debug(f"Extracted hash '{product_hash}' from filename '{filename}'")
                
                # Create metadata dictionary
                metadata = {
                    'path': img_path,
                    'filename': filename,
                    'extension': file_ext.lower(),
                    'is_original': not file_root.endswith('_nobg'),
                    'original_jpg_path': original_jpg_path,
                    'original_png_path': original_png_path,
                    'nobg_jpg_path': nobg_jpg_path,
                    'nobg_png_path': nobg_png_path,
                    'source': prefix.rstrip('_'),
                    'base_name': base_name,
                    'product_hash': product_hash  # Add product hash to metadata
                }
                
                # Store metadata in image_info dictionary
                image_info[img_path] = metadata
                
                # Debug some sample entries
                if len(image_info) <= 2 or len(image_info) % 50 == 0:
                    logging.debug(f"Image metadata sample: {img_path} -> {metadata}")
            
            except Exception as e:
                import traceback
                stack_trace = traceback.format_exc()
                logging.error(f"Error processing image file {base_name}: {e}")
                logging.debug(f"Exception traceback: {stack_trace}")
                continue  # Skip this image but continue processing others
        
        # Log summary
        logging.info(f"Processed {len(image_info)} {prefix} images")
        
        # Additional debug information
        if image_info:
            logging.debug(f"First 3 image keys in {prefix} image_info: {list(image_info.keys())[:3]}")
        else:
            logging.warning(f"No images were successfully processed for {prefix}")
        
        return image_info
        
    except Exception as e:
        import traceback
        stack_trace = traceback.format_exc()
        logging.error(f"Error preparing image metadata from {abs_image_dir}: {e}")
        logging.debug(f"Exception traceback: {stack_trace}")
        return {}

def calculate_similarity(product_tokens: List[str], image_tokens: List[str]) -> float:
    """
    상품명과 이미지 이름 간의 유사도를 계산합니다.
    
    주의: 이 함수는 레거시 호환성을 위해서만 유지됩니다.
    실제 매칭에서는 해시 기반 정확한 매칭만 사용합니다.
    
    Args:
        product_tokens: 상품명에서 추출한 토큰 목록
        image_tokens: 이미지 이름에서 추출한 토큰 목록
        
    Returns:
        유사도 점수 (0.0 ~ 1.0) - 해시 매칭에서는 사용되지 않음
    """
    # 해시 매칭 시스템에서는 이 함수가 사용되지 않습니다
    # 레거시 호환성을 위해서만 유지
    
    if not product_tokens or not image_tokens:
        return 0.0
    
    # 토큰 기반 유사도 계산 (사용되지 않음)
    common_tokens = set(product_tokens) & set(image_tokens)
    
    # 더 정확한 유사도 계산 - 토큰의 길이와 수를 고려
    total_tokens = len(set(product_tokens) | set(image_tokens))
    if total_tokens == 0:
        return 0.0
        
    similarity = len(common_tokens) / total_tokens
    
    # 더 긴 토큰이 매칭되면 가중치 부여
    weight = 1.0
    for token in common_tokens:
        if len(token) >= 4:  # 4글자 이상 토큰에 가중치
            weight += 0.1
    
    return min(similarity * weight, 1.0) # Ensure score doesn't exceed 1.0

def find_best_image_matches(product_names: List[str], 
                           haereum_images: Dict[str, Dict], 
                           kogift_images: Dict[str, Dict], 
                           naver_images: Dict[str, Dict],
                           similarity_threshold: float = 0.8,  # 더 엄격한 임계값으로 변경
                           config: Optional[configparser.ConfigParser] = None,
                           df: Optional[pd.DataFrame] = None) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    개선된 2단계 상품 이미지 매칭 시스템
    
    단계 1: 해시 기반 정확한 매칭 (MD5 해시 비교)
    단계 2: 이미지 유사도 검증 (0.8 임계값)
    
    Args:
        product_names: 매칭할 상품명 리스트
        haereum_images: 해오름 이미지 메타데이터 딕셔너리
        kogift_images: 고려기프트 이미지 메타데이터 딕셔너리
        naver_images: 네이버 이미지 메타데이터 딕셔너리
        similarity_threshold: 이미지 유사도 임계값 (기본: 0.8)
        config: 설정 객체
        df: 상품 정보 DataFrame
        
    Returns:
        각 상품에 대한 (haereum_match, kogift_match, naver_match) 튜플 리스트
    """
    
    logging.info("🚀 개선된 2단계 매칭 시스템 시작")
    logging.info(f"📊 입력 데이터: 상품 {len(product_names)}개, 해오름 {len(haereum_images)}개, "
                f"고려기프트 {len(kogift_images)}개, 네이버 {len(naver_images)}개")
    
    # 설정값 로드
    if config:
        try:
            similarity_threshold = config.getfloat('ImageMatching', 'similarity_threshold', fallback=similarity_threshold)
        except (configparser.Error, ValueError):
            logging.warning(f"설정에서 similarity_threshold를 읽을 수 없음, 기본값 사용: {similarity_threshold}")
    
    # 고급 이미지 매처 초기화
    enhanced_matcher = None
    try:
        from enhanced_image_matcher import EnhancedImageMatcher
        enhanced_matcher = EnhancedImageMatcher(config)
        use_gpu = getattr(enhanced_matcher, 'use_gpu', False)
        logging.info(f"✅ 고급 이미지 매처 초기화 완료 (GPU: {use_gpu})")
    except Exception as e:
        logging.error(f"❌ 고급 이미지 매처 초기화 실패: {e}")
        logging.warning("기본 매칭 방식을 사용합니다.")
    
    # 매칭 결과 및 사용된 이미지 추적
    best_matches = []
    used_haereum = set()
    used_kogift = set()
    used_naver = set()
    
    # 통계 변수
    hash_matches = 0
    image_verified = 0
    no_matches = 0
    
    # 각 상품에 대해 매칭 수행
    for idx, product_name in enumerate(product_names):
        if (idx + 1) % 10 == 0:
            logging.info(f"진행 상황: {idx + 1}/{len(product_names)} 처리 중...")
        
        logging.debug(f"\n📦 상품 '{product_name}' 매칭 시작")
        
        # === 단계 1: 해시 기반 정확한 매칭 ===
        product_hash = generate_product_name_hash(product_name)
        hash_candidates = {
            'haereum': [],
            'kogift': [],
            'naver': []
        }
        
        if product_hash:
            logging.debug(f"🔑 생성된 해시: {product_hash}")
            
            # 각 소스에서 해시 매칭 후보 찾기
            for h_path, h_info in haereum_images.items():
                if h_path not in used_haereum:
                    img_hash = h_info.get('product_hash')
                    if img_hash and img_hash == product_hash:
                        hash_candidates['haereum'].append((h_path, h_info))
            
            for k_path, k_info in kogift_images.items():
                if k_path not in used_kogift:
                    img_hash = k_info.get('product_hash')
                    if img_hash and img_hash == product_hash:
                        hash_candidates['kogift'].append((k_path, k_info))
            
            for n_path, n_info in naver_images.items():
                if n_path not in used_naver:
                    img_hash = n_info.get('product_hash')
                    if img_hash and img_hash == product_hash:
                        hash_candidates['naver'].append((n_path, n_info))
            
            total_hash_candidates = (len(hash_candidates['haereum']) + 
                                   len(hash_candidates['kogift']) + 
                                   len(hash_candidates['naver']))
            
            logging.debug(f"🎯 해시 매칭 후보: 해오름 {len(hash_candidates['haereum'])}개, "
                         f"고려기프트 {len(hash_candidates['kogift'])}개, 네이버 {len(hash_candidates['naver'])}개")
            
            # === 단계 2: 이미지 유사도 검증 (해시 매칭 후보가 있을 때만) ===
            final_matches = {'haereum': None, 'kogift': None, 'naver': None}
            
            if total_hash_candidates > 0:
                # enhanced_matcher가 없어도 해시 매칭은 수행
                if enhanced_matcher:
                    logging.debug(f"🔍 이미지 유사도 검증 시작 (임계값: {similarity_threshold})")
                else:
                    logging.debug(f"🔍 Enhanced matcher 없음 - 해시 매칭만으로 진행")
                
                # 기준 이미지 선택 (해오름 > 고려기프트 > 네이버 순)
                reference_path = None
                reference_source = None
                
                if hash_candidates['haereum']:
                    ref_path, ref_info = hash_candidates['haereum'][0]
                    reference_path = ref_info.get('path', ref_path)
                    reference_source = 'haereum'
                elif hash_candidates['kogift']:
                    ref_path, ref_info = hash_candidates['kogift'][0]
                    reference_path = ref_info.get('path', ref_path)
                    reference_source = 'kogift'
                elif hash_candidates['naver']:
                    ref_path, ref_info = hash_candidates['naver'][0]
                    reference_path = ref_info.get('path', ref_path)
                    reference_source = 'naver'
                
                # Enhanced matcher가 없으면 해시 매칭만으로 확정
                if not enhanced_matcher:
                    # 해시가 일치하는 모든 이미지를 매칭으로 확정
                    for source in ['haereum', 'kogift', 'naver']:
                        if hash_candidates[source]:
                            path, info = hash_candidates[source][0]
                            final_matches[source] = (path, info)
                            if source == 'haereum':
                                used_haereum.add(path)
                            elif source == 'kogift':
                                used_kogift.add(path)
                            elif source == 'naver':
                                used_naver.add(path)
                            logging.info(f"✅ {source} 해시 매칭 성공: {os.path.basename(path)}")
                
                elif reference_path and os.path.exists(reference_path):
                    logging.debug(f"📍 기준 이미지: {reference_source} - {os.path.basename(reference_path)}")
                    
                    # 기준 이미지의 매칭 확정
                    if reference_source == 'haereum':
                        final_matches['haereum'] = hash_candidates['haereum'][0]
                        used_haereum.add(hash_candidates['haereum'][0][0])
                    elif reference_source == 'kogift':
                        final_matches['kogift'] = hash_candidates['kogift'][0]
                        used_kogift.add(hash_candidates['kogift'][0][0])
                    elif reference_source == 'naver':
                        final_matches['naver'] = hash_candidates['naver'][0]
                        used_naver.add(hash_candidates['naver'][0][0])
                    
                    # 다른 소스들과 이미지 유사도 검증
                    for source, candidates in hash_candidates.items():
                        if source == reference_source or not candidates:
                            continue
                        
                        for candidate_path, candidate_info in candidates:
                            candidate_img_path = candidate_info.get('path', candidate_path)
                            
                            if os.path.exists(candidate_img_path):
                                try:
                                    similarity = enhanced_matcher.calculate_similarity(reference_path, candidate_img_path)
                                    logging.debug(f"🔍 유사도 검사: {reference_source} vs {source} = {similarity:.3f}")
                                    
                                    if similarity >= similarity_threshold:
                                        final_matches[source] = (candidate_path, candidate_info)
                                        if source == 'haereum':
                                            used_haereum.add(candidate_path)
                                        elif source == 'kogift':
                                            used_kogift.add(candidate_path)
                                        elif source == 'naver':
                                            used_naver.add(candidate_path)
                                        
                                        logging.info(f"✅ {source} 매칭 성공: {os.path.basename(candidate_path)} (유사도: {similarity:.3f})")
                                        break
                                    else:
                                        logging.debug(f"❌ 유사도 부족: {source} {similarity:.3f} < {similarity_threshold}")
                                        
                                except Exception as e:
                                    logging.error(f"이미지 유사도 계산 오류: {e}")
                            else:
                                logging.warning(f"이미지 파일 없음: {candidate_img_path}")
                
                # 매칭 결과 정리
                if any(final_matches.values()):
                    hash_matches += 1
                    if total_hash_candidates > 1:  # 2개 이상 소스에서 해시 매칭된 경우
                        image_verified += 1
                    
                    logging.info(f"🎉 '{product_name}' 해시+이미지 매칭 완료")
                    
                    # 결과 추가
                    best_matches.append((
                        (final_matches['haereum'][0], 0.95) if final_matches['haereum'] else None,
                        (final_matches['kogift'][0], 0.95) if final_matches['kogift'] else None,
                        (final_matches['naver'][0], 0.95) if final_matches['naver'] else None
                    ))
                    continue
        
        # === 해시 매칭 실패 시 매칭 없음으로 처리 ===
        logging.debug(f"❌ '{product_name}' 해시 매칭 실패 - 매칭 없음으로 처리")
        no_matches += 1
        
        best_matches.append((None, None, None))
    
    # 최종 통계 출력
    success_rate = (hash_matches / len(product_names) * 100) if product_names else 0
    verification_rate = (image_verified / hash_matches * 100) if hash_matches > 0 else 0
    
    logging.info("\n📈 === 매칭 완료 통계 ===")
    logging.info(f"✅ 해시 매칭 성공: {hash_matches}/{len(product_names)} ({success_rate:.1f}%)")
    logging.info(f"🔍 이미지 검증 완료: {image_verified}/{hash_matches} ({verification_rate:.1f}%)")
    logging.info(f"❌ 매칭 실패: {no_matches}/{len(product_names)} ({100-success_rate:.1f}%)")
    logging.info(f"🏃‍♂️ 사용된 이미지: 해오름 {len(used_haereum)}, 고려기프트 {len(used_kogift)}, 네이버 {len(used_naver)}")
    
    # 성능 통계
    if len(product_names) > 0:
        efficiency_score = hash_matches / len(product_names)
        if efficiency_score >= 0.8:
            logging.info("🏆 매칭 효율성: 우수 (80% 이상)")
        elif efficiency_score >= 0.6:
            logging.info("👍 매칭 효율성: 양호 (60% 이상)")
        else:
            logging.info("⚠️ 매칭 효율성: 개선 필요 (60% 미만)")
    
    return best_matches

def find_best_match_for_product(product_tokens: List[str], 
                               image_info: Dict[str, Dict], 
                               used_images: Set[str] = None,
                               similarity_threshold: float = 0.45,  # 임계값 상향 조정 (0.3에서 0.45으로)
                               source_name_for_log: str = "UnknownSource",
                               config: Optional[configparser.ConfigParser] = None) -> Optional[Tuple[str, float]]:
    """
    Find the best matching image for a product based on hash matching only.
    No text similarity calculation - only hash-based matching.
    
    Args:
        product_tokens: Tokens of the product name (used for hash generation)
        image_info: Dictionary of image metadata
        used_images: Set of already used image paths
        similarity_threshold: Not used in hash matching, kept for compatibility
        source_name_for_log: Source name for logging
        config: Configuration object for retrieving settings
        
    Returns:
        Tuple of (best_match_path, hash_match_score) or None if no hash match found
    """
    if not product_tokens:
        return None
        
    if used_images is None:
        used_images = set()
    
    # Generate product hash from tokens
    product_name_str = ' '.join(product_tokens)
    product_hash = generate_product_name_hash(product_name_str)
    
    if not product_hash:
        logging.debug(f"[{source_name_for_log}] Could not generate hash for product: {product_name_str}")
        return None
    
    logging.debug(f"[{source_name_for_log}] Looking for hash match: {product_hash}")
    
    # Look for exact hash matches only
    for img_path, img_data in image_info.items():
        # Skip if already used
        if img_path in used_images:
            continue
            
        # Get hash from image metadata
        img_hash = img_data.get('product_hash')
        if not img_hash:
            continue
            
        # Check for exact hash match
        if img_hash == product_hash:
            img_name = img_data.get('original_name', os.path.basename(img_path))
            logging.info(f"{source_name_for_log}: Hash match found for '{product_name_str}': '{img_name}' (hash: {product_hash})")
            return img_path, 0.95  # High score for hash match
    
    logging.debug(f"[{source_name_for_log}] No hash match found for: {product_name_str} (hash: {product_hash})")
    return None

def find_best_match_with_enhanced_matcher(
    source_img_path: str, 
    target_images: Dict[str, Dict], 
    used_images: Set[str] = None,
    enhanced_matcher: Any = None
) -> Optional[Tuple[str, float]]:
    """
    Enhanced image matching with stricter thresholds for higher quality matches.
    """
    if not enhanced_matcher:
        logging.warning("Enhanced image matcher not available. Falling back to text-based matching.")
        return None
        
    if used_images is None:
        used_images = set()
        
    best_match = None
    best_score = 0
    
    # Using higher thresholds for stricter matching
    high_confidence_threshold = 0.30   # 0.15에서 0.30으로 상향
    min_confidence_threshold = 0.15    # 0.00001에서 0.15로 대폭 상향
    
    gpu_info = "GPU enabled" if getattr(enhanced_matcher, 'use_gpu', False) else "CPU mode"
    logging.info(f"Running enhanced matching on {len(target_images)} target images against source: {os.path.basename(source_img_path)} ({gpu_info})")
    
    # Check if source image exists
    if not os.path.exists(source_img_path):
        logging.error(f"Source image doesn't exist: {source_img_path}")
        return None
    
    # Track timing for performance analysis
    start_time = time.time()
    matches_checked = 0
    high_conf_matches = 0
    
    # Debug: Log some target image paths for verification
    sample_keys = list(target_images.keys())[:3] if len(target_images) > 3 else list(target_images.keys())
    for key in sample_keys:
        logging.info(f"Sample target image key: {key}")
        if isinstance(target_images[key], dict):
            img_path = target_images[key].get('path', key)
            logging.info(f"  - Path from dict: {img_path}")
            if os.path.exists(img_path):
                logging.info(f"  - This path exists on disk")
            else:
                logging.info(f"  - This path does NOT exist on disk")
        else:
            logging.info(f"  - Value is not a dict: {type(target_images[key])}")
    
    for image_path, image_info in target_images.items():
        # Skip if already used
        if image_path in used_images:
            continue
        
        # Determine the actual path to use
        actual_path = image_path
        if isinstance(image_info, dict):
            if 'path' in image_info:
                actual_path = image_info['path']
            
            # Try to use nobg version if available (for potentially better matching)
            if enhanced_matcher.USE_BACKGROUND_REMOVAL and isinstance(image_info, dict):
                # Check nobg paths
                nobg_png = image_info.get('nobg_png_path')
                nobg_jpg = image_info.get('nobg_jpg_path')
                
                # Prefer PNG version for nobg (usually better quality)
                if nobg_png and os.path.exists(nobg_png):
                    logging.debug(f"Using background-removed PNG version for matching: {os.path.basename(nobg_png)}")
                    actual_path = nobg_png
                elif nobg_jpg and os.path.exists(nobg_jpg):
                    logging.debug(f"Using background-removed JPG version for matching: {os.path.basename(nobg_jpg)}")
                    actual_path = nobg_jpg
            
        # Check if image exists
        if not os.path.exists(actual_path):
            logging.warning(f"Target image doesn't exist: {actual_path} (key: {image_path})")
            continue
        
        try:
            logging.debug(f"Comparing source {os.path.basename(source_img_path)} with target {os.path.basename(actual_path)}")
            # Call the calculate_similarity method directly instead of is_match to get raw similarity
            similarity = enhanced_matcher.calculate_similarity(source_img_path, actual_path)
            
            # Log similarity score for debugging
            logging.debug(f"Raw similarity score: {similarity:.4f} for {os.path.basename(actual_path)}")
            
            matches_checked += 1
            
            if similarity > high_confidence_threshold:
                high_conf_matches += 1
                logging.info(f"High confidence match: {os.path.basename(actual_path)} = {similarity:.4f}")
                
            if similarity > best_score:
                best_score = similarity
                best_match = image_path  # Keep the original key, not the resolved path
                logging.info(f"New best match: {os.path.basename(actual_path)} with score {similarity:.4f}")
                
                # Early exit for very high confidence matches to save processing time
                if similarity > 0.75:
                    logging.info(f"Found very high confidence match ({similarity:.4f}), early exit")
                    break
                    
        except Exception as e:
            logging.error(f"Error comparing images: {e}")
            
    # Compute time spent
    elapsed = time.time() - start_time
    
    # Don't return matches below the absolute minimum threshold
    if best_match and best_score < min_confidence_threshold:
        logging.info(f"Best match score ({best_score:.4f}) below min threshold ({min_confidence_threshold})")
        return None
        
    # Log summary of matching results
    if best_match:
        if isinstance(target_images[best_match], dict) and 'path' in target_images[best_match]:
            best_path = target_images[best_match]['path']
        else:
            best_path = best_match
        logging.info(f"Best image match: {os.path.basename(best_path)} ({best_score:.4f}) [checked {matches_checked} images in {elapsed:.2f}s, {high_conf_matches} high confidence]")
    else:
        logging.info(f"No image match found after checking {matches_checked} images in {elapsed:.2f}s")
    
    return (best_match, best_score) if best_match else None

def verify_image_matches(best_matches, product_names, haereum_images, kogift_images, naver_images):
    """
    이미지 매칭 결과를 검증하는 함수입니다.
    프로덕트 이름과 파일 이름 간의 공통 토큰을 확인하여 매칭 품질을 검증합니다.
    
    Args:
        best_matches: find_best_image_matches 함수의 결과
        product_names: 상품명 목록
        haereum_images: 해오름 이미지 정보
        kogift_images: 고려기프트 이미지 정보
        naver_images: 네이버 이미지 정보
        
    Returns:
        검증된 매칭 결과
    """
    verified_matches = []
    
    # ID 기반 매칭에 사용되는 정규 표현식
    id_pattern = re.compile(r'_([a-f0-9]{10})(?:\.jpg|\.png|_nobg\.png)?$')
    
    for idx, (product_name, match_set) in enumerate(zip(product_names, best_matches)):
        # Handle cases where match_set may not have exactly 3 elements
        # or if elements within are not as expected (e.g. None instead of (path, score) tuple)
        haereum_data, kogift_data, naver_data = None, None, None # Initialize with None

        if match_set and len(match_set) == 3:
            haereum_data, kogift_data, naver_data = match_set
        else:
            logging.warning(f"Invalid match_set for product '{product_name}': {match_set}. Using None values for all sources.")
            # haereum_data, kogift_data, naver_data remain None as initialized

        product_tokens = set(tokenize_product_name(product_name))
        
        # 매칭 품질 기록
        match_quality = {
            'haereum': {'score': 0, 'match': haereum_data, 'id': None}, # Store data tuple directly
            'kogift': {'score': 0, 'match': kogift_data, 'id': None},  # Store data tuple directly
            'naver': {'score': 0, 'match': naver_data, 'id': None}    # Store data tuple directly
        }
        
        # 해오름 매칭 검증
        if haereum_data and isinstance(haereum_data, tuple) and len(haereum_data) == 2:
            haereum_path, haereum_score = haereum_data
            match_quality['haereum']['score'] = haereum_score # Use the propagated score
            
            if haereum_path and haereum_path in haereum_images:
                haereum_filename = os.path.basename(haereum_path)
                haereum_id = None
                id_match = id_pattern.search(haereum_filename)
                if id_match:
                    haereum_id = id_match.group(1)
                
                haereum_tokens = set(tokenize_product_name(haereum_images[haereum_path].get('clean_name', 
                                                        haereum_images[haereum_path].get('name_for_matching', ''))))
                common_tokens = product_tokens & haereum_tokens
                token_ratio = len(common_tokens) / max(len(product_tokens), 1)
                
                # Refine score, but start with the propagated score
                match_quality['haereum']['score'] = haereum_score * (1 + token_ratio) 
                match_quality['haereum']['id'] = haereum_id
            else:
                logging.warning(f"Haereum path '{haereum_path}' not found in haereum_images or path is None.")
                match_quality['haereum']['match'] = None # Invalidate if path issue
                match_quality['haereum']['score'] = 0
        elif haereum_data: # Was not None, but not a (path, score) tuple
             logging.warning(f"Unexpected format for haereum_data: {haereum_data}. Clearing Haereum match.")
             match_quality['haereum']['match'] = None
             match_quality['haereum']['score'] = 0
        
        # 고려기프트 매칭 검증
        if kogift_data and isinstance(kogift_data, tuple) and len(kogift_data) == 2:
            kogift_path, kogift_score = kogift_data
            match_quality['kogift']['score'] = kogift_score # Use the propagated score

            if kogift_path and kogift_path in kogift_images:
                kogift_filename = os.path.basename(kogift_path)
                kogift_id = None
                id_match = id_pattern.search(kogift_filename)
                if id_match:
                    kogift_id = id_match.group(1)
                
                haereum_id_for_comparison = match_quality['haereum'].get('id')
                if haereum_id_for_comparison and haereum_id_for_comparison == kogift_id:
                    match_quality['kogift']['score'] = max(kogift_score, 0.8) * 1.5
                else:
                    kogift_tokens = set(tokenize_product_name(kogift_images[kogift_path].get('clean_name',
                                                         kogift_images[kogift_path].get('name_for_matching', ''))))
                    common_tokens = product_tokens & kogift_tokens
                    token_ratio = len(common_tokens) / max(len(product_tokens), 1)
                    match_quality['kogift']['score'] = kogift_score * (1 + token_ratio)
                match_quality['kogift']['id'] = kogift_id
            else:
                logging.warning(f"Kogift path '{kogift_path}' not found in kogift_images or path is None.")
                match_quality['kogift']['match'] = None # Invalidate if path issue
                match_quality['kogift']['score'] = 0
        elif kogift_data: # Was not None, but not a (path, score) tuple
             logging.warning(f"Unexpected format for kogift_data: {kogift_data}. Clearing Kogift match.")
             match_quality['kogift']['match'] = None
             match_quality['kogift']['score'] = 0

        # 네이버 매칭 검증
        if naver_data and isinstance(naver_data, tuple) and len(naver_data) == 2:
            naver_path, naver_score = naver_data
            match_quality['naver']['score'] = naver_score # Use the propagated score

            if naver_path and naver_path in naver_images:
                naver_filename = os.path.basename(naver_path)
                naver_id = None
                id_match = id_pattern.search(naver_filename)
                if id_match:
                    naver_id = id_match.group(1)
                
                haereum_id_for_comparison = match_quality['haereum'].get('id')
                if haereum_id_for_comparison and haereum_id_for_comparison == naver_id:
                    match_quality['naver']['score'] = max(naver_score, 0.8) * 1.5
                else:
                    naver_tokens = set(tokenize_product_name(naver_images[naver_path].get('clean_name',
                                                        naver_images[naver_path].get('name_for_matching', ''))))
                    common_tokens = product_tokens & naver_tokens
                    token_ratio = len(common_tokens) / max(len(product_tokens), 1)
                    # Refine score, but start with the propagated score
                    match_quality['naver']['score'] = naver_score * (1 + token_ratio) 
                match_quality['naver']['id'] = naver_id
            else:
                logging.warning(f"Naver path '{naver_path}' not found in naver_images or path is None.")
                match_quality['naver']['match'] = None # Invalidate if path issue
                match_quality['naver']['score'] = 0
        elif naver_data: # Was not None, but not a (path, score) tuple
             logging.warning(f"Unexpected format for naver_data: {naver_data}. Clearing Naver match.")
             match_quality['naver']['match'] = None
             match_quality['naver']['score'] = 0
        
        # 검증 결과를 로그로 출력
        logging.debug(f"Product: '{product_name}' - Verification scores: Haereum={match_quality['haereum']['score']:.2f}, Kogift={match_quality['kogift']['score']:.2f}, Naver={match_quality['naver']['score']:.2f}")
        
        # 최종 검증된 매칭 결과 추가
        verified_matches.append((
            match_quality['haereum']['match'],
            match_quality['kogift']['match'],
            match_quality['naver']['match']
        ))
    
    return verified_matches

def integrate_images(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    세 가지 이미지 소스(해오름, 고려기프트, 네이버)의 이미지를 DataFrame에 통합합니다.
    상품별로 일관된 이미지 매칭을 보장합니다.
    
    Note: Haereum images (본사 이미지) are ALWAYS included, regardless of matching scores.
    
    Args:
        df: 처리할 DataFrame (data_processing.py의 format_product_data_for_output을 거친 상태여야 함)
        config: 설정 파일
        
    Returns:
        이미지가 통합된 DataFrame
    """
    try:
        logging.info("통합: 이미지 통합 프로세스 시작...")
        result_df = df.copy() # df는 이미 format_product_data_for_output을 거쳐 이미지 컬럼에 dict가 있을 것으로 예상

        # These column names are expected to be in the input df, potentially holding original scraped URLs
        # if they were not already incorporated into the image dictionaries by data_processing.py
        scraped_haereum_url_col_input_df = '본사이미지URL' 
        scraped_kogift_url_col_input_df = '고려기프트이미지URL' 
        scraped_naver_url_col_input_df = '네이버이미지URL'
        
        # 이미지 디렉토리 경로
        main_img_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\\\RPA\\\\Image\\\\Main'))
        haereum_dir = main_img_dir / 'Haereum'
        kogift_dir = main_img_dir / 'Kogift'
        naver_dir = main_img_dir / 'Naver'
        
        # 디렉토리 존재 체크
        if not haereum_dir.exists():
            logging.warning(f"해오름 이미지 디렉토리가 존재하지 않습니다: {haereum_dir}")
        if not kogift_dir.exists():
            logging.warning(f"고려기프트 이미지 디렉토리가 존재하지 않습니다: {kogift_dir}")
        if not naver_dir.exists():
            logging.warning(f"네이버 이미지 디렉토리가 존재하지 않습니다: {naver_dir}")
        
        # 이미지 메타데이터 준비
        haereum_images = prepare_image_metadata(haereum_dir, 'haereum_', prefer_original=True, prefer_jpg=True)
        kogift_images = prepare_image_metadata(kogift_dir, 'kogift_', prefer_original=True, prefer_jpg=True)
        naver_images = prepare_image_metadata(naver_dir, 'naver_', prefer_original=True, prefer_jpg=True)
        
        # 필요한 열 추가
        if '본사 이미지' not in result_df.columns:
            result_df['본사 이미지'] = None
        if '고려기프트 이미지' not in result_df.columns:
            result_df['고려기프트 이미지'] = None
        if '네이버 이미지' not in result_df.columns:
            result_df['네이버 이미지'] = None
        
        # Ensure target columns for image data exist before processing
        # These are the final column names used for output (e.g., in Excel)
        target_cols = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
        for col in target_cols:
            if col not in result_df.columns:
                # Initialize with a suitable default, e.g., None or '-'
                # Using None initially might be better if subsequent logic checks for None
                result_df[col] = None 
                logging.debug(f"Added missing target image column: {col}")

        # 상품 목록 추출
        product_names = result_df['상품명'].tolist()
        
        # 제품 수와 이미지 수 로깅
        logging.info(f"제품 수: {len(product_names)}개")
        logging.info(f"해오름 이미지: {len(haereum_images)}개")
        logging.info(f"고려기프트 이미지: {len(kogift_images)}개")
        logging.info(f"네이버 이미지: {len(naver_images)}개")
        
        # 상품명 샘플 로깅
        if product_names:
            sample_products = product_names[:3] if len(product_names) > 3 else product_names
            logging.debug(f"제품 샘플: {sample_products}")
        
        # Retrieve similarity threshold from config with higher quality defaults
        # 1) Primary key: Matching.text_threshold (higher quality standard)
        # 2) Secondary key: Matching.image_threshold 
        # 3) Tertiary key: ImageMatching.minimum_match_confidence
        # 4) Fallback: 0.4 (high quality default)
        
        # Get thresholds in priority order
        text_threshold = None
        image_threshold = None
        min_match_confidence = None
        
        try:
            if config.has_option('Matching', 'text_threshold'):
                text_threshold = config.getfloat('Matching', 'text_threshold')
                logging.debug(f"Read text_threshold: {text_threshold}")
        except (configparser.Error, ValueError) as e:
            logging.warning(f"Could not read [Matching] text_threshold: {e}")
            
        try:
            if config.has_option('Matching', 'image_threshold'):
                image_threshold = config.getfloat('Matching', 'image_threshold')
                logging.debug(f"Read image_threshold: {image_threshold}")
        except (configparser.Error, ValueError) as e:
            logging.warning(f"Could not read [Matching] image_threshold: {e}")

        try:
            if config.has_option('ImageMatching', 'minimum_match_confidence'):
                min_match_confidence = config.getfloat('ImageMatching', 'minimum_match_confidence')
                logging.debug(f"Read minimum_match_confidence: {min_match_confidence}")
        except (configparser.Error, ValueError) as e:
            logging.warning(f"Could not read [ImageMatching] minimum_match_confidence: {e}")

        # Use the first available threshold, with higher defaults
        if text_threshold is not None:
            similarity_threshold = text_threshold
        elif image_threshold is not None:
            similarity_threshold = image_threshold
        elif min_match_confidence is not None:
            similarity_threshold = min_match_confidence
        else:
            similarity_threshold = 0.4  # Higher quality default
            logging.warning(f"Using higher quality default similarity_threshold of {similarity_threshold} as specific values not found or invalid in config.")
        
        # Set the initial matching threshold
        initial_matching_threshold = similarity_threshold # Use the general similarity_threshold from config or default
        
        logging.info(f"이미지 매칭 유사도 임계값 (for find_best_image_matches): {initial_matching_threshold}")
        
        # Initialize enhanced matcher for image similarity calculations
        enhanced_matcher = None
        try:
            if ENHANCED_MATCHER_AVAILABLE:
                enhanced_matcher = EnhancedImageMatcher(config)
                use_gpu = getattr(enhanced_matcher, 'use_gpu', False)
                logging.info(f"✅ 고급 이미지 매처 초기화 완료 (GPU: {use_gpu})")
        except Exception as e:
            logging.warning(f"⚠️ 고급 이미지 매처 초기화 실패: {e}")
            logging.warning("기본 매칭 방식을 사용합니다.")

        # 최적 매치 찾기 (일관성 보장)
        best_matches = find_best_image_matches(
            product_names,
            haereum_images,
            kogift_images,
            naver_images,
            similarity_threshold=initial_matching_threshold,  # Use lower threshold for initial matching
            config=config,
            df=result_df  # Pass DataFrame for product code matching
        )
        
        # 매칭 결과 검증
        logging.info(f"이미지 매칭 검증 중...")
        verified_matches = verify_image_matches(
            best_matches,
            product_names,
            haereum_images,
            kogift_images,
            naver_images
        )
        
        # 결과를 DataFrame에 적용
        # Map for matching web URL columns with their correct names in the dataframe
        assumed_url_cols = {
            'haereum': '본사상품링크',      # Changed from '본사링크'
            'kogift': '고려기프트 상품링크', # Changed from '고려 링크'
            'naver': '네이버 쇼핑 링크'     # Changed from '네이버 링크'
        }

        # Pre-compute Koreagift product info existence for all rows
        # Will be used to determine if image should be assigned
        kogift_product_info_exists = []
        for idx in range(len(result_df)):
            if idx >= len(result_df):
                kogift_product_info_exists.append(False)
                continue
                
            row_data = result_df.iloc[idx]
            has_kogift_info = False
            
            # Check for Koreagift link
            kogift_link_col = '고려기프트 상품링크'
            if kogift_link_col in row_data and row_data[kogift_link_col]:
                if isinstance(row_data[kogift_link_col], str) and row_data[kogift_link_col].strip() not in ['', '-', 'None', None]:
                    has_kogift_info = True
            
            # Check for Koreagift price
            if not has_kogift_info:
                kogift_price_col = '판매가(V포함)(2)'
                if kogift_price_col in row_data and pd.notna(row_data[kogift_price_col]) and row_data[kogift_price_col] not in [0, '-', '', None]:
                    has_kogift_info = True
                    
            # Check for alternative price column
            if not has_kogift_info:
                alt_kogift_price_col = '판매단가(V포함)(2)'
                if alt_kogift_price_col in row_data and pd.notna(row_data[alt_kogift_price_col]) and row_data[alt_kogift_price_col] not in [0, '-', '', None]:
                    has_kogift_info = True
            
            kogift_product_info_exists.append(has_kogift_info)
        
        logging.info(f"Pre-computed Koreagift product info existence for {len(kogift_product_info_exists)} rows")
        logging.info(f"Found {sum(kogift_product_info_exists)} rows with Koreagift product info")

        # Apply results to DataFrame
        mismatch_count = 0
        naver_mismatch_count = 0
        
        # Keep track of used Haereum images
        used_haereum_images = set()
        
        for idx, (match_set, product_name) in enumerate(zip(verified_matches, product_names)):
            if idx >= len(result_df):
                continue
                
            if match_set:
                haereum_data, kogift_data, naver_data = match_set
                
                # Process Haereum image
                if haereum_data and isinstance(haereum_data, tuple) and len(haereum_data) >= 2:
                    haereum_path, haereum_score = haereum_data[:2]  # Correctly extract path and score
                    if haereum_path and haereum_path in haereum_images:
                        used_haereum_images.add(haereum_path)
                        haereum_image_info_from_metadata = haereum_images[haereum_path] # Metadata from disk scan
                        
                        # Determine web URL for the Haereum image
                        current_haereum_url = None
                        
                        # 1. Try extracting from existing image dict in the input df
                        if idx < len(df) and '본사 이미지' in df.columns and isinstance(df.iloc[idx].get('본사 이미지'), dict):
                            url_from_dict = df.iloc[idx]['본사 이미지'].get('url')
                            if isinstance(url_from_dict, str) and url_from_dict.startswith(('http://', 'https://')):
                                current_haereum_url = url_from_dict
                        
                        # 2. Fallback: Check a separate URL column in input df (e.g., '본사이미지URL')
                        if not current_haereum_url and idx < len(df) and scraped_haereum_url_col_input_df in df.columns:
                            url_val_from_separate_col = df.iloc[idx].get(scraped_haereum_url_col_input_df)
                            if isinstance(url_val_from_separate_col, str) and url_val_from_separate_col.startswith(('http://', 'https://')):
                                current_haereum_url = url_val_from_separate_col
                        
                        # 3. Fallback: URL from metadata (less likely to be a web URL)
                        if not current_haereum_url:
                            current_haereum_url = haereum_image_info_from_metadata.get('url', '')

                        image_data = {
                            'url': current_haereum_url, 
                            'local_path': haereum_image_info_from_metadata.get('path', haereum_path),
                            'source': 'haereum',
                            'product_name': product_name,
                            'similarity': haereum_score,
                            'original_path': haereum_path
                        }
                        result_df.at[idx, '본사 이미지'] = image_data
                        
                        # Also update the separate URL column in result_df for consistency if it exists
                        if scraped_haereum_url_col_input_df not in result_df.columns:
                            result_df[scraped_haereum_url_col_input_df] = None
                        result_df.at[idx, scraped_haereum_url_col_input_df] = image_data['url']
                
                # Process Kogift image
                if kogift_data and isinstance(kogift_data, tuple) and len(kogift_data) >= 2:
                    kogift_path, kogift_score = kogift_data[:2]
                    if kogift_path and kogift_path in kogift_images:
                        has_kogift_product_info = idx < len(kogift_product_info_exists) and kogift_product_info_exists[idx]
                        if has_kogift_product_info:
                            kogift_image_info_from_metadata = kogift_images[kogift_path]
                            
                            current_kogift_url = None
                            original_url_for_dict = None
                            original_crawled_url_for_dict = None

                            # 1. Check URL from the dictionary in the input df's '고려기프트 이미지' column
                            input_kogift_dict = None
                            if idx < len(df) and '고려기프트 이미지' in df.columns and isinstance(df.iloc[idx].get('고려기프트 이미지'), dict):
                                input_kogift_dict = df.iloc[idx]['고려기프트 이미지']
                                url_from_dict = input_kogift_dict.get('url')
                                if isinstance(url_from_dict, str) and url_from_dict.startswith(('http://', 'https://')):
                                    current_kogift_url = url_from_dict
                                # Preserve original_url and original_crawled_url if they exist in the input dict
                                original_url_for_dict = input_kogift_dict.get('original_url')
                                original_crawled_url_for_dict = input_kogift_dict.get('original_crawled_url')
                            
                            # 2. Fallback: Check a separate URL column in input df (e.g., '고려기프트이미지URL')
                            if not current_kogift_url and idx < len(df) and scraped_kogift_url_col_input_df in df.columns:
                                url_val_from_separate_col = df.iloc[idx].get(scraped_kogift_url_col_input_df)
                                if isinstance(url_val_from_separate_col, str) and url_val_from_separate_col.startswith(('http://', 'https://')):
                                    current_kogift_url = url_val_from_separate_col
                            
                            # 3. Fallback: URL from metadata (less likely to be a web URL)
                            if not current_kogift_url:
                                current_kogift_url = kogift_image_info_from_metadata.get('url', '')
                            
                            # If original_url/original_crawled_url were not in input_dict, try metadata
                            if not original_url_for_dict:
                                original_url_for_dict = kogift_image_info_from_metadata.get('original_url')
                            if not original_crawled_url_for_dict:
                                original_crawled_url_for_dict = kogift_image_info_from_metadata.get('original_crawled_url')

                            image_data = {
                                'url': current_kogift_url, 
                                'local_path': kogift_image_info_from_metadata.get('path', kogift_path),
                                'source': 'kogift',
                                'product_name': product_name,
                                'similarity': kogift_score,  
                                'original_path': kogift_path,
                                'original_url': original_url_for_dict,
                                'original_crawled_url': original_crawled_url_for_dict
                            }
                            result_df.at[idx, '고려기프트 이미지'] = image_data
                            logger.info(f"Row {idx} ('{product_name}'): Assigned Kogift image '{os.path.basename(kogift_path)}' with URL: {current_kogift_url}")
                        else:
                            logger.warning(f"Row {idx} ('{product_name}'): Matched Kogift image '{os.path.basename(kogift_path)}' (Score: {kogift_score:.4f}) BUT product info missing. Discarding image.")
                            mismatch_count += 1
                    else:
                        logger.warning(f"Row {idx} ('{product_name}'): Kogift matched path '{kogift_path}' not in kogift_images dict or path is None.")
                elif kogift_data: # Matched but not in expected tuple format
                    logger.warning(f"Row {idx} ('{product_name}'): Kogift image matched but data format unexpected: {kogift_data}")
                else: # No Kogift match found by find_best_image_matches
                    logger.debug(f"Row {idx} ('{product_name}'): No Kogift image matched by find_best_image_matches.")

                # Process Naver image
                if naver_data and isinstance(naver_data, tuple) and len(naver_data) >= 2:
                    naver_path, naver_score = naver_data[:2]  # Correctly extract path and score
                    if naver_path and naver_path in naver_images:
                        # Check if there's Naver product info
                        naver_info_exists = False
                        
                        # Check for Naver link
                        naver_link_col = '네이버 쇼핑 링크'
                        if naver_link_col in result_df.columns and pd.notna(result_df.at[idx, naver_link_col]) and result_df.at[idx, naver_link_col] not in ['', '-', 'None', None]:
                            naver_info_exists = True
                            
                        # Check for alternative Naver link column
                        alt_naver_link_col = '네이버 링크'
                        if not naver_info_exists and alt_naver_link_col in result_df.columns and pd.notna(result_df.at[idx, alt_naver_link_col]) and result_df.at[idx, alt_naver_link_col] not in ['', '-', 'None', None]:
                            naver_info_exists = True
                            
                        # Check for Naver price
                        naver_price_cols = ['판매단가(V포함)(3)', '네이버 판매단가']
                        for price_col in naver_price_cols:
                            if not naver_info_exists and price_col in result_df.columns and pd.notna(result_df.at[idx, price_col]) and result_df.at[idx, price_col] not in [0, '-', '', None]:
                                naver_info_exists = True
                                break
                                
                        # For Naver, we're more lenient - still add the image even without product info,
                        # but log the mismatch for tracking
                        if not naver_info_exists:
                            naver_mismatch_count += 1
                            logging.warning(f"Adding Naver image despite missing product info: {product_name}")
                            
                        naver_image_info_from_metadata = naver_images[naver_path]
                        
                        current_naver_image_url = None
                        product_page_url_for_dict = None

                        # 1. Check URL from the dictionary in the input df's '네이버 이미지' column
                        input_naver_dict = None
                        if idx < len(df) and '네이버 이미지' in df.columns and isinstance(df.iloc[idx].get('네이버 이미지'), dict):
                            input_naver_dict = df.iloc[idx]['네이버 이미지']
                            url_from_dict = input_naver_dict.get('url') # This should be direct image URL
                            if isinstance(url_from_dict, str) and (('phinf.pstatic.net' in url_from_dict) or any(url_from_dict.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif'])):
                                current_naver_image_url = url_from_dict
                            # Preserve product_page_url if it exists and is not an image URL
                            prod_page_url = input_naver_dict.get('product_page_url')
                            if isinstance(prod_page_url, str) and prod_page_url.startswith(('http://', 'https://')) and not (('phinf.pstatic.net' in prod_page_url) or any(prod_page_url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif'])):
                                product_page_url_for_dict = prod_page_url
                        
                        # 2. Fallback: Check a separate URL column in input df for direct image URL
                        if not current_naver_image_url and idx < len(df) and scraped_naver_url_col_input_df in df.columns:
                            url_val_from_separate_col = df.iloc[idx].get(scraped_naver_url_col_input_df)
                            if isinstance(url_val_from_separate_col, str) and (('phinf.pstatic.net' in url_val_from_separate_col) or any(url_val_from_separate_col.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif'])):
                                current_naver_image_url = url_val_from_separate_col
                        
                        # 3. Fallback: URL from metadata (less likely to be a direct image web URL)
                        if not current_naver_image_url:
                            meta_url = naver_image_info_from_metadata.get('url')
                            if isinstance(meta_url, str) and (('phinf.pstatic.net' in meta_url) or any(meta_url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif'])):
                                current_naver_image_url = meta_url

                        # For product_page_url, if not found in input_dict, try metadata's product_url
                        if not product_page_url_for_dict:
                            meta_prod_page_url = naver_image_info_from_metadata.get('product_url')
                            if isinstance(meta_prod_page_url, str) and meta_prod_page_url.startswith(('http://', 'https://')) and not (('phinf.pstatic.net' in meta_prod_page_url) or any(meta_prod_page_url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif'])):
                                product_page_url_for_dict = meta_prod_page_url
                        
                        # Calculate actual image similarity for Naver images if needed
                        final_naver_score = naver_score
                        needs_recalc = naver_image_info_from_metadata.get('needs_image_similarity_calc', False)
                        
                        if needs_recalc and config.getboolean('ImageFiltering', 'enable_naver_image_similarity', fallback=True):
                            # Use enhanced image matcher to calculate real similarity
                            if enhanced_matcher and hasattr(enhanced_matcher, 'calculate_similarity') and os.path.exists(haereum_path):
                                naver_local_path = naver_image_info_from_metadata.get('path', naver_path)
                                if naver_local_path and os.path.exists(naver_local_path):
                                    try:
                                        image_similarity = enhanced_matcher.calculate_similarity(haereum_path, naver_local_path)
                                        logger.info(f"🔍 실제 이미지 유사도 계산 - {product_name}: {image_similarity:.3f} (기존 텍스트 기반: {naver_score:.3f})")
                                        final_naver_score = image_similarity
                                    except Exception as e:
                                        logger.warning(f"네이버 이미지 유사도 계산 실패 - {product_name}: {e}")
                                        final_naver_score = naver_score  # Keep original score on error
                                else:
                                    logger.warning(f"네이버 이미지 파일 없음 - {product_name}: {naver_local_path}")
                                    final_naver_score = naver_score  # Keep original score if file missing
                            else:
                                logger.debug(f"Enhanced matcher 사용 불가 - {product_name}: 텍스트 유사도 유지")
                                final_naver_score = naver_score  # Keep original score if enhanced matcher unavailable
                        else:
                            # Use original score if recalculation not needed or disabled
                            final_naver_score = naver_score
                        
                        image_data = {
                            'url': current_naver_image_url, 
                            'local_path': naver_image_info_from_metadata.get('path', naver_path),
                            'source': 'naver',
                            'product_name': product_name,
                            'similarity': final_naver_score,  # Use calculated similarity
                            'original_path': naver_path,
                            'product_page_url': product_page_url_for_dict
                        }
                        result_df.at[idx, '네이버 이미지'] = image_data
                        
                        # Logic for '네이버 쇼핑 링크' (product page link in result_df)
                        shopping_link_col_in_result_df = '네이버 쇼핑 링크'
                        final_product_page_link = None

                        # A. Prioritize product_page_url_for_dict if available from above
                        if product_page_url_for_dict:
                            final_product_page_link = product_page_url_for_dict
                        
                        # B. Fallback to checking a separate '네이버 쇼핑 링크' column in the input df
                        if not final_product_page_link and idx < len(df) and shopping_link_col_in_result_df in df.columns:
                             url_val_from_input_shopping_link_col = df.iloc[idx].get(shopping_link_col_in_result_df)
                             if isinstance(url_val_from_input_shopping_link_col, str) and url_val_from_input_shopping_link_col.startswith(('http://', 'https://')) and not (('phinf.pstatic.net' in url_val_from_input_shopping_link_col) or any(url_val_from_input_shopping_link_col.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif'])):
                                 final_product_page_link = url_val_from_input_shopping_link_col
                        
                        # C. Fallback to naver_image_info_from_metadata's product_url or url if it's a page link (already covered by product_page_url_for_dict logic)
                        # No new logic needed here for C as it's incorporated into product_page_url_for_dict

                        if final_product_page_link:
                            if shopping_link_col_in_result_df not in result_df.columns: result_df[shopping_link_col_in_result_df] = None
                            # Set only if current value is empty/placeholder OR if the new link is different and valid
                            current_shopping_link_val = result_df.at[idx, shopping_link_col_in_result_df]
                            if not pd.notna(current_shopping_link_val) or current_shopping_link_val in ['', '-', 'None', None] or current_shopping_link_val != final_product_page_link:
                                result_df.at[idx, shopping_link_col_in_result_df] = final_product_page_link
        
        # Final check: Ensure EVERY row has a Haereum image 
        # If any row is missing a Haereum image, assign a random one
        haereum_count_before = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '본사 이미지'], dict))
        haereum_added_count = 0
        
        # Get available Haereum images that haven't been used yet
        available_haereum = [path for path in haereum_images if path not in used_haereum_images]
        if not available_haereum and haereum_images:
            # If all images are used but we still need more, reset and use them again
            available_haereum = list(haereum_images.keys())
            remaining_rows = sum(1 for i in range(len(result_df)) if not isinstance(result_df.at[i, '본사 이미지'], dict))
            logging.info(f"All {len(haereum_images)} Haereum images have been used. Reusing images for {remaining_rows} remaining rows to ensure complete coverage.")
        
        for idx in range(len(result_df)):
            if idx >= len(result_df):
                continue
                
            # Check if this row is missing a Haereum image
            try:
                haereum_img_value = result_df.at[idx, '본사 이미지']
                if not isinstance(haereum_img_value, dict):
                    product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Row {idx+1}"
                    logging.info(f"Row {idx} ('{product_name}'): Missing Haereum image. Assigning available image for complete coverage.")
                else:
                    continue  # Already has a valid image, skip to next row
            except Exception as e:
                logging.error(f"Error checking Haereum image for row {idx}: {e}")
                product_name = f"Row {idx+1}"
                
                # Assign a random Haereum image
                if available_haereum:
                    haereum_path = available_haereum.pop(0)  # Take the first available
                    if not available_haereum and haereum_images:
                        # If we ran out, reset the list
                        available_haereum = list(haereum_images.keys())
                    
                    haereum_image_info_from_metadata = haereum_images[haereum_path]
                    
                    # Create image data
                    image_data = {
                        'url': haereum_image_info_from_metadata.get('url', ''), 
                        'local_path': haereum_image_info_from_metadata.get('path', haereum_path),
                        'source': 'haereum',
                        'product_name': product_name,
                        'similarity': 0.01,  # Very low score to indicate this is a desperate assignment
                        'original_path': haereum_path
                    }
                    result_df.at[idx, '본사 이미지'] = image_data
                    haereum_added_count += 1
        
        # Log the results of ensuring Haereum images
        haereum_count_after = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '본사 이미지'], dict))
        logging.info(f"Haereum image count: {haereum_count_before} -> {haereum_count_after} (Added {haereum_added_count} random images)")
        
        # 이미지 경로 불일치 수정 (로컬 파일이 이동된 경우)
        for idx in range(len(result_df)):
            if idx >= len(result_df):
                continue
                
            # Check and fix Koreagift image paths
            kogift_img = result_df.at[idx, '고려기프트 이미지']
            if isinstance(kogift_img, dict) and 'local_path' in kogift_img:
                local_path = kogift_img['local_path']
                if not os.path.exists(local_path):
                    # Try to find the file in the Kogift directory by basename
                    basename = os.path.basename(local_path)
                    for directory in [kogift_dir, main_img_dir]:
                        possible_path = os.path.join(directory, basename)
                        if os.path.exists(possible_path):
                            kogift_img['local_path'] = possible_path
                            result_df.at[idx, '고려기프트 이미지'] = kogift_img
                            logging.info(f"Fixed Koreagift image path from {local_path} to {possible_path}")
                            break
            
            # Check and fix Naver image paths
            naver_img = result_df.at[idx, '네이버 이미지']
            if isinstance(naver_img, dict) and 'local_path' in naver_img:
                local_path = naver_img['local_path']
                if not os.path.exists(local_path):
                    # Try to find the file in the Naver directory by basename or URL hash
                    basename = os.path.basename(local_path)
                    url_hash = None
                    if 'url' in naver_img and naver_img['url']:
                        url_hash = hashlib.md5(naver_img['url'].encode()).hexdigest()[:10]
                    
                    found = False
                    for directory in [naver_dir, main_img_dir]:
                        if os.path.exists(directory):
                            for filename in os.listdir(directory):
                                file_path = os.path.join(directory, filename)
                                if os.path.isfile(file_path) and (
                                    basename == filename or 
                                    (url_hash and url_hash in filename)
                                ):
                                    naver_img['local_path'] = file_path
                                    result_df.at[idx, '네이버 이미지'] = naver_img
                                    logging.info(f"Fixed Naver image path from {local_path} to {file_path}")
                                    found = True
                                    break
                        if found:
                            break
        
        # Count final images
        haereum_count = result_df['본사 이미지'].apply(lambda x: isinstance(x, dict)).sum()
        kogift_count = result_df['고려기프트 이미지'].apply(lambda x: isinstance(x, dict)).sum()
        naver_count = result_df['네이버 이미지'].apply(lambda x: isinstance(x, dict)).sum()
        
        logging.info(f"통합: 이미지 매칭 완료 - 해오름: {haereum_count}개, 고려기프트: {kogift_count}개, 네이버: {naver_count}개")
        
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 이미지 통합 중 오류 발생: {e}", exc_info=True)
        return df

def improved_kogift_image_matching(df: pd.DataFrame) -> pd.DataFrame:
    """
    고려기프트 이미지 매칭 개선 함수
    
    이미지 URL이 누락되었거나 잘못된 경우 실제 상품 링크를 활용해 올바른 이미지 URL을 가져옵니다.
    
    Args:
        df: 현재 DataFrame
        
    Returns:
        updated DataFrame with improved Kogift image URLs
    """
    try:
        if '고려기프트 이미지' not in df.columns or '고려기프트 상품링크' not in df.columns:
            logging.warning("필요한 컬럼(고려기프트 이미지 또는 고려기프트 상품링크)이 없어 이미지 링크 수정 불가")
            return df
        
        update_count = 0
        result_df = df.copy()
        
        for idx, row in result_df.iterrows():
            # Check if already has a valid URL
            img_data = row.get('고려기프트 이미지')
            if not isinstance(img_data, dict):
                continue
                
            # Check if URL is missing or a placeholder
            url = img_data.get('url')
            if url and isinstance(url, str) and not url.startswith('http://placeholder.url/') and url.startswith(('http://', 'https://')):
                # 이미 유효한 URL이 있는 경우
                continue
                
            # Check if we have an original_url - 추가된 부분
            original_url = img_data.get('original_url')
            if original_url and isinstance(original_url, str) and original_url.startswith(('http://', 'https://')):
                # 원본 URL 정보가 있으면 사용
                img_data['url'] = original_url
                result_df.at[idx, '고려기프트 이미지'] = img_data
                update_count += 1
                logging.info(f"Row {idx}: Using original URL {original_url[:50]}... for Kogift image")
                continue
                
            # Check if we have an original_crawled_url
            original_crawled_url = img_data.get('original_crawled_url')
            if original_crawled_url and isinstance(original_crawled_url, str) and original_crawled_url.startswith(('http://', 'https://')):
                # original_crawled_url 정보가 있으면 사용
                img_data['url'] = original_crawled_url
                result_df.at[idx, '고려기프트 이미지'] = img_data
                update_count += 1
                logging.info(f"Row {idx}: Using original crawled URL {original_crawled_url[:50]}... for Kogift image")
                continue
                
            # 상품 링크가 있는지 확인
            product_link = row.get('고려기프트 상품링크')
            if not product_link or not isinstance(product_link, str) or not product_link.startswith(('http://', 'https://')):
                continue
                
            # Get product code from URL
            try:
                # 상품 코드 추출 (URL 패턴에 따라 조정 필요)
                product_code = None
                
                # URL에서 상품 코드 추출 시도 (여러 패턴 지원)
                if 'goods_view.php' in product_link:
                    # goods_view.php?goodsno=12345 패턴 처리
                    parts = product_link.split('goodsno=')
                    if len(parts) > 1:
                        product_code = parts[1].split('&')[0]
                elif '/goods/' in product_link:
                    # /goods/1234 패턴 처리
                    parts = product_link.split('/goods/')
                    if len(parts) > 1:
                        product_code = parts[1].split('/')[0].split('?')[0]
                elif 'goodsDetail' in product_link:
                    # goodsDetail?goodsNo=1234 패턴 처리
                    parts = product_link.split('goodsNo=')
                    if len(parts) > 1:
                        product_code = parts[1].split('&')[0]
                # 고려기프트 특화 패턴
                elif 'no=' in product_link:
                    # 고려기프트 URL 패턴 처리 (no=12345)
                    parts = product_link.split('no=')
                    if len(parts) > 1:
                        product_code = parts[1].split('&')[0]
                
                if not product_code:
                    logging.warning(f"Row {idx}: 상품 링크에서 코드를 추출할 수 없음: {product_link}")
                    # 상품 코드를 추출할 수 없는 경우 상품 링크 자체를 이미지 URL로 사용
                    img_data['url'] = product_link
                    img_data['original_url'] = product_link  # 추가: 원본 URL 저장
                    result_df.at[idx, '고려기프트 이미지'] = img_data
                    update_count += 1
                    logging.info(f"Row {idx}: 코드 추출 실패, 상품 링크를 이미지 URL로 사용 - {product_link}")
                    continue
                    
                # 상품 이미지 URL 생성
                if 'koreagift.com' in product_link.lower():
                    # 고려기프트 이미지 패턴
                    # 1. 기본 패턴: shop_{product_code}.jpg
                    # Ensure product_code is just the base number (e.g., 1707873892937710, not 1707873892937710_0)
                    image_url = f"https://koreagift.com/ez/upload/mall/shop_{product_code}_0.jpg"
                else:
                    # 일반적인 쇼핑몰 이미지 패턴
                    domain_parts = product_link.split('/')
                    if len(domain_parts) > 2:
                        base_domain = domain_parts[2]
                        image_url = f"https://{base_domain}/data/item/goods{product_code}/thumb-{product_code}_500x500.jpg"
                    else:
                        # 도메인을 추출할 수 없는 경우 상품 링크 자체를 이미지 URL로 사용
                        image_url = product_link
                        logging.warning(f"Row {idx}: 상품 링크 {product_link}에서 도메인을 추출할 수 없어 상품 링크 자체를 사용")
                
                # 기존 이미지 데이터 업데이트
                img_data['url'] = image_url
                img_data['original_url'] = image_url  # 추가: 원본 URL 저장
                img_data['original_crawled_url'] = image_url  # 추가: 크롤링된 URL 저장
                img_data['product_id'] = product_code  # 추가: 상품 코드 저장
                result_df.at[idx, '고려기프트 이미지'] = img_data
                update_count += 1
                logging.debug(f"Row {idx}: 고려기프트 URL 추가 - {image_url}")
                
            except Exception as e:
                logging.error(f"Row {idx}: 고려기프트 이미지 URL 생성 오류 - {str(e)}")
                # 오류가 발생한 경우에도 상품 링크 자체를 이미지 URL로 사용
                if product_link and isinstance(product_link, str) and product_link.startswith(('http://', 'https://')):
                    img_data['url'] = product_link
                    img_data['original_url'] = product_link
                    result_df.at[idx, '고려기프트 이미지'] = img_data
                    update_count += 1
                    logging.info(f"Row {idx}: 오류 발생, 상품 링크를 이미지 URL로 사용 - {product_link}")
                continue
                
        logging.info(f"improved_kogift_image_matching fixed {update_count} image links")
        return result_df
        
    except Exception as e:
        logging.error(f"고려기프트 이미지 링크 개선 중 오류: {str(e)}")
        return df

def integrate_and_filter_images(df: pd.DataFrame, config: configparser.ConfigParser, 
                            save_excel_output=False) -> pd.DataFrame:
    """
    Integrates and filters images from all sources, applying strict quality controls.
    
    This function performs the following steps:
    1. Integrate images from all sources with higher thresholds
    2. Improve Kogift image matching
    3. Apply strict filtering based on similarity scores
    4. Perform URL validation to reject invalid URLs (especially front/ URLs)
    5. Final quality control check
    
    Args:
        df: DataFrame with product data
        config: Configuration settings
        save_excel_output: Whether to save an Excel output file with images
        
    Returns:
        DataFrame with high-quality integrated and filtered images
    """
    logger.info("Integrating and filtering images with enhanced quality controls")
    
    # Step 1: Integrate images from all sources with higher thresholds
    df_with_images = integrate_images(df, config)
    logger.info(f"Image integration completed. DataFrame shape: {df_with_images.shape}")
    
    # Step 2: Improve Kogift image matching
    logger.info("Improving Kogift image matching with strict quality controls...")
    df_kogift_improved = improved_kogift_image_matching(df_with_images)
    logger.info(f"Kogift image matching improvement completed. DataFrame shape: {df_kogift_improved.shape}")
    
    # Step 3: Apply image filtering based on similarity and URL validity
    df_filtered = filter_images_by_similarity(df_kogift_improved, config)
    logger.info(f"Image filtering completed. DataFrame shape: {df_filtered.shape}")
    
    result_df = df_filtered.copy()
    
    # Ensure essential image columns exist in result_df before further processing and counting
    # This is crucial if any of the upstream functions returned the original df without these columns due to an error.
    required_image_columns = ['본사 이미지', '고려기프트 이미지', '네이버 이미지']
    for col_name in required_image_columns:
        if col_name not in result_df.columns:
            logger.warning(f"Column '{col_name}' missing in result_df before final validation and counting. Adding it with None values.")
            result_df[col_name] = None
            
    # Step 4: Final validation - ensure all URLs are valid and reject any problematic images
    # Now with less strict validation for Naver images
    logger.info("Performing final URL validation and quality check...")
    
    # Add an ImageFiltering section to config if it doesn't exist
    if 'ImageFiltering' not in config:
        config.add_section('ImageFiltering')
    
    # Get validation settings from config
    try:
        skip_naver_validation = config.getboolean('ImageFiltering', 'skip_naver_validation', fallback=True)
    except (configparser.NoSectionError, configparser.NoOptionError):
        skip_naver_validation = True  # Default to skipping Naver validation to avoid filtering
    
    for idx in range(len(result_df)):
        product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Index {idx}"
        
        # Check Naver image URLs - much more lenient validation
        if '네이버 이미지' in result_df.columns:
            naver_data = result_df.at[idx, '네이버 이미지']
            if isinstance(naver_data, dict):
                # Check if there's a valid local_path
                local_path = naver_data.get('local_path')
                has_valid_local_path = local_path and os.path.exists(str(local_path))
                
                # Check URL validity but be very lenient
                url = naver_data.get('url')
                has_valid_url = url and isinstance(url, str) and url.startswith(('http://', 'https://'))
                
                # Special case: Keep Naver images with valid local path even without valid URL
                if has_valid_local_path:
                    logger.info(f"Row {idx} (Product: '{product_name}'): Keeping Naver image with invalid URL but valid local path.")
                    
                    # Try to fix/add URL from other available data
                    original_path = naver_data.get('original_path')
                    image_url = naver_data.get('original_url')
                    product_id = naver_data.get('product_id')
                    
                    # Try to find a valid URL but don't clear data if none found
                    if not has_valid_url:
                        if image_url and isinstance(image_url, str) and image_url.startswith(('http://', 'https://')):
                            naver_data['url'] = image_url
                        elif original_path and isinstance(original_path, str) and original_path.startswith(('http://', 'https://')):
                            naver_data['url'] = original_path
                        elif product_id:
                            # Construct URL from product_id as a last resort
                            constructed_url = f"https://shopping-phinf.pstatic.net/main_{product_id}/{product_id}.jpg"
                            naver_data['url'] = constructed_url
                            logger.info(f"Row {idx}: Constructed Naver URL from product_id: {constructed_url}")
                    
                    # Update DataFrame with possibly updated naver_data
                    result_df.at[idx, '네이버 이미지'] = naver_data
                elif not has_valid_url and skip_naver_validation:
                    # If we're skipping validation and there's no valid URL, warn but don't remove
                    logger.warning(f"Row {idx}: No valid URL found for Naver image, but keeping data due to skip_naver_validation=True.")
                elif not has_valid_url and not has_valid_local_path:
                    # Only clear if both URL and local file are invalid AND we're not skipping validation
                    logger.warning(f"Row {idx}: No valid URL or local path found for Naver image. Clearing Naver image data.")
                    result_df.at[idx, '네이버 이미지'] = None
                else:
                    # For all other cases, keep the data as is
                    pass
    
    # Count valid images after all processing
    naver_count = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '네이버 이미지'], dict))
    kogift_count = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '고려기프트 이미지'], dict))
    haereum_count = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '본사 이미지'], dict))
    
    logger.info(f"Final image counts after validation: Haereum={haereum_count}, Kogift={kogift_count}, Naver={naver_count}")
    
    # Save Excel output if requested
    if save_excel_output:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_output = f"image_integration_result_{timestamp}.xlsx"
            
            # Note: Excel creation functionality would need to be implemented separately
            logger.info(f"Excel output requested but create_excel_with_images function not available. Would create: {excel_output}")
        except Exception as e:
            logger.error(f"Error preparing Excel output: {e}")
    
    return result_df

def filter_images_by_similarity(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    Filter images based on similarity scores and URL validity.
    When an image is filtered out, also clear related data columns.
    
    Args:
        df: DataFrame containing image data
        config: Configuration settings
        
    Returns:
        Filtered DataFrame
    """
    logger.info("Filtering images based on similarity scores...")
    
    # Get similarity thresholds from config
    try:
        # General threshold for all images
        similarity_threshold = config.getfloat('ImageFiltering', 'similarity_threshold', fallback=0.4)
        
        # Specific threshold for Naver images (more lenient)
        naver_similarity_threshold = config.getfloat('ImageFiltering', 'naver_similarity_threshold', fallback=0.3)
        
        # Specific threshold for Kogift images
        kogift_similarity_threshold = config.getfloat('ImageFiltering', 'kogift_similarity_threshold', fallback=0.25)
        
        # 해오름 기프트(본사) 이미지는 임계값 필터링을 하지 않음 (무조건 유지)
        
    except (configparser.NoSectionError, configparser.NoOptionError):
        similarity_threshold = 0.4
        naver_similarity_threshold = 0.3  # Updated from 0.1 to match log
        kogift_similarity_threshold = 0.25  # Updated from 0.4 to match log
    
    logger.info(f"Using similarity thresholds - General: {similarity_threshold}, Naver: {naver_similarity_threshold}, Kogift: {kogift_similarity_threshold}, Haereum: Always kept (no filtering)")
    
    # Create a copy of the DataFrame to avoid modifying the original
    filtered_df = df.copy()
    
    # Define related columns for each image source
    naver_related_columns = [
        '네이버 쇼핑 링크', '공급사 상품링크', '공급사명', 
        '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', 
        '기본수량(3)'
    ]
    
    kogift_related_columns = [
        '고려기프트 상품링크', '판매가(V포함)(2)', '판매단가(V포함)(2)',
        '가격차이(2)', '가격차이(2)(%)', '기본수량(2)'
    ]
    
    # Process each row
    for idx in range(len(filtered_df)):
        # Check each image source with specific thresholds
        for col_name in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
            if col_name in filtered_df.columns:
                image_data = filtered_df.at[idx, col_name]
                
                # Skip if no image data
                if not isinstance(image_data, dict):
                    continue
                
                # Get similarity score - check both 'similarity' and 'score' keys
                score = image_data.get('similarity', image_data.get('score', 0.0))
                
                # Convert to float if it's a string
                if isinstance(score, str):
                    try:
                        score = float(score)
                    except ValueError:
                        logger.warning(f"Invalid similarity score format for {col_name} at row {idx}: {score}")
                        score = 0.0
                
                # Determine the appropriate threshold for this image type
                # 해오름 기프트(본사) 이미지는 무조건 유지 (필터링하지 않음)
                if '본사' in col_name:
                    logger.debug(f"Keeping {col_name} for row {idx} - Haereum images are always kept (score: {score:.3f})")
                    continue  # 해오름 이미지는 필터링하지 않고 건너뜀
                elif '네이버' in col_name:
                    threshold = naver_similarity_threshold
                    related_columns = naver_related_columns
                elif '고려기프트' in col_name:
                    threshold = kogift_similarity_threshold
                    related_columns = kogift_related_columns
                else:
                    threshold = similarity_threshold
                    related_columns = []
                
                # Filter out low similarity scores (해오름 이미지 제외)
                if score < threshold:
                    logger.info(f"Filtering out {col_name} for row {idx} due to low similarity score: {score:.3f} < {threshold:.3f}")
                    
                    # Clear the image data
                    filtered_df.at[idx, col_name] = None
                    
                    # Clear related data columns
                    for related_col in related_columns:
                        if related_col in filtered_df.columns:
                            current_value = filtered_df.at[idx, related_col]
                            # Only clear if there's actual data (not already None or '-')
                            if current_value is not None and current_value != '-' and str(current_value).strip() != '':
                                logger.debug(f"Clearing related data in '{related_col}' for row {idx}: '{current_value}' -> '-'")
                                # Handle different column types properly
                                try:
                                    # For numeric columns, try to maintain type compatibility
                                    col_dtype = filtered_df[related_col].dtype
                                    if pd.api.types.is_numeric_dtype(col_dtype):
                                        # For numeric columns, use None instead of '-' to avoid dtype conflicts
                                        filtered_df.at[idx, related_col] = None
                                    else:
                                        # For object/string columns, use '-'
                                        filtered_df.at[idx, related_col] = '-'
                                except Exception as e:
                                    logger.debug(f"Error setting column type for {related_col}: {e}. Using string default.")
                                    filtered_df.at[idx, related_col] = '-'
                else:
                    logger.debug(f"Keeping {col_name} for row {idx} with similarity score: {score:.3f} >= {threshold:.3f}")
    
    return filtered_df

def print_system_status(config: configparser.ConfigParser = None):
    """시스템 상태를 콘솔에 출력합니다. (단순화된 버전)"""
    
    print("\n" + "="*60)
    print("🚀 해시 기반 이미지 매칭 시스템")
    print("="*60)
    print("✅ 해시 매칭만 사용하는 단순화된 시스템")
    print("📋 텍스트 유사도 계산 없음 - 파일명 해시값으로만 매칭")
    print("🔧 설정: 해시 기반 정확한 매칭")
    print("="*60)

def get_image_integration_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    이미지 통합 결과의 요약 정보를 반환합니다.
    
    Args:
        df: 이미지가 통합된 DataFrame
        
    Returns:
        통합 결과 요약 딕셔너리
    """
    try:
        summary = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_products': len(df),
            'image_counts': {
                'haereum': 0,
                'kogift': 0,
                'naver': 0
            },
            'success_rates': {
                'haereum': 0.0,
                'kogift': 0.0,
                'naver': 0.0
            }
        }
        
        # Count valid images for each source
        if '본사 이미지' in df.columns:
            summary['image_counts']['haereum'] = df['본사 이미지'].apply(lambda x: isinstance(x, dict)).sum()
            summary['success_rates']['haereum'] = summary['image_counts']['haereum'] / len(df) * 100
            
        if '고려기프트 이미지' in df.columns:
            summary['image_counts']['kogift'] = df['고려기프트 이미지'].apply(lambda x: isinstance(x, dict)).sum()
            summary['success_rates']['kogift'] = summary['image_counts']['kogift'] / len(df) * 100
            
        if '네이버 이미지' in df.columns:
            summary['image_counts']['naver'] = df['네이버 이미지'].apply(lambda x: isinstance(x, dict)).sum()
            summary['success_rates']['naver'] = summary['image_counts']['naver'] / len(df) * 100
        
        return summary
        
    except Exception as e:
        logging.error(f"이미지 통합 요약 생성 중 오류: {e}")
        return {'error': str(e)}

def print_image_integration_summary(df: pd.DataFrame):
    """이미지 통합 결과 요약을 콘솔에 출력합니다."""
    
    summary = get_image_integration_summary(df)
    
    print("\n" + "="*60)
    print("🖼️ 이미지 통합 결과 요약")
    print("="*60)
    
    if 'error' in summary:
        print(f"❌ 오류: {summary['error']}")
        return
    
    print(f"📅 처리 시간: {summary['timestamp']}")
    print(f"📦 총 상품 수: {summary['total_products']}개")
    
    print(f"\n📊 이미지 매칭 결과:")
    for source, count in summary['image_counts'].items():
        success_rate = summary['success_rates'][source]
        source_name = {
            'haereum': '해오름(본사)',
            'kogift': '고려기프트', 
            'naver': '네이버'
        }.get(source, source)
        
        if count > 0:
            print(f"   {source_name}: ✅ {count}개 ({success_rate:.1f}%)")
        else:
            print(f"   {source_name}: ❌ 0개 (0.0%)")
    
    total_images = sum(summary['image_counts'].values())
    print(f"\n🎯 전체 매칭된 이미지: {total_images}개")
    
    print("\n" + "="*60)