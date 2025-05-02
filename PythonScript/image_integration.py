import os
import logging
import pandas as pd
from pathlib import Path
import configparser
from typing import Dict, Any, Optional, List, Tuple, Set
from openpyxl import Workbook
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
import shutil
import sys
import re
import hashlib

# Import enhanced image matcher
try:
    from PythonScript.enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
    ENHANCED_MATCHER_AVAILABLE = True
    logging.info("Enhanced image matcher is available")
except ImportError:
    try:
        # Try direct import without PythonScript prefix
        from enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
        ENHANCED_MATCHER_AVAILABLE = True
        logging.info("Enhanced image matcher is available")
    except ImportError:
        ENHANCED_MATCHER_AVAILABLE = False
        logging.warning("Enhanced image matcher is not available, falling back to text-based matching")

def prepare_image_metadata(image_dir: Path, prefix: str) -> Dict[str, Dict]:
    """
    이미지 메타데이터를 준비합니다.
    
    Args:
        image_dir: 이미지 디렉토리 경로
        prefix: 이미지 파일명 접두사 (예: 'haereum_', 'kogift_', 'naver_')
        
    Returns:
        이미지 메타데이터 사전
    """
    image_info = {}
    
    if not image_dir.exists():
        logging.warning(f"이미지 디렉토리를 찾을 수 없습니다: {image_dir}")
        return image_info
    
    # JPG와 PNG 파일 모두 찾기 (nobg 파일 제외)
    images = sorted([f for f in image_dir.glob("*.jpg") if "_nobg" not in f.name]) + \
             sorted([f for f in image_dir.glob("*.png") if "_nobg" not in f.name])
    
    logging.info(f"{len(images)}개의 {prefix} 이미지 발견")
    
    # 샘플 이미지 몇 개 로깅
    if images:
        sample_images = images[:3] if len(images) > 3 else images
        logging.debug(f"샘플 {prefix} 이미지: {[img.name for img in sample_images]}")
    
    for img_path in images:
        # 이미지 파일명에서 상품명 추출
        img_name = img_path.stem
        original_img_name = img_name
        
        # 접두사 제거
        if img_name.startswith(prefix):
            img_name = img_name[len(prefix):]  # 접두사 제거
        
        # 해시 부분 제거 - 파일명 끝에 있는 해시값 (언더스코어 + 알파벳/숫자 10자리 이내)
        if '_' in img_name:
            parts = img_name.split('_')
            # 마지막 부분이 해시처럼 생겼는지 확인
            if len(parts[-1]) <= 10 and parts[-1].isalnum():
                img_name = '_'.join(parts[:-1])
        
        # 이미지 이름을 토큰화하여 저장 (공백과 밑줄로 분리)
        clean_name = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in img_name.lower()])
        tokens = [t.lower() for t in clean_name.replace('_', ' ').split() if len(t) > 1]
        
        image_info[str(img_path)] = {
            'original_name': original_img_name,
            'clean_name': img_name,
            'tokens': tokens,
            'path': img_path
        }
    
    return image_info

def calculate_similarity(product_tokens: List[str], image_tokens: List[str]) -> float:
    """
    상품명과 이미지 이름 간의 유사도를 계산합니다.
    
    Args:
        product_tokens: 상품명에서 추출한 토큰 목록
        image_tokens: 이미지 이름에서 추출한 토큰 목록
        
    Returns:
        유사도 점수 (0.0 ~ 1.0)
    """
    # 토큰 기반 유사도 계산
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

def tokenize_product_name(product_name: str) -> List[str]:
    """
    상품명을 토큰화합니다.
    
    Args:
        product_name: 상품명
        
    Returns:
        토큰 목록
    """
    # 특수문자를 공백으로 변환하고, 소문자로 변환
    clean_product = ''.join([c if c.isalnum() or c.isspace() else ' ' for c in product_name.lower()])
    # 2자 이상의 토큰만 추출
    return [t.lower() for t in clean_product.split() if len(t) > 1]

def find_best_image_matches(product_names: List[str], 
                           haereum_images: Dict[str, Dict], 
                           kogift_images: Dict[str, Dict], 
                           naver_images: Dict[str, Dict],
                           similarity_threshold: float = 0.1,
                           config: Optional[configparser.ConfigParser] = None) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    각 상품에 대해 세 가지 이미지 소스에서 가장 적합한 이미지를 찾습니다.
    세 이미지가 서로 일관성을 유지하도록 합니다.
    
    Args:
        product_names: 상품명 목록
        haereum_images: 해오름 이미지 정보
        kogift_images: 고려기프트 이미지 정보
        naver_images: 네이버 이미지 정보
        similarity_threshold: 최소 유사도 점수
        config: 설정 파일 객체
        
    Returns:
        각 상품별 (해오름 이미지 경로, 고려기프트 이미지 경로, 네이버 이미지 경로) 튜플 목록
    """
    results = []
    
    # 이미지 매칭 시 이미 사용한 이미지 추적
    used_haereum = set()
    used_kogift = set()
    used_naver = set()
    
    # 향상된 이미지 매처 초기화 (가능한 경우)
    enhanced_matcher = None
    use_enhanced_matcher = False
    
    # 향상된 이미지 매칭 사용 여부
    if config:
        use_enhanced_matcher = config.getboolean('ImageMatching', 'use_enhanced_matcher', fallback=True)
        
    if ENHANCED_MATCHER_AVAILABLE and use_enhanced_matcher:
        try:
            # Explicitly specify use_gpu=True
            enhanced_matcher = EnhancedImageMatcher(config, use_gpu=True)
            # Verify the matcher has been initialized correctly
            if enhanced_matcher.model is None:
                logging.warning("Enhanced image matcher model was not initialized properly")
                enhanced_matcher = None
            else:
                logging.info(f"향상된 이미지 매칭을 사용합니다 (GPU: {enhanced_matcher.use_gpu})")
        except Exception as e:
            logging.error(f"향상된 이미지 매처 초기화 실패: {e}")
            enhanced_matcher = None
    
    # 개선된 이미지 매칭 알고리즘 - 파일명에서 상품 식별자 추출
    def extract_product_id_from_filename(filename):
        # 파일명에서 ID 부분 추출 (예: haereum_목쿠션_메모리폼_목베개_여행용목베개_bda60bd016.jpg에서 bda60bd016 추출)
        match = re.search(r'_([a-f0-9]{10})(?:\.jpg|\.png|_nobg\.png)?$', filename)
        if match:
            return match.group(1)
        return None
    
    # 파일명 기반 매칭을 위한 이미지 ID 맵 생성
    haereum_id_map = {}
    kogift_id_map = {}
    naver_id_map = {}
    
    for img_path, info in haereum_images.items():
        product_id = extract_product_id_from_filename(img_path)
        if product_id:
            haereum_id_map[product_id] = img_path
    
    for img_path, info in kogift_images.items():
        product_id = extract_product_id_from_filename(img_path)
        if product_id:
            kogift_id_map[product_id] = img_path
    
    for img_path, info in naver_images.items():
        product_id = extract_product_id_from_filename(img_path)
        if product_id:
            naver_id_map[product_id] = img_path
    
    # 모든 이미지 소스를 한번에 처리하여 일관된 매칭 보장
    for product_name in product_names:
        product_tokens = tokenize_product_name(product_name)
        
        # 각 소스별 최적 매치 찾기
        haereum_best = find_best_match_for_product(product_tokens, haereum_images, used_haereum, similarity_threshold)
        if haereum_best:
            used_haereum.add(haereum_best[0]) # Add path to used set
            
            # 해오름 이미지에서 제품 ID 추출
            haereum_path, haereum_score = haereum_best
            haereum_id = extract_product_id_from_filename(haereum_path)
            
            # ID 매칭을 통한 고려기프트, 네이버 이미지 찾기
            kogift_best = None
            naver_best = None
            
            # ID 기반 정확한 매칭 시도
            if haereum_id:
                # 고려기프트 매칭
                if haereum_id in kogift_id_map and kogift_id_map[haereum_id] not in used_kogift:
                    kogift_path = kogift_id_map[haereum_id]
                    kogift_best = (kogift_path, 1.0)  # 정확한 매칭으로 점수를 1.0으로 설정
                    used_kogift.add(kogift_path)
                    
                # 네이버 매칭
                if haereum_id in naver_id_map and naver_id_map[haereum_id] not in used_naver:
                    naver_path = naver_id_map[haereum_id]
                    naver_best = (naver_path, 1.0)  # 정확한 매칭으로 점수를 1.0으로 설정
                    used_naver.add(naver_path)
        else:
            # 해오름 이미지가 없는 경우 다음 단계로 진행
            pass
            
        # ID 기반 매칭이 실패한 경우, 기존 방식으로 매칭 시도    
        # 이미 매칭된 해오름 이미지가 있다면, 그 이미지를 기준으로 다른 소스 매칭 시도
        if haereum_best:
            # 고려기프트 매칭이 없는 경우에만 기존 방식 시도
            if not kogift_best:
                # 해오름 이미지 이름에서 토큰 추출
                haereum_path, haereum_score = haereum_best
                haereum_tokens = tokenize_product_name(haereum_images[haereum_path]['clean_name'])
                
                # 향상된 이미지 매처 사용 시 이미지 기반 매칭
                if enhanced_matcher:
                    kogift_best = find_best_match_with_enhanced_matcher(
                        str(haereum_images[haereum_path]['path']),
                        kogift_images,
                        used_kogift,
                        enhanced_matcher
                    )
                else:
                    # 텍스트 기반 매칭 (use haereum tokens as base)
                    kogift_best = find_best_match_for_product(haereum_tokens, kogift_images, used_kogift, 0.05) # Lower threshold for secondary match
            
            # 네이버 매칭이 없는 경우에만 기존 방식 시도
            if not naver_best:
                # 해오름 이미지 이름에서 토큰 추출
                haereum_path, haereum_score = haereum_best
                haereum_tokens = tokenize_product_name(haereum_images[haereum_path]['clean_name'])
                
                # 향상된 이미지 매처 사용 시 이미지 기반 매칭
                if enhanced_matcher:
                    naver_best = find_best_match_with_enhanced_matcher(
                        str(haereum_images[haereum_path]['path']),
                        naver_images,
                        used_naver,
                        enhanced_matcher
                    )
                else:
                    # 텍스트 기반 매칭 (use haereum tokens as base)
                    naver_best = find_best_match_for_product(haereum_tokens, naver_images, used_naver, 0.05) # Lower threshold for secondary match
        else:
            # 원래 상품명으로 매칭 시도 (해오름 이미지가 없는 경우)
            kogift_best = find_best_match_for_product(product_tokens, kogift_images, used_kogift, similarity_threshold)
            naver_best = find_best_match_for_product(product_tokens, naver_images, used_naver, similarity_threshold)
        
        if kogift_best:
            # Ensure path is added correctly (first element of tuple)
            if isinstance(kogift_best, tuple) and len(kogift_best) > 0:
                used_kogift.add(kogift_best[0])
        if naver_best:
            # Ensure path is added correctly (first element of tuple)
            if isinstance(naver_best, tuple) and len(naver_best) > 0:
                used_naver.add(naver_best[0])
            
        # 결과 추가
        results.append((haereum_best, kogift_best, naver_best))
        
        # 로깅
        haereum_name = haereum_images[haereum_best[0]]['clean_name'] if haereum_best else "없음"
        kogift_name = kogift_images[kogift_best[0]]['clean_name'] if kogift_best else "없음"
        naver_name = naver_images[naver_best[0]]['clean_name'] if naver_best else "없음"
        haereum_score_log = f"{haereum_best[1]:.3f}" if haereum_best else "N/A"
        kogift_score_log = f"{kogift_best[1]:.3f}" if kogift_best else "N/A"
        naver_score_log = f"{naver_best[1]:.3f}" if naver_best else "N/A"
        
        # Log the final selected matches for this product
        logging.info(f"Final Match Set for '{product_name}': Haereum='{haereum_name}' ({haereum_score_log}), Kogift='{kogift_name}' ({kogift_score_log}), Naver='{naver_name}' ({naver_score_log})")
    
    return results

def find_best_match_for_product(product_tokens: List[str], 
                               image_info: Dict[str, Dict], 
                               used_images: Set[str] = None,
                               similarity_threshold: float = 0.1) -> Optional[Tuple[str, float]]:
    """
    상품에 대해 가장 유사한 이미지를 찾습니다.
    
    Args:
        product_tokens: 상품명 토큰
        image_info: 이미지 정보 사전
        used_images: 이미 사용된 이미지 경로 집합
        similarity_threshold: 최소 유사도 점수
        
    Returns:
        가장 유사한 이미지 경로 또는 None
        (가장 유사한 이미지 경로, 유사도 점수) 튜플 또는 None
    """
    best_match = None
    best_score = 0
    
    if used_images is None:
        used_images = set()
    
    # 상품 토큰 정보 로깅
    if product_tokens:
        logging.debug(f"매칭 시도 - 제품 토큰: {product_tokens}")
    
    # 이미지 수와 사용된 이미지 수 로깅
    available_images = len(image_info) - len(used_images)
    logging.debug(f"사용 가능한 이미지: {available_images}개 (전체: {len(image_info)}개, 사용됨: {len(used_images)}개)")
    
    # 매칭 결과를 추적하기 위한 리스트
    match_scores = [] # Stores (path, score, clean_name) tuples
    
    for img_path, info in image_info.items():
        # 이미 사용된 이미지는 건너뜀
        if img_path in used_images:
            continue
            
        similarity = calculate_similarity(product_tokens, info['tokens'])
        
        # 모든 매칭 점수 추적
        if similarity > 0:
            # Store path, score, and clean name for logging
            match_scores.append((img_path, similarity, info['clean_name']))
        
        if similarity > best_score and similarity >= similarity_threshold:
            best_score = similarity
            best_match = img_path
    
    # 상위 3개 매칭 점수 로깅
    if match_scores:
        # Sort by score (descending)
        top_matches = sorted(match_scores, key=lambda x: x[1], reverse=True)
        # Log top candidates (show clean_name and score)
        top_log = [(name, f"{score:.3f}") for path, score, name in top_matches[:3]]
        logging.debug(f"  Top 3 candidates (text-based): {top_log}")
    
    # 최종 매칭 결과 로깅
    if best_match:
        best_match_name = image_info[best_match]['clean_name']
        logging.info(f"  --> Best Match Selected (text-based): {best_match_name} (Score: {best_score:.3f})")
        return best_match, best_score
    else:
        logging.debug(f"매치 없음 (임계값: {similarity_threshold})")
        return None
    
    return best_match

def find_best_match_with_enhanced_matcher(
    source_img_path: str, 
    target_images: Dict[str, Dict], 
    used_images: Set[str] = None,
    enhanced_matcher: Any = None
) -> Optional[str]:
    """
    향상된 이미지 매처를 이용하여 가장 유사한 이미지를 찾습니다.
    
    Args:
        source_img_path: 소스 이미지 경로
        target_images: 대상 이미지 정보 사전
        used_images: 이미 사용된 이미지 경로 집합
        enhanced_matcher: 향상된 이미지 매처 객체
        
    Returns:
        가장 유사한 이미지 경로 또는 None
        (가장 유사한 이미지 경로, 유사도 점수) 튜플 또는 None
    """
    if not enhanced_matcher:
        logging.warning("향상된 이미지 매처가 없습니다. 기본 텍스트 매칭으로 대체합니다.")
        return None
        
    if used_images is None:
        used_images = set()
        
    best_match = None
    best_score = 0
    # FIXED: Lowered thresholds to ensure more image matches
    high_confidence_threshold = 0.40  # 높은 신뢰도 임계값 (0.60에서 0.40으로 낮춤)
    min_confidence_threshold = 0.15   # 최소 신뢰도 임계값 (0.25에서 0.15로 낮춤)
    
    gpu_info = "GPU 활성화" if getattr(enhanced_matcher, "use_gpu", False) else "CPU 모드"
    logging.info(f"향상된 이미지 매칭 시도 - 이미지: {os.path.basename(source_img_path)} ({gpu_info})")
    logging.debug(f"사용 가능한 대상 이미지: {len(target_images) - len(used_images)}개")
    
    # 매칭 결과를 추적하기 위한 리스트
    match_scores = []
    
    # FIXED: Add secondary verification for better matching
    secondary_matches = []  # Store multiple high-scoring matches for verification
    
    for img_path, info in target_images.items():
        # 이미 사용된 이미지는 건너뜀
        if img_path in used_images:
            continue
            
        # 이미지 유사도 계산
        try:
            similarity = enhanced_matcher.calculate_similarity(source_img_path, str(info['path']))
            
            # 모든 매칭 점수 추적
            if similarity > 0:
                match_scores.append((img_path, similarity, info['clean_name']))
                
                # FIXED: Store high-scoring candidates for verification
                if similarity >= min_confidence_threshold:
                    secondary_matches.append((img_path, similarity, info['clean_name']))
                
            if similarity > best_score:
                best_score = similarity
                best_match = img_path
        except Exception as e:
            logging.warning(f"이미지 유사도 계산 중 오류 발생: {e}")
    
    # 상위 3개 매칭 점수 로깅
    if match_scores:
        # Sort by score (descending)
        top_matches = sorted(match_scores, key=lambda x: x[1], reverse=True)
        # Log top candidates (show clean_name and score)
        top_log = [(name, f"{score:.3f}") for path, score, name in top_matches[:3]]
        logging.debug(f"  Top 3 candidates: {top_log}")
    
    # FIXED: Add additional verification for close matches
    # If we have multiple high-scoring matches, verify they're consistent
    if len(secondary_matches) >= 2:
        secondary_matches.sort(key=lambda x: x[1], reverse=True)
        # Check if second-best match has a similar score (within 80% of best)
        if len(secondary_matches) >= 2:
            best_score = secondary_matches[0][1]
            second_best_score = secondary_matches[1][1]
            score_ratio = second_best_score / best_score if best_score > 0 else 0
            
            # If scores are too close, it might indicate ambiguity
            if score_ratio > 0.9 and best_score < high_confidence_threshold:
                logging.warning(f"Ambiguous image matching: Best={secondary_matches[0][2]} ({best_score:.3f}), Second={secondary_matches[1][2]} ({second_best_score:.3f})")
                
                # Check if names are similar - if they are completely different, be more cautious
                from Levenshtein import ratio as text_similarity
                name_sim = text_similarity(secondary_matches[0][2], secondary_matches[1][2])
                
                if name_sim < 0.4:  # Names are very different
                    logging.warning(f"Product names are very different between top matches (sim={name_sim:.2f})")
                    
                    # Require a higher threshold for ambiguous matches with different names
                    if best_score < high_confidence_threshold * 1.2:
                        logging.warning(f"Rejecting ambiguous match due to insufficient confidence")
                        return None
    
    # 최종 매칭 결과 로깅
    if best_match:
        best_match_name = target_images[best_match]['clean_name']
        logging.info(f"  --> Best Match Selected: {best_match_name} (Score: {best_score:.3f})")
        
        # FIXED: More lenient thresholds to avoid rejecting matches
        if best_score < min_confidence_threshold:
            logging.warning(f"매칭 점수가 최소 임계값({min_confidence_threshold})보다 낮아 매칭을 거부합니다: {best_match_name} (점수: {best_score:.3f})")
            return None
        elif best_score < high_confidence_threshold:
            logging.warning(f"낮은 신뢰도로 매칭되었습니다: {best_match_name} (점수: {best_score:.3f})")
            
            # FIXED: More lenient checks for low confidence matches
            try:
                from Levenshtein import ratio as text_similarity
                source_name = os.path.basename(source_img_path).split('_', 1)[1] if '_' in os.path.basename(source_img_path) else ''
                target_name = best_match_name
                
                # Clean up names for comparison (remove file extensions and common prefixes)
                source_name = re.sub(r'\.(jpg|png|jpeg)$', '', source_name)
                source_name = re.sub(r'_[a-f0-9]{8,}$', '', source_name)  # Remove hash suffixes
                
                # Calculate text similarity between product names
                name_sim = text_similarity(source_name, target_name)
                logging.debug(f"Name similarity check: '{source_name}' vs '{target_name}' = {name_sim:.3f}")
                
                # FIXED: Made threshold much more lenient to return more matches
                if best_score < high_confidence_threshold * 0.5 and name_sim < 0.2:
                    logging.warning(f"이미지 유사도({best_score:.3f})와 이름 유사도({name_sim:.3f})가 모두 매우 낮아 매칭을 거부합니다")
                    return None
            except Exception as e:
                logging.warning(f"이름 유사도 확인 중 오류 발생: {e}")
        
        # Return the match with score
        return best_match, best_score
    else:
        logging.debug("이미지 매치 없음")
        return None

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
        haereum_match, kogift_match, naver_match = match_set
        product_tokens = set(tokenize_product_name(product_name))
        
        # 매칭 품질 기록
        match_quality = {
            'haereum': {'score': 0, 'match': haereum_match},
            'kogift': {'score': 0, 'match': kogift_match},
            'naver': {'score': 0, 'match': naver_match}
        }
        
        # 해오름 매칭 검증
        if haereum_match:
            haereum_path, haereum_score = haereum_match
            haereum_filename = os.path.basename(haereum_path)
            
            # 파일명에서 ID 추출
            haereum_id = None
            id_match = id_pattern.search(haereum_filename)
            if id_match:
                haereum_id = id_match.group(1)
            
            # 파일명에서 토큰 추출
            haereum_tokens = set(tokenize_product_name(haereum_images[haereum_path]['clean_name']))
            
            # 토큰 중복 확인
            common_tokens = product_tokens & haereum_tokens
            token_ratio = len(common_tokens) / max(len(product_tokens), 1)
            
            # 품질 점수 계산
            match_quality['haereum']['score'] = haereum_score * (1 + token_ratio)
            match_quality['haereum']['id'] = haereum_id
        
        # 고려기프트 매칭 검증
        if kogift_match:
            kogift_path, kogift_score = kogift_match
            kogift_filename = os.path.basename(kogift_path)
            
            # 파일명에서 ID 추출
            kogift_id = None
            id_match = id_pattern.search(kogift_filename)
            if id_match:
                kogift_id = id_match.group(1)
            
            # 해오름 ID와 비교
            if haereum_match and match_quality['haereum']['id'] and match_quality['haereum']['id'] == kogift_id:
                # ID가 일치하면 점수 증가
                match_quality['kogift']['score'] = max(kogift_score, 0.8) * 1.5
            else:
                # 토큰 비교
                kogift_tokens = set(tokenize_product_name(kogift_images[kogift_path]['clean_name']))
                common_tokens = product_tokens & kogift_tokens
                token_ratio = len(common_tokens) / max(len(product_tokens), 1)
                match_quality['kogift']['score'] = kogift_score * (1 + token_ratio)
        
        # 네이버 매칭 검증
        if naver_match:
            naver_path, naver_score = naver_match
            naver_filename = os.path.basename(naver_path)
            
            # 파일명에서 ID 추출
            naver_id = None
            id_match = id_pattern.search(naver_filename)
            if id_match:
                naver_id = id_match.group(1)
            
            # 해오름 ID와 비교
            if haereum_match and match_quality['haereum']['id'] and match_quality['haereum']['id'] == naver_id:
                # ID가 일치하면 점수 증가
                match_quality['naver']['score'] = max(naver_score, 0.8) * 1.5
            else:
                # 토큰 비교
                naver_tokens = set(tokenize_product_name(naver_images[naver_path]['clean_name']))
                common_tokens = product_tokens & naver_tokens
                token_ratio = len(common_tokens) / max(len(product_tokens), 1)
                match_quality['naver']['score'] = naver_score * (1 + token_ratio)
        
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
    
    Args:
        df: 처리할 DataFrame
        config: 설정 파일
        
    Returns:
        이미지가 통합된 DataFrame
    """
    try:
        logging.info("통합: 이미지 통합 프로세스 시작...")
        result_df = df.copy()
        
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
        haereum_images = prepare_image_metadata(haereum_dir, 'haereum_')
        kogift_images = prepare_image_metadata(kogift_dir, 'kogift_')
        naver_images = prepare_image_metadata(naver_dir, 'naver_')
        
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
        
        # Retrieve similarity threshold from config.
        # 1) Primary key: Matching.image_threshold  (defined in config.ini)
        # 2) Secondary key: ImageMatching.minimum_match_confidence
        # 3) Fallback: 0.1  (legacy default)
        try:
            similarity_threshold = config.getfloat('Matching', 'image_threshold',
                                                  fallback=config.getfloat('ImageMatching', 'minimum_match_confidence',
                                                                           fallback=0.1))
        except (configparser.Error, ValueError) as e:
            logging.warning(f"이미지 매칭 임계값 설정 오류: {e}. 기본값 0.1을 사용합니다.")
            similarity_threshold = 0.1

        logging.info(f"이미지 매칭 유사도 임계값: {similarity_threshold}")
        
        # 최적 매치 찾기 (일관성 보장)
        best_matches = find_best_image_matches(
            product_names,
            haereum_images,
            kogift_images,
            naver_images,
            similarity_threshold=similarity_threshold,
            config=config
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

        for idx, (haereum_match, kogift_match, naver_match) in enumerate(verified_matches):
            # Check index bounds
            if idx >= len(result_df):
                logging.warning(f"Index {idx} out of bounds for result_df (length {len(result_df)}). Skipping image assignment.")
                continue
            row_data = result_df.iloc[idx] # Get the current row's data to access scraped URLs

            # --- Process Haoreum Image --- 
            target_col_haereum = '본사 이미지'
            existing_haereum_data = row_data.get(target_col_haereum)
            haereum_data_preserved = False

            # Check if data already exists and has a valid URL (placed by format_product_data_for_output)
            if isinstance(existing_haereum_data, dict) and \
               existing_haereum_data.get('url') and \
               isinstance(existing_haereum_data.get('url'), str) and \
               existing_haereum_data['url'].startswith(('http://', 'https://')):
                logging.debug(f"Row {idx}: Preserving existing Haoreum image data (with URL) placed by format_product_data_for_output.")
                haereum_data_preserved = True
                # Ensure local_path is also present if possible (it might have been added by format_product_data)
                # Check if path exists in dict AND on disk
                if 'local_path' not in existing_haereum_data or not os.path.exists(existing_haereum_data.get('local_path', '')):
                    if haereum_match: # Try to get path from current match results if needed
                         haereum_path, _ = haereum_match
                         local_path = haereum_images.get(haereum_path, {}).get('path')
                         if local_path and os.path.exists(str(local_path)):
                              existing_haereum_data['local_path'] = str(local_path)
                              existing_haereum_data['original_path'] = str(local_path)
                              result_df.at[idx, target_col_haereum] = existing_haereum_data # Update the dict in DF
                              logging.debug(f"Row {idx}: Added missing local_path to preserved Haoreum data.")
                # No further assignment needed for Haoreum if data was preserved

            # If data wasn't preserved (no valid URL found beforehand), use the match result from find_best_image_matches
            if not haereum_data_preserved:
                logging.debug(f"Row {idx}: No valid pre-existing Haoreum data found. Using match results.")
                if haereum_match:
                    haereum_path, haereum_score = haereum_match
                    img_path_obj = haereum_images.get(haereum_path, {}).get('path')
                    if not img_path_obj:
                         logging.warning(f"Row {idx}: Haoreum match found ({haereum_path}) but no corresponding image path in metadata.")
                         result_df.at[idx, target_col_haereum] = '-'
                         continue # Skip to next source if path object missing
                         
                    img_path = str(img_path_obj)
                    
                    # Try to get URL from existing data again (e.g., if format_product only added path)
                    web_url = None
                    if isinstance(existing_haereum_data, dict):
                        potential_url = existing_haereum_data.get('url')
                        if isinstance(potential_url, str) and potential_url.startswith(('http://', 'https://')):
                             web_url = potential_url
                    if not web_url: # Default to empty if no URL found
                         web_url = "" 

                    image_data = {
                        'local_path': img_path,
                        'source': 'haereum',
                        'url': web_url, # Use found or empty URL
                        'original_path': img_path,
                        'score': haereum_score,
                        'product_name': product_names[idx]
                    }
                    result_df.at[idx, target_col_haereum] = image_data
                else:
                     # Handle case where no match was found *and* no prior data existed
                     if target_col_haereum in result_df.columns:
                         # Check again to avoid overwriting dicts that maybe just lack URL
                         current_val = result_df.loc[idx, target_col_haereum]
                         if not isinstance(current_val, dict):
                              result_df.loc[idx, target_col_haereum] = '-'
                     else:
                         # This case should not happen due to earlier column addition
                         logging.warning(f"Target column '{target_col_haereum}' unexpectedly missing at index {idx}.")

            # --- Process Kogift Image (Keep existing logic, but ensure URL preservation) ---
            target_col_kogift = '고려기프트 이미지'
            if kogift_match:
                kogift_path, kogift_score = kogift_match
                img_path_obj = kogift_images.get(kogift_path, {}).get('path')
                if not img_path_obj:
                    logging.warning(f"Row {idx}: Kogift match found ({kogift_path}) but no corresponding image path in metadata.")
                    # Check existing data before setting to '-'
                    existing_kogift_data = row_data.get(target_col_kogift)
                    if not isinstance(existing_kogift_data, dict):
                         result_df.at[idx, target_col_kogift] = '-'
                    continue # Skip Kogift if path is missing
                    
                img_path = str(img_path_obj)
                
                # Prioritize URL from existing data if available
                existing_kogift_data = row_data.get(target_col_kogift)
                web_url = None
                if isinstance(existing_kogift_data, dict):
                    potential_url = existing_kogift_data.get('url')
                    if isinstance(potential_url, str) and potential_url.startswith(('http://', 'https://')):
                        web_url = potential_url
                        logging.debug(f"Row {idx}: Preserving existing Kogift URL: {web_url[:60]}...")
                
                if not web_url: # If URL wasn't preserved from earlier step
                     logging.warning(f"Row {idx}: Could not find existing valid web URL in input data for Kogift ({target_col_kogift}). URL will be empty.")
                     web_url = ""

                image_data = {
                    'local_path': img_path,
                    'source': 'kogift',
                    'url': web_url, # Use preserved or empty URL
                    'original_path': str(img_path),
                    'score': kogift_score,
                    'product_name': product_names[idx] # 상품명 추가
                }
                result_df.at[idx, target_col_kogift] = image_data # Use .at for scalar assignment
            else:
                 if target_col_kogift in result_df.columns:
                     # If no match found by integrate_images, keep existing data or set to '-'
                     existing_data = result_df.loc[idx, target_col_kogift]
                     if not isinstance(existing_data, dict): # Don't overwrite potentially correct data from previous steps
                          result_df.loc[idx, target_col_kogift] = '-'
                 else:
                     # This case should theoretically not happen anymore
                     logging.warning(f"Target column '{target_col_kogift}' unexpectedly not found at index {idx} during else block.")

            # 네이버 이미지 column
            target_col_naver = '네이버 이미지'
            link_col_naver = '네이버 쇼핑 링크' # Define the fallback link column
            
            # Check if there's actual Naver product information before trying to match images
            has_naver_product_info = False
            
            # Check key columns that indicate Naver product exists
            if link_col_naver in row_data and row_data[link_col_naver]:
                if isinstance(row_data[link_col_naver], str) and row_data[link_col_naver].strip() not in ['', '-', 'None', None]:
                    has_naver_product_info = True
            
            # If no Naver product link found, check for other potential Naver product indicators
            if not has_naver_product_info:
                naver_price_col = '판매단가(V포함)(3)'
                if naver_price_col in row_data and pd.notna(row_data[naver_price_col]) and row_data[naver_price_col] not in [0, '-', '', None]:
                    has_naver_product_info = True
            
            logging.debug(f"Row {idx}: Naver product info exists: {has_naver_product_info}")
            
            # Only try to match Naver images if we have Naver product information
            if has_naver_product_info and naver_match:
                naver_path, naver_score = naver_match
                img_path_obj = naver_images.get(naver_path, {}).get('path')
                if not img_path_obj:
                    logging.warning(f"Row {idx}: Naver match found ({naver_path}) but no corresponding image path in metadata.")
                    # Check existing data before setting to '-'
                    existing_naver_data = row_data.get(target_col_naver)
                    if not isinstance(existing_naver_data, dict):
                         result_df.at[idx, target_col_naver] = '-'
                    continue # Skip Naver if path is missing
                    
                img_path = str(img_path_obj)

                # Prioritize URL from existing data if available
                existing_naver_data = row_data.get(target_col_naver)
                web_url = None
                if isinstance(existing_naver_data, dict):
                    potential_url = existing_naver_data.get('url')
                    if isinstance(potential_url, str) and potential_url.startswith(('http://', 'https://')):
                        web_url = potential_url
                        logging.debug(f"Row {idx}: Preserving existing Naver URL: {web_url[:60]}...")
                
                if not web_url: # If URL wasn't preserved
                     logging.warning(f"Row {idx}: Could not find existing valid web URL in input data for Naver ({target_col_naver}). URL will be empty.")
                     web_url = ""

                image_data = {
                    'local_path': img_path,
                    'source': 'naver',
                    'url': web_url, # Use preserved or empty URL
                    'original_path': str(img_path),
                    'score': naver_score,
                    'product_name': product_names[idx] # 상품명 추가
                }
                result_df.at[idx, target_col_naver] = image_data # Use .at for scalar assignment
            else:
                 if target_col_naver in result_df.columns:
                     # If no Naver product info or no match found, ensure Naver image is not included
                     result_df.loc[idx, target_col_naver] = '-'
                 else:
                     # This case should theoretically not happen anymore
                     logging.warning(f"Target column '{target_col_naver}' unexpectedly not found at index {idx} during else block.")

        # 매칭 결과 요약 - Use new target column names
        # These checks are now safer as columns are guaranteed to exist
        haereum_count = result_df['본사 이미지'].apply(lambda x: isinstance(x, dict)).sum()
        kogift_count = result_df['고려기프트 이미지'].apply(lambda x: isinstance(x, dict)).sum()
        naver_count = result_df['네이버 이미지'].apply(lambda x: isinstance(x, dict)).sum()
        
        logging.info(f"통합: 이미지 매칭 완료 - 해오름: {haereum_count}개, 고려기프트: {kogift_count}개, 네이버: {naver_count}개")
        
        return result_df
    
    except Exception as e:
        logging.error(f"통합: 이미지 통합 중 오류 발생: {e}", exc_info=True)
        return df

def filter_images_by_similarity(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    이미지 유사도에 따라 고려기프트 및 네이버 이미지를 필터링합니다.
    임계값보다 낮은 유사도를 가진 이미지는 표시하지 않습니다.
    해오름(본사) 이미지는 유사도에 관계없이 항상 유지합니다.
    
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
            # FIXED: 이미지 표시 임계값을 더 낮게 설정 - 0.05에서 0.01로 대폭 인하하여 거의 모든 매칭 유지
            similarity_threshold = config.getfloat('Matching', 'image_display_threshold', fallback=0.01)
            # 필터링이 사실상 비활성화되어 있음을 표시
            logging.info(f"통합: 이미지 표시 임계값: {similarity_threshold} (매우 낮은 임계값으로 대부분의 매칭을 유지)")
        except ValueError as e:
            logging.warning(f"임계값 읽기 오류: {e}. 매우 낮은 기본값 0.01을 사용합니다.")
            similarity_threshold = 0.01
        
        # -------------------------------------------------------------
        # 이미지 유사도 필터링
        # -------------------------------------------------------------
        # 필터링 기준:
        #   • '이미지_유사도' 컬럼이 존재하고 수치형 값 < similarity_threshold
        #   • 해당 행에 대해 고려기프트·네이버 이미지를 '-' 로 치환 (본사 이미지는 유지)
        #   • 유사도 정보가 없거나 파싱 실패 → 그대로 둠 (보수적)
        #   • 이미지 데이터가 딕셔너리 형태인 경우, 'score' 키의 값 < similarity_threshold
        #   • 해당 행에 대해 고려기프트·네이버 이미지를 '-' 로 치환 (본사 이미지는 유지)

        # 필터링 임계값을 매우 낮게 설정하여 대부분의 매칭을 유지
        # Remove the redundant filtering block based on the old '이미지_유사도' column.
        # The filtering is now done based on the 'score' key in the image dictionary below.
        logging.debug("Skipping obsolete filtering based on '이미지_유사도' column.")

        # 너무 낮은 점수에만 필터링 적용 (대부분 유지)
        filtered_count = 0
        rows_affected = set() # Track unique rows affected
        # Define Haereum column name
        haoreum_col_name = '본사 이미지'

        for idx, row in result_df.iterrows():
            # Iterate only through Kogift and Naver columns for filtering
            for col_name in ['고려기프트 이미지', '네이버 이미지']:
                if col_name not in result_df.columns:
                    continue
                
                # Explicitly skip Haereum column if it somehow gets included here (redundant safety check)
                if col_name == haoreum_col_name:
                    logging.debug(f"Skipping Haereum column '{haoreum_col_name}' in similarity filtering loop at index {idx}")
                    continue

                img_data = row[col_name]
                
                # Check if it's a dictionary and contains a score
                if isinstance(img_data, dict) and 'score' in img_data:
                    try:
                        score = float(img_data['score'])
                        # 임계값이 매우 낮으므로, 정말 형편없는 매칭만 제거
                        if score < similarity_threshold:
                            result_df.at[idx, col_name] = '-' # Filter out very low-score image
                            filtered_count += 1
                            rows_affected.add(idx)
                    except (ValueError, TypeError):
                        # If score is not a valid number, keep the image data (conservative approach)
                        logging.warning(f"Invalid score value '{img_data.get('score')}' found in {col_name} at index {idx}. Skipping filtering for this cell.")
                # If not a dict with score, or already filtered ('-'), leave it as is

        # Log count based on unique rows affected
        final_filtered_count = len(rows_affected)
        logging.info(f"통합: 이미지 점수 기준으로 고려/네이버 이미지를 필터링 ({filtered_count}개 셀 수정됨, {final_filtered_count}개 행 영향 받음, 임계값 < {similarity_threshold})")
        logging.info(f"통합: 해오름 이미지는 점수와 관계없이 유지됩니다.")
        
        # 이미지 존재 여부 확인 로깅
        haereum_count = 0
        kogift_count = 0
        naver_count = 0
        
        for i in range(len(result_df)):
            # 이미지 열별 존재 카운트
            if '본사 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['본사 이미지']) and result_df.iloc[i]['본사 이미지'] not in [None, '-', '']:
                    haereum_count += 1
                    
            if '고려기프트 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['고려기프트 이미지']) and result_df.iloc[i]['고려기프트 이미지'] not in [None, '-', '']:
                    kogift_count += 1
                    
            if '네이버 이미지' in result_df.columns:
                if pd.notna(result_df.iloc[i]['네이버 이미지']) and result_df.iloc[i]['네이버 이미지'] not in [None, '-', '']:
                    naver_count += 1
        
        logging.info(f"통합: 이미지 현황 (필터링 후) - 해오름: {haereum_count}개, 고려기프트: {kogift_count}개, 네이버: {naver_count}개")
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
        
        # 기본 헤더 및 데이터 컬럼 정의 (Use new column names)
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
            ws.row_dimensions[row].height = 200  # 데이터 행 높이 (doubled from 100)
        
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
                column_widths[col_letter] = 30  # Image columns width doubled from 15
        
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
            # Use new image column names
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
                            # 이미지 크기 조정 (최대 200x200, doubled from 100x100)
                            img.width = 200
                            img.height = 200
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
        
        # 새로운 통합 이미지 함수로 모든 이미지 소스를 한번에 처리
        result_df = integrate_images(df, config)
        
        # --- Add Filtering Step Back In --- 
        # Apply similarity filtering AFTER integration
        # This step now correctly skips the Haoreum column
        logging.info("Applying similarity filtering to integrated images (Kogift/Naver only)...")
        result_df = filter_images_by_similarity(result_df, config)
        # ---------------------------------
        
        # 필요한 경우에만 결과를 별도의 엑셀 파일로 저장 (이미지 포함)
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
        level=logging.DEBUG, # Change level to DEBUG for testing
        format='%(asctime)s - %(levelname)s - %(name)s - [%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # 설정 파일 로드
    config = configparser.ConfigParser()
    # Assuming config.ini is in the parent directory of PythonScript
    config_path = Path(__file__).resolve().parent.parent / 'config.ini'
    if not config_path.exists():
        print(f"Error: config.ini not found at {config_path}")
        sys.exit(1)
    config.read(config_path, encoding='utf-8')
    
    # Test data setup needs careful handling of image paths
    # Ensure the image paths used for testing actually exist or simulate them.
    # For this example, we'll assume the paths are placeholders.
    
    # Get image dirs from config
    main_img_dir = Path(config.get('Paths', 'image_main_dir', fallback='C:\\\\RPA\\\\Image\\\\Main'))
    haereum_dir = main_img_dir / 'Haereum'
    kogift_dir = main_img_dir / 'Kogift'
    naver_dir = main_img_dir / 'Naver'

    # Create dummy image files for testing if they don't exist
    # (This part might need adjustment based on your actual test environment)
    dummy_haereum_img = haereum_dir / "haereum_test_product_1_dummy.jpg"
    dummy_kogift_img = kogift_dir / "kogift_test_product_2_dummy.jpg"
    dummy_naver_img = naver_dir / "naver_test_product_3_dummy.jpg"
    
    for d in [haereum_dir, kogift_dir, naver_dir]:
        d.mkdir(parents=True, exist_ok=True)
        
    for img_file in [dummy_haereum_img, dummy_kogift_img, dummy_naver_img]:
        if not img_file.exists():
            try:
                img_file.touch() # Create empty file
                print(f"Created dummy image file: {img_file}")
            except Exception as e:
                print(f"Could not create dummy file {img_file}: {e}")

    test_df = pd.DataFrame({
        '번호': [1, 2, 3, 4],
        '상품명': ['테스트 상품 1', 'Test Product 2', '해오름 테스트', '저 유사도 상품'],
        # Use source URL columns from scraping (example names)
        '해오름이미지URL': ['http://example.com/hae1.jpg', None, 'https://www.jclgift.com/upload/product/simg3/DDAC0001000s.jpg', 'http://example.com/hae4.jpg'],
        '고려기프트 URL': [None, 'https://koreagift.com/ez/upload/mall/shop_1707873892937710_0.jpg', None, 'http://example.com/ko4.jpg'],
        '네이버이미지 URL': ['https://shop-phinf.pstatic.net/20240101_1/image.jpg', None, None, 'http://example.com/na4.jpg'],
        '이미지_유사도': [0.6, 0.8, 0.9, 0.2], # This column should now be ignored by filter_images_by_similarity
        # Add other necessary columns from FINAL_COLUMN_ORDER for the test
        '구분': ['A', 'A', 'P', 'A'], '담당자': ['Test']*4, '업체명': ['Test']*4, '업체코드': ['123']*4, 'Code': ['T01', 'T02', 'T03', 'T04'], '중분류카테고리': ['Test']*4,
        '기본수량(1)': [100]*4, '판매단가(V포함)': [1000]*4, '본사상품링크': ['http://example.com/1']*4,
        '기본수량(2)': [100]*4, '판매가(V포함)(2)': [1100]*4, '가격차이(2)': [100]*4, '가격차이(2)(%)': [10]*4, '고려기프트 상품링크': ['http://example.com/2']*4,
        '기본수량(3)': [100]*4, '판매단가(V포함)(3)': [900]*4, '가격차이(3)': [-100]*4, '가격차이(3)(%)': [-10]*4, '공급사명': ['Test']*4, '네이버 쇼핑 링크': ['http://example.com/3']*4, '공급사 상품링크': ['http://example.com/supplier']*4
    })
    
    # --- Simulate adding image dicts (as would be done by integrate_images) ---
    # This is crucial for testing filter_images_by_similarity correctly
    # We manually add the 'score' key here based on example values
    test_df['해오름(이미지링크)'] = [
        {'local_path': str(dummy_haereum_img), 'url': 'http://example.com/hae1.jpg', 'source': 'haereum', 'score': 0.85},
        None,
        {'local_path': str(dummy_haereum_img), 'url': 'https://www.jclgift.com/upload/product/simg3/DDAC0001000s.jpg', 'source': 'haereum', 'score': 0.95},
         {'local_path': str(dummy_haereum_img), 'url': 'http://example.com/hae4.jpg', 'source': 'haereum', 'score': 0.90} # High score, should not be filtered
    ]
    test_df['고려기프트(이미지링크)'] = [
        None,
        {'local_path': str(dummy_kogift_img), 'url': 'https://koreagift.com/ez/upload/mall/shop_1707873892937710_0.jpg', 'source': 'kogift', 'score': 0.75},
        None,
        {'local_path': str(dummy_kogift_img), 'url': 'http://example.com/ko4.jpg', 'source': 'kogift', 'score': 0.25} # Low score, should be filtered
    ]
    test_df['네이버쇼핑(이미지링크)'] = [
        {'local_path': str(dummy_naver_img), 'url': 'https://shop-phinf.pstatic.net/20240101_1/image.jpg', 'source': 'naver', 'score': 0.65},
        None,
        None,
        {'local_path': str(dummy_naver_img), 'url': 'http://example.com/na4.jpg', 'source': 'naver', 'score': 0.15} # Low score, should be filtered
    ]
    
    # --- Run only the filtering part for isolated testing ---
    logging.info("--- Testing filter_images_by_similarity ---")
    filtered_df = filter_images_by_similarity(test_df.copy(), config) # Use copy
    
    logging.info(f"Test filter results - DataFrame shape: {filtered_df.shape}")
    logging.info(f"해오름(이미지링크) after filter: {filtered_df['해오름(이미지링크)'].tolist()}")
    logging.info(f"고려기프트(이미지링크) after filter: {filtered_df['고려기프트(이미지링크)'].tolist()}")
    logging.info(f"네이버쇼핑(이미지링크) after filter: {filtered_df['네이버쇼핑(이미지링크)'].tolist()}")
    
    # --- Run the full integrate_and_filter process ---
    logging.info("--- Testing integrate_and_filter_images ---")
    # Use a fresh copy for the full test
    full_result_df = integrate_and_filter_images(test_df.copy(), config, save_excel_output=True) 
    
    # 결과 출력 (using the new final column names)
    logging.info(f"Full process result - DataFrame shape: {full_result_df.shape}")
    logging.info(f"해오름(이미지링크) final data: {full_result_df['해오름(이미지링크)'].tolist()}")
    logging.info(f"고려기프트(이미지링크) final data: {full_result_df['고려기프트(이미지링크)'].tolist()}")
    logging.info(f"네이버쇼핑(이미지링크) final data: {full_result_df['네이버쇼핑(이미지링크)'].tolist()}") 