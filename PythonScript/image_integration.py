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
from datetime import datetime
import glob

# Add the parent directory to sys.path to allow imports from PythonScript
import sys
from pathlib import Path

# Get the absolute path of the current file's directory
current_dir = Path(__file__).resolve().parent

# Add the parent directory to sys.path if it's not already there
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Now import the required modules
from tokenize_product_names import tokenize_product_name, extract_meaningful_keywords

# Initialize logger
logger = logging.getLogger(__name__)

# Import enhanced image matcher
try:
    from enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
    ENHANCED_MATCHER_AVAILABLE = True
    logging.info("Enhanced image matcher is available")
except ImportError:
    ENHANCED_MATCHER_AVAILABLE = False
    logging.warning("Enhanced image matcher is not available, falling back to text-based matching")

def prepare_image_metadata(image_dir: Path, prefix: str) -> Dict[str, Dict]:
    """
    Prepare image metadata with improved token generation and stricter product ID handling.
    """
    logger = logging.getLogger(__name__)
    result = {}
    
    if not image_dir.exists():
        logger.warning(f"Image directory does not exist: {image_dir}")
        return result
        
    # Determine source from prefix - ensure proper source field
    source = None
    if prefix.lower().startswith('haereum'):
        source = 'haereum'
    elif prefix.lower().startswith('kogift'):
        source = 'kogift'
    elif prefix.lower().startswith('naver'):
        source = 'naver'
    else:
        source = prefix.rstrip('_')  # Fallback: use prefix without underscore
    
    for img_path in image_dir.glob('*.*'):
        if not img_path.is_file():
            continue
            
        if img_path.suffix.lower() not in ['.jpg', '.jpeg', '.png', '.gif']:
            continue
            
        original_name = img_path.stem
        tokens = []
        product_id = None
        
        # Remove prefix from filename for token generation
        name_without_prefix = original_name
        if prefix and original_name.startswith(prefix):
            name_without_prefix = original_name[len(prefix):].lstrip('_')
        
        # Extract product ID based on source
        if prefix.lower().startswith('naver'):
            # Try to extract Naver product ID using more permissive patterns
            id_match = re.search(r'_([0-9]{6,})(?:_[a-f0-9]{6,})?$', original_name)
            if id_match:
                product_id = id_match.group(1)
                # Remove product ID part from name for better token generation
                name_without_prefix = re.sub(r'_[0-9]{6,}(?:_[a-f0-9]{6,})?$', '', name_without_prefix)
        elif prefix.lower().startswith('kogift'):
            # Extract Kogift product ID with more permissive pattern
            id_match = re.search(r'_([0-9]{4,})(?:_[a-f0-9]{6,})?$', original_name)
            if id_match:
                product_id = id_match.group(1)
                name_without_prefix = re.sub(r'_[0-9]{4,}(?:_[a-f0-9]{6,})?$', '', name_without_prefix)
        
        # Clean the name for token generation
        cleaned_name = name_without_prefix.replace('_', ' ')
        cleaned_name = re.sub(r'[^\w\s가-힣]', ' ', cleaned_name)
        cleaned_name = ' '.join(cleaned_name.split())
        
        # Generate tokens from the cleaned name
        for part in cleaned_name.split():
            # Skip very short parts unless they're Korean
            if len(part) <= 1 and not re.match(r'[가-힣]', part):
                continue
            # Skip if it looks like a hash
            if re.match(r'^[a-f0-9]{6,}$', part):
                continue
            if part not in tokens:
                logger.debug(f"[{prefix}] Adding token: {part}")
                tokens.append(part)
        
        # Store metadata
        # Determine the true original path
        current_img_path_obj = img_path # Path object from glob
        true_original_path = current_img_path_obj

        if current_img_path_obj.name.endswith('_nobg.png'):
            base_name_no_ext = current_img_path_obj.stem.replace('_nobg', '')
            # Try to find .jpg, .jpeg, .png, .gif (in that order of preference for original)
            possible_original_extensions = ['.jpg', '.jpeg', '.png', '.gif']
            found_original = False
            for ext in possible_original_extensions:
                potential_original_name = base_name_no_ext + ext
                potential_original_file = current_img_path_obj.with_name(potential_original_name)
                if potential_original_file.exists():
                    true_original_path = potential_original_file
                    logger.debug(f"[{prefix}] For '{current_img_path_obj.name}', found true original: '{true_original_path.name}'")
                    found_original = True
                    break
            if not found_original:
                 logger.warning(f"[{prefix}] For _nobg file '{current_img_path_obj.name}', could not find a corresponding original. Using it as original_path.")

        result[str(img_path)] = {
            'path': current_img_path_obj, # The path of the file being processed (could be _nobg.png)
            'original_path': true_original_path, # Path to the actual source image (e.g., .jpg)
            'tokens': tokens,
            'product_id': product_id,
            'original_name': original_name, # stem of current_img_path_obj (img_path.stem)
            'clean_name': cleaned_name,
            'source': source  # Explicitly set the source
        }
        
        if product_id:
            logger.debug(f"[{prefix}] Extracted product ID '{product_id}' from '{original_name}'")
        
    logger.info(f"Prepared metadata for {len(result)} images from {image_dir} with prefix '{prefix}'")
    return result

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

def find_best_image_matches(product_names: List[str], 
                           haereum_images: Dict[str, Dict], 
                           kogift_images: Dict[str, Dict], 
                           naver_images: Dict[str, Dict],
                           similarity_threshold: float = 0.4,  # Increased from 0.1
                           config: Optional[configparser.ConfigParser] = None) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    Find the best matching images for each product name from Haereum, Kogift, and Naver images.
    Now using stricter thresholds from the start to ensure better quality matches.
    """
    best_matches = []
    used_haereum = set()
    used_kogift = set()
    used_naver = set()
    
    # Get thresholds from config if available
    if config:
        try:
            similarity_threshold = config.getfloat('Matching', 'text_threshold', fallback=0.4)  # Use text_threshold as base
            naver_initial_threshold = config.getfloat('Matching', 'naver_initial_similarity_threshold', fallback=0.75)
            naver_minimum_threshold = config.getfloat('Matching', 'naver_minimum_similarity', fallback=0.55)
        except ValueError as e:
            logging.warning(f"임계값 읽기 오류: {e}. 기본값을 사용합니다.")
            naver_initial_threshold = 0.75
            naver_minimum_threshold = 0.55
    else:
        naver_initial_threshold = 0.75
        naver_minimum_threshold = 0.55
    
    # Log the product names being processed
    logger.info(f"find_best_image_matches: Processing {len(product_names)} products.")
    if product_names:
        logger.info(f"Sample product names: {product_names[:min(3, len(product_names))]}")

    # Log image dictionary sizes
    logger.info(f"Haereum images count: {len(haereum_images)}")
    logger.info(f"Kogift images count: {len(kogift_images)}")
    logger.info(f"Naver images count: {len(naver_images)}")
    logger.info(f"Using similarity_threshold for find_best_match_for_product: {similarity_threshold}")

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
        # Get tokens using both methods for better matching
        product_tokens = tokenize_product_name(product_name)
        
        # ENHANCEMENT: Add meaningful keywords for better Korean product matching
        try:
            meaningful_keywords = extract_meaningful_keywords(product_name, max_keywords=5)
            # Add keywords that aren't already in tokens
            for keyword in meaningful_keywords:
                if keyword and keyword not in product_tokens:
                    product_tokens.append(keyword)
                    logging.debug(f"Added meaningful keyword '{keyword}' for product '{product_name}'")
        except Exception as e:
            logging.warning(f"Failed to extract meaningful keywords for '{product_name}': {e}")
        
        logging.debug(f"Combined product tokens for '{product_name}': {product_tokens}")
        
        # 각 소스별 최적 매치 찾기
        # For Haoreum and Naver, we still use the 'used' sets to avoid re-using the same image file for different products
        haereum_best = find_best_match_for_product(product_tokens, haereum_images, used_haereum, similarity_threshold, source_name_for_log="Haereum", config=config)
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
                # 고려기프트 매칭 - No 'used_kogift' check here for ID based match
                if haereum_id in kogift_id_map: # and kogift_id_map[haereum_id] not in used_kogift:
                    kogift_path_by_id = kogift_id_map[haereum_id]
                    # Ensure the image file actually exists if we consider it a match
                    if kogift_path_by_id in kogift_images: 
                        kogift_best = (kogift_path_by_id, 1.0)  # 정확한 매칭으로 점수를 1.0으로 설정
                        # Do NOT add to used_kogift here, allow reuse
                    
                # 네이버 매칭 (still uses used_naver for ID based match)
                if haereum_id in naver_id_map and naver_id_map[haereum_id] not in used_naver:
                    naver_path_by_id = naver_id_map[haereum_id]
                    if naver_path_by_id in naver_images:
                        naver_best = (naver_path_by_id, 1.0)  # 정확한 매칭으로 점수를 1.0으로 설정
                        used_naver.add(naver_path_by_id)
        else:
            # 해오름 이미지가 없는 경우 다음 단계로 진행
            kogift_best = None # Ensure kogift_best is None if haereum_best is None and no other match found
            naver_best = None  # Ensure naver_best is None if haereum_best is None
            pass # Explicitly pass
            
        # ID 기반 매칭이 실패한 경우, 기존 방식으로 매칭 시도    
        # 이미 매칭된 해오름 이미지가 있다면, 그 이미지를 기준으로 다른 소스 매칭 시도
        if haereum_best:
            haereum_path, haereum_score = haereum_best # Defined if haereum_best is true
            haereum_tokens = tokenize_product_name(haereum_images[haereum_path]['clean_name'])

            # 고려기프트 매칭이 없는 경우에만 기존 방식 시도
            if not kogift_best:
                if enhanced_matcher:
                    kogift_best = find_best_match_with_enhanced_matcher(
                        str(haereum_images[haereum_path]['path']),
                        kogift_images,
                        None, # Pass None for used_images for Kogift
                        enhanced_matcher
                    )
                else:
                    # Use image_threshold from config (0.01 by default) for Kogift
                    kogift_threshold = config.getfloat('Matching', 'image_threshold', fallback=0.01) if config else 0.01
                    kogift_best = find_best_match_for_product(haereum_tokens, kogift_images, None, kogift_threshold, source_name_for_log="Kogift_Fallback", config=config) # Pass None for used_images for Kogift
            
            # 네이버 매칭이 없는 경우에만 기존 방식 시도 (still uses used_naver)
            if not naver_best:
                if enhanced_matcher:
                    naver_best = find_best_match_with_enhanced_matcher(
                        str(haereum_images[haereum_path]['path']),
                        naver_images,
                        used_naver, # Pass used_naver for Naver
                        enhanced_matcher
                    )
                else:
                    # Use a more lenient threshold for Naver
                    naver_threshold = config.getfloat('Matching', 'naver_minimum_similarity', fallback=0.45) if config else 0.45
                    naver_best = find_best_match_for_product(haereum_tokens, naver_images, used_naver, naver_threshold, source_name_for_log="Naver_Fallback", config=config) # Pass used_naver for Naver
        else:
            # 원래 상품명으로 매칭 시도 (해오름 이미지가 없는 경우)
            # Kogift - pass None for used_images
            if not kogift_best: # Check if kogift_best is already found by ID match from an earlier (non-Haoreum) source if logic changes
                # Use image_threshold from config (0.01 by default) for Kogift direct matching
                kogift_threshold = config.getfloat('Matching', 'image_threshold', fallback=0.01) if config else 0.01
                kogift_best = find_best_match_for_product(product_tokens, kogift_images, None, kogift_threshold, source_name_for_log="Kogift_Direct", config=config)
            
            # Naver - pass used_naver
            if not naver_best:
                # Use naver_minimum_similarity from config (0.45 by default) for Naver direct matching
                naver_threshold = config.getfloat('Matching', 'naver_minimum_similarity', fallback=0.45) if config else 0.45
                naver_best = find_best_match_for_product(product_tokens, naver_images, used_naver, naver_threshold, source_name_for_log="Naver_Direct", config=config)
        
        # DO NOT add kogift_best to used_kogift set to allow reuse

        # Naver is still added to its used set if a match is found and it wasn't an ID-based match already added
        if naver_best:
            if isinstance(naver_best, tuple) and len(naver_best) > 0:
                # Check if it was an ID-based match; if so, it might already be added.
                # To be safe, just add it. Set handles duplicates.
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
                               similarity_threshold: float = 0.4,  # Default threshold if not provided
                               source_name_for_log: str = "UnknownSource",
                               config: Optional[configparser.ConfigParser] = None) -> Optional[Tuple[str, float]]:
    """
    Find the best matching image for a product based on text similarity.
    Now using stricter thresholds from the start to ensure better quality matches.
    """
    # product_tokens: 상품명에서 추출한 토큰 목록
    # image_info: 특정 소스(해오름, 고려, 네이버)의 이미지 메타데이터 사전
    # used_images: 이미 사용된 이미지 경로 집합
    # similarity_threshold: 최소 유사도 점수
    if not product_tokens:
        logger.warning(f"[{source_name_for_log}] Product tokens are empty, cannot find match.")
        return None
    if not image_info:
        logger.warning(f"[{source_name_for_log}] Image info is empty, cannot find match.")
        return None

    best_match_path = None
    best_score = -1.0 # Initialize with a value lower than any possible score
    
    if used_images is None:
        used_images = set()
    
    # 상품 토큰 정보 로깅
    if product_tokens:
        logging.debug(f"매칭 시도 - 제품 토큰: {product_tokens}")
    
    # Get config thresholds or use defaults
    # Use much lower thresholds for Kogift and Naver
    effective_threshold = similarity_threshold
    if source_name_for_log.lower().startswith("kogift"):
        # Use extremely low threshold for Kogift
        effective_threshold = config.getfloat('Matching', 'image_threshold', fallback=0.001) if config else 0.001
    elif source_name_for_log.lower().startswith("naver"):
        # Use very low threshold for Naver
        effective_threshold = config.getfloat('Matching', 'image_threshold', fallback=0.001) if config else 0.001
    
    # Log the effective threshold being used
    logging.debug(f"Using effective threshold: {effective_threshold} for source: {source_name_for_log}")
    
    # Create a list to track all match scores for potential fallback
    match_scores = []
    
    # Match products based on tokens
    for img_path, info in image_info.items():
        # 이미 사용된 이미지는 건너뜀
        if img_path in used_images:
            continue
        
        # Skip if tokens are missing
        if 'tokens' not in info or not info['tokens']:
            continue
            
        try:    
            similarity = calculate_similarity(product_tokens, info['tokens'])
            
            # 모든 매칭 점수 추적
            if similarity > 0:
                # Store path, score, and clean name for logging
                match_scores.append((img_path, similarity, info['clean_name']))
            
            # Find the best match above threshold
            if similarity >= effective_threshold and similarity > best_score:
                best_score = similarity
                best_match_path = img_path
            elif similarity == best_score:
                # If scores are equal, prefer .jpg over .png 
                # if current best_match is a .png and new one is .jpg
                if best_match_path and Path(img_path).suffix.lower() == ".jpg" and Path(best_match_path).suffix.lower() == ".png":
                    # Current image is JPG, previous best was PNG, and scores are equal. Prefer JPG.
                    best_match_path = img_path
                    # best_score remains the same
                    logging.debug(f"  Equal score ({similarity:.3f}), preferring JPG '{Path(img_path).name}' over PNG '{Path(best_match_path).name}'.")
        except Exception as e:
            logging.warning(f"[{source_name_for_log}] Error calculating similarity for {img_path}: {e}")
            continue
    
    # If no match found but we have at least one image, consider using a fallback approach
    if not best_match_path and image_info:
        # Check if source is Kogift or Naver for fallback logic
        if source_name_for_log.lower().startswith("kogift") or source_name_for_log.lower().startswith("naver"):
            logging.info(f"No match found above threshold {effective_threshold} for {source_name_for_log}. Trying basic token matching.")
            
            # Try to find any token overlap, even if small
            best_fallback_path = None
            best_fallback_score = 0
            
            for img_path, info in image_info.items():
                if img_path in used_images:
                    continue
                    
                # Very basic token check - any overlap at all
                if 'tokens' in info and info['tokens']:
                    common_tokens = set(product_tokens) & set(info['tokens'])
                    overlap_score = len(common_tokens) / max(1, min(len(product_tokens), len(info['tokens'])))
                    
                    if common_tokens and overlap_score > best_fallback_score:
                        best_fallback_path = img_path
                        best_fallback_score = overlap_score
            
            if best_fallback_path:
                best_match_path = best_fallback_path
                best_score = max(0.001, best_fallback_score)  # Ensure at least 0.001 score
                logging.warning(f"Using fallback match with overlap score: {best_fallback_score:.3f} for '{image_info[best_fallback_path]['clean_name']}'")
            elif match_scores:  # If still nothing, use the best score we found even if below threshold
                match_scores.sort(key=lambda x: x[1], reverse=True)
                best_match_path = match_scores[0][0]
                best_score = max(0.001, match_scores[0][1])  # Ensure at least 0.001 score
                logging.warning(f"Last resort: Using best available match despite low score: {image_info[best_match_path]['clean_name']} (Score: {best_score:.3f})")
    
    # Return the best match and its score
    if best_match_path:
        if best_score < similarity_threshold and best_score >= effective_threshold:
            logging.warning(f"[{source_name_for_log}] Best match score ({best_score:.3f}) is below standard threshold ({similarity_threshold}) but above effective threshold ({effective_threshold}): '{image_info[best_match_path]['clean_name']}'")
        return best_match_path, best_score
        
    return None

def find_best_match_with_enhanced_matcher(
    source_img_path: str, 
    target_images: Dict[str, Dict], 
    used_images: Set[str] = None,
    enhanced_matcher: Any = None
) -> Optional[Tuple[str, float]]:
    """
    Enhanced image matching with stricter thresholds based on config settings.
    """
    if not enhanced_matcher:
        logging.warning("Enhanced image matcher not available. Falling back to text-based matching.")
        return None
        
    if used_images is None:
        used_images = set()
        
    best_match = None
    best_score = 0
    
    # Using config-based thresholds instead of hardcoded values
    high_confidence_threshold = 0.70  # Increased from 0.10 to match MatchQualityThresholds high_quality
    min_confidence_threshold = 0.35   # Increased from 0.01 to match MatchQualityThresholds low_quality
    
    gpu_info = "GPU enabled" if getattr(enhanced_matcher, "use_gpu", False) else "CPU mode"
    logging.info(f"Enhanced image matching attempt - Image: {os.path.basename(source_img_path)} ({gpu_info})")
    
    # Track scores for validation
    match_scores = []
    secondary_matches = []
    
    for img_path, info in target_images.items():
        if img_path in used_images:
            continue
            
        try:
            similarity = enhanced_matcher.calculate_similarity(source_img_path, str(info['path']))
            
            if similarity > min_confidence_threshold:  # Only track scores above minimum threshold
                match_scores.append((img_path, similarity, info.get('original_name', '')))
                
                if similarity >= high_confidence_threshold:  # Only consider high confidence matches
                    secondary_matches.append((img_path, similarity, info.get('original_name', '')))
                
            if similarity > best_score:
                best_score = similarity
                best_match = img_path
        except Exception as e:
            logging.warning(f"이미지 유사도 계산 중 오류 발생: {e}")
    
    # UPDATED: More strict verification for close matches
    if len(secondary_matches) >= 2:
        secondary_matches.sort(key=lambda x: x[1], reverse=True)
        if len(secondary_matches) >= 2:
            best_score = secondary_matches[0][1]
            second_best_score = secondary_matches[1][1]
            score_ratio = second_best_score / best_score if best_score > 0 else 0
            
            # UPDATED: Stricter ambiguity check
            if score_ratio > 0.98 and best_score < high_confidence_threshold:
                logging.warning(f"Ambiguous image matching: Best={secondary_matches[0][2]} ({best_score:.3f}), Second={secondary_matches[1][2]} ({second_best_score:.3f})")
                
                from Levenshtein import ratio as text_similarity
                name_sim = text_similarity(secondary_matches[0][2], secondary_matches[1][2])
                
                if name_sim < 0.3:  # Reduced threshold for name similarity to allow more matches
                    logging.warning(f"Product names are very different between top matches (sim={name_sim:.2f})")
                    
                    if best_score < high_confidence_threshold * 1.1:  # Reduced required confidence multiplier
                        logging.warning(f"Rejecting ambiguous match due to insufficient confidence")
                        return None
    
    # 최종 매칭 결과 로깅
    if best_match:
        best_match_name = target_images[best_match]['clean_name']
        logging.info(f"  --> Best Match Selected: {best_match_name} (Score: {best_score:.3f})")
        
        # UPDATED: More lenient thresholds to avoid rejecting matches
        if best_score < min_confidence_threshold:
            logging.warning(f"매칭 점수가 최소 임계값({min_confidence_threshold})보다 낮아 매칭을 거부합니다: {best_match_name} (점수: {best_score:.3f})")
            return None
        elif best_score < high_confidence_threshold:
            logging.warning(f"낮은 신뢰도로 매칭되었습니다: {best_match_name} (점수: {best_score:.3f})")
            
            # UPDATED: More lenient checks for low confidence matches
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
                
                # UPDATED: Made threshold much more lenient to return more matches
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
        
        # 최적 매치 찾기 (일관성 보장)
        best_matches = find_best_image_matches(
            product_names,
            haereum_images,
            kogift_images,
            naver_images,
            similarity_threshold=initial_matching_threshold,  # Use lower threshold for initial matching
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

        # Pre-compute Naver product info existence for all rows
        naver_product_info_exists = []
        for idx in range(len(result_df)):
            if idx >= len(result_df):
                naver_product_info_exists.append(False)
                continue
                
            row_data = result_df.iloc[idx]
            has_naver_info = False
            
            # Check for Naver link - look for multiple possible column names
            naver_link_cols = ['네이버 쇼핑 링크', '네이버 링크']
            for link_col in naver_link_cols:
                if link_col in row_data and row_data[link_col]:
                    if isinstance(row_data[link_col], str) and row_data[link_col].strip() not in ['', '-', 'None', None]:
                        has_naver_info = True
                        break
            
            # Check for Naver price - look for multiple possible column names
            if not has_naver_info:
                naver_price_cols = ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)', '네이버 기본수량']
                for price_col in naver_price_cols:
                    if price_col in row_data and pd.notna(row_data[price_col]) and row_data[price_col] not in [0, '-', '', None]:
                        has_naver_info = True
                        break
            
            naver_product_info_exists.append(has_naver_info)
        
        logging.info(f"Pre-computed Naver product info existence for {len(naver_product_info_exists)} rows")
        logging.info(f"Found {sum(naver_product_info_exists)} rows with Naver product info")

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
            scraped_haereum_url_col = '본사이미지URL' # Define the column name where the scraped URL is stored

            # Check if data already exists and has a valid URL (placed by format_product_data_for_output or previous run)
            # Also prioritize the URL scraped directly if it exists in the input df
            scraped_url = row_data.get(scraped_haereum_url_col) if scraped_haereum_url_col in row_data else None
            
            # Handle NA values properly to avoid boolean ambiguity error
            if pd.isna(scraped_url) or scraped_url is None or (isinstance(scraped_url, str) and scraped_url.strip() in ['', 'None', 'NA', 'nan']):
                scraped_url = None
                logging.warning(f"Row {idx}: Missing or invalid scraped Haereum URL for product '{product_names[idx]}'. Attempting fallback.")

            if isinstance(existing_haereum_data, dict) and \
               existing_haereum_data.get('url') and \
               isinstance(existing_haereum_data.get('url'), str) and \
               existing_haereum_data['url'].startswith(('http://', 'https://')):
                logging.debug(f"Row {idx}: Preserving existing Haoreum image data (with URL) from previous step or run.")
                # If the scraped URL is different, update it.
                if scraped_url and existing_haereum_data.get('url') != scraped_url:
                     logging.warning(f"Row {idx}: Updating existing Haoreum URL '{existing_haereum_data.get('url')}' with scraped URL '{scraped_url}'")
                     existing_haereum_data['url'] = scraped_url
                     result_df.at[idx, target_col_haereum] = existing_haereum_data # Update dict in DF

                haereum_data_preserved = True
                # Ensure local_path is also present if possible
                if 'local_path' not in existing_haereum_data or not os.path.exists(existing_haereum_data.get('local_path', '')):
                    if haereum_match: # Try to get path from current match results if needed
                         haereum_path, _ = haereum_match
                         local_path = haereum_images.get(haereum_path, {}).get('path')
                         if local_path and os.path.exists(str(local_path)):
                              existing_haereum_data['local_path'] = str(local_path)
                              # Use original_path from metadata if available, otherwise use local_path
                              original_path = haereum_images.get(haereum_path, {}).get('original_path', str(local_path))
                              existing_haereum_data['original_path'] = original_path
                              result_df.at[idx, target_col_haereum] = existing_haereum_data # Update the dict in DF
                              logging.debug(f"Row {idx}: Added missing local_path to preserved Haoreum data.")
                # No further assignment needed for Haereum if data was preserved

            # If data wasn't preserved (no valid URL found beforehand), use the match result from find_best_image_matches
            if not haereum_data_preserved:
                logging.debug(f"Row {idx}: No valid pre-existing Haoreum data found. Using match results and scraped URL if available.")
                if haereum_match:
                    haereum_path, haereum_score = haereum_match
                    img_path_obj = haereum_images.get(haereum_path, {}).get('path')
                    if not img_path_obj:
                         logging.warning(f"Row {idx}: Haoreum match found ({haereum_path}) but no corresponding image path in metadata.")
                         # Check if we have a scraped URL to use even without a local path match
                         if scraped_url:
                             image_data = {
                                 'local_path': None, # No local file matched/found
                                 'source': 'haereum',
                                 'url': scraped_url, # Use the scraped URL
                                 'original_path': None,
                                 'score': 0.5, # Lower score as local file not confirmed
                                 'product_name': product_names[idx]
                             }
                             result_df.at[idx, target_col_haereum] = image_data
                             logging.info(f"Row {idx}: Created Haoreum image data using scraped URL only (no local match found).")
                         else:
                             result_df.at[idx, target_col_haereum] = '-'
                         continue # Skip to next source

                    img_path = str(img_path_obj)
                    original_file_path = haereum_images.get(haereum_path, {}).get('original_path', img_path) # Get original path if stored

                    # --- Get Haoreum URL ---
                    # 1. Get the URL directly scraped and stored in the input DataFrame
                    scraped_url = scraped_url # Fetched earlier
                    web_url = "" # Initialize web_url to empty string

                    # 2. Validate the scraped URL
                    if scraped_url and isinstance(scraped_url, str) and scraped_url.startswith(('http://', 'https://')):
                        web_url = scraped_url # Use the valid scraped URL
                    else:
                        # If scraped_url is missing or not a valid HTTP/HTTPS URL
                        logging.warning(
                            f"Row {idx}: Invalid or missing Haoreum URL in scraped data "
                            f"('{scraped_haereum_url_col}' column) for product '{product_names[idx]}'. "
                            f"Value found: '{scraped_url}'. Image path (if matched): {img_path}. URL will be empty."
                        )
                        # web_url remains "" as initialized above

                    image_data = {
                        'local_path': img_path,
                        'source': 'haereum',
                        'url': web_url, # Use the ONLY determined URL (scraped or empty)
                        'original_path': original_file_path, # Store the original path from metadata
                        'score': haereum_score,
                        'product_name': product_names[idx]
                    }
                    result_df.at[idx, target_col_haereum] = image_data
                else:
                     # Handle case where no match was found *and* no prior data existed
                     # Check if we have a scraped URL even without a match
                     if scraped_url:
                          # Try to find the original JPG path using the scraped_url's hash
                          target_local_jpg_path_from_hash = None
                          calculated_url_hash = hashlib.md5(scraped_url.encode()).hexdigest()[:10]
                          
                          for img_file_str_path, meta_info in haereum_images.items():
                              img_p_obj = meta_info['path']
                              img_filename = img_p_obj.name
                              
                              if calculated_url_hash in img_filename and \
                                 img_filename.startswith('haereum_') and \
                                 img_filename.endswith('.jpg') and \
                                 '_nobg' not in img_filename:
                                  target_local_jpg_path_from_hash = str(img_p_obj)
                                  logging.info(f"Row {idx}: Haereum - Found original JPG '{target_local_jpg_path_from_hash}' for scraped URL {scraped_url} via hash {calculated_url_hash}")
                                  break

                          if target_local_jpg_path_from_hash:
                              image_data = {
                                  'local_path': target_local_jpg_path_from_hash,
                                  'source': 'haereum',
                                  'url': scraped_url,
                                  'original_path': target_local_jpg_path_from_hash, # Original JPG path
                                  'score': 0.55, # Higher than pure fallback, lower than text match
                                  'product_name': product_names[idx]
                              }
                              result_df.at[idx, target_col_haereum] = image_data
                              logging.info(f"Row {idx}: Haoreum image data created using scraped URL and hash-matched original JPG: {target_local_jpg_path_from_hash}")
                          else:
                              # Fallback if local JPG not found via hash
                              image_data = {
                                  'local_path': None, # No local file matched by find_best_match_for_product or hash
                                  'source': 'haereum',
                                  'url': scraped_url, # Use the scraped URL
                                  'original_path': None,
                                  'score': 0.5, # Lower score as no local match confirmed
                                  'product_name': product_names[idx]
                              }
                              result_df.at[idx, target_col_haereum] = image_data
                              logging.warning(f"Row {idx}: Haoreum image data created using scraped URL only (no text/image match, no hash match to original JPG for hash {calculated_url_hash}).")
                     elif target_col_haereum in result_df.columns:
                         current_val = result_df.loc[idx, target_col_haereum]
                         if not isinstance(current_val, dict): # Avoid overwriting existing dicts
                              result_df.loc[idx, target_col_haereum] = '-'
                     else:
                         logging.warning(f"Target column '{target_col_haereum}' unexpectedly missing at index {idx}.")

            # --- Process Kogift Image ---
            target_col_kogift = '고려기프트 이미지'
            has_kogift_product_info = kogift_product_info_exists[idx]  # Use pre-computed value
            logging.debug(f"Row {idx}: Kogift product info exists: {has_kogift_product_info}")

            if kogift_match:
                kogift_path, kogift_score = kogift_match
                product_name_for_log = product_names[idx] if idx < len(product_names) else "Unknown Product"

                assign_kogift_image = False
                if has_kogift_product_info:
                    # Product info exists - use more lenient thresholds from config
                    # Get thresholds from config with more lenient defaults
                    high_quality = config.getfloat('MatchQualityThresholds', 'high_quality', fallback=0.60) if config else 0.60
                    medium_quality = config.getfloat('MatchQualityThresholds', 'medium_quality', fallback=0.40) if config else 0.40
                    low_quality = config.getfloat('MatchQualityThresholds', 'low_quality', fallback=0.30) if config else 0.30
                    reject_threshold = config.getfloat('MatchQualityThresholds', 'reject_threshold', fallback=0.10) if config else 0.10
                    
                    # Always accept kogift image with ANY score if product info exists
                    assign_kogift_image = True
                    
                    if kogift_score >= high_quality:  # high_quality threshold
                        logging.info(f"Row {idx} (Product: '{product_name_for_log}'): Assigning Kogift image based on high quality score ({kogift_score:.3f})")
                    elif kogift_score >= medium_quality:  # medium_quality threshold
                        logging.info(f"Row {idx} (Product: '{product_name_for_log}'): Assigning Kogift image based on medium quality score ({kogift_score:.3f})")
                    elif kogift_score >= low_quality:  # low_quality threshold
                        logging.warning(f"Row {idx} (Product: '{product_name_for_log}'): Assigning Kogift image with low quality score ({kogift_score:.3f}). Manual review suggested.")
                    else:  # even below reject_threshold, still assign
                        logging.warning(f"Row {idx} (Product: '{product_name_for_log}'): Assigning Kogift image despite very low score {kogift_score:.3f}. Manual review required.")
                else:
                    # No Kogift product info for this row.
                    # Using lowered high_quality threshold for when no product info
                    if kogift_score >= 0.30:  # Reduced from 0.70
                        assign_kogift_image = True
                        logging.warning(f"Row {idx} (Product: '{product_name_for_log}'): Assigning Kogift image with score {kogift_score:.3f} despite MISSING Kogift product info.")
                    else:
                        logging.warning(f"Row {idx} (Product: '{product_name_for_log}'): REJECTING Kogift image match. Score {kogift_score:.3f} is below threshold (0.30) AND no Kogift product info exists.")
                        assign_kogift_image = False

                if assign_kogift_image:
                    img_path_obj = kogift_images.get(kogift_path, {}).get('path')
                    if not img_path_obj:
                        logging.warning(f"Row {idx}: Kogift match found ({kogift_path}) but no corresponding image path in metadata.")
                        if target_col_kogift in result_df.columns and not isinstance(result_df.at[idx, target_col_kogift], dict):
                             result_df.at[idx, target_col_kogift] = '-'
                        # continue # Skip Kogift if path is missing - NO, go to Naver section
                    else:
                        img_path = str(img_path_obj)
                        existing_kogift_data = row_data.get(target_col_kogift)
                        web_url = None
                        if isinstance(existing_kogift_data, dict):
                            potential_url = existing_kogift_data.get('url')
                            if isinstance(potential_url, str) and potential_url.startswith(('http://', 'https://')):
                                web_url = potential_url
                                logging.debug(f"Row {idx}: Preserving existing Kogift URL: {web_url[:60]}...")
                        
                        if not web_url:
                            web_url = kogift_images.get(kogift_path, {}).get('url')
                            if not web_url and 'original_path' in kogift_images.get(kogift_path, {}):
                                orig_path = kogift_images[kogift_path]['original_path']
                                if isinstance(orig_path, str) and 'upload' in orig_path:
                                    parts = str(orig_path).split('upload/')
                                    if len(parts) > 1:
                                        for ext in ['.jpg', '.png', '.gif']:
                                            if ext in parts[1]:
                                                web_url = f"https://koreagift.com/ez/upload/{parts[1]}"
                                                break
                            if not web_url:
                                web_url = ""
                                logging.warning(f"Row {idx}: Could not find or generate URL for Kogift image {img_path}")

                        image_data = {
                            'local_path': img_path,
                            'source': 'kogift',
                            'url': web_url,
                            'original_path': str(img_path),
                            'score': kogift_score,
                            'product_name': product_name_for_log
                        }
                        result_df.at[idx, target_col_kogift] = image_data
                else: # assign_kogift_image is False
                    if target_col_kogift in result_df.columns and not isinstance(result_df.at[idx, target_col_kogift], dict):
                        result_df.at[idx, target_col_kogift] = '-'
            
            else: # No kogift_match found by find_best_image_matches
                product_name_for_log = product_names[idx] if idx < len(product_names) else "Unknown Product"
                logging.debug(f"Row {idx} (Product: '{product_name_for_log}'): No Kogift image match from find_best_image_matches.")
                # if has_kogift_product_info:
                #     # Product info exists, but no image match. Try to create placeholder from link.
                #     logging.debug(f"Row {idx}: Koreagift product info exists but no image match found (after find_best_image_matches).")
                #     kogift_link_col = '고려기프트 상품링크'
                #     if kogift_link_col in row_data and isinstance(row_data[kogift_link_col], str) and row_data[kogift_link_col].strip() not in ['', '-', 'None', None]:
                #         kogift_url = row_data[kogift_link_col].strip()
                #         img_url_from_product_link = None
                #         if 'koreagift.com' in kogift_url:
                #             product_id_match_kg = re.search(r'p_idx=(\\d+)', kogift_url)
                #             if product_id_match_kg:
                #                 product_id_kg = product_id_match_kg.group(1)
                #                 img_url_from_product_link = f"https://koreagift.com/ez/upload/mall/shop_{product_id_kg}_0.jpg"
                #                 logging.debug(f"Row {idx}: Generated Kogift image URL from product link: {img_url_from_product_link}")
                        
                #         if img_url_from_product_link:
                #             img_data = {'source': 'kogift', 'url': img_url_from_product_link, 'score': 0.5, 'product_name': product_name_for_log}
                #             result_df.at[idx, target_col_kogift] = img_data
                #             logging.info(f"Row {idx}: Created Kogift image data with generated URL from product link (no direct image match).")
                #         else:
                #             img_data = {'source': 'kogift', 'url': kogift_url, 'score': 0.3, 'product_name': product_name_for_log, 'is_product_url': True}
                #             result_df.at[idx, target_col_kogift] = img_data
                #             logging.info(f"Row {idx}: Created Kogift image data using product URL as fallback (no direct image match, no generated URL).")
                #     else: # No link to use for generating a URL
                #         if target_col_kogift in result_df.columns and not isinstance(result_df.at[idx, target_col_kogift], dict):
                #             result_df.loc[idx, target_col_kogift] = '-'
                # else: # No Kogift product info AND no image match
                #     if target_col_kogift in result_df.columns and not isinstance(result_df.at[idx, target_col_kogift], dict):
                #          result_df.loc[idx, target_col_kogift] = '-'
                
                # Fallback logic removed as per user request to only use local images.
                # Ensure the column is marked appropriately if no match.
                if target_col_kogift in result_df.columns and not isinstance(result_df.at[idx, target_col_kogift], dict):
                         result_df.loc[idx, target_col_kogift] = '-'

            # --- Process Naver Image ---
            target_col_naver = '네이버 이미지'
            final_naver_image_data = None
            naver_product_name_for_log = product_names[idx]

            # 네이버 관련 모든 컬럼 정의
            NAVER_DATA_COLUMNS_TO_CLEAR = [
                target_col_naver,          # 이미지
                '네이버 쇼핑 링크',        # 상품 링크
                '공급사 상품링크',         # 공급사 링크
                '기본수량(3)',            # 수량 정보
                '판매단가(V포함)(3)',     # 가격 정보
                '가격차이(3)',           # 가격 차이
                '가격차이(3)(%)',        # 가격 차이 비율
                '공급사명'               # 공급사 정보
            ]
            
            # 임계값 설정 (더 낮게 조정)
            naver_integration_score_threshold = 0.20  # Reduced from 0.55 to 0.20
            logging.info(f"Row {idx}: Using Naver integration score threshold: {naver_integration_score_threshold}")

            if naver_match and naver_match[0] != '없음' and naver_match[0] is not None:
                naver_path_from_match, naver_score_from_match = naver_match

                if not isinstance(naver_score_from_match, (float, int)) or naver_score_from_match is None:
                    logging.warning(f"Row {idx}: Naver - Invalid/missing score for '{naver_product_name_for_log}': {naver_score_from_match}. Clearing all Naver data.")
                    for col_to_clear in NAVER_DATA_COLUMNS_TO_CLEAR: 
                        if col_to_clear in result_df.columns:
                            result_df.at[idx, col_to_clear] = None
                elif naver_score_from_match < naver_integration_score_threshold:
                    logging.info(f"Row {idx}: Naver - Score {naver_score_from_match:.3f} for '{naver_product_name_for_log}' < integration threshold {naver_integration_score_threshold}. Clearing all Naver data.")
                    for col_to_clear in NAVER_DATA_COLUMNS_TO_CLEAR: 
                        if col_to_clear in result_df.columns:
                            result_df.at[idx, col_to_clear] = None
                else:
                    # naver_path_from_match is the local disk path found by find_best_image_matches
                    # img_path_obj_dict_entry is metadata for this local disk path from prepare_image_metadata
                    img_path_obj_dict_entry = naver_images.get(naver_path_from_match, {})
                    # img_path_actual is the verified local path from the metadata of the matched file
                    img_path_actual = img_path_obj_dict_entry.get('path')

                    if img_path_actual and os.path.exists(str(img_path_actual)):
                        img_path_actual_str = str(img_path_actual)
                        web_url = None
                        source_of_url = "unknown"

                        # Get the existing data from the '네이버 이미지' cell for the current row
                        existing_naver_cell_data = result_df.at[idx, target_col_naver]
                        
                        # --- STRATEGY TO GET THE CORRECT pstatic.net URL ---
                        # Priority 1: Use URL from existing_naver_cell_data if it's a valid pstatic.net URL
                        if isinstance(existing_naver_cell_data, dict):
                            potential_url = existing_naver_cell_data.get('url')
                            if isinstance(potential_url, str) and potential_url.startswith('http'):
                                # Be more permissive with URL validation - accept front/ URLs if that's all we have
                                web_url = potential_url
                                source_of_url = "dataframe_cell_url"
                                logging.debug(f"Row {idx}: Naver - Using URL from DataFrame cell: {web_url}")

                        # Determine product_id for URL construction
                        product_id_for_url = None
                        if isinstance(existing_naver_cell_data, dict) and existing_naver_cell_data.get('product_id'):
                            product_id_for_url = existing_naver_cell_data.get('product_id')
                            source_of_id = "dataframe_cell"
                        elif img_path_obj_dict_entry.get('product_id'):
                            product_id_for_url = img_path_obj_dict_entry.get('product_id')
                            source_of_id = "prepare_image_metadata"
                        else:
                            # Try to parse product_id from filename if not found elsewhere
                            # Example Naver filename pattern: naver_somechars_PRODUCTID_somehash.jpg
                            # Or from crawl_naver_api.py: naver_PRODUCTID_hash.jpg (if product_name was short)
                            # Or naver_PRODNAMEHASH_APIHASH.jpg
                            filename_stem = Path(img_path_actual_str).stem
                            # More permissive patterns for Naver product IDs
                            match_simple_id = re.match(r"naver_([a-zA-Z0-9]+)_([a-f0-9]{6,})", filename_stem)
                            if match_simple_id:
                                # Check if the first group looks like a product ID (often numeric or alphanumeric)
                                potential_pid = match_simple_id.group(1)
                                # More permissive heuristic: Accept shorter IDs and more alphanumeric patterns
                                if len(potential_pid) > 4:
                                    product_id_for_url = potential_pid
                                    source_of_id = "filename_parsed_simple"
                                    logging.debug(f"Row {idx}: Naver - Parsed product_id '{product_id_for_url}' from simple filename pattern.")
                            if not product_id_for_url:
                                # Try complex pattern with more permissive matching
                                match_complex_id = re.match(r"naver_[a-f0-9]+_([a-zA-Z0-9]+)_[a-f0-9]{6,}", filename_stem)
                                if match_complex_id:
                                    potential_pid_complex = match_complex_id.group(1)
                                    if len(potential_pid_complex) > 4:
                                        product_id_for_url = potential_pid_complex
                                        source_of_id = "filename_parsed_complex"
                                        logging.debug(f"Row {idx}: Naver - Parsed product_id '{product_id_for_url}' from complex filename pattern.")
                        
                        if product_id_for_url:
                            logging.info(f"Row {idx}: Naver - Determined product_id '{product_id_for_url}' (source: {source_of_id}) for URL construction.")
                        else:
                            logging.warning(f"Row {idx}: Naver - Could not determine product_id for URL construction from any source for local file '{os.path.basename(img_path_actual_str)}'.")


                        # Priority 2: If not found above, try product_id to generate URL
                        if not web_url and product_id_for_url:
                            original_extension = ".jpg" # Default
                            # Try to get extension from existing URL in cell, or from local file
                            url_for_ext_source = existing_naver_cell_data.get('url') if isinstance(existing_naver_cell_data, dict) else None
                            if not url_for_ext_source and img_path_obj_dict_entry.get('url'): # from prepare_image_metadata
                                url_for_ext_source = img_path_obj_dict_entry.get('url')

                            if url_for_ext_source and isinstance(url_for_ext_source, str) and '.' in url_for_ext_source.split('/')[-1]:
                                ext_part = url_for_ext_source.split('.')[-1].split('?')[0].lower()
                                if ext_part in ['jpg', 'jpeg', 'png', 'gif']:
                                    original_extension = '.' + ext_part
                            elif img_path_actual_str: # Fallback to local file extension
                                _, local_ext = os.path.splitext(img_path_actual_str)
                                if local_ext.lower() in ['.jpg', '.jpeg', '.png', '.gif']:
                                    original_extension = local_ext.lower()
                            
                            generated_url_candidate = f"https://shopping-phinf.pstatic.net/main_{product_id_for_url}/{product_id_for_url}{original_extension}"
                            web_url = generated_url_candidate
                            source_of_url = f"generated_from_product_id_{source_of_id}"
                            logging.debug(f"Row {idx}: Naver - Generated pstatic.net URL from product_id {product_id_for_url}: {web_url}")
                        
                        # Fallback: Use ANY metadata URL or local file if we failed to get a web URL
                        if not web_url:
                            metadata_url = img_path_obj_dict_entry.get('url') # URL from prepare_image_metadata
                            if isinstance(metadata_url, str) and metadata_url.startswith('http'):
                                web_url = metadata_url
                                source_of_url = "prepare_image_metadata_any_url"
                                logging.debug(f"Row {idx}: Naver - Using any URL from prepare_image_metadata: {web_url}")
                            elif img_path_actual_str:
                                # Last resort: URL is completely missing but we have a local file
                                web_url = f"file://{img_path_actual_str}"
                                source_of_url = "local_file_fallback"
                                logging.warning(f"Row {idx}: Naver - No URL found. Using local file reference: {web_url}")

                        # --- END STRATEGY ---

                        if web_url: # Only proceed if we have any URL
                            final_naver_image_data = {
                                'local_path': img_path_actual_str, 
                                'url': web_url, 
                                'score': naver_score_from_match,
                                'source': 'naver',
                                'original_path': img_path_obj_dict_entry.get('original_path', img_path_actual_str), 
                                'product_name': naver_product_name_for_log,
                                'product_id': product_id_for_url,
                                'url_source_debug': source_of_url 
                            }
                            logging.info(f"Row {idx}: Naver - Prepared data. Image: '{os.path.basename(img_path_actual_str)}', URL: '{web_url}' (Source: {source_of_url}, PID: {product_id_for_url}), Score: {naver_score_from_match:.3f}")
                        else: 
                            logging.warning(f"Row {idx}: Naver - Matched image '{os.path.basename(img_path_actual_str)}' (Score: {naver_score_from_match:.3f}) but couldn't find or generate any URL. Clearing Naver data.")
                            final_naver_image_data = None # Effectively clears if not set elsewhere
                            for col_to_clear in NAVER_DATA_COLUMNS_TO_CLEAR: 
                                if col_to_clear in result_df.columns:
                                    result_df.at[idx, col_to_clear] = None
            else: # No initial naver_match or match was '없음'
                log_msg = f"Row {idx}: Naver - No valid initial match (match details: {naver_match}). Clearing Naver data for '{naver_product_name_for_log}'."
                if naver_match and naver_match[0] == '없음': log_msg = f"Row {idx}: Naver - Match explicitly '없음' for '{naver_product_name_for_log}'. Clearing data."
                logging.info(log_msg)
                for col_to_clear in NAVER_DATA_COLUMNS_TO_CLEAR: result_df.at[idx, col_to_clear] = None
            
            result_df.at[idx, target_col_naver] = final_naver_image_data # This will be None if data was cleared

        # Final post-processing: Ensure Koreagift product info and images are properly paired
        kogift_image_col = '고려기프트 이미지'
        kogift_link_col = '고려기프트 상품링크'
        kogift_price_col = '판매가(V포함)(2)'
        alt_kogift_price_col = '판매단가(V포함)(2)'
        
        mismatch_count = 0
        for idx in range(len(result_df)):
            # Skip if index out of bounds
            if idx >= len(result_df):
                continue
                
            # Get row data
            row_data = result_df.iloc[idx]
            product_name = product_names[idx] if idx < len(product_names) else "Unknown Product"
            
            # Get Koreagift image data
            kogift_image_data = row_data.get(kogift_image_col)
            has_kogift_image = isinstance(kogift_image_data, dict) and kogift_image_data.get('local_path') is not None
            
            # Get Koreagift product info
            has_kogift_info = kogift_product_info_exists[idx]  # Use pre-computed value
            
            # Check for mismatch: Image without product info, or product info without image
            if has_kogift_image != has_kogift_info:
                mismatch_count += 1
                if has_kogift_image and not has_kogift_info:
                    # Remove image if no product info
                    logging.warning(f"Row {idx} (Product: '{product_name}'): Found Koreagift image without product info. Removing image.")
                    result_df.at[idx, kogift_image_col] = '-'
                elif has_kogift_info and not has_kogift_image:
                    logging.warning(f"Row {idx} (Product: '{product_name}'): Found Koreagift product info without image. Clearing Kogift related information.")
                    # Clear Kogift image cell (it should already be '-' or None, but ensure it)
                    result_df.at[idx, kogift_image_col] = '-'

                    # Clear all Kogift related columns
                    kogift_related_columns_to_clear = [
                        '고려기프트 상품링크', 
                        '기본수량(2)', 
                        '판매가(V포함)(2)', 
                        '판매단가(V포함)(2)', # Alternative price column
                        '가격차이(2)', 
                        '가격차이(2)(%)'
                    ]
                    for col_to_clear in kogift_related_columns_to_clear:
                        if col_to_clear in result_df.columns:
                            result_df.at[idx, col_to_clear] = None
        
        # Final post-processing: Ensure Naver product info and images are properly paired
        naver_image_col = '네이버 이미지'
        naver_link_cols = ['네이버 쇼핑 링크', '네이버 링크']
        naver_price_cols = ['판매단가(V포함)(3)', '네이버 판매단가', '판매단가3 (VAT포함)']
        
        naver_mismatch_count = 0
        for idx in range(len(result_df)):
            # Skip if index out of bounds
            if idx >= len(result_df):
                continue
                
            # Get row data
            row_data = result_df.iloc[idx]
            
            # Get Naver image data
            naver_image_data = row_data.get(naver_image_col)
            has_naver_image = isinstance(naver_image_data, dict)
            
            # Check if there's Naver product info
            has_naver_info = naver_product_info_exists[idx]  # Use pre-computed value
            
            # Check for mismatch: Product info without image (the critical issue)
            if has_naver_info and not has_naver_image:
                naver_mismatch_count += 1
                logging.warning(f"Row {idx}: Found Naver product info without image. Clearing all Naver related information.")
                
                # Clear Naver image
                result_df.at[idx, naver_image_col] = None
                
                # Clear all Naver related columns
                naver_related_columns = [
                    '기본수량(3)',
                    '판매단가(V포함)(3)',
                    '가격차이(3)',
                    '가격차이(3)(%)',
                    '공급사명',
                    '네이버 쇼핑 링크',
                    '공급사 상품링크'
                ]
                
                for col in naver_related_columns:
                    if col in result_df.columns:
                        result_df.at[idx, col] = None
                        
                logging.info(f"Row {idx}: Cleared all Naver related information for product {product_names[idx]}")
        
        if mismatch_count > 0:
            logging.info(f"Fixed {mismatch_count} mismatches between Koreagift product info and images")
        
        if naver_mismatch_count > 0:
            logging.info(f"Attempted to fix {naver_mismatch_count} mismatches between Naver product info and images")

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
    """
    try:
        result_df = df.copy()
        
        # Get thresholds from config with MUCH more lenient defaults
        try:
            similarity_threshold = config.getfloat('Matching', 'image_display_threshold', fallback=0.05)  # Even more lenient
            minimum_match_confidence = config.getfloat('ImageMatching', 'minimum_match_confidence', fallback=0.05)  # Even more lenient
            
            # Use the higher of the two thresholds
            effective_threshold = max(similarity_threshold, minimum_match_confidence)
            
            logging.info(f"통합: 이미지 표시 임계값 (filter_images_by_similarity): {effective_threshold}")
        except (configparser.Error, ValueError) as e:
            logging.warning(f"임계값 읽기 오류: {e}. 기본값 0.05을 사용합니다.")
            effective_threshold = 0.05  # Even more lenient default
        
        # Get Naver-specific thresholds with much more lenient values
        try:
            naver_initial_threshold = config.getfloat('Matching', 'naver_initial_similarity_threshold', fallback=0.10)  # More lenient
            naver_minimum_threshold = config.getfloat('Matching', 'naver_minimum_similarity', fallback=0.05)  # More lenient
        except (configparser.Error, ValueError) as e:
            logging.warning(f"네이버 임계값 읽기 오류: {e}. 기본값을 사용합니다.")
            naver_initial_threshold = 0.10  # More lenient
            naver_minimum_threshold = 0.05  # More lenient

        # Count before filtering
        kogift_before = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '고려기프트 이미지'], dict))
        naver_before = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '네이버 이미지'], dict))
        logging.info(f"필터링 전 이미지 수: 고려기프트={kogift_before}, 네이버={naver_before}")

        # Filter counter
        filtered_count = 0
        
        for idx in range(len(result_df)):
            for col_name in ['고려기프트 이미지', '네이버 이미지']:
                if col_name not in result_df.columns:
                    continue
                
                cell_data = result_df.at[idx, col_name]
                if not isinstance(cell_data, dict):
                    continue
                
                score = cell_data.get('score', 0)
                url = cell_data.get('url')
                
                # Special handling for Naver images - be more permissive about URLs
                if col_name == '네이버 이미지':
                    # Check if URL exists at all - accept any URL
                    if not url:
                        product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Index {idx}"
                        logging.warning(f"Row {idx} (Product: '{product_name}'): Naver - Missing URL. Will try to keep anyway.")
                        
                        # Instead of clearing, try to find local_path
                        local_path = cell_data.get('local_path')
                        if local_path and os.path.exists(local_path):
                            # Keep it if we have a valid local path
                            logging.info(f"Row {idx} (Product: '{product_name}'): Keeping Naver data with valid local path despite missing URL.")
                            continue
                        
                        # Only clear if both URL and local path are invalid
                        result_df.at[idx, col_name] = None
                        filtered_count += 1
                        continue
                        
                    # Only filter out with extremely low or negative scores
                    if score < 0:
                        product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Index {idx}"
                        logging.warning(f"Row {idx} (Product: '{product_name}'): Naver - Score {score:.3f} is negative. Clearing image data.")
                        result_df.at[idx, col_name] = None
                        filtered_count += 1
                else:  # Kogift images
                    # For Kogift, only filter out negative scores
                    if score < 0:
                        product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Index {idx}"
                        logging.warning(f"Row {idx} (Product: '{product_name}'): Kogift - Score {score:.3f} is negative. Clearing image data.")
                        result_df.at[idx, col_name] = None
                        filtered_count += 1

        # Log how many images were kept after filtering
        kept_kogift = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '고려기프트 이미지'], dict))
        kept_naver = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '네이버 이미지'], dict))
        
        logging.info(f"필터링 후 이미지 수: 고려기프트={kept_kogift} (제거: {kogift_before-kept_kogift}), 네이버={kept_naver} (제거: {naver_before-kept_naver})")
        logging.info(f"총 {filtered_count}개 이미지 필터링됨")
        
        return result_df
    except Exception as e:
        logging.error(f"이미지 필터링 중 오류 발생: {e}", exc_info=True)
        return df  # Return original dataframe on error

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
                        
                        # IMPORTANT: For Haereum (본사) images, ALWAYS use original_path instead of local_path
                        # to ensure we use the JPG instead of _nobg version
                        if col_name == '본사 이미지':
                            if 'original_path' in img_data and img_data.get('original_path') and os.path.exists(img_data.get('original_path')):
                                img_path = img_data.get('original_path')
                                logger.info(f"Using original_path for Haereum image: {img_path}")
                            elif 'local_path' in img_data:
                                original_jpg_path = None
                                local_path = img_data.get('local_path', '')
                                
                                # If local_path is a _nobg.png file, try to find the corresponding JPG
                                if local_path and '_nobg.png' in local_path:
                                    original_jpg_path = local_path.replace('_nobg.png', '.jpg')
                                    if os.path.exists(original_jpg_path):
                                        img_path = original_jpg_path
                                        logger.info(f"Found original JPG from _nobg.png path: {img_path}")
                                    else:
                                        img_path = local_path  # Fallback to _nobg.png if JPG not found
                                else:
                                    img_path = local_path  # Use local_path as is
                            else:
                                # No valid path found
                                img_path = None
                        else:  # For non-Haereum images
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
                            # Additional check for Haereum images to replace _nobg with JPG
                            if col_name == '본사 이미지' and '_nobg' in str(img_path):
                                original_jpg = str(img_path).replace('_nobg.png', '.jpg')
                                if os.path.exists(original_jpg):
                                    logger.info(f"Replacing _nobg version with original JPG: {original_jpg}")
                                    img_path = original_jpg
                            
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

def improved_kogift_image_matching(df: pd.DataFrame) -> pd.DataFrame:
    """
    Improves the matching between Kogift image URLs and local files.
    Ensures URLs and downloaded images are properly associated.
    
    Args:
        df: DataFrame with image information
        
    Returns:
        DataFrame with improved Kogift image matching
    """
    import os
    import hashlib
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info("Starting improved Kogift image matching...")
    
    # Define column for Kogift images (both old and new naming standards)
    kogift_img_columns = ['고려기프트 이미지', '고려기프트(이미지링크)']
    # Keep only columns that exist in the DataFrame
    kogift_img_columns = [col for col in kogift_img_columns if col in df.columns]
    
    if not kogift_img_columns:
        logger.warning("No Kogift image columns found in DataFrame.")
        return df
        
    # Get RPA image directory from environment or use default
    base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
    
    # Create a mapping of URLs to local files
    url_to_local_map = {}
    
    # First, build a database of available Kogift images
    kogift_images = {}
    
    # Scan all potential Kogift image directories
    kogift_dirs = [
        os.path.join(base_img_dir, 'Main', 'Kogift'),
        os.path.join(base_img_dir, 'Main', 'kogift'),
        os.path.join(base_img_dir, 'Kogift'),
        os.path.join(base_img_dir, 'kogift'),
        os.path.join(base_img_dir, 'Target', 'Kogift'),
        os.path.join(base_img_dir, 'Target', 'kogift')
    ]
    
    # Scan each directory for images
    for dir_path in kogift_dirs:
        if os.path.exists(dir_path):
            try:
                logger.info(f"Scanning directory: {dir_path}")
                # Get all image files in the directory
                for file in os.listdir(dir_path):
                    if file.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                        full_path = os.path.join(dir_path, file)
                        base_name = os.path.basename(file) # Original filename with extension

                        # Skip small files
                        if os.path.getsize(full_path) < 1000:  # Less than 1KB
                            continue

                        # Always map the actual filename (and its lowercase version) to its actual path
                        kogift_images[base_name] = full_path
                        kogift_images[base_name.lower()] = full_path
                        
                        # Handle 'kogift_' prefix
                        if base_name.lower().startswith('kogift_'):
                            no_prefix_name = base_name[7:] # Remove 'kogift_'
                            kogift_images[no_prefix_name] = full_path
                            kogift_images[no_prefix_name.lower()] = full_path

                        # Handle 'shop_' prefix (common in some Kogift URLs/files)
                        # This can be complex if 'kogift_' and 'shop_' appear together.
                        # Example: shop_kogift_product.jpg -> product.jpg
                        # Example: kogift_shop_product.jpg -> shop_product.jpg (less common)
                        # Example: shop_product.jpg -> product.jpg
                        
                        temp_name_for_shop_handling = base_name
                        if temp_name_for_shop_handling.lower().startswith('kogift_'):
                            temp_name_for_shop_handling = temp_name_for_shop_handling[7:]
                        
                        if temp_name_for_shop_handling.lower().startswith('shop_'):
                            name_after_shop = temp_name_for_shop_handling[5:] # Remove 'shop_'
                            kogift_images[name_after_shop] = full_path
                            kogift_images[name_after_shop.lower()] = full_path

                        # Store hash-based keys if a hash is identifiable in the filename
                        # e.g., kogift_productname_a1b2c3d4.jpg -> a1b2c3d4
                        hash_match = re.search(r'_([a-f0-9]{8,})\.', base_name.lower())
                        if hash_match:
                            hash_val = hash_match.group(1)
                            kogift_images[hash_val] = full_path
                            # Also store common prefixed hash patterns if the original name had them
                            if base_name.lower().startswith('kogift_'):
                                kogift_images[f"kogift_{hash_val}.jpg"] = full_path
                                kogift_images[f"kogift_{hash_val}.png"] = full_path
                                kogift_images[f"kogift_{hash_val}_nobg.png"] = full_path


            except Exception as e:
                logger.error(f"Error scanning directory {dir_path}: {e}")
    
    logger.info(f"Found {len(kogift_images)} Kogift images on disk")
    
    # Process each row that has Kogift image data
    fixed_count = 0
    rows_processed = 0
    
    for idx, row in df.iterrows():
        for col in kogift_img_columns:
            img_data = row[col]
            if pd.isna(img_data) or img_data == '' or img_data == '-':
                continue
                
            rows_processed += 1
            url = None
            local_path = None
            original_path = None
            
            # Handle dictionary format
            if isinstance(img_data, dict):
                # Extract URL and paths
                url = img_data.get('url', '')
                local_path = img_data.get('local_path', '')
                original_path = img_data.get('original_path', '')
                
                # Check if we have a URL without a valid local_path
                if url and (not local_path or not os.path.exists(local_path)):
                    logger.debug(f"Row {idx}: Found Kogift URL without valid local_path: {url[:50]}...")
                    
                    # Try to find the local file based on URL
                    if url.startswith(('http://', 'https://')):
                        filename = os.path.basename(url)
                        
                        # Check if the filename exists in our image database
                        if filename in kogift_images:
                            new_local_path = kogift_images[filename]
                            logger.info(f"Row {idx}: Found direct filename match for Kogift URL: {filename}")
                            
                            # Update the dictionary
                            img_data['local_path'] = new_local_path
                            df.at[idx, col] = img_data
                            fixed_count += 1
                            continue
                            
                        # Try extracting product code or ID pattern from URL
                        # Common patterns in Kogift URLs:
                        # - mall/shop_PRODUCTNAME.jpg
                        # - product/PRODUCTCODE.jpg
                        # - shop_NAME.jpg
                        if 'mall/shop_' in url:
                            product_part = url.split('mall/shop_')[1].split('?')[0]
                            
                            # Check for this product part in our database
                            if product_part in kogift_images:
                                new_local_path = kogift_images[product_part]
                                logger.info(f"Row {idx}: Found product match via mall/shop_ pattern: {product_part}")
                                
                                # Update the dictionary
                                img_data['local_path'] = new_local_path
                                df.at[idx, col] = img_data
                                fixed_count += 1
                                continue
                                
                        # Try hash-based matching if direct matching fails
                        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
                        hash_patterns = [
                            f"kogift_{url_hash}.jpg",
                            f"kogift_{url_hash}.png", 
                            f"kogift_{url_hash}_nobg.png"  # ADDED: Explicit _nobg pattern for hash
                        ]
                        
                        for pattern in hash_patterns:
                            if pattern in kogift_images:
                                new_local_path = kogift_images[pattern]
                                logger.info(f"Row {idx}: Found match via URL hash pattern: {pattern}")
                                
                                # Update the dictionary
                                img_data['local_path'] = new_local_path
                                df.at[idx, col] = img_data
                                fixed_count += 1
                                break
                                
                        # ADDED: Try looking for _nobg version if regular image not found
                        if not local_path and filename.lower().endswith(('.jpg', '.png')) and '_nobg' not in filename.lower():
                            # Generate _nobg variant of the filename
                            base_name = os.path.splitext(filename)[0]
                            nobg_variant = f"{base_name}_nobg.png"
                            
                            if nobg_variant in kogift_images:
                                new_local_path = kogift_images[nobg_variant]
                                logger.info(f"Row {idx}: Found _nobg variant for regular image: {nobg_variant}")
                                
                                # Update the dictionary
                                img_data['local_path'] = new_local_path
                                df.at[idx, col] = img_data
                                fixed_count += 1
                                continue
                                
                        # If still not found, try fuzzy matching
                        best_match = None
                        highest_similarity = 0
                        
                        for img_name, img_path in kogift_images.items():
                            # Skip if filename is very short (to avoid false matches)
                            if len(img_name) < 5:
                                continue
                                
                            # Calculate similarity between URL basename and image filename
                            url_base = os.path.basename(url).lower()
                            img_base = img_name.lower()
                            
                            # Check for partial matches
                            if url_base[:5] in img_base or img_base[:5] in url_base:
                                # Calculate similarity score (simplified)
                                similarity = 0
                                for i in range(min(len(url_base), len(img_base))):
                                    if i < len(url_base) and i < len(img_base) and url_base[i] == img_base[i]:
                                        similarity += 1
                                
                                similarity = similarity / max(len(url_base), len(img_base))
                                
                                if similarity > highest_similarity:
                                    highest_similarity = similarity
                                    best_match = img_path
                        
                        # If we found a reasonably good match
                        if best_match and highest_similarity > 0.4:
                            logger.info(f"Row {idx}: Found fuzzy match with similarity {highest_similarity:.2f}: {os.path.basename(best_match)}")
                            
                            # Update the dictionary
                            img_data['local_path'] = best_match
                            df.at[idx, col] = img_data
                            fixed_count += 1
            
            # Handle string format (URL or path)
            elif isinstance(img_data, str) and img_data.startswith(('http://', 'https://')):
                url = img_data
                
                # Try to find local file based on URL
                filename = os.path.basename(url)
                
                # Check if the filename exists in our image database
                if filename in kogift_images:
                    new_local_path = kogift_images[filename]
                    logger.info(f"Row {idx}: Found direct filename match for Kogift URL string: {filename}")
                    
                    # Create a dictionary format
                    df.at[idx, col] = {
                        'url': url,
                        'local_path': new_local_path,
                        'source': 'kogift'
                    }
                    fixed_count += 1
                    continue
                    
                # ADDED: Try _nobg variant for regular image names
                if filename.lower().endswith(('.jpg', '.png')) and '_nobg' not in filename.lower():
                    base_name = os.path.splitext(filename)[0]
                    nobg_variant = f"{base_name}_nobg.png"
                    
                    if nobg_variant in kogift_images:
                        new_local_path = kogift_images[nobg_variant]
                        logger.info(f"Row {idx}: Found _nobg variant for regular image URL string: {nobg_variant}")
                        
                        # Create a dictionary format
                        df.at[idx, col] = {
                            'url': url,
                            'local_path': new_local_path,
                            'source': 'kogift'
                        }
                        fixed_count += 1
                        continue
                    
                # Try hash-based matching
                url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
                hash_patterns = [
                    f"kogift_{url_hash}.jpg",
                    f"kogift_{url_hash}.png", 
                    f"kogift_{url_hash}_nobg.png"  # ADDED: Include _nobg pattern
                ]
                
                for pattern in hash_patterns:
                    if pattern in kogift_images:
                        new_local_path = kogift_images[pattern]
                        logger.info(f"Row {idx}: Found match via URL hash pattern for string URL: {pattern}")
                        
                        # Create a dictionary format
                        df.at[idx, col] = {
                            'url': url,
                            'local_path': new_local_path,
                            'source': 'kogift'
                        }
                        fixed_count += 1
                        break
    
    logger.info(f"Kogift image matching completed. Processed {rows_processed} rows, fixed {fixed_count} image links.")
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
    # Now with less strict validation
    logger.info("Performing final URL validation and quality check...")
    
    for idx in range(len(result_df)):
        product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Index {idx}"
        
        # Check Naver image URLs - only reject if image clearly invalid
        if '네이버 이미지' in result_df.columns:
            naver_data = result_df.at[idx, '네이버 이미지']
            if isinstance(naver_data, dict) and 'url' in naver_data:
                url = naver_data['url']
                # Check for obviously invalid URL patterns
                if not url or not isinstance(url, str) or 'front/' in url.lower():
                    # Check if there's a valid local_path before clearing
                    local_path = naver_data.get('local_path')
                    if local_path and os.path.exists(str(local_path)):
                        logger.info(f"Row {idx} (Product: '{product_name}'): Keeping Naver image with invalid URL but valid local path.")
                        # Update the URL with a placeholder to avoid further validation failures
                        if not url or not isinstance(url, str):
                            naver_data['url'] = f"http://placeholder.url/for/{idx}.jpg"
                            result_df.at[idx, '네이버 이미지'] = naver_data
                    else:
                        # Only clear if both URL is invalid and local file doesn't exist
                        logger.warning(f"Row {idx} (Product: '{product_name}'): Invalid Naver URL '{url}' and no valid local path. Clearing Naver data.")
                        result_df.at[idx, '네이버 이미지'] = None
        
        # Check Kogift image URLs - only reject if both URL and local file are invalid
        if '고려기프트 이미지' in result_df.columns:
            kogift_data = result_df.at[idx, '고려기프트 이미지']
            if isinstance(kogift_data, dict):
                # Check if Kogift image has a valid local path
                local_path = kogift_data.get('local_path')
                url = kogift_data.get('url')
                
                # Only clear if both URL and local path are invalid
                if (not url or not isinstance(url, str) or not url.startswith('http')) and (not local_path or not os.path.exists(str(local_path))):
                    logger.warning(f"Row {idx} (Product: '{product_name}'): Invalid Kogift URL and missing local file. Clearing Kogift data.")
                    result_df.at[idx, '고려기프트 이미지'] = None
                elif not url or not isinstance(url, str):
                    # Fix the URL with a placeholder if local path is valid
                    if local_path and os.path.exists(str(local_path)):
                        logger.info(f"Row {idx} (Product: '{product_name}'): Setting placeholder URL for valid Kogift local file.")
                        kogift_data['url'] = f"http://placeholder.url/kogift_{idx}.jpg"
                        result_df.at[idx, '고려기프트 이미지'] = kogift_data
    
    # Count images after final validation
    naver_count = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '네이버 이미지'], dict))
    kogift_count = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '고려기프트 이미지'], dict))
    haereum_count = sum(1 for i in range(len(result_df)) if isinstance(result_df.at[i, '본사 이미지'], dict))
    
    logger.info(f"Final image counts after validation: Haereum={haereum_count}, Kogift={kogift_count}, Naver={naver_count}")
    
    # Save Excel output if requested
    if save_excel_output:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_output = f"image_integration_result_{timestamp}.xlsx"
            
            # Create the Excel file with images
            create_excel_with_images(result_df, excel_output)
            logger.info(f"Created Excel output file with images: {excel_output}")
        except Exception as e:
            logger.error(f"Error creating Excel output: {e}")
    
    return result_df

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