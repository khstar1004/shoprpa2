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
import sys
from pathlib import Path

# Get the absolute path of the current file's directory
current_dir = Path(__file__).resolve().parent

# Add the parent directory to sys.path if it's not already there
parent_dir = current_dir.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Now import the required modules
from .tokenize_product_names import tokenize_product_name, extract_meaningful_keywords

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
    
    # This is critical - make sure image_dir is an absolute path
    abs_image_dir = os.path.abspath(str(image_dir))
    logging.info(f"Preparing image metadata from directory: {abs_image_dir} (prefix: {prefix}, prefer_original: {prefer_original}, prefer_jpg: {prefer_jpg})")
    
    # Handle case where directory doesn't exist
    if not os.path.exists(abs_image_dir):
        logging.warning(f"Image directory does not exist: {abs_image_dir}")
        return {}
    
    # Make sure we're dealing with a string path
    image_dir_str = str(abs_image_dir)
    
    # First look for image files in the directory
    all_image_files = []
    valid_extensions = ('.jpg', '.jpeg', '.png', '.gif')
    
    try:
        # Get all image files
        for root, _, files in os.walk(image_dir_str):
            for file in files:
                if file.lower().endswith(valid_extensions):
                    full_path = os.path.join(root, file)
                    all_image_files.append(full_path)
        
        logging.info(f"Found {len(all_image_files)} total images in {image_dir_str}")
        
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
                
                # Use filename as unique key for matching purposes                # Remove prefix (haereum_, kogift_, naver_) if present                if file_root.startswith(f"{prefix}_"):                    name_for_matching = file_root[len(f"{prefix}_"):]                else:                    name_for_matching = file_root                                # Remove _nobg suffix for matching                if name_for_matching.endswith('_nobg'):                    name_for_matching = name_for_matching[:-5]                                # Extract 16-digit product hash from filename                product_hash = extract_product_hash_from_filename(filename)                                # Prepare a clean name for display and matching                clean_name = name_for_matching.replace('_', ' ').strip()                                # Create metadata entry                image_info[img_path] = {                    'path': str(img_path),  # Store the absolute path as string for direct access                    'original_path': str(original_jpg_path or original_png_path or img_path),  # Prefer JPG for original path                    'original_name': filename,                    'nobg_png_path': str(nobg_png_path) if nobg_png_path else None,                    'nobg_jpg_path': str(nobg_jpg_path) if nobg_jpg_path else None,                    'has_nobg': nobg_png_path is not None or nobg_jpg_path is not None,                    'name_for_matching': name_for_matching,                    'clean_name': clean_name,                    'source': prefix,                    'is_jpg': file_ext.lower() in ['.jpg', '.jpeg'],                    'is_original': not file_root.endswith('_nobg'),                    'url': None,  # Initialize URL field, will be populated later if needed                    'product_hash': product_hash  # Store the 16-digit product hash for matching                }
                
                # Debug some sample entries
                if len(image_info) <= 2 or len(image_info) % 50 == 0:
                    logging.debug(f"Image metadata sample: {img_path} -> {image_info[img_path]}")
            
            except Exception as e:
                import traceback
                stack_trace = traceback.format_exc()
                logging.error(f"Error processing image file {base_name}: {e}")
                logging.debug(f"Exception traceback: {stack_trace}")
        
        # Log summary
        logging.info(f"Processed {len(image_info)} {prefix} images")
        
        # Additional debug information
        logging.debug(f"First 3 image keys in {prefix} image_info: {list(image_info.keys())[:3]}")
        
        return image_info
        
    except Exception as e:
        import traceback
        stack_trace = traceback.format_exc()
        logging.error(f"Error preparing image metadata from {image_dir_str}: {e}")
        logging.debug(f"Exception traceback: {stack_trace}")
        return {}

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
                           similarity_threshold: float = 0.4,  # 높게 조정 (0.2에서 0.4로)
                           config: Optional[configparser.ConfigParser] = None,
                           df: Optional[pd.DataFrame] = None) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    Find the best matching images for each product name from Haereum, Kogift, and Naver images.
    Using higher thresholds for more strict matching.
    
    Args:
        product_names: List of product names to match
        haereum_images: Dictionary of Haereum images metadata
        kogift_images: Dictionary of Kogift images metadata
        naver_images: Dictionary of Naver images metadata
        similarity_threshold: Minimum similarity score for matching
        config: Configuration object for retrieving settings
        df: Optional DataFrame containing product information
        
    Returns:
        List of tuples containing (haereum_match, kogift_match, naver_match) for each product
    """
    best_matches = []
    used_haereum = set()
    used_kogift = set()
    used_naver = set()
    
    # Get thresholds from config if available
    if config:
        try:
            similarity_threshold = config.getfloat('ImageMatching', 'similarity_threshold', fallback=similarity_threshold)
        except (configparser.Error, ValueError):
            logging.warning(f"Cannot read similarity_threshold from config, using default: {similarity_threshold}")
    
    # Default thresholds if not found in config
    # These are for the fallback text-based matching in find_best_match_for_product
    default_text_threshold_naver = 0.35 
    default_text_threshold_kogift = 0.30

    # Get specific thresholds from config for text-based fallback matching
    if config:
        try:
            # For Naver
            naver_text_sim_threshold = config.getfloat('Matching', 'naver_initial_similarity_threshold', 
                                                      fallback=default_text_threshold_naver)
            # For Kogift
            kogift_text_sim_threshold = config.getfloat('Matching', 'kogift_initial_similarity_threshold',
                                                       fallback=default_text_threshold_kogift)
        except (configparser.Error, ValueError) as e:
            logging.warning(f"Cannot read initial similarity thresholds from config: {e}. Using defaults.")
            naver_text_sim_threshold = default_text_threshold_naver
            kogift_text_sim_threshold = default_text_threshold_kogift
    else:
        naver_text_sim_threshold = default_text_threshold_naver
        kogift_text_sim_threshold = default_text_threshold_kogift

    logging.info(f"Using Naver text similarity threshold for fallback: {naver_text_sim_threshold} (from config: naver_initial_similarity_threshold)")
    logging.info(f"Using Kogift text similarity threshold for fallback: {kogift_text_sim_threshold} (from config: kogift_initial_similarity_threshold)")
    
    # Print counts for debugging
    logging.info(f"Haereum images count: {len(haereum_images)}")
    logging.info(f"Kogift images count: {len(kogift_images)}")
    logging.info(f"Naver images count: {len(naver_images)}")
    logging.info(f"Using similarity_threshold for find_best_match_for_product: {similarity_threshold}")
    
    # Initialize enhanced image matcher if deep learning is enabled
    enhanced_matcher = None
    try:
        from enhanced_image_matcher import EnhancedImageMatcher
        enhanced_matcher = EnhancedImageMatcher(config)
        use_gpu = getattr(enhanced_matcher, 'use_gpu', False)
        logging.info(f"향상된 이미지 매칭을 사용합니다 (GPU: {use_gpu})")
    except Exception as e:
        logging.error(f"Error initializing EnhancedImageMatcher: {e}")
        logging.warning("Enhanced image matching not available. Using basic matching.")
    
    # Debug image paths to ensure they exist
    def check_image_existence(image_dict, source_name):
        if not image_dict:
            logging.warning(f"No {source_name} images available")
            return
            
        sample_count = min(3, len(image_dict))
        sample_keys = list(image_dict.keys())[:sample_count]
        for key in sample_keys:
            img_path = image_dict[key].get('path', key)
            exists = os.path.exists(img_path)
            logging.info(f"{source_name} sample image: {img_path} - Exists: {exists}")
    
    # Check a sample of images from each source
    check_image_existence(haereum_images, "Haereum")
    check_image_existence(kogift_images, "Kogift")
    check_image_existence(naver_images, "Naver")
    
    # Process each product
    for product_name in product_names:
        logging.debug(f"Processing product: {product_name}")
        product_tokens = product_name.split()
        
        # --- STEP 1: Hash-based filtering ---
        # Try hash-based matching first for fast and accurate matching
        haereum_match = None
        haereum_score = 0
        kogift_match = None
        kogift_score = 0
        naver_match = None
        naver_score = 0
        
        # Generate product hash for matching
        product_hash = generate_product_name_hash(product_name)
        
        if product_hash:
            logging.debug(f"Generated hash {product_hash} for product '{product_name}'")
            
            # Find hash-matching images from each source
            hash_haereum_candidates = []
            hash_kogift_candidates = []
            hash_naver_candidates = []
            
            # Check Haereum images for hash match
            for h_path, h_info in haereum_images.items():
                if h_path not in used_haereum:
                    img_hash = h_info.get('product_hash')
                    if img_hash and img_hash == product_hash:
                        hash_haereum_candidates.append((h_path, h_info))
            
            # Check Kogift images for hash match
            for k_path, k_info in kogift_images.items():
                if k_path not in used_kogift:
                    img_hash = k_info.get('product_hash')
                    if img_hash and img_hash == product_hash:
                        hash_kogift_candidates.append((k_path, k_info))
            
            # Check Naver images for hash match
            for n_path, n_info in naver_images.items():
                if n_path not in used_naver:
                    img_hash = n_info.get('product_hash')
                    if img_hash and img_hash == product_hash:
                        hash_naver_candidates.append((n_path, n_info))
            
            logging.debug(f"Hash matching found: Haereum={len(hash_haereum_candidates)}, "
                         f"Kogift={len(hash_kogift_candidates)}, Naver={len(hash_naver_candidates)}")
            
            # --- STEP 2: Image similarity check (0.8 threshold) for hash matches ---
            if enhanced_matcher and (hash_haereum_candidates or hash_kogift_candidates or hash_naver_candidates):
                logging.info(f"Applying 0.8 image similarity threshold to hash matches for '{product_name}'")
                
                # Start with Haereum as reference if available
                if hash_haereum_candidates:
                    h_path, h_info = hash_haereum_candidates[0]  # Take first hash match
                    haereum_ref_path = h_info.get('path', h_path)
                    
                    # Check Kogift hash candidates with image similarity
                    for k_path, k_info in hash_kogift_candidates:
                        kogift_img_path = k_info.get('path', k_path)
                        if os.path.exists(haereum_ref_path) and os.path.exists(kogift_img_path):
                            img_sim = enhanced_matcher.calculate_similarity(haereum_ref_path, kogift_img_path)
                            logging.debug(f"Hash+Image similarity check: Haereum vs Kogift = {img_sim:.3f}")
                            if img_sim >= 0.8:
                                kogift_match = k_path
                                kogift_score = img_sim
                                used_kogift.add(k_path)
                                logging.info(f"Hash+Image match found for Kogift: {os.path.basename(k_path)} (similarity: {img_sim:.3f})")
                                break
                    
                    # Check Naver hash candidates with image similarity
                    for n_path, n_info in hash_naver_candidates:
                        naver_img_path = n_info.get('path', n_path)
                        if os.path.exists(haereum_ref_path) and os.path.exists(naver_img_path):
                            img_sim = enhanced_matcher.calculate_similarity(haereum_ref_path, naver_img_path)
                            logging.debug(f"Hash+Image similarity check: Haereum vs Naver = {img_sim:.3f}")
                            if img_sim >= 0.8:
                                naver_match = n_path
                                naver_score = img_sim
                                used_naver.add(n_path)
                                logging.info(f"Hash+Image match found for Naver: {os.path.basename(n_path)} (similarity: {img_sim:.3f})")
                                break
                    
                    # Use the Haereum reference image
                    haereum_match = h_path
                    haereum_score = 0.95  # High score for hash match
                    used_haereum.add(h_path)
                    logging.info(f"Hash match found for Haereum: {os.path.basename(h_path)}")
                
                # If Haereum not found but Kogift or Naver found, use them as reference
                elif hash_kogift_candidates or hash_naver_candidates:
                    ref_path = None
                    if hash_kogift_candidates:
                        k_path, k_info = hash_kogift_candidates[0]
                        ref_path = k_info.get('path', k_path)
                        kogift_match = k_path
                        kogift_score = 0.95
                        used_kogift.add(k_path)
                        logging.info(f"Hash match found for Kogift: {os.path.basename(k_path)}")
                    
                    if hash_naver_candidates:
                        n_path, n_info = hash_naver_candidates[0]
                        if not ref_path:
                            ref_path = n_info.get('path', n_path)
                        naver_match = n_path
                        naver_score = 0.95
                        used_naver.add(n_path)
                        logging.info(f"Hash match found for Naver: {os.path.basename(n_path)}")
            
            # If hash matching with image similarity was successful, skip to next product
            if haereum_match or kogift_match or naver_match:
                logging.info(f"Hash-based matching successful for '{product_name}', skipping fallback methods")
                best_matches.append((
                    (haereum_match, haereum_score) if haereum_match else None,
                    (kogift_match, kogift_score) if kogift_match else None,
                    (naver_match, naver_score) if naver_match else None
                ))
                continue
        
        # --- STEP 3: Fallback to original matching logic ---
        # Reset variables for fallback matching
        haereum_match = None
        haereum_score = 0
        
        # Process Haereum images with enhanced matcher (direct image matching)
        if enhanced_matcher:
            # Try to find a direct match for product name in Haereum images
            # This assumes haereum images might have product names in the file name
            haereum_candidates = {}
            for h_path, h_info in haereum_images.items():
                # Improved candidate selection - use product code if available
                product_code = None
                
                # First check for product code in the file name
                file_name = os.path.basename(h_path)
                code_match = re.search(r'CODE(\d+)', file_name)
                if code_match:
                    product_code = code_match.group(1)
                
                # Also check product_code in the image info
                if not product_code and 'product_code' in h_info:
                    product_code = str(h_info['product_code'])
                
                # First check for exact product code match with row data if available
                if product_code and df is not None and 'Code' in df.columns:
                    try:
                        row_idx = product_names.index(product_name)
                        row_code = str(df.iloc[row_idx]['Code']) if row_idx < len(df) else None
                        if row_code and product_code == row_code:
                            # Found exact product code match - this should be the correct image
                            logging.info(f"Found exact product code match for '{product_name}': Code={product_code}")
                            haereum_candidates = {h_path: h_info}  # Use only this candidate
                            break
                    except (ValueError, IndexError, KeyError) as e:
                        logging.debug(f"Error checking product code match: {e}")

                # If we haven't found an exact code match, use the text similarity method
                if not haereum_candidates and any(token.lower() in h_info.get('name_for_matching', '').lower() for token in product_tokens if len(token) > 2):
                    haereum_candidates[h_path] = h_info
            
            logging.debug(f"Found {len(haereum_candidates)} potential Haereum matches for '{product_name}'")
            
            # Use the first Haereum candidate found or find the best text-matching one
            if haereum_candidates:
                # Sort candidates by text similarity to product name
                candidates_with_scores = []
                for path, info in haereum_candidates.items():
                    name_for_matching = info.get('name_for_matching', '')
                    text_sim = calculate_text_similarity(product_name, name_for_matching)
                    candidates_with_scores.append((path, info, text_sim))
                
                # Sort by text similarity
                candidates_with_scores.sort(key=lambda x: x[2], reverse=True)
                
                # Take the best matching candidate
                best_candidate_path = candidates_with_scores[0][0]
                best_candidate_info = candidates_with_scores[0][1]
                
                # Use this Haereum image path for further matching
                haereum_match = best_candidate_path
                haereum_score = candidates_with_scores[0][2]  # Text similarity score
                
                logging.info(f"Selected Haereum image for '{product_name}': {os.path.basename(haereum_match)} (text similarity: {haereum_score:.3f})")
        
        # Now, try to find matching Kogift and Naver images
        kogift_match = None
        kogift_score = 0
        naver_match = None
        naver_score = 0
        
        # If we have a Haereum match and enhanced_matcher, use image-based matching
        if haereum_match and enhanced_matcher:
            # Get the actual file path from the haereum match info
            haereum_path = haereum_images[haereum_match].get('path', haereum_match)
            
            # First try with enhanced matcher (direct image comparison)
            logging.info(f"Using enhanced image matcher to find Kogift match for '{product_name}'")
            kogift_result = find_best_match_with_enhanced_matcher(
                haereum_path, kogift_images, used_kogift, enhanced_matcher)
                
            if kogift_result:
                kogift_match, kogift_score = kogift_result
                used_kogift.add(kogift_match)
                logging.info(f"Found Kogift match for '{product_name}': {os.path.basename(kogift_match)} (score: {kogift_score:.3f})")
            else:
                logging.info(f"No Kogift match found for '{product_name}' with enhanced matcher")
                
            # Try to find Naver match using enhanced matcher
            logging.info(f"Using enhanced image matcher to find Naver match for '{product_name}'")
            naver_result = find_best_match_with_enhanced_matcher(
                haereum_path, naver_images, used_naver, enhanced_matcher)
                
            if naver_result:
                naver_match, naver_score = naver_result
                used_naver.add(naver_match)
                logging.info(f"Found Naver match for '{product_name}': {os.path.basename(naver_match)} (score: {naver_score:.3f})")
            else:
                logging.info(f"No Naver match found for '{product_name}' with enhanced matcher")
        else:
            # If no Haereum match or no enhanced matcher, log but continue with text-based matching
            if not haereum_match:
                logging.info(f"No Haereum match found for '{product_name}'. Will still attempt Kogift/Naver matching.")
            elif not enhanced_matcher:
                logging.info(f"Enhanced matcher not available for '{product_name}'. Will use text-based matching.")
        
        # ALWAYS try fallback text-based matching for Kogift if no match with enhanced matcher
        if not kogift_match:
            logging.info(f"Trying fallback text-based matching for Kogift images for '{product_name}'")
            kogift_result = find_best_match_for_product(
                product_tokens, kogift_images, used_kogift, 
                kogift_text_sim_threshold, "Kogift_Direct", config) # Use Kogift specific threshold
                
            if kogift_result:
                kogift_match, kogift_score = kogift_result
                used_kogift.add(kogift_match)

        # ALWAYS try fallback text-based matching for Naver if no match with enhanced matcher
        if not naver_match:
            logging.info(f"Trying fallback text-based matching for Naver images for '{product_name}'")
            naver_result = find_best_match_for_product(
                product_tokens, naver_images, used_naver, 
                naver_text_sim_threshold, "Naver_Direct", config) # Use Naver specific threshold
                
            if naver_result:
                naver_match, naver_score = naver_result
                used_naver.add(naver_match)
        
        # If there is no Haereum match, try to find one using any available match as a reference
        if not haereum_match and (kogift_match or naver_match) and enhanced_matcher:
            source_img_path = None
            
            # Use the best available match as source
            if kogift_match and naver_match:
                # Use the one with higher score
                if kogift_score > naver_score:
                    source_img_path = kogift_images[kogift_match].get('path', kogift_match)
                    source_name = "Kogift"
                else:
                    source_img_path = naver_images[naver_match].get('path', naver_match)
                    source_name = "Naver"
            elif kogift_match:
                source_img_path = kogift_images[kogift_match].get('path', kogift_match)
                source_name = "Kogift"
            elif naver_match:
                source_img_path = naver_images[naver_match].get('path', naver_match)
                source_name = "Naver"
                
            if source_img_path:
                logging.info(f"Trying to find Haereum match using {source_name} image as reference for '{product_name}'")
                haereum_result = find_best_match_with_enhanced_matcher(
                    source_img_path, haereum_images, used_haereum, enhanced_matcher)
                    
                if haereum_result:
                    haereum_match, haereum_score = haereum_result
                    used_haereum.add(haereum_match)
                    logging.info(f"Found Haereum match using {source_name} reference for '{product_name}': {os.path.basename(haereum_match)} (score: {haereum_score:.3f})")
                    
        # Fallback to full text search for Haereum if still not found (try to find any match)
        if not haereum_match:
            logging.info(f"Trying fallback text-based matching for Haereum images for '{product_name}'")
            # Use a very low threshold to increase chances of finding any match
            haereum_text_sim_threshold = 0.05  # Very low threshold as Haereum should always have a match
            haereum_result = find_best_match_for_product(
                product_tokens, haereum_images, used_haereum, 
                haereum_text_sim_threshold, "Haereum_Fallback", config)
                
            if haereum_result:
                haereum_match, haereum_score = haereum_result
                used_haereum.add(haereum_match)
                logging.info(f"Found Haereum match with text fallback for '{product_name}': {os.path.basename(haereum_match)} (score: {haereum_score:.3f})")
            else:
                # If still no match, try to get any random unused Haereum image as a last resort
                logging.warning(f"No Haereum match found for '{product_name}' even with text fallback. Attempting to assign any available Haereum image.")
                available_haereum = [path for path in haereum_images if path not in used_haereum]
                if available_haereum:
                    haereum_match = available_haereum[0]  # Just take the first available
                    haereum_score = 0.01  # Very low score to indicate this is a desperate assignment
                    used_haereum.add(haereum_match)
                    logging.warning(f"Assigned random unused Haereum image to '{product_name}': {os.path.basename(haereum_match)} (desperate assignment)")
        
        # Add the best matches to the result list
        best_matches.append((
            (haereum_match, haereum_score) if haereum_match else None,
            (kogift_match, kogift_score) if kogift_match else None,
            (naver_match, naver_score) if naver_match else None
        ))
        
    return best_matches

def find_best_match_for_product(product_tokens: List[str], 
                               image_info: Dict[str, Dict], 
                               used_images: Set[str] = None,
                               similarity_threshold: float = 0.45,  # 임계값 상향 조정 (0.3에서 0.45으로)
                               source_name_for_log: str = "UnknownSource",
                               config: Optional[configparser.ConfigParser] = None) -> Optional[Tuple[str, float]]:
    """
    Find the best matching image for a product based on name tokens.
    Updated with higher thresholds for stricter matching.
    
    Args:
        product_tokens: Tokens of the product name
        image_info: Dictionary of image metadata
        used_images: Set of already used image paths
        similarity_threshold: Minimum similarity score for matching
        source_name_for_log: Source name for logging
        config: Configuration object for retrieving settings
        
    Returns:
        Tuple of (best_match_path, similarity_score) or None if no match found
    """
    if not product_tokens:
        return None
        
    if used_images is None:
        used_images = set()
        
    best_match_path = None
    best_match_score = 0
    
    # Use the similarity_threshold passed as argument directly.
    # This threshold is expected to be set by the caller (find_best_image_matches)
    # and should be appropriate for text-based similarity.
    effective_similarity_threshold = similarity_threshold

    # Log which threshold is being used.
    logging.info(f"[{source_name_for_log}] Using text similarity threshold: {effective_similarity_threshold} (passed from caller)")
    
    # Log the number of images we're searching through
    logging.info(f"[{source_name_for_log}] Searching through {len(image_info)} images for a match")

    for img_path, img_data in image_info.items():
        # Skip if already used
        if img_path in used_images:
            continue
            
        # Get the name for matching from metadata
        if 'name_for_matching' in img_data:
            img_name = img_data['name_for_matching']
        elif 'original_name' in img_data:
            img_name = img_data['original_name']
        else:
            # Use the filename if no metadata is available
            img_name = os.path.basename(img_path)
        
        # Convert to string and calculate text similarity
        img_name_str = str(img_name)
        product_name_str = ' '.join(product_tokens)
        
        similarity = calculate_text_similarity(product_name_str, img_name_str)
        
        if similarity > best_match_score:
            best_match_score = similarity
            best_match_path = img_path
    
    # Check threshold - use the minimum threshold for extreme lenience
    if best_match_score >= effective_similarity_threshold:
        if best_match_path:
            img_name = image_info[best_match_path].get('original_name', os.path.basename(best_match_path))
            logging.info(f"{source_name_for_log}: Best match for '{' '.join(product_tokens)}': '{img_name}' with score {best_match_score:.3f}")
            return best_match_path, best_match_score
    elif best_match_path:  # We found a match but score is below threshold
        img_name = image_info[best_match_path].get('original_name', os.path.basename(best_match_path))
        logging.info(f"{source_name_for_log}: Found match below threshold. Product: '{' '.join(product_tokens)}', Image: '{img_name}', Score: {best_match_score:.3f} (threshold: {effective_similarity_threshold})")
    
    # No match found with sufficient similarity
    logging.info(f"No match found above threshold {effective_similarity_threshold} for {source_name_for_log}. Trying basic token matching.")
    
    # Try more basic matching as fallback
    for img_path, img_data in image_info.items():
        # Skip if already used
        if img_path in used_images:
            continue
            
        # Get image name from metadata
        if 'name_for_matching' in img_data:
            img_name = img_data['name_for_matching']
        elif 'original_name' in img_data:
            img_name = img_data['original_name']
        else:
            img_name = os.path.basename(img_path)
            
        # Convert to lowercase for case-insensitive matching
        img_name_lower = str(img_name).lower()
        product_name_lower = ' '.join(product_tokens).lower()
        
        # 1. 우선 전체 상품명의 일부가 이미지 이름에 포함되어 있는지 확인
        basic_match_score = 0.0
        if len(product_name_lower) >= 4 and product_name_lower[:4] in img_name_lower:
            basic_match_score = 0.4
            logging.info(f"{source_name_for_log}: Product name prefix match found: '{product_name_lower[:4]}' in '{img_name}'")
            return img_path, basic_match_score
        
        # 2. 개별 토큰 매칭 (길이가 2 이상인 중요 토큰)
        matched_tokens = []
        for token in product_tokens:
            if len(token) >= 2 and token.lower() in img_name_lower:
                matched_tokens.append(token)
        
        # 매칭된 토큰 수에 따라 점수 계산
        if matched_tokens:
            # 토큰 길이에 따라 가중치 적용
            token_weight = sum(len(token) for token in matched_tokens) / sum(len(token) for token in product_tokens)
            # 토큰 개수에 따라 가중치 적용
            count_weight = len(matched_tokens) / len(product_tokens)
            # 최종 점수 계산 (길이와 개수를 모두 고려)
            basic_match_score = 0.3 * token_weight + 0.2 * count_weight
            
            # 임계값을 0.05로 설정하여 매칭을 허용
            if basic_match_score >= 0.05:
                logging.info(f"{source_name_for_log}: Basic token match found: '{matched_tokens}' in '{img_name}' with score {basic_match_score:.3f}")
                return img_path, basic_match_score
    
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
                        
                        image_data = {
                            'url': current_naver_image_url, 
                            'local_path': naver_image_info_from_metadata.get('path', naver_path),
                            'source': 'naver',
                            'product_name': product_name,
                            'similarity': naver_score,  
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
            logging.warning(f"All Haereum images have been used. Reusing images for remaining rows.")
        
        for idx in range(len(result_df)):
            if idx >= len(result_df):
                continue
                
            # Check if this row is missing a Haereum image
            if not isinstance(result_df.at[idx, '본사 이미지'], dict):
                product_name = result_df.at[idx, '상품명'] if '상품명' in result_df.columns else f"Row {idx+1}"
                logging.warning(f"Row {idx} ('{product_name}'): Missing Haereum image. Assigning random image.")
                
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
            
            # Create the Excel file with images
            create_excel_with_images(result_df, excel_output)
            logger.info(f"Created Excel output file with images: {excel_output}")
        except Exception as e:
            logger.error(f"Error creating Excel output: {e}")
    
    return result_df

def filter_images_by_similarity(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """
    Filter images based on similarity scores and URL validity.
    
    Args:
        df: DataFrame containing image data
        config: Configuration settings
        
    Returns:
        Filtered DataFrame
    """
    logger.info("Filtering images based on similarity scores...")
    
    # Get similarity threshold from config
    try:
        similarity_threshold = config.getfloat('ImageFiltering', 'similarity_threshold', fallback=0.4)
    except (configparser.NoSectionError, configparser.NoOptionError):
        similarity_threshold = 0.4  # Default threshold
    
    # Create a copy of the DataFrame to avoid modifying the original
    filtered_df = df.copy()
    
    # Process each row
    for idx in range(len(filtered_df)):
        # Check each image source
        for col_name in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
            if col_name in filtered_df.columns:
                image_data = filtered_df.at[idx, col_name]
                
                # Skip if no image data
                if not isinstance(image_data, dict):
                    continue
                
                # Get similarity score
                score = image_data.get('score', 0.0)
                
                # Filter out low similarity scores
                if score < similarity_threshold:
                    logger.info(f"Filtering out {col_name} for row {idx} due to low similarity score: {score:.3f}")
                    filtered_df.at[idx, col_name] = None
    
    return filtered_df

def create_excel_with_images(df: pd.DataFrame, output_file: str):
    """
    Create an Excel file with embedded images.
    
    Args:
        df: DataFrame containing image data
        output_file: Path to output Excel file
    """
    logger.info(f"Creating Excel file with images: {output_file}")
    
    # Create a new Excel writer
    writer = pd.ExcelWriter(output_file, engine='openpyxl')
    
    # Write the DataFrame to Excel
    df.to_excel(writer, index=False, sheet_name='Images')
    
    # Get the worksheet
    worksheet = writer.sheets['Images']
    
    # Process each row
    for idx in range(len(df)):
        row_num = idx + 2  # Excel rows start at 1, and we have a header row
        
        # Process each image column
        for col_name in ['본사 이미지', '고려기프트 이미지', '네이버 이미지']:
            if col_name in df.columns:
                image_data = df.at[idx, col_name]
                
                # Skip if no image data
                if not isinstance(image_data, dict):
                    continue
                
                # Get image path
                image_path = image_data.get('local_path')
                if not image_path or not os.path.exists(str(image_path)):
                    continue
                
                # Add image to Excel
                try:
                    img = Image.open(str(image_path))
                    img_width, img_height = img.size
                    
                    # Resize image if too large
                    max_width = 200
                    if img_width > max_width:
                        ratio = max_width / img_width
                        img_width = max_width
                        img_height = int(img_height * ratio)
                    
                    # Create image cell
                    cell = worksheet.cell(row=row_num, column=df.columns.get_loc(col_name) + 1)
                    cell.value = f"Image: {os.path.basename(str(image_path))}"
                    
                    # Add image
                    img = openpyxl.drawing.image.Image(str(image_path))
                    img.width = img_width
                    img.height = img_height
                    worksheet.add_image(img, cell.coordinate)
                    
                except Exception as e:
                    logger.error(f"Error adding image to Excel: {e}")
    
    # Save the Excel file
    writer.close()
    logger.info(f"Excel file created successfully: {output_file}")

def calculate_text_similarity(text1: str, text2: str) -> float:
    """
    Calculate text similarity between two strings.
    Uses a combination of Levenshtein distance, token overlap, and character n-gram matching.
    """
    # Convert to strings if needed
    str1 = str(text1).lower()
    str2 = str(text2).lower()
    
    # Handle empty strings
    if not str1 or not str2:
        return 0.0
        
    try:
        # Try to use Levenshtein distance if available
        try:
            from Levenshtein import ratio
            lev_ratio = ratio(str1, str2)
        except ImportError:
            # Fallback to a basic similarity measure
            lev_ratio = len(set(str1) & set(str2)) / max(len(set(str1)), len(set(str2)))
        
        # Calculate token overlap
        tokens1 = set(str1.split())
        tokens2 = set(str2.split())
        
        # If either set is empty, default to character-based ratio
        if not tokens1 or not tokens2:
            return lev_ratio
            
        # Calculate Jaccard similarity coefficient
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        if not union:
            return 0.0
            
        jaccard = len(intersection) / len(union)
        
        # Character n-gram matching (더 관대한 매칭을 위해 추가)
        # 2-gram과 3-gram 매칭 계산
        ngram_similarity = 0.0
        
        # 2-gram 매칭
        ngrams1_2 = set(str1[i:i+2] for i in range(len(str1)-1))
        ngrams2_2 = set(str2[i:i+2] for i in range(len(str2)-1))
        
        if ngrams1_2 and ngrams2_2:
            ngram_intersection_2 = ngrams1_2.intersection(ngrams2_2)
            ngram_union_2 = ngrams1_2.union(ngrams2_2)
            if ngram_union_2:
                ngram2_sim = len(ngram_intersection_2) / len(ngram_union_2)
                ngram_similarity += ngram2_sim
        
        # 3-gram 매칭 (더 긴 문자열 패턴 매칭)
        if len(str1) >= 3 and len(str2) >= 3:
            ngrams1_3 = set(str1[i:i+3] for i in range(len(str1)-2))
            ngrams2_3 = set(str2[i:i+3] for i in range(len(str2)-2))
            
            if ngrams1_3 and ngrams2_3:
                ngram_intersection_3 = ngrams1_3.intersection(ngrams2_3)
                ngram_union_3 = ngrams1_3.union(ngrams2_3)
                if ngram_union_3:
                    ngram3_sim = len(ngram_intersection_3) / len(ngram_union_3)
                    ngram_similarity += ngram3_sim
        
        # Normalize n-gram similarity (if both n-grams used)
        ngram_similarity = ngram_similarity / 2 if len(str1) >= 3 and len(str2) >= 3 else ngram_similarity
        
        # Check for exact substring matches (부분 문자열 일치 확인)
        # 길이가 3 이상인 토큰이 다른 문자열에 포함되어 있으면 보너스 점수
        substring_bonus = 0.0
        for token in tokens1:
            if len(token) >= 3 and token in str2:
                substring_bonus = max(substring_bonus, 0.15)  # 최대 0.15 보너스
                break
                
        for token in tokens2:
            if len(token) >= 3 and token in str1:
                substring_bonus = max(substring_bonus, 0.15)  # 최대 0.15 보너스
                break
        
        # Weighted average of all similarity measures
        # 가중치 조정으로 더 관대한 매칭 허용
        combined_similarity = 0.2 * lev_ratio + 0.4 * jaccard + 0.25 * ngram_similarity + substring_bonus
        
        # 너무 낮은 점수일 경우 최소값으로 조정 (완전히 관련 없는 항목도 있을 수 있으므로)
        return max(combined_similarity, 0.01)  # 최소 0.01의 유사도 반환
        
    except Exception as e:
        logging.error(f"Error calculating text similarity: {e}")
        return 0.0

def extract_product_hash_from_filename(filename: str) -> Optional[str]:
    """
    파일명에서 16자리 상품명 해시값을 추출합니다.
        
    파일명 패턴:
    - prefix_[16자해시]_[8자랜덤].jpg (예: haereum_1234567890abcdef_12345678.jpg)
    - prefix_[16자해시].jpg
        
    Args:
        filename: 이미지 파일명
            
    Returns:
        16자리 상품명 해시값 또는 None
    """
    try:
        # 확장자 제거
        name_without_ext = os.path.splitext(os.path.basename(filename))[0]
        
        # '_'로 분리
        parts = name_without_ext.split('_')
        
        # prefix_hash_random 또는 prefix_hash 패턴 확인
        if len(parts) >= 2:
            # prefix를 제거하고 두 번째 부분이 16자리 해시인지 확인
            potential_hash = parts[1]
            if len(potential_hash) == 16 and all(c in '0123456789abcdef' for c in potential_hash.lower()):
                return potential_hash.lower()
        
        # 전체가 16자리 해시인 경우도 확인 (prefix가 없는 경우)
        if len(name_without_ext) == 16 and all(c in '0123456789abcdef' for c in name_without_ext.lower()):
            return name_without_ext.lower()
                    
        return None
    except Exception as e:
        logging.debug(f"Error extracting hash from filename {filename}: {e}")
        return None

def generate_product_name_hash(product_name: str) -> str:
    """
    상품명으로부터 16자리 해시값을 생성합니다.
        
    Args:
        product_name: 상품명
            
    Returns:
        16자리 해시값
    """
    try:
        # 상품명 정규화 (공백 제거, 소문자 변환)
        normalized_name = ''.join(product_name.split()).lower()
        # MD5 해시 생성 후 첫 16자리 사용
        hash_obj = hashlib.md5(normalized_name.encode('utf-8'))
        return hash_obj.hexdigest()[:16]
    except Exception as e:
        logging.error(f"Error generating hash for product name {product_name}: {e}")
        return ""

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