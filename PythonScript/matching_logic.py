import os
import logging
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import numpy as np
from PIL import Image
import tensorflow as tf # Keep TF import for image similarity within the class
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError, as_completed
import time
import configparser # Import configparser
from typing import Dict, Any, Optional, Tuple, List, Union # Add List to imports
from collections import OrderedDict # 추가: LRU 캐시 구현을 위한 OrderedDict
import psutil # 메모리 모니터링 (필요시 pip install psutil 설치 필요)
import json  # 영구 캐시 파일용
import datetime # 캐시 만료일 관리
import pickle # 특성 저장
import shutil # 디렉토리 관리
import hashlib # 파일 해시

# Import the enhanced image matcher if available
try:
    from enhanced_image_matcher import EnhancedImageMatcher
    ENHANCED_MATCHER_AVAILABLE = True
    logging.info("Enhanced image matcher imported successfully")
except ImportError:
    ENHANCED_MATCHER_AVAILABLE = False
    logging.warning("Enhanced image matcher not available, falling back to basic image similarity")

# --- Global variable for ProcessPoolExecutor worker ---
# This needs careful handling during refactoring. It might be better managed
# within the match_products function or passed differently.
# For now, keep it here, associated with the worker initializer.
worker_matcher_instance = None

def _init_worker_matcher(config: configparser.ConfigParser):
    """Initializer for ProcessPoolExecutor workers. Accepts ConfigParser."""
    global worker_matcher_instance
    pid = os.getpid()
    logging.info(f"Initializing ProductMatcher in worker process {pid}...")
    
    # Set lower TensorFlow memory growth to avoid OOM issues
    try:
        gpus = tf.config.experimental.list_physical_devices('GPU')
        if gpus:
            # Memory growth must be set before GPUs have been initialized
            for gpu in gpus:
                tf.config.experimental.set_memory_growth(gpu, True)
            logging.info(f"Worker {pid}: Set TensorFlow GPU memory growth")
    except Exception as e:
        logging.warning(f"Worker {pid}: Failed to configure TensorFlow GPU memory: {e}")
    
    # Initialize ProductMatcher with retry
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            worker_matcher_instance = ProductMatcher(config) # Pass ConfigParser
            if worker_matcher_instance.text_model is None or worker_matcher_instance.image_model is None:
                logging.error(f"Worker {pid}: Failed to load models during initialization.")
                retry_count += 1
                if retry_count < max_retries:
                    logging.info(f"Worker {pid}: Retrying initialization ({retry_count}/{max_retries})...")
                    time.sleep(2)  # Short delay before retry
                    continue
                else:
                    raise RuntimeError("Model loading failed in worker initializer after retries.")
            
            logging.info(f"ProductMatcher initialized successfully in worker process {pid}.")
            break  # Success, exit the retry loop
            
        except Exception as e:
            logging.error(f"Error initializing ProductMatcher in worker {pid}: {e}", exc_info=True)
            retry_count += 1
            if retry_count < max_retries:
                logging.info(f"Worker {pid}: Retrying initialization ({retry_count}/{max_retries})...")
                time.sleep(2)  # Short delay before retry
            else:
                logging.critical(f"Worker {pid}: Failed to initialize after {max_retries} attempts.")
                worker_matcher_instance = None

# --- 전역 상수 ---
CACHE_VERSION = "1.0.0"  # 캐시 형식이 변경되면 버전 올림

class FeatureCache:
    """이미지 특성 캐시 관리 클래스 (메모리 + 디스크)"""
    
    def __init__(self, config: configparser.ConfigParser):
        """캐시 초기화"""
        self.config = config
        self.memory_cache = OrderedDict()
        self.max_memory_items = config.getint('Matching', 'max_cache_size', fallback=1000)
        self.use_persistent_cache = config.getboolean('Matching', 'use_persistent_cache', fallback=False)
        self.cache_dir = config.get('Paths', 'cached_features_dir', fallback=os.path.join(config.get('Paths', 'temp_dir', fallback='./temp'), 'image_features'))
        self.cache_expiry_days = config.getint('Matching', 'cache_expiry_days', fallback=30)
        
        # 영구 캐시 디렉토리 생성
        if self.use_persistent_cache:
            os.makedirs(self.cache_dir, exist_ok=True)
            self._clean_expired_cache()
    
    def _get_cache_filename(self, img_path: str) -> str:
        """이미지 경로에서 캐시 파일 이름 생성"""
        # 이미지 수정 시간과 크기 정보 포함하여 더 정확한 캐시
        try:
            img_stat = os.stat(img_path)
            mtime = img_stat.st_mtime
            size = img_stat.st_size
            hash_input = f"{img_path}_{mtime}_{size}"
        except:
            hash_input = img_path
            
        hash_val = hashlib.md5(hash_input.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hash_val}.pkl")
    
    def _clean_expired_cache(self) -> None:
        """만료된 캐시 파일 정리"""
        if not self.use_persistent_cache:
            return
            
        try:
            now = datetime.datetime.now()
            expiry_delta = datetime.timedelta(days=self.cache_expiry_days)
            count_removed = 0
            
            for file in os.listdir(self.cache_dir):
                if file.endswith('.pkl'):
                    file_path = os.path.join(self.cache_dir, file)
                    file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    if now - file_time > expiry_delta:
                        os.remove(file_path)
                        count_removed += 1
            
            if count_removed > 0:
                logging.info(f"Cleaned {count_removed} expired cache files.")
        except Exception as e:
            logging.error(f"Error cleaning cache: {e}")
    
    def get(self, img_path: str) -> Optional[np.ndarray]:
        """캐시에서 특성 가져오기"""
        # 메모리 캐시 확인
        if img_path in self.memory_cache:
            # LRU 갱신
            features = self.memory_cache.pop(img_path)
            self.memory_cache[img_path] = features
            return features
        
        # 디스크 캐시 확인
        if self.use_persistent_cache:
            cache_file = self._get_cache_filename(img_path)
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'rb') as f:
                        features = pickle.load(f)
                    # 메모리 캐시에도 추가
                    self.put(img_path, features)
                    return features
                except Exception as e:
                    logging.debug(f"Failed to load cache for {img_path}: {e}")
        
        return None
    
    def put(self, img_path: str, features: np.ndarray) -> None:
        """특성을 캐시에 저장"""
        # 메모리 캐시 업데이트
        if len(self.memory_cache) >= self.max_memory_items:
            self.memory_cache.popitem(last=False)  # 가장 오래전에 사용된 항목 제거
        self.memory_cache[img_path] = features
        
        # 디스크 캐시 업데이트
        if self.use_persistent_cache:
            cache_file = self._get_cache_filename(img_path)
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(features, f)
            except Exception as e:
                logging.debug(f"Failed to save cache for {img_path}: {e}")

class MatchQualityEvaluator:
    """매칭 품질 평가 및 분류 클래스"""
    
    def __init__(self, config: configparser.ConfigParser):
        """매칭 품질 평가 설정 초기화"""
        self.high_threshold = config.getfloat('MatchQualityThresholds', 'high_quality', fallback=0.85)
        self.medium_threshold = config.getfloat('MatchQualityThresholds', 'medium_quality', fallback=0.70)
        self.low_threshold = config.getfloat('MatchQualityThresholds', 'low_quality', fallback=0.50)
        self.reject_threshold = config.getfloat('MatchQualityThresholds', 'reject_threshold', fallback=0.40)
    
    def evaluate_match(self, combined_score: float) -> str:
        """매칭 품질 레벨 반환"""
        if combined_score >= self.high_threshold:
            return "high"
        elif combined_score >= self.medium_threshold:
            return "medium"
        elif combined_score >= self.low_threshold:
            return "low"
        elif combined_score >= self.reject_threshold:
            return "questionable"
        else:
            return "rejected"
    
    def apply_quality_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터프레임에 품질 레이블 적용"""
        if df.empty:
            return df
            
        # 고려 매칭 품질
        if '_고려_Combined' in df.columns:
            df['고려_매칭품질'] = df['_고려_Combined'].apply(
                lambda x: self.evaluate_match(x) if pd.notna(x) else "none"
            )
            
        # 네이버 매칭 품질
        if '_네이버_Combined' in df.columns:
            df['네이버_매칭품질'] = df['_네이버_Combined'].apply(
                lambda x: self.evaluate_match(x) if pd.notna(x) else "none"
            )
            
        return df

# --- Product Matching Logic ---
class ProductMatcher:
    def __init__(self, config: configparser.ConfigParser):
        """Initialize ProductMatcher with configuration"""
        self.config = config
        
        # Enhanced image matching settings
        self.use_enhanced_matcher = config.getboolean('Matching', 'use_enhanced_matcher', fallback=ENHANCED_MATCHER_AVAILABLE)
        self.use_gpu = config.getboolean('Matching', 'use_gpu', fallback=False)
        
        # Text similarity settings
        self.text_model_path = config.get('Paths', 'text_model_path', fallback='sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
        self.text_similarity_threshold = config.getfloat('Matching', 'text_similarity_threshold', fallback=0.7)
        
        # Image similarity settings
        self.image_resize_dimension = config.getint('Matching', 'image_resize_dimension', fallback=224)
        self.image_similarity_threshold = config.getfloat('Matching', 'image_similarity_threshold', fallback=0.7)
        self.skip_image_if_size_exceeds_mb = config.getint('Matching', 'skip_image_if_size_exceeds_mb', fallback=0)
        self.image_similarity_timeout_sec = config.getfloat('Matching', 'image_similarity_timeout_sec', fallback=10.0)
        
        # Weighting for combined text/image similarity
        self.text_weight = config.getfloat('Matching', 'text_weight', fallback=0.7)
        self.image_weight = config.getfloat('Matching', 'image_weight', fallback=0.3)
        self.price_weight = config.getfloat('Matching', 'price_weight', fallback=0.1)
        self.use_price_similarity = config.getboolean('Matching', 'use_price_similarity', fallback=False)
        self.minimum_combined_score = config.getfloat('Matching', 'minimum_combined_score', fallback=0.4)
        
        # Category-specific thresholds
        self.use_category_thresholds = True
        self.category_thresholds = self._load_category_thresholds(config)
        
        # Image feature caching
        self.feature_cache = FeatureCache(config)
        
        # Initialize models
        self.text_model = None
        self.image_model = None
        
        # GPU settings for TensorFlow
        if not self.use_gpu:
            try:
                tf.config.set_visible_devices([], 'GPU')
                logging.info("GPU disabled for TensorFlow")
            except Exception as e:
                logging.warning(f"Failed to disable GPU: {e}")
        
        # Initialize sentence transformer model
        self._initialize_text_model()
        
        # Initialize EfficientNetB0 model (if enhanced matcher isn't being used or as fallback)
        if not self.use_enhanced_matcher or not ENHANCED_MATCHER_AVAILABLE:
            self._initialize_image_model()
        elif ENHANCED_MATCHER_AVAILABLE:
            # When using enhanced matcher, we'll defer image model loading until needed
            logging.info("Using enhanced image matcher, deferring basic image model initialization")
            # The enhanced matcher will be initialized on demand in calculate_image_similarity

    def __del__(self):
        """Cleanup method to release resources when the instance is being destroyed."""
        try:
            # Clear TensorFlow session to free GPU memory
            tf.keras.backend.clear_session()
            logging.debug("TensorFlow session cleared in ProductMatcher destructor")
        except Exception as e:
            logging.warning(f"Error during TensorFlow cleanup: {e}")
            
        # Explicitly delete large objects
        self.text_model = None
        self.image_model = None

    def get_thresholds_for_category(self, category: Optional[str]) -> Tuple[float, float]:
        """카테고리에 따라 적절한 임계값을 반환"""
        if not self.use_category_thresholds or not category or category not in self.category_thresholds:
            return self.text_similarity_threshold, self.image_similarity_threshold
            
        cat_thresholds = self.category_thresholds[category]
        return cat_thresholds['text_threshold'], cat_thresholds['image_threshold']

    def calculate_text_similarity(self, text1: Optional[str], text2: Optional[str]) -> float:
        """Calculate text similarity between two product names."""
        if not self.text_model:
            logging.warning("Text model not loaded, cannot calculate text similarity.")
            return 0.0
        if not text1 or not text2 or pd.isna(text1) or pd.isna(text2):
            return 0.0

        try:
            text1_str = str(text1)
            text2_str = str(text2)
            embedding1 = self.text_model.encode(text1_str, convert_to_tensor=True)
            embedding2 = self.text_model.encode(text2_str, convert_to_tensor=True)
            similarity = util.cos_sim(embedding1, embedding2).item()
            final_similarity = max(0.0, min(1.0, similarity)) # Ensure score is within [0, 1]
            logging.debug(f"Text Similarity('{text1_str}', '{text2_str}') = {final_similarity:.4f}")
            return final_similarity
        except Exception as e:
            logging.error(f"Error calculating text similarity between '{text1}' and '{text2}': {e}", exc_info=True)
            return 0.0

    def calculate_image_similarity(self, img_path1: Optional[str], img_path2: Optional[str]) -> float:
        """Calculate image similarity between two product images."""
        if not self.image_model:
             logging.warning("Image model not loaded, cannot calculate image similarity.")
             return 0.0
             
        # Early return if any path is invalid
        if not img_path1 or not img_path2:
            logging.debug(f"Image path missing, cannot calculate similarity: {img_path1}, {img_path2}")
            return 0.0
            
        # Check if files exist
        if not os.path.exists(img_path1) or not os.path.isfile(img_path1):
            logging.debug(f"First image path does not exist or is not a file: {img_path1}")
            return 0.0
            
        if not os.path.exists(img_path2) or not os.path.isfile(img_path2):
            logging.debug(f"Second image path does not exist or is not a file: {img_path2}")
            return 0.0
            
        # Check file size if configured to skip large images
        if self.skip_image_if_size_exceeds_mb > 0:
            try:
                if os.path.getsize(img_path1) > (self.skip_image_if_size_exceeds_mb * 1024 * 1024):
                    logging.warning(f"Skipping oversized image: {img_path1}")
                    return 0.0
                if os.path.getsize(img_path2) > (self.skip_image_if_size_exceeds_mb * 1024 * 1024):
                    logging.warning(f"Skipping oversized image: {img_path2}")
                    return 0.0
            except Exception as e:
                logging.error(f"Error checking image size: {e}")

        # Use enhanced matcher if available
        if ENHANCED_MATCHER_AVAILABLE:
            try:
                # Create enhanced matcher instance on demand if needed
                if not hasattr(self, 'enhanced_matcher'):
                    self.enhanced_matcher = EnhancedImageMatcher(use_gpu=self.use_gpu)
                    logging.info("Initialized enhanced image matcher")
                
                # Get combined similarity (which is a weighted average of SIFT, AKAZE, and EfficientNet)
                start_time = time.time()
                custom_weights = {
                    'sift': 0.3, 
                    'akaze': 0.2, 
                    'deep': 0.5
                }
                
                # Get combined score and individual scores
                combined_score, scores = self.enhanced_matcher.calculate_combined_similarity(
                    img_path1, 
                    img_path2, 
                    weights=custom_weights
                )
                
                duration = time.time() - start_time
                logging.debug(f"Enhanced matching - SIFT: {scores['sift']:.4f}, AKAZE: {scores['akaze']:.4f}, " 
                             f"Deep: {scores['deep']:.4f}, Combined: {combined_score:.4f} (took {duration:.2f}s)")
                
                return combined_score
            except Exception as e:
                logging.warning(f"Enhanced matcher failed: {e}. Falling back to basic similarity")
                # Continue to basic similarity method if enhanced matcher fails
        
        # Basic similarity calculation with EfficientNetB0
        try:
            # 이미지 처리 타임아웃 적용
            start_time = time.time()
            
            # Get features from cache or calculate them
            features1 = self.feature_cache.get(img_path1)
            if features1 is None:
                img1 = tf.keras.preprocessing.image.load_img(img_path1, target_size=(self.image_resize_dimension, self.image_resize_dimension))
                img1_array = tf.keras.preprocessing.image.img_to_array(img1)
                img1_batch = tf.expand_dims(img1_array, 0)
                img1_preprocessed = tf.keras.applications.efficientnet.preprocess_input(img1_batch)
                features1 = self.image_model(img1_preprocessed).numpy().flatten()
                self.feature_cache.put(img_path1, features1)
                
            # 타임아웃 체크
            if self.image_similarity_timeout_sec > 0 and (time.time() - start_time) > self.image_similarity_timeout_sec:
                logging.warning(f"Image similarity timeout after first feature extraction: {img_path1}")
                return 0.0
                
            features2 = self.feature_cache.get(img_path2)
            if features2 is None:
                img2 = tf.keras.preprocessing.image.load_img(img_path2, target_size=(self.image_resize_dimension, self.image_resize_dimension))
                img2_array = tf.keras.preprocessing.image.img_to_array(img2)
                img2_batch = tf.expand_dims(img2_array, 0)
                img2_preprocessed = tf.keras.applications.efficientnet.preprocess_input(img2_batch)
                features2 = self.image_model(img2_preprocessed).numpy().flatten()
                self.feature_cache.put(img_path2, features2)

            # 타임아웃 체크
            if self.image_similarity_timeout_sec > 0 and (time.time() - start_time) > self.image_similarity_timeout_sec:
                logging.warning(f"Image similarity timeout after second feature extraction: {img_path2}")
                return 0.0

            # Calculate cosine similarity
            norm1 = np.linalg.norm(features1)
            norm2 = np.linalg.norm(features2)
            
            # Check for zero norms to avoid division by zero
            if norm1 == 0 or norm2 == 0:
                logging.warning(f"Zero norm detected for image features, returning 0 similarity: {img_path1}, {img_path2}")
                return 0.0
                
            similarity = np.dot(features1, features2) / (norm1 * norm2)
            # Clip similarity to [0, 1] as minor numerical instability can occur
            final_similarity = max(0.0, min(1.0, similarity))
            logging.debug(f"Basic Image Similarity('{os.path.basename(img_path1)}', '{os.path.basename(img_path2)}') = {final_similarity:.4f}")
            return final_similarity
        except (IOError, OSError) as e:
            # Handle I/O errors separately for better diagnostics
            logging.error(f"I/O error during image processing: {e}. Paths: {img_path1}, {img_path2}")
            return 0.0
        except tf.errors.InvalidArgumentError as e:
            # Handle TensorFlow specific errors
            logging.error(f"TensorFlow error during image processing: {e}. Paths: {img_path1}, {img_path2}")
            return 0.0
        except Exception as e:
            logging.error(f"Unexpected error during image similarity calculation for {img_path1} and {img_path2}: {e}", exc_info=True)
            return 0.0

    def is_match(self, product1: Dict[str, Any], product2: Dict[str, Any]) -> Tuple[bool, float, float, float]:
        """Determine if two products match based on text and image similarity."""
        # 카테고리별 임계값 적용
        category = product1.get('카테고리(중분류)')
        text_threshold, image_threshold = self.get_thresholds_for_category(category)
        
        text_similarity = self.calculate_text_similarity(product1.get('name'), product2.get('name'))

        image_similarity = 0.0
        img_path1 = product1.get('image_path')
        img_path2 = product2.get('image_path')

        if img_path1 and img_path2 and os.path.exists(img_path1) and os.path.exists(img_path2):
            image_similarity = self.calculate_image_similarity(img_path1, img_path2)
        else:
            logging.debug(f"Skipping image similarity: One or both image paths missing/invalid for product pair ({product1.get('name', '?')} vs {product2.get('name', '?')}). Paths: '{img_path1}', '{img_path2}'")

        combined_score = (text_similarity * self.text_weight) + (image_similarity * self.image_weight)
        text_match = text_similarity >= text_threshold
        image_match = image_similarity >= image_threshold if (img_path1 and img_path2 and os.path.exists(img_path1) and os.path.exists(img_path2)) else False
        final_match = text_match or image_match

        logging.debug(f"Matching '{product1.get('name')}' vs '{product2.get('name')}': Text Sim={text_similarity:.4f} (Thresh={text_threshold}, Match={text_match}), Image Sim={image_similarity:.4f} (Thresh={image_threshold}, Match={image_match}), Combined={combined_score:.4f}, Final Match={final_match}")

        return final_match, combined_score, text_similarity, image_similarity

    # New method to handle matching with three images
    def is_match_triple(self, product_haoreum: Dict[str, Any], product_kogift: Optional[Dict[str, Any]], product_naver: Optional[Dict[str, Any]]) -> Tuple[bool, float, float, float, float, float]:
        """Determine if Kogift/Naver products match Haoreum based on text and combined image similarity."""
        
        # --- Text Similarity --- 
        # Calculate text similarity for Haoreum vs Kogift and Haoreum vs Naver
        text_sim_hk = self.calculate_text_similarity(product_haoreum.get('name'), product_kogift.get('name')) if product_kogift else 0.0
        text_sim_hn = self.calculate_text_similarity(product_haoreum.get('name'), product_naver.get('name')) if product_naver else 0.0

        # --- Image Similarity ---        
        img_path_h = product_haoreum.get('image_path')
        img_path_k = product_kogift.get('image_path') if product_kogift else None
        img_path_n = product_naver.get('image_path') if product_naver else None

        img_sim_hk = 0.0
        img_sim_hn = 0.0
        valid_comparisons = 0

        if img_path_h and img_path_k and os.path.exists(img_path_h) and os.path.exists(img_path_k):
            img_sim_hk = self.calculate_image_similarity(img_path_h, img_path_k)
            valid_comparisons += 1
        elif img_path_h or img_path_k: # Log if only one exists
             logging.debug(f"Skipping HK image sim: H='{img_path_h}', K='{img_path_k}'")

        if img_path_h and img_path_n and os.path.exists(img_path_h) and os.path.exists(img_path_n):
            img_sim_hn = self.calculate_image_similarity(img_path_h, img_path_n)
            valid_comparisons += 1
        elif img_path_h or img_path_n: # Log if only one exists
             logging.debug(f"Skipping HN image sim: H='{img_path_h}', N='{img_path_n}'")

        # Calculate average image similarity only from valid comparisons
        avg_image_similarity = (img_sim_hk + img_sim_hn) / valid_comparisons if valid_comparisons > 0 else 0.0

        # --- Combined Score & Matching Decision --- 
        # Decide which text similarity to use for combined score (e.g., average, max? Let's use average for now)
        avg_text_similarity = (text_sim_hk + text_sim_hn) / 2 if product_kogift and product_naver else (text_sim_hk or text_sim_hn) # Handle cases where one is missing

        combined_score = (avg_text_similarity * self.text_weight) + (avg_image_similarity * self.image_weight)

        # Determine match based on thresholds (apply to individual sims? average sims?)
        # Let's keep the original logic: match if EITHER text OR image sim meets threshold, applied to the average
        text_match = avg_text_similarity >= self.text_similarity_threshold
        image_match = avg_image_similarity >= self.image_similarity_threshold
        # Final match could be based on individual pairs or the combined score/average thresholds
        # Sticking to the original OR logic applied to averages for simplicity, adjust if needed
        final_match = text_match or image_match 

        logging.debug(
            f"Triple Match '{product_haoreum.get('name')}': "
            f"  Text HK={text_sim_hk:.4f}, Text HN={text_sim_hn:.4f}, AvgText={avg_text_similarity:.4f} (Thresh={self.text_similarity_threshold}, Match={text_match})"
            f"  Image HK={img_sim_hk:.4f}, Image HN={img_sim_hn:.4f}, AvgImage={avg_image_similarity:.4f} (Thresh={self.image_similarity_threshold}, Match={image_match})"
            f"  CombinedScore={combined_score:.4f}, Final Match={final_match}"
        )

        # Return individual similarities along with overall match status and combined score
        return final_match, combined_score, text_sim_hk, text_sim_hn, img_sim_hk, img_sim_hn

    def _load_category_thresholds(self, config):
        """Load category-specific thresholds from config if available"""
        category_thresholds = {}
        if self.use_category_thresholds:
            try:
                # Try to load from [CategoryThresholds] section
                if 'CategoryThresholds' in config:
                    for category, threshold_str in config['CategoryThresholds'].items():
                        try:
                            values = [float(x.strip()) for x in threshold_str.split(',')]
                            if len(values) >= 2:
                                category_thresholds[category] = {
                                    'text_threshold': values[0],
                                    'image_threshold': values[1]
                                }
                        except (ValueError, IndexError) as e:
                            logging.warning(f"Invalid threshold format for category {category}: {threshold_str}. Error: {e}")
                    logging.info(f"Loaded {len(category_thresholds)} category-specific thresholds")
            except Exception as e:
                logging.warning(f"Failed to load category thresholds: {e}. Using default thresholds.")
        return category_thresholds

    def _initialize_text_model(self):
        """Initialize the text similarity model"""
        try:
            logging.info(f"Loading text similarity model from {self.text_model_path}...")
            self.text_model = SentenceTransformer(self.text_model_path)
            logging.info("Text similarity model loaded successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to load text similarity model: {e}", exc_info=True)
            self.text_model = None
            return False

    def _initialize_image_model(self):
        """Initialize EfficientNetB0 image model"""
        try:
            logging.info("Loading EfficientNetB0 image model...")
            base_model = tf.keras.applications.EfficientNetB0(
                weights='imagenet', 
                include_top=False, 
                input_shape=(self.image_resize_dimension, self.image_resize_dimension, 3)
            )
            global_avg_layer = tf.keras.layers.GlobalAveragePooling2D()(base_model.output)
            model = tf.keras.Model(inputs=base_model.input, outputs=global_avg_layer)
            self.image_model = model
            logging.info("Image model loaded successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to load image model: {e}", exc_info=True)
            self.image_model = None
            return False

def _match_single_product(i: int, haoreum_row_dict: Dict, kogift_data: Optional[List[Dict]], naver_data: Optional[List[Dict]], product_type: str, matcher: ProductMatcher, haoreum_img_path: Optional[str]) -> Tuple[int, Optional[Dict]]:
    """Matches a single Haoreum product against Kogift and Naver data using the provided matcher."""
    if not matcher:
         logging.error(f"Matcher object missing for index {i}.")
         return i, None

    logging.debug(f"Matching product index {i}: {haoreum_row_dict.get('상품명')}")
    product_category = haoreum_row_dict.get('카테고리(중분류)')
    
    haoreum_product = {
        'name': haoreum_row_dict.get('상품명'),
        'price': pd.to_numeric(haoreum_row_dict.get('판매단가(V포함)'), errors='coerce'),
        'link': haoreum_row_dict.get('본사상품링크'),
        'image_path': haoreum_img_path,
        'code': haoreum_row_dict.get('Code'),
        '담당자': haoreum_row_dict.get('담당자'),
        '업체명': haoreum_row_dict.get('업체명'),
        '업체코드': haoreum_row_dict.get('업체코드'),
        '공급사명': haoreum_row_dict.get('공급사명'),
        '공급처코드': haoreum_row_dict.get('공급처코드'),
        '상품코드': haoreum_row_dict.get('상품코드'),
        '카테고리(중분류)': product_category,
        '본사 기본수량': haoreum_row_dict.get('본사 기본수량')
    }

    best_kogift_match = None
    best_naver_match = None
    max_kogift_score = -1
    max_naver_score = -1
    kogift_text_sim = 0.0
    kogift_img_sim = 0.0
    naver_text_sim = 0.0
    naver_img_sim = 0.0
    kogift_price_sim = 0.0
    naver_price_sim = 0.0

    # 가격 유사도 계산 여부
    use_price_sim = getattr(matcher, 'use_price_similarity', False)
    price_weight = getattr(matcher, 'price_weight', 0.1)
    
    # 최소 결합 점수
    min_combined_score = getattr(matcher, 'minimum_combined_score', 0.4)

    # Find best Kogift match
    if kogift_data:
        for item in kogift_data:
            kogift_product = {
                'name': item.get('name'), 
                'price': pd.to_numeric(item.get('price'), errors='coerce'),
                'link': item.get('link'), 
                'image_path': item.get('image_path'),
                '카테고리(중분류)': product_category  # 카테고리별 임계값 사용을 위해 전달
            }
            
            # 기본 텍스트/이미지 유사도 계산
            match, score, text_s, img_s = matcher.is_match(haoreum_product, kogift_product)
            
            # 가격 유사도 계산 (옵션)
            price_s = 0.0
            if use_price_sim and haoreum_product['price'] > 0 and kogift_product['price'] > 0:
                # 가격 차이 비율 계산 (0~1 사이 값 반환)
                price_diff_ratio = abs(haoreum_product['price'] - kogift_product['price']) / haoreum_product['price']
                price_s = max(0.0, 1.0 - min(1.0, price_diff_ratio))  # 가격이 비슷할수록 1에 가까움
                
                # 결합 점수 재계산 (가격 유사도 포함)
                if price_s > 0:
                    total_weight = matcher.text_weight + matcher.image_weight + price_weight
                    score = ((text_s * matcher.text_weight) + (img_s * matcher.image_weight) + (price_s * price_weight)) / total_weight
            
            # 최소 점수 이상이면 매치로 간주
            final_match = match or (score >= min_combined_score)
            
            if final_match and score > max_kogift_score:
                max_kogift_score = score
                best_kogift_match = item
                kogift_text_sim = text_s
                kogift_img_sim = img_s
                kogift_price_sim = price_s

    # Find best Naver match
    if naver_data:
        for item in naver_data:
            naver_product = {
                'name': item.get('name'), 
                'price': pd.to_numeric(item.get('price'), errors='coerce'),
                'link': item.get('link'), 
                'image_path': item.get('image_path'),
                'seller': item.get('seller'),
                '카테고리(중분류)': product_category  # 카테고리별 임계값 사용을 위해 전달
            }
            
            # 기본 텍스트/이미지 유사도 계산
            match, score, text_s, img_s = matcher.is_match(haoreum_product, naver_product)
            
            # 가격 유사도 계산 (옵션)
            price_s = 0.0
            if use_price_sim and haoreum_product['price'] > 0 and naver_product['price'] > 0:
                # 가격 차이 비율 계산 (0~1 사이 값 반환)
                price_diff_ratio = abs(haoreum_product['price'] - naver_product['price']) / haoreum_product['price']
                price_s = max(0.0, 1.0 - min(1.0, price_diff_ratio))  # 가격이 비슷할수록 1에 가까움
                
                # 결합 점수 재계산 (가격 유사도 포함)
                if price_s > 0:
                    total_weight = matcher.text_weight + matcher.image_weight + price_weight
                    score = ((text_s * matcher.text_weight) + (img_s * matcher.image_weight) + (price_s * price_weight)) / total_weight
            
            # 최소 점수 이상이면 매치로 간주
            final_match = match or (score >= min_combined_score)
            
            if final_match and score > max_naver_score:
                max_naver_score = score
                best_naver_match = item
                naver_text_sim = text_s
                naver_img_sim = img_s
                naver_price_sim = price_s

    # Combine results if matches found
    if best_kogift_match or best_naver_match:
        # Start with all original fields from haoreum_row_dict
        result = {**haoreum_row_dict}  # Copy all original fields
        
        # Then add or overwrite specific fields
        result.update({
            '구분(승인관리:A/가격관리:P)': product_type,
            # Kogift data
            '고려 링크': best_kogift_match.get('link') if best_kogift_match else None,
            '고려기프트(이미지링크)': best_kogift_match.get('image_path') if best_kogift_match else None,
            '판매단가2(VAT포함)': best_kogift_match.get('price') if best_kogift_match else None,
            '_고려_TextSim': kogift_text_sim,
            '_해오름_고려_ImageSim': kogift_img_sim,
            '_고려_PriceSim': kogift_price_sim if use_price_sim else None,
            '_고려_Combined': max_kogift_score if max_kogift_score > -1 else None,
            # Set default kogift quantity
            '고려 기본수량': best_kogift_match.get('quantity', '-'), # Get from match if available

            # Naver data
            '네이버 공급사명': best_naver_match.get('seller') if best_naver_match else None,
            '네이버 링크': best_naver_match.get('link') if best_naver_match else None,
            '네이버쇼핑(이미지링크)': best_naver_match.get('image_path') if best_naver_match else None,
            '판매단가3 (VAT포함)': best_naver_match.get('price') if best_naver_match else None,
            '_네이버_TextSim': naver_text_sim,
            '_해오름_네이버_ImageSim': naver_img_sim,
            '_네이버_PriceSim': naver_price_sim if use_price_sim else None,
            '_네이버_Combined': max_naver_score if max_naver_score > -1 else None,
            # Set default naver quantity
            '네이버 기본수량': best_naver_match.get('quantity', '-') if best_naver_match else '-'
        })
        
        # Standard field mappings for clarity
        if 'link' in result:
            result['본사링크'] = result.pop('link')
        if 'price' in result:
            result['판매단가(V포함)'] = result.pop('price')  # Changed from 판매단가1(VAT포함)
        if 'image_path' in result:
            result['해오름이미지경로'] = result.pop('image_path')
        
        return i, result
    else:
        # No match found for either Kogift or Naver
        logging.debug(f"No sufficient match found for product index {i}: {haoreum_product.get('name')}")
        return i, None

# Wrapper for ProcessPoolExecutor compatibility
def _match_single_product_wrapper(i: int, haoreum_row_dict: Dict, kogift_data: Optional[List[Dict]], naver_data: Optional[List[Dict]], product_type: str, haoreum_img_path: Optional[str]) -> Tuple[int, Optional[Dict]]:
    """Wrapper to call _match_single_product using the global worker instance."""
    global worker_matcher_instance
    if worker_matcher_instance is None:
        # This might happen if initializer failed
        logging.error(f"Matcher instance is None in worker {os.getpid()} for index {i}. Cannot match.")
        return i, None
    try:
        # Call the actual matching logic
        return _match_single_product(
            i, haoreum_row_dict, kogift_data, naver_data, 
            product_type, worker_matcher_instance, haoreum_img_path
        )
    except Exception as e:
        logging.error(f"Error in _match_single_product_wrapper for index {i}: {e}", exc_info=True)
        return i, None # Return None for the data part on error

def match_products(
    haoreum_df: pd.DataFrame,
    kogift_map: Dict[str, List[Dict]],
    naver_map: Dict[str, List[Dict]],
    input_file_image_map: Dict[Any, str], # Map Code -> image path
    config: configparser.ConfigParser,
    gpu_available: bool,
    progress_queue=None,
    max_workers: Optional[int] = None
) -> pd.DataFrame:
    """Matches Haoreum products against Kogift and Naver data using ThreadPoolExecutor or ProcessPoolExecutor."""
    # 메모리 사용량 모니터링 시작
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    logging.info(f"Initial memory usage: {initial_memory:.2f} MB")
    
    # 매칭 시작 시간 기록
    start_time = time.time()
    
    if haoreum_df.empty:
        logging.warning("Haoreum DataFrame is empty. No products to match.")
        return pd.DataFrame()

    results = []
    total_products = len(haoreum_df)
    logging.info(f"Starting product matching for {total_products} Haoreum products...")
    
    # 메모리 제한 적용
    memory_limit_mb = config.getint('Matching', 'memory_limit_mb', fallback=0)
    if memory_limit_mb > 0:
        try:
            import resource
            # 메모리 제한 (soft, hard) 설정
            resource.setrlimit(resource.RLIMIT_AS, (memory_limit_mb * 1024 * 1024, memory_limit_mb * 1024 * 1024))
            logging.info(f"Set memory limit to {memory_limit_mb} MB")
        except (ImportError, AttributeError, ValueError) as e:
            logging.warning(f"Failed to set memory limit: {e}")

    # 배치 크기
    batch_size = config.getint('Matching', 'batch_size', fallback=0)
    
    # Decide on executor type based on GPU availability and config
    executor_type = config.get('Concurrency', 'matcher_executor_type', fallback='thread').lower()
    
    # If GPU not available, override to thread executor
    if not gpu_available and executor_type == 'process':
        logging.warning("GPU not available. Overriding to ThreadPoolExecutor for safety.")
        executor_type = 'thread'
    
    # Adjust max_workers based on executor type and config
    default_workers = max(1, min(os.cpu_count() // 2, 4))  # More conservative default
    min_workers = config.getint('Concurrency', 'min_match_workers', fallback=1)
    
    if max_workers is None:
        try:
            max_workers = config.getint('Concurrency', 'max_match_workers', fallback=default_workers)
        except (configparser.Error, ValueError):
            max_workers = default_workers
    
    # 동적 워커 조정 활성화 여부
    dynamic_scaling = config.getboolean('Matching', 'dynamic_worker_scaling', fallback=False)
    
    # 작업 청크 크기
    task_chunk_size = config.getint('Concurrency', 'task_chunk_size', fallback=20)
    
    # 시스템 리소스 확인하여 워커 수 조정
    if dynamic_scaling:
        try:
            # 현재 시스템 부하 확인
            cpu_usage = psutil.cpu_percent(interval=0.5)
            memory_usage = psutil.virtual_memory().percent
            
            # 부하가 높으면 워커 수 줄임
            if cpu_usage > 80 or memory_usage > 80:
                adjusted_workers = max(min_workers, max_workers // 2)
                logging.info(f"High system load detected (CPU: {cpu_usage}%, Memory: {memory_usage}%), reducing workers from {max_workers} to {adjusted_workers}")
                max_workers = adjusted_workers
        except Exception as e:
            logging.warning(f"Error during dynamic worker scaling: {e}")
             
    logging.info(f"Using {executor_type} executor with max_workers={max_workers}")

    # Initialize matcher here if using ThreadPoolExecutor
    matcher_instance = None
    if executor_type == 'thread':
        try:
            matcher_instance = ProductMatcher(config)
            if matcher_instance.text_model is None or matcher_instance.image_model is None:
                logging.error("Failed to load models for ThreadPoolExecutor. Falling back to single-threaded execution.")
                max_workers = 1  # Fall back to single worker
        except Exception as e:
            logging.error(f"Error initializing ProductMatcher for ThreadPoolExecutor: {e}", exc_info=True)
            logging.info("Falling back to single-threaded execution.")
            max_workers = 1  # Fall back to single worker
            # Try again with reduced expectations
            try:
                matcher_instance = ProductMatcher(config)
            except Exception as e:
                logging.critical(f"Critical error initializing ProductMatcher: {e}. Cannot perform matching.")
                return pd.DataFrame()  # Return empty DataFrame

    executor_class = ProcessPoolExecutor if executor_type == 'process' else ThreadPoolExecutor
    initializer = _init_worker_matcher if executor_type == 'process' else None
    initargs = (config,) if executor_type == 'process' else ()

    processed_count = 0
    futures = []
    timeout_per_task = config.getint('Concurrency', 'thread_pool_timeout_sec', fallback=300)
    
    # 배치 처리 준비
    if batch_size > 0 and batch_size < total_products:
        # 배치로 나누어 처리
        batch_count = (total_products + batch_size - 1) // batch_size  # 올림 나눗셈
        logging.info(f"Processing products in {batch_count} batches of size {batch_size}")
        
        all_results = []
        for batch_index in range(batch_count):
            start_idx = batch_index * batch_size
            end_idx = min(start_idx + batch_size, total_products)
            
            logging.info(f"Processing batch {batch_index+1}/{batch_count}, items {start_idx+1}-{end_idx}")
            
            # 배치에 해당하는 데이터프레임 슬라이스
            batch_df = haoreum_df.iloc[start_idx:end_idx]
            
            # 배치 처리 후 결과 병합
            batch_results = _process_batch(
                batch_df, kogift_map, naver_map, input_file_image_map, 
                config, gpu_available, progress_queue, max_workers, 
                executor_type, matcher_instance, processed_count)
                
            all_results.extend(batch_results)
            processed_count += len(batch_df)
            
            # 메모리 현황 로깅
            current_memory = process.memory_info().rss / 1024 / 1024
            logging.info(f"After batch {batch_index+1}/{batch_count}: Memory usage {current_memory:.2f} MB, Processed {processed_count}/{total_products}")
            
            # 중간 결과 백업 (선택사항)
            if config.getboolean('Matching', 'save_intermediate_results', fallback=False):
                temp_df = pd.DataFrame(all_results)
                temp_output = os.path.join(config.get('Paths', 'temp_dir'), f"match_results_batch_{batch_index+1}.csv")
                temp_df.to_csv(temp_output, index=False)
                logging.info(f"Saved intermediate results to {temp_output}")
                
            # 메모리 정리 힌트
            if config.getboolean('Matching', 'gc_after_batch', fallback=True):
                import gc
                gc.collect()
        
        results = all_results
    else:
        # 기존 방식으로 한 번에 처리
        try:
            with executor_class(max_workers=max_workers, initializer=initializer, initargs=initargs) as executor:
                for i, row in haoreum_df.iterrows():
                    haoreum_row_dict = row.to_dict()
                    product_name = haoreum_row_dict.get('상품명')
                    product_code = haoreum_row_dict.get('Code')
                    product_type = haoreum_row_dict.get('구분', 'A')
                    
                    # 필수 정보 누락 체크
                    if not product_name or pd.isna(product_name):
                        logging.warning(f"Skipping matching for index {i}: Missing or invalid '상품명'.")
                        processed_count += 1
                        continue
                    
                    # 매칭에 필요한 데이터 가져오기
                    kogift_data = kogift_map.get(product_name, [])
                    naver_data = naver_map.get(product_name, [])
                    haoreum_img_path = input_file_image_map.get(product_code)
                    
                    # 이미지 경로 검증
                    if haoreum_img_path and not os.path.exists(haoreum_img_path):
                        logging.warning(f"Image path for product {product_code} does not exist: {haoreum_img_path}")
                    
                    try:
                        if executor_type == 'process':
                            future = executor.submit(_match_single_product_wrapper, i, haoreum_row_dict, kogift_data, naver_data, product_type, haoreum_img_path)
                        else:
                            future = executor.submit(_match_single_product, i, haoreum_row_dict, kogift_data, naver_data, product_type, matcher_instance, haoreum_img_path)
                        futures.append(future)
                    except Exception as e:
                        logging.error(f"Error submitting batch task for product {product_name}: {e}")
                        processed_count += 1
            
            # 완료된 작업 처리
            for future in as_completed(futures):
                try:
                    original_index, result_data = future.result(timeout=timeout_per_task)
                    if result_data:
                        results.append(result_data)
                except TimeoutError:
                    logging.error(f"A batch matching task timed out after {timeout_per_task} seconds.")
                except Exception as e:
                    logging.error(f"Error processing batch matching result: {e}", exc_info=True)
                finally:
                    processed_count += 1
                    total_processed = processed_count
                    
                    # 진행 상황 업데이트
                    if processed_count % 20 == 0 or processed_count == total_products:
                        logging.info(f"Batch matching progress: {processed_count}/{total_products}")
                        if progress_queue:
                            try:
                                progress_queue.put(("match", total_processed, processed_count))
                            except Exception as e:
                                logging.error(f"Error updating progress queue in batch: {e}")
    
        except Exception as e:
            logging.error(f"Error during batch executor execution: {e}", exc_info=True)
    
    # Clean up
    if executor_type == 'thread' and matcher_instance:
        try:
            # Clean up GPU memory
            del matcher_instance
            tf.keras.backend.clear_session()
            logging.debug("Cleaned up matcher instance and TensorFlow session")
        except Exception as e:
            logging.warning(f"Error during cleanup: {e}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Finished product matching. Found {len(results)} potential matches. Duration: {elapsed_time:.2f} sec")

    if not results:
        logging.warning("No matches found. Returning DataFrame with original data to ensure output in report.")
        # Create base results with main Haoreum columns that match the column names in matched results
        no_match_results = []
        for _, row in haoreum_df.iterrows():
            haoreum_row_dict = row.to_dict()
            product_code = haoreum_row_dict.get('Code')
            haoreum_img_path = input_file_image_map.get(product_code)  # Get image path using Code
            
            # Start with all original fields from the row
            base_data = {**haoreum_row_dict}  # Copy all original fields
            
            # Add standard transformed fields and default values for missing data
            base_data.update({
                '구분(승인관리:A/가격관리:P)': haoreum_row_dict.get('구분', 'A'),
                'name': haoreum_row_dict.get('상품명'),
                '본사링크': haoreum_row_dict.get('본사상품링크'),
                '판매단가(V포함)': haoreum_row_dict.get('판매단가(V포함)'),
                '해오름이미지경로': haoreum_img_path,
                # Set empty values for Kogift and Naver fields to maintain consistent structure
                '고려 링크': None,
                '고려기프트(이미지링크)': None,
                '판매단가2(VAT포함)': None,
                '_고려_TextSim': 0.0,
                '_해오름_고려_ImageSim': 0.0,
                '_고려_Combined': None,
                '고려 기본수량': '-',
                '네이버 공급사명': None,
                '네이버 링크': None,
                '네이버쇼핑(이미지링크)': None,
                '판매단가3 (VAT포함)': None,
                '_네이버_TextSim': 0.0,
                '_해오름_네이버_ImageSim': 0.0,
                '_네이버_Combined': None,
                '네이버 기본수량': '-'
            })
            no_match_results.append(base_data)
            
        return pd.DataFrame(no_match_results)

    # Convert results list of dicts to DataFrame
    matched_df = pd.DataFrame(results)
    
    # 가격 비교 및 분석 데이터 추가
    if not matched_df.empty:
        try:
            # 가격 열 숫자 타입으로 변환 (이미 숫자일 수 있음)
            for col in ['판매단가(V포함)', '판매단가2(VAT포함)', '판매단가3 (VAT포함)']:
                if col in matched_df.columns:
                    matched_df[col] = pd.to_numeric(matched_df[col], errors='coerce')
            
            # 가격차이 계산
            matched_df['고려가격차이'] = matched_df['판매단가2(VAT포함)'] - matched_df['판매단가(V포함)']
            matched_df['네이버가격차이'] = matched_df['판매단가3 (VAT포함)'] - matched_df['판매단가(V포함)']
            
            # 가격차이 비율 계산 (%)
            matched_df['고려가격비율(%)'] = (matched_df['고려가격차이'] / matched_df['판매단가(V포함)'] * 100).round(1)
            matched_df['네이버가격비율(%)'] = (matched_df['네이버가격차이'] / matched_df['판매단가(V포함)'] * 100).round(1)
            
            # 매칭 품질 레이블 적용
            quality_evaluator = MatchQualityEvaluator(config)
            matched_df = quality_evaluator.apply_quality_labels(matched_df)
            
            # 매칭 품질 분석
            kogift_matched = matched_df['고려 링크'].notna().sum()
            naver_matched = matched_df['네이버 링크'].notna().sum()
            both_matched = matched_df['고려 링크'].notna() & matched_df['네이버 링크'].notna()
            logging.info(f"Matching quality: {kogift_matched}/{len(matched_df)} Kogift matches, "
                         f"{naver_matched}/{len(matched_df)} Naver matches, "
                         f"{both_matched.sum()}/{len(matched_df)} products matched to both sources.")
                         
            # 품질 기준별 분류 (고려 기준)
            if '고려_매칭품질' in matched_df.columns:
                quality_counts = matched_df['고려_매칭품질'].value_counts()
                logging.info(f"Kogift matching quality distribution: {quality_counts.to_dict()}")
        except Exception as e:
            logging.error(f"Error calculating price differences and quality metrics: {e}", exc_info=True)
    
    # 매칭 보고서 생성 (선택사항)
    if config.getboolean('Matching', 'generate_matching_report', fallback=False) and not matched_df.empty:
        try:
            report_file = os.path.join(config.get('Paths', 'output_dir'), "matching_quality_report.csv")
            # 보고서용 요약 데이터 생성
            report_data = {
                '총 제품 수': len(matched_df),
                '고려 매칭 수': matched_df['고려 링크'].notna().sum(),
                '네이버 매칭 수': matched_df['네이버 링크'].notna().sum(),
                '양쪽 매칭 수': (matched_df['고려 링크'].notna() & matched_df['네이버 링크'].notna()).sum(),
                '고려 고품질 매칭': matched_df[matched_df['고려_매칭품질'] == 'high'].shape[0] if '고려_매칭품질' in matched_df.columns else 0,
                '고려 중품질 매칭': matched_df[matched_df['고려_매칭품질'] == 'medium'].shape[0] if '고려_매칭품질' in matched_df.columns else 0,
                '고려 저품질 매칭': matched_df[matched_df['고려_매칭품질'] == 'low'].shape[0] if '고려_매칭품질' in matched_df.columns else 0,
                '평균 텍스트 유사도': matched_df['_고려_TextSim'].mean() if '_고려_TextSim' in matched_df.columns else 0,
                '평균 이미지 유사도': matched_df['_해오름_고려_ImageSim'].mean() if '_해오름_고려_ImageSim' in matched_df.columns else 0,
                '실행 시간(초)': elapsed_time,
                '메모리 사용(MB)': process.memory_info().rss / 1024 / 1024
            }
            
            # 보고서 저장
            pd.DataFrame([report_data]).to_csv(report_file, index=False)
            logging.info(f"Generated matching quality report at {report_file}")
        except Exception as e:
            logging.error(f"Error generating matching report: {e}")
    
    # 메모리 사용량 모니터링 종료
    final_memory = process.memory_info().rss / 1024 / 1024  # MB
    memory_diff = final_memory - initial_memory
    logging.info(f"Final memory usage: {final_memory:.2f} MB (Change: {memory_diff:+.2f} MB)")
    
    return matched_df 

def _process_batch(
    batch_df: pd.DataFrame,
    kogift_map: Dict[str, List[Dict]],
    naver_map: Dict[str, List[Dict]],
    input_file_image_map: Dict[Any, str],
    config: configparser.ConfigParser,
    gpu_available: bool,
    progress_queue=None,
    max_workers: Optional[int] = None,
    executor_type: str = 'thread',
    matcher_instance = None,
    start_count: int = 0
) -> List[Dict]:
    """한 배치의 제품을 매칭하는 헬퍼 함수"""
    results = []
    total_in_batch = len(batch_df)
    processed_in_batch = 0
    futures = []
    
    # 기존 logic과 유사하게 실행기 설정
    executor_class = ProcessPoolExecutor if executor_type == 'process' else ThreadPoolExecutor
    initializer = _init_worker_matcher if executor_type == 'process' else None
    initargs = (config,) if executor_type == 'process' else ()
    
    timeout_per_task = config.getint('Concurrency', 'thread_pool_timeout_sec', fallback=300)
    
    try:
        with executor_class(max_workers=max_workers, initializer=initializer, initargs=initargs) as executor:
            for i, row in batch_df.iterrows():
                haoreum_row_dict = row.to_dict()
                product_name = haoreum_row_dict.get('상품명')
                product_code = haoreum_row_dict.get('Code')
                product_type = haoreum_row_dict.get('구분', 'A')
                
                # 필수 정보 누락 체크
                if not product_name or pd.isna(product_name):
                    logging.warning(f"Skipping matching for index {i}: Missing or invalid '상품명'.")
                    processed_in_batch += 1
                    continue
                
                # 매칭에 필요한 데이터 가져오기
                kogift_data = kogift_map.get(product_name, [])
                naver_data = naver_map.get(product_name, [])
                haoreum_img_path = input_file_image_map.get(product_code)
                
                # 이미지 경로 검증
                if haoreum_img_path and not os.path.exists(haoreum_img_path):
                    logging.warning(f"Image path for product {product_code} does not exist: {haoreum_img_path}")
                
                try:
                    if executor_type == 'process':
                        future = executor.submit(_match_single_product_wrapper, i, haoreum_row_dict, kogift_data, naver_data, product_type, haoreum_img_path)
                    else:
                        future = executor.submit(_match_single_product, i, haoreum_row_dict, kogift_data, naver_data, product_type, matcher_instance, haoreum_img_path)
                    futures.append(future)
                except Exception as e:
                    logging.error(f"Error submitting batch task for product {product_name}: {e}")
                    processed_in_batch += 1
            
            # 완료된 작업 처리
            for future in as_completed(futures):
                try:
                    original_index, result_data = future.result(timeout=timeout_per_task)
                    if result_data:
                        results.append(result_data)
                except TimeoutError:
                    logging.error(f"A batch matching task timed out after {timeout_per_task} seconds.")
                except Exception as e:
                    logging.error(f"Error processing batch matching result: {e}", exc_info=True)
                finally:
                    processed_in_batch += 1
                    total_processed = start_count + processed_in_batch
                    
                    # 진행 상황 업데이트
                    if processed_in_batch % 20 == 0 or processed_in_batch == total_in_batch:
                        logging.info(f"Batch matching progress: {processed_in_batch}/{total_in_batch}")
                        if progress_queue:
                            try:
                                progress_queue.put(("match", total_processed, start_count + total_in_batch))
                            except Exception as e:
                                logging.error(f"Error updating progress queue in batch: {e}")
    
    except Exception as e:
        logging.error(f"Error during batch executor execution: {e}", exc_info=True)
    
    return results 

def combine_match_results(df_input, kogift_matches, naver_matches, config):
    """
    Combine matched products from Kogift and Naver into the main dataframe.
    
    Args:
        df_input: Original input DataFrame with product information
        kogift_matches: DataFrame with Kogift matches
        naver_matches: DataFrame with Naver matches
        config: Configuration
        
    Returns:
        DataFrame: Combined DataFrame with all match information
    """
    logging.info(f"Combining match results: {len(kogift_matches)} Kogift matches and {len(naver_matches)} Naver matches")
    
    # Make sure both match dataframes have the same columns structure
    df_combined = df_input.copy()
    
    # Ensure we don't lose any rows during the merge operations
    original_row_count = len(df_combined)
    
    # Add Kogift data if available
    if not kogift_matches.empty:
        logging.info(f"Adding {len(kogift_matches)} Kogift matches to the results")
        # Use left join to keep all original rows
        kogift_columns = [col for col in kogift_matches.columns if col not in df_combined.columns]
        if kogift_columns:
            df_combined = pd.merge(
                df_combined, 
                kogift_matches[['match_id'] + kogift_columns],
                left_on='match_id',  # Assuming 'match_id' is the key to join on
                right_on='match_id',
                how='left'
            )
    
    # Add Naver data if available - CRITICAL: This part might be missing or not working
    if not naver_matches.empty:
        logging.info(f"Adding {len(naver_matches)} Naver matches to the results")
        # Use left join to keep all original rows
        naver_columns = [col for col in naver_matches.columns if col not in df_combined.columns]
        if naver_columns:
            df_combined = pd.merge(
                df_combined, 
                naver_matches[['match_id'] + naver_columns],
                left_on='match_id',  # Assuming 'match_id' is the key to join on
                right_on='match_id',
                how='left'
            )
    
    # Verify we haven't lost any rows
    if len(df_combined) != original_row_count:
        logging.error(f"Row count changed during combine! Original: {original_row_count}, Current: {len(df_combined)}")
        # Handle error - ideally recover the missing rows
    
    logging.info(f"Combined match results: Final dataframe has {len(df_combined)} rows")
    return df_combined 