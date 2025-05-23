import os
import logging
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import numpy as np
from PIL import Image

# Configure TensorFlow GPU memory growth BEFORE importing TensorFlow
# This must be done before ANY other code that might import TensorFlow
import os

# Set environment variable for TensorFlow GPU memory growth
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'

# Now import TensorFlow
import tensorflow as tf

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
import torch
import multiprocessing

# --- 인코딩 관련 전역 설정 ---
# 항상 UTF-8 인코딩 사용
DEFAULT_ENCODING = 'utf-8'

# --- Hash-based filtering functions (import from utils for consistency) ---
try:
    from .utils import generate_product_name_hash, extract_product_hash_from_filename
    logging.info("Hash functions imported from utils module")
except ImportError:
    try:
        from utils import generate_product_name_hash, extract_product_hash_from_filename
        logging.info("Hash functions imported from utils module (direct import)")
    except ImportError:
        logging.error("Failed to import hash functions from utils. Defining local fallback functions.")
        
        def generate_product_name_hash(product_name: str) -> str:
            """Fallback hash generation function"""
            try:
                normalized_name = ''.join(product_name.split()).lower()
                hash_obj = hashlib.md5(normalized_name.encode('utf-8'))
                return hash_obj.hexdigest()[:16]
            except Exception as e:
                logging.error(f"Error generating hash for product name {product_name}: {e}")
                return ""
        
        def extract_product_hash_from_filename(filename: str) -> Optional[str]:
            """Fallback hash extraction function"""
            try:
                name_without_ext = os.path.splitext(os.path.basename(filename))[0]
                parts = name_without_ext.split('_')
                if len(parts) >= 2:
                    potential_hash = parts[1]
                    if len(potential_hash) == 16 and all(c in '0123456789abcdef' for c in potential_hash.lower()):
                        return potential_hash.lower()
                if len(name_without_ext) == 16 and all(c in '0123456789abcdef' for c in name_without_ext.lower()):
                    return name_without_ext.lower()
                return None
            except Exception as e:
                logging.debug(f"Error extracting hash from filename {filename}: {e}")
                return None

def filter_images_by_hash(haereum_product_name: str, candidate_images: List[str]) -> List[str]:
    """
    해시값을 기반으로 이미지 후보군을 필터링합니다.
    
    개선사항:
    - 더 정확한 로깅 시스템
    - 성능 최적화된 필터링
    - 에러 처리 강화
        
    Args:
        haereum_product_name: 해오름 상품명
        candidate_images: 후보 이미지 경로 리스트
            
    Returns:
        해시값이 일치하는 이미지 경로 리스트
    """
    try:
        if not haereum_product_name or not candidate_images:
            logging.debug(f"빈 입력값: 상품명='{haereum_product_name}', 후보 개수={len(candidate_images) if candidate_images else 0}")
            return candidate_images or []
        
        # 해오름 상품명의 해시값 생성 (16자리)
        target_hash = generate_product_name_hash(haereum_product_name)
        if not target_hash:
            logging.warning(f"상품 해시 생성 실패: {haereum_product_name}")
            return candidate_images  # 해시 생성 실패시 모든 후보 반환
        
        # 성능 최적화: 빈 리스트 미리 체크
        if not candidate_images:
            return []
                
        filtered_images = []
        hash_matches = 0
        
        for img_path in candidate_images:
            try:
                # 파일명에서 해시 추출 (16자리)
                img_hash = extract_product_hash_from_filename(img_path)
                if img_hash and img_hash == target_hash:
                    filtered_images.append(img_path)
                    hash_matches += 1
            except Exception as e:
                logging.debug(f"이미지 해시 추출 오류 '{img_path}': {e}")
                continue
        
        # 결과 로깅 개선
        if hash_matches > 0:
            logging.info(f"✅ 해시 매칭 성공: '{haereum_product_name}' (해시: {target_hash[:8]}...) "
                        f"-> {hash_matches}/{len(candidate_images)} 이미지 매칭")
        else:
            logging.debug(f"❌ 해시 매칭 실패: '{haereum_product_name}' (해시: {target_hash[:8]}...) "
                         f"-> 0/{len(candidate_images)} 이미지 매칭")
                
        return filtered_images
            
    except Exception as e:
        logging.error(f"해시 기반 이미지 필터링 오류 '{haereum_product_name}': {e}")
        return candidate_images  # 오류시 모든 후보 반환

# --- 나머지 코드는 그대로 유지 ---
# ... existing code ...

# Configure TensorFlow GPU memory globally at module level
try:
    # Global GPU configuration - must happen before any model is loaded
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        # Set memory growth on all GPUs
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)
        logging.info(f"Set memory growth for {len(gpus)} GPUs at module level")
except Exception as e:
    logging.warning(f"Failed to configure TensorFlow GPU memory at module level: {e}")

# Import the enhanced image matcher if available
try:
    from PythonScript.enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
    ENHANCED_MATCHER_AVAILABLE = True
    logging.info("Enhanced image matcher imported successfully")
except ImportError:
    try:
        # Try direct import without PythonScript prefix
        from enhanced_image_matcher import EnhancedImageMatcher, check_gpu_status
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
    
    # GPU configuration now happens at module level import time
    # We don't need to configure GPU in each worker, just log status
    try:
        gpus = tf.config.list_physical_devices('GPU')
        if gpus:
            logging.info(f"Worker {pid}: Found {len(gpus)} GPUs")
        else:
            logging.info(f"Worker {pid}: No GPUs detected")
    except Exception as e:
        logging.warning(f"Worker {pid}: Error checking GPU devices: {e}")
    
    # config.ini 직접 읽기 (UTF-8 인코딩 명시적 지정)
    if isinstance(config, str):
        try:
            parser = configparser.ConfigParser()
            parser.read(config, encoding=DEFAULT_ENCODING)
            config = parser
            logging.info(f"Worker {pid}: Read config from path {config} using {DEFAULT_ENCODING} encoding")
        except Exception as e:
            logging.error(f"Worker {pid}: Error reading config from path: {e}")
            # retry with fallback encoding
            try:
                parser = configparser.ConfigParser()
                parser.read(config)
                config = parser
                logging.info(f"Worker {pid}: Read config using default encoding")
            except Exception as e2:
                logging.error(f"Worker {pid}: Error reading config with default encoding: {e2}")
    
    # Initialize ProductMatcher with retry
    max_retries = 3
    retry_count = 0
    retry_delay = 2
    
    # Rest of the function remains the same
    while retry_count < max_retries:
        try:
            worker_matcher_instance = ProductMatcher(config)
            
            # Explicitly initialize models
            worker_matcher_instance._initialize_text_model()
            worker_matcher_instance._initialize_image_model()
            
            if worker_matcher_instance.text_model is None or worker_matcher_instance.image_model is None:
                logging.error(f"Worker {pid}: Failed to load models during initialization.")
                retry_count += 1
                if retry_count < max_retries:
                    logging.info(f"Worker {pid}: Retrying initialization ({retry_count}/{max_retries})...")
                    time.sleep(retry_delay * retry_count)  # Exponential backoff
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
                time.sleep(retry_delay * retry_count)  # Exponential backoff
            else:
                raise RuntimeError(f"Failed to initialize ProductMatcher after {max_retries} attempts: {str(e)}")

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
            
        hash_val = hashlib.md5(hash_input.encode(DEFAULT_ENCODING)).hexdigest()
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
        if pd.isna(combined_score):
            return "none"
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
            df['고려_매칭품질'] = df['_고려_Combined'].apply(self.evaluate_match)
            
        # 네이버 매칭 품질
        if '_네이버_Combined' in df.columns:
            df['네이버_매칭품질'] = df['_네이버_Combined'].apply(self.evaluate_match)
            
        return df

# --- Product Matching Logic ---
class ProductMatcher:
    def __init__(self, config: configparser.ConfigParser):
        """Initialize the product matcher with configuration."""
        self.feature_cache = {}
        self.config = config
        
        # Set default thresholds and weights - will be overridden if in config
        self.text_similarity_threshold = 0.45
        self.image_similarity_threshold = 0.42
        self.combined_threshold = 0.48
        self.minimum_combined_score = 0.40
        self.text_weight = 0.65
        self.image_weight = 0.35
        self.price_similarity_weight = 0.15
        self.exact_match_bonus = 0.25
        self.image_model_name = 'EfficientNetB0'
        self.text_model_name = 'jhgan/ko-sroberta-multitask'
        self.use_tfidf = False
        self.use_ensemble_models = True
        
        # Initialize missing attributes that were causing errors
        self.token_match_weight = 0.35
        self.ensemble_models = True
        self.image_ensemble = False
        self.fuzzy_match_threshold = 0.8
        self.use_gpu = False
        self.text_model_path = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
        self.image_resize_dimension = 256
        
        # Initialize category thresholds
        self.use_category_thresholds = False
        self.category_thresholds = {}
        
        try:
            # Load thresholds from config if available
            self.text_similarity_threshold = config.getfloat('Matching', 'text_threshold', fallback=self.text_similarity_threshold)
            self.image_similarity_threshold = config.getfloat('Matching', 'image_threshold', fallback=self.image_similarity_threshold)
            self.combined_threshold = config.getfloat('Matching', 'combined_threshold', fallback=self.combined_threshold)
            self.minimum_combined_score = config.getfloat('Matching', 'minimum_combined_score', fallback=self.minimum_combined_score)
            self.text_weight = config.getfloat('Matching', 'text_weight', fallback=self.text_weight)
            self.image_weight = config.getfloat('Matching', 'image_weight', fallback=self.image_weight)
            self.price_similarity_weight = config.getfloat('Matching', 'price_similarity_weight', fallback=self.price_similarity_weight)
            self.exact_match_bonus = config.getfloat('Matching', 'exact_match_bonus', fallback=self.exact_match_bonus)
            self.use_category_thresholds = config.getboolean('Matching', 'use_category_thresholds', fallback=False)
            self.image_model_name = config.get('Matching', 'image_model_name', fallback=self.image_model_name)
            self.text_model_name = config.get('Matching', 'text_model_name', fallback=self.text_model_name)
            self.use_tfidf = config.getboolean('Matching', 'use_tfidf', fallback=self.use_tfidf)
            self.use_ensemble_models = config.getboolean('Matching', 'use_ensemble_models', fallback=self.use_ensemble_models)
            
            # Load additional configuration parameters
            self.token_match_weight = config.getfloat('Matching', 'token_match_weight', fallback=self.token_match_weight)
            self.ensemble_models = config.getboolean('Matching', 'use_ensemble_models', fallback=self.ensemble_models)
            self.fuzzy_match_threshold = config.getfloat('Matching', 'fuzzy_match_threshold', fallback=self.fuzzy_match_threshold)
            self.use_gpu = config.getboolean('Matching', 'use_gpu', fallback=self.use_gpu)
            self.image_resize_dimension = config.getint('Matching', 'image_resize_dimension', fallback=self.image_resize_dimension)
            self.image_ensemble = config.getboolean('ImageMatching', 'use_multiple_models', fallback=self.image_ensemble)
            
            # Load text model path from Paths section if available
            if config.has_option('Paths', 'text_model_path'):
                self.text_model_path = config.get('Paths', 'text_model_path')
            else:
                # Fall back to the text_model_name if text_model_path not defined
                self.text_model_path = self.text_model_name
            
            # Load category thresholds if enabled
            if self.use_category_thresholds:
                self._load_category_thresholds(config)
        except Exception as e:
            logging.error(f"Error reading matching thresholds/weights/models from [Matching] config: {e}. Using defaults.")
        
        # Initialize text model
        self.text_model = None
        self.text_tokenizer = None
        self.tfidf_vectorizer = None
        try:
            self._initialize_text_model()
        except Exception as e:
            logging.error(f"Error initializing text model: {e}")
            logging.warning("Text matching will use fallback methods only.")
        
        # Initialize image model
        self.image_model = None
        try:
            self._initialize_image_model()
        except Exception as e:
            logging.error(f"Error initializing image model: {e}")
            logging.warning("Image matching will be limited or disabled.")
        
        # Initialize additional tools and caches for enhanced accuracy
        self.feature_cache = FeatureCache(config)
        
        # Initialize tfidf vectorizer if used
        if self.use_tfidf:
            from koSBERT_text_similarity import initialize_tfidf_vectorizer
            self.tfidf_vectorizer = initialize_tfidf_vectorizer()
        
        logging.info(f"ProductMatcher initialized with thresholds: text={self.text_similarity_threshold}, "
                   f"image={self.image_similarity_threshold}, combined={self.combined_threshold}, "
                   f"minimum_combined={self.minimum_combined_score}")
        
        # Log advanced settings
        logging.info(f"Enhanced matching settings: token_weight={self.token_match_weight}, "
                   f"use_tfidf={self.use_tfidf}, ensemble_models={self.ensemble_models}, "
                   f"image_ensemble={self.image_ensemble}, use_category_thresholds={self.use_category_thresholds}, "
                   f"fuzzy_match_threshold={self.fuzzy_match_threshold}")

    def _initialize_image_model(self):
        """Initialize EfficientNetB0 image model with improved error handling and GPU management"""
        try:
            logging.info("Loading EfficientNetB0 image model...")
            
            # GPU memory growth is now handled at module level, we just need to check status
            if self.use_gpu:
                gpus = tf.config.list_physical_devices('GPU')
                if gpus:
                    logging.info(f"Using {len(gpus)} GPUs for image model")
                else:
                    logging.warning("No GPUs available despite GPU flag being set")
                    logging.info("Falling back to CPU for image model")
                    self.use_gpu = False
            
            # Initialize base model with proper input shape
            base_model = tf.keras.applications.EfficientNetB0(
                weights='imagenet', 
                include_top=False, 
                input_shape=(self.image_resize_dimension, self.image_resize_dimension, 3)
            )
            
            # Add pooling layer
            global_avg_layer = tf.keras.layers.GlobalAveragePooling2D()(base_model.output)
            
            # Create final model
            model = tf.keras.Model(inputs=base_model.input, outputs=global_avg_layer)
            
            # Compile model with proper settings
            model.compile(optimizer='adam', loss='mse')
            
            self.image_model = model
            logging.info("Image model loaded successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to load image model: {e}", exc_info=True)
            self.image_model = None
            return False

    def calculate_text_similarity(self, text1: Optional[str], text2: Optional[str]) -> float:
        """
        두 텍스트 간의 유사도를 계산합니다.
        개선된 텍스트 유사도 계산 로직을 사용합니다.
        """
        if not text1 or not text2:
            return 0.0
        
        try:
            # 개선된 koSBERT_text_similarity 모듈 사용
            from koSBERT_text_similarity import (
                calculate_text_similarity, 
                calculate_ensemble_similarity,
                calculate_token_similarity,
                calculate_fuzzy_similarity,
                calculate_tfidf_similarity,
                get_number_match_score,
                preprocess_text
            )
            
            # 텍스트 전처리 (개선된 버전)
            text1_prep = preprocess_text(text1)
            text2_prep = preprocess_text(text2)
            
            if not text1_prep or not text2_prep:
                return 0.0
            
            # 완전히 동일한 경우
            if text1_prep == text2_prep:
                return 1.0
            
            # 토큰 기반 유사도 (개선된 버전)
            token_sim = calculate_token_similarity(text1_prep, text2_prep)
            
            # 숫자 매칭 점수 (개선된 버전)
            number_sim = get_number_match_score(text1_prep, text2_prep)
            
            # 퍼지 매칭 (개선된 버전)
            fuzzy_sim = calculate_fuzzy_similarity(text1_prep, text2_prep)
            
            # TF-IDF 유사도 (개선된 버전)
            tfidf_sim = 0.0
            if self.use_tfidf:
                tfidf_sim = calculate_tfidf_similarity(text1_prep, text2_prep)
            
            # 인코딩 기반 유사도 (앙상블 또는 단일 모델)
            if self.ensemble_models:
                model_sim = calculate_ensemble_similarity(text1_prep, text2_prep)
            else:
                # 단일 모델 유사도 계산
                model_sim = calculate_text_similarity(text1_prep, text2_prep, self.text_model_path)
            
            # 가중치 적용하여 최종 점수 계산 (개선된 버전)
            final_sim = (
                0.55 * model_sim +    # 인코딩 기반 (앙상블)
                0.20 * token_sim +    # 토큰 기반
                0.10 * fuzzy_sim +    # 퍼지 매칭
                0.10 * tfidf_sim +    # TF-IDF
                0.05 * number_sim     # 숫자 매칭
            )
            
            # 정확히 일치하는 경우 보너스
            if text1_prep == text2_prep:
                final_sim = min(1.0, final_sim + self.exact_match_bonus)
            
            logging.debug(f"Text similarity [{text1[:20]}...] vs [{text2[:20]}...]: {final_sim:.4f}")
            
            return final_sim
            
        except Exception as e:
            logging.error(f"텍스트 유사도 계산 중 오류 발생: {e}", exc_info=True)
            
            # 모듈 임포트 실패 또는 다른 오류 발생 시 개선된 fallback 방식으로 처리
            return self._fallback_text_similarity(text1, text2)

    def _fallback_text_similarity(self, text1: str, text2: str) -> float:
        """개선된 fallback 텍스트 유사도 계산 방식"""
        try:
            if self.text_model is None:
                self._initialize_text_model()
                if self.text_model is None:
                    logging.error("텍스트 모델 로드 실패")
                    return 0.0
            
            # 텍스트 전처리
            text1 = str(text1).strip()
            text2 = str(text2).strip()
            
            if not text1 or not text2:
                return 0.0
            
            # 기존 모델 사용 로직
            embedding1 = self.text_model.encode(text1, convert_to_tensor=True)
            embedding2 = self.text_model.encode(text2, convert_to_tensor=True)
            
            similarity = util.pytorch_cos_sim(embedding1, embedding2).item()
            
            # 기본적인 토큰 매칭 점수 추가
            tokens1 = set(text1.split())
            tokens2 = set(text2.split())
            token_overlap = len(tokens1.intersection(tokens2)) / max(len(tokens1), len(tokens2))
            
            # 최종 점수 계산 (기본 유사도 + 토큰 매칭)
            final_score = (0.7 * similarity) + (0.3 * token_overlap)
            
            return float(np.clip(final_score, 0.0, 1.0))
            
        except Exception as e:
            logging.error(f"기본 텍스트 유사도 계산 중 오류 발생: {e}")
            return 0.0

    def _initialize_text_model(self):
        """Initialize the text similarity model"""
        try:
            logging.info(f"Loading text similarity model from {self.text_model_path}...")
            
            # Determine device (GPU if available, else CPU)
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            logging.info(f"Using device: {device} for text model")

            self.text_model = SentenceTransformer(self.text_model_path, device=device)
            # The model is loaded directly onto the specified device.
            # No need for .to(device) after initialization.
            
            logging.info("Text similarity model loaded successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to load text similarity model: {e}", exc_info=True)
            self.text_model = None
            return False

    def calculate_image_similarity(self, image_path1: str, image_path2: str) -> float:
        """
        Calculate visual similarity between two images.
        
        Args:
            image_path1: Path to the first image
            image_path2: Path to the second image
            
        Returns:
            float: Similarity score between 0 and 1
        """
        # Check if enhanced image matcher is available and should be used
        if ENHANCED_MATCHER_AVAILABLE and self.image_ensemble:
            try:
                from enhanced_image_matcher import EnhancedImageMatcher
                enhanced_matcher = EnhancedImageMatcher(use_gpu=True)
                similarity = enhanced_matcher.calculate_similarity(image_path1, image_path2)
                return similarity
            except Exception as e:
                logging.warning(f"Enhanced image matcher failed: {e}. Falling back to basic image similarity.")
        
        # Use cached features if available
        img1_features = self.feature_cache.get(image_path1)
        img2_features = self.feature_cache.get(image_path2)
        
        # Extract features if not in cache
        if img1_features is None:
            img1_features = self._extract_image_features(image_path1)
            if img1_features is not None:
                self.feature_cache.put(image_path1, img1_features)
                
        if img2_features is None:
            img2_features = self._extract_image_features(image_path2)
            if img2_features is not None:
                self.feature_cache.put(image_path2, img2_features)
        
        # Calculate similarity if features were extracted successfully
        if img1_features is not None and img2_features is not None:
            # Calculate cosine similarity between feature vectors
            similarity = np.dot(img1_features, img2_features) / (
                np.linalg.norm(img1_features) * np.linalg.norm(img2_features)
            )
            return float(similarity)
        
        # Return 0 if feature extraction failed
        return 0.0
    
    def _extract_image_features(self, image_path: str) -> Optional[np.ndarray]:
        """
        Extract feature vector from an image.
        
        Args:
            image_path: Path to the image
            
        Returns:
            np.ndarray: Feature vector or None if extraction failed
        """
        try:
            # Initialize image model if not already done
            if self.image_model is None:
                success = self._initialize_image_model()
                if not success:
                    logging.error(f"Failed to initialize image model for feature extraction")
                    return None
            
            # Load and preprocess the image
            img = self._preprocess_image(image_path)
            if img is None:
                return None
            
            # Ensure image is in batch format [1, height, width, channels]
            if len(img.shape) == 3:
                img = np.expand_dims(img, axis=0)
            
            # Extract features
            features = self.image_model.predict(img, verbose=0)
            
            # Normalize features to unit length
            features = features / np.linalg.norm(features)
            return features.flatten()
            
        except Exception as e:
            logging.error(f"Error extracting image features from {image_path}: {e}")
            return None
    
    def _preprocess_image(self, image_path: str) -> Optional[np.ndarray]:
        """
        Preprocess image for the model.
        
        Args:
            image_path: Path to the image
            
        Returns:
            np.ndarray: Preprocessed image or None if preprocessing failed
        """
        try:
            # Check if image exists
            if not os.path.exists(image_path):
                logging.error(f"Image does not exist: {image_path}")
                
                # Try to fix the path - look for the file in standard directories
                base_img_dir = os.environ.get('RPA_IMAGE_DIR', 'C:\\RPA\\Image')
                filename = os.path.basename(image_path)
                
                # Common image locations to check
                possible_paths = []
                
                # Check if this is a Haereum, Kogift or Naver image based on filename
                if 'haereum' in filename.lower():
                    possible_paths = [
                        os.path.join(base_img_dir, 'Main', 'Haereum', filename),
                        os.path.join(base_img_dir, 'Target', 'Haereum', filename)
                    ]
                elif 'kogift' in filename.lower():
                    possible_paths = [
                        os.path.join(base_img_dir, 'Main', 'Kogift', filename),
                        os.path.join(base_img_dir, 'Main', 'kogift', filename),
                        os.path.join(base_img_dir, 'Target', 'Kogift', filename),
                        os.path.join(base_img_dir, 'Target', 'kogift', filename)
                    ]
                elif 'naver' in filename.lower():
                    possible_paths = [
                        os.path.join(base_img_dir, 'Main', 'Naver', filename),
                        os.path.join(base_img_dir, 'Target', 'Naver', filename)
                    ]
                
                # Check each possible path
                for path in possible_paths:
                    if os.path.exists(path):
                        logging.info(f"Found image at alternative path: {path}")
                        image_path = path
                        break
                
                # If still not found, return None
                if not os.path.exists(image_path):
                    return None
            
            # Open and resize image
            img = Image.open(image_path).convert('RGB')
            img = img.resize((self.image_resize_dimension, self.image_resize_dimension))
            
            # Convert to numpy array and preprocess for EfficientNet
            img_array = np.array(img)
            img_array = tf.keras.applications.efficientnet.preprocess_input(img_array)
            
            return img_array
            
        except Exception as e:
            logging.error(f"Error preprocessing image {image_path}: {e}")
            return None

    def _load_category_thresholds(self, config):
        """Load category-specific thresholds from config."""
        if not self.use_category_thresholds:
            return
            
        category_section = 'CategoryThresholds'
        if not config.has_section(category_section):
            logging.warning("Category thresholds enabled but no CategoryThresholds section in config")
            return
            
        for category in config.options(category_section):
            try:
                thresholds = config.get(category_section, category).split(',')
                if len(thresholds) == 2:
                    text_thresh = float(thresholds[0].strip())
                    image_thresh = float(thresholds[1].strip())
                    self.category_thresholds[category] = (text_thresh, image_thresh)
                else:
                    logging.warning(f"Invalid threshold format for category {category}")
            except (ValueError, configparser.Error) as e:
                logging.error(f"Error loading thresholds for category {category}: {e}")

    def get_thresholds_for_category(self, category: Optional[str]) -> Tuple[float, float]:
        """Get text and image thresholds for a specific category."""
        if not self.use_category_thresholds or not category or category not in self.category_thresholds:
            return self.text_similarity_threshold, self.image_similarity_threshold
            
        return self.category_thresholds[category]

def _match_single_product(i: int, haoreum_row_dict: Dict, kogift_data: List[Dict], naver_data: List[Dict], product_type: str, matcher: ProductMatcher, haoreum_img_path: Optional[str]) -> Tuple[int, Optional[Dict]]:
    """Matches a single Haoreum product against Kogift and Naver data."""
    if not matcher:
        logging.error(f"Matcher object missing for index {i}. Cannot process.")
        return i, None

    # Wrap the main logic in a single try-except block
    try:
        # Validate input data
        if not isinstance(haoreum_row_dict, dict):
            logging.error(f"Invalid haoreum_row_dict type for index {i}: {type(haoreum_row_dict)}")
            return i, None
            
        if not isinstance(kogift_data, list) or not isinstance(naver_data, list):
            logging.error(f"Invalid candidate data type for index {i}: Kogift={type(kogift_data)}, Naver={type(naver_data)}")
            return i, None
            
        # Validate product name
        product_name = haoreum_row_dict.get('상품명')
        if not product_name or not isinstance(product_name, str):
            # Try alternative key '상품명(자체)'
            product_name = haoreum_row_dict.get('상품명(자체)')
            if not product_name or not isinstance(product_name, str):
                logging.error(f"Invalid or missing product name (checked '상품명' and '상품명(자체)') for index {i}: {product_name}")
                return i, None
            
        logging.debug(f"Matching product index {i}: {product_name}")
        
        # --- Get Haoreum specific data ---
        haoreum_scraped_image_url = haoreum_row_dict.get('본사이미지URL')
        # Add logging to check the fetched URL
        logging.debug(f"Index {i}: Fetched Haereum scraped URL: {haoreum_scraped_image_url}") 
        if not haoreum_scraped_image_url or not isinstance(haoreum_scraped_image_url, str) or not haoreum_scraped_image_url.startswith(('http://', 'https://')):
            logging.warning(f"Row {i} ('{product_name}'): Invalid or missing scraped Haereum image URL: '{haoreum_scraped_image_url}'. Attempting fallback or proceeding without URL.")
            haoreum_scraped_image_url = None # Set URL to None if invalid

        # Prepare Haoreum product data structure for matching logic
        haoreum_product_for_match = {
            'name': product_name,
            'price': pd.to_numeric(haoreum_row_dict.get('판매단가(V포함)'), errors='coerce'),
            'image_path': haoreum_img_path, # This is the LOCAL path passed in
            'code': haoreum_row_dict.get('Code'),
            '카테고리(중분류)': haoreum_row_dict.get('카테고리(중분류)')
        }

        # Find best matches
        best_kogift_match = None
        best_naver_match = None
        
        if kogift_data:  
            best_kogift_match = _find_best_match(haoreum_product_for_match, kogift_data, matcher, 'kogift')
        
        if naver_data:  
            best_naver_match = _find_best_match(haoreum_product_for_match, naver_data, matcher, 'naver')

        # --- Combine results into the final structure --- 
        # Start with the original Haoreum row data
        result = {**haoreum_row_dict} 

        # --- Add/Overwrite Haoreum Image Data --- 
        # Ensure the scraped URL is prioritized
        haoreum_image_data = {
             'url': haoreum_scraped_image_url, # Use the fetched URL directly
             'local_path': haoreum_img_path,
             'source': 'haereum'
        }
        result['본사 이미지'] = haoreum_image_data
        # Remove the old column if it exists to avoid confusion
        result.pop('해오름이미지경로', None) 
        # Also remove the raw URL column if it's just duplicated here
        result.pop('본사이미지URL', None) 

        # --- Add/Update other non-image Haoreum fields --- 
        result.update({
            '구분': product_type,
            '본사상품링크': haoreum_row_dict.get('본사상품링크'),
        })

        # --- Add Kogift Data --- 
        if best_kogift_match:
             kogift_img_path = best_kogift_match['match_data'].get('image_path')
             kogift_url = best_kogift_match['match_data'].get('image_url') or best_kogift_match['match_data'].get('link')
             kogift_image_data = None
             if kogift_url or kogift_img_path:
                 kogift_image_data = {
                     'url': kogift_url,
                     'local_path': kogift_img_path,
                     'source': 'kogift'
                 }

             result.update({
                '고려기프트 상품링크': best_kogift_match['match_data'].get('link'),
                '고려기프트 이미지': kogift_image_data,
                '판매단가(V포함)(2)': best_kogift_match['match_data'].get('price'), 
                '_고려_TextSim': best_kogift_match['text_similarity'],
                '_해오름_고려_ImageSim': best_kogift_match['image_similarity'],
                '_고려_Combined': best_kogift_match['combined_score'],
                '기본수량(2)': best_kogift_match['match_data'].get('quantity', '-') 
            })
        else:
             result.update({
                '고려기프트 상품링크': None,
                '고려기프트 이미지': None,
                '판매단가(V포함)(2)': None,
                '_고려_TextSim': 0.0,
                '_해오름_고려_ImageSim': 0.0,
                '_고려_Combined': None,
                '기본수량(2)': '-'
            })

        # --- Add Naver Data --- 
        if best_naver_match:
            naver_img_path = best_naver_match['match_data'].get('image_path')
            naver_url = (best_naver_match['match_data'].get('image_url') or 
                         best_naver_match['match_data'].get('image') or 
                         best_naver_match['match_data'].get('imageUrl') or 
                         best_naver_match['match_data'].get('네이버 이미지'))
            
            existing_naver_img_dict = best_naver_match['match_data'].get('네이버 이미지')
            if isinstance(existing_naver_img_dict, dict) and existing_naver_img_dict.get('url'):
                naver_url = existing_naver_img_dict.get('url')
                if not naver_img_path and existing_naver_img_dict.get('local_path'):
                    naver_img_path = existing_naver_img_dict.get('local_path')

            naver_image_data = None
            if naver_url or naver_img_path:
                 naver_image_data = {
                     'url': naver_url,
                     'local_path': naver_img_path,
                     'source': 'naver'
                 }
                
            result.update({
                '매칭_사이트': 'Naver',
                '공급사명': best_naver_match['match_data'].get('mallName', best_naver_match['match_data'].get('seller', '')), 
                '네이버 쇼핑 링크': best_naver_match['match_data'].get('link'), 
                '공급사 상품링크': best_naver_match['match_data'].get('mallProductUrl', best_naver_match['match_data'].get('originallink')),
                '네이버 이미지': naver_image_data,
                '판매단가(V포함)(3)': best_naver_match['match_data'].get('price'), 
                '텍스트_유사도': best_naver_match['text_similarity'],
                '이미지_유사도': best_naver_match['image_similarity'],
                '매칭_정확도': best_naver_match['combined_score'],
                '기본수량(3)': best_naver_match['match_data'].get('quantity', '1'),
                '매칭_여부': 'Y',
                '매칭_품질': '상' if best_naver_match['combined_score'] > 0.8 else '중' if best_naver_match['combined_score'] > 0.6 else '하'
            })
        else:
            result.update({
                '매칭_여부': 'Y' if best_kogift_match else 'N', 
                '매칭_품질': '실패' if not best_kogift_match else result.get('매칭_품질', '-'),
                '공급사명': None,
                '네이버 쇼핑 링크': None,
                '공급사 상품링크': None,
                '네이버 이미지': None,
                '판매단가(V포함)(3)': None,
                '기본수량(3)': None,
                '텍스트_유사도': result.get('_고려_TextSim', 0.0) if best_kogift_match else None,
                '이미지_유사도': result.get('_해오름_고려_ImageSim', 0.0) if best_kogift_match else None,
                '매칭_정확도': result.get('_고려_Combined') if best_kogift_match else None,
                '매칭_사이트': 'Kogift' if best_kogift_match else None
            })

        # If combination is successful, return the result
        logging.debug(f"Successfully processed product {product_name} (index {i})")
        return i, result 
            
    except Exception as e:
        # Catch any error during the processing of this product
        logging.error(f"Error processing product {product_name} (index {i}): {e}", exc_info=True)
        return i, None # Return None if processing fails for this product

def _find_best_match(haereum_product: Dict, candidates: List[Dict], matcher: ProductMatcher, source: str) -> Optional[Dict]:
    """
    Find the best matching product from a list of candidates.
    Enhanced with hash-based filtering and improved image similarity threshold.
    
    Args:
        haereum_product: The reference product (Haereum)
        candidates: List of candidate products to match against
        matcher: The ProductMatcher instance to use
        source: Source of candidates ('kogift' or 'naver')
        
    Returns:
        Dict with match data and similarity scores, or None if no match found
    """
    if not candidates or not haereum_product:
        return None
        
    # Get product name
    product_name = haereum_product.get('name', '')
    if not product_name:
        return None
        
    logging.info(f"🔍 Starting hash-based matching for '{product_name}' from {len(candidates)} {source} candidates")
    
    # --- STEP 1: Enhanced Hash-based filtering ---
    # First filter candidates by product name hash to reduce search space
    target_hash = generate_product_name_hash(product_name)
    hash_filtered_candidates = []
    
    if target_hash:
        logging.debug(f"🔑 Target hash for '{product_name}': {target_hash}")
        
        for i, candidate in enumerate(candidates):
            candidate_img_path = candidate.get('image_path')
            if candidate_img_path:
                candidate_hash = extract_product_hash_from_filename(candidate_img_path)
                if candidate_hash:
                    logging.debug(f"   Candidate {i+1}: hash={candidate_hash}, path={os.path.basename(candidate_img_path)}")
                    if candidate_hash == target_hash:
                        hash_filtered_candidates.append(candidate)
                        logging.debug(f"   ✅ Hash match found for candidate {i+1}")
                else:
                    logging.debug(f"   ❌ No hash extracted from candidate {i+1}: {os.path.basename(candidate_img_path) if candidate_img_path else 'No path'}")
            else:
                logging.debug(f"   ❌ Candidate {i+1} has no image_path")
        
        if hash_filtered_candidates:
            efficiency_gain = (len(candidates) - len(hash_filtered_candidates)) / len(candidates) * 100
            logging.info(f"🎯 Hash filtering: {len(candidates)} → {len(hash_filtered_candidates)} candidates ({efficiency_gain:.1f}% reduction)")
            candidates = hash_filtered_candidates
        else:
            logging.warning(f"⚠️ No hash matches found for '{product_name}' (hash: {target_hash}). Proceeding with all {len(candidates)} candidates")
            # 해시 매치가 없을 때는 전체 후보군을 사용하되, 더 엄격한 임계값 적용
            logging.info("🔄 Falling back to full candidate set with stricter thresholds")
    else:
        logging.warning(f"⚠️ Could not generate hash for product '{product_name}', skipping hash filtering")
    
    # --- STEP 2: Image similarity matching with 0.8 threshold ---
    best_match = None
    best_text_sim = 0
    best_img_sim = 0
    best_combined = 0
    
    # Get thresholds based on category
    category = haereum_product.get('중분류카테고리') or haereum_product.get('카테고리(중분류)')
    text_threshold, img_threshold = matcher.get_thresholds_for_category(category)
    
    # Apply user-requested image similarity threshold of 0.8
    img_threshold = 0.8
    logging.info(f"📊 Using thresholds - Text: {text_threshold:.2f}, Image: {img_threshold:.2f} (user-requested)")
    
    # Enhanced thresholds based on source and hash filtering results
    if source == 'naver':
        text_threshold = max(text_threshold, 0.5)  # Stricter for Naver
        if not hash_filtered_candidates:  # No hash matches found
            text_threshold = max(text_threshold, 0.6)  # Even stricter
            logging.info(f"🔍 Using enhanced Naver thresholds due to no hash matches - Text: {text_threshold:.2f}")
    
    candidates_processed = 0
    candidates_text_passed = 0
    candidates_image_passed = 0
    
    for i, candidate in enumerate(candidates):
        candidate_name = candidate.get('name', '')
        if not candidate_name:
            continue
            
        candidates_processed += 1
        
        # Calculate text similarity
        text_sim = matcher.calculate_text_similarity(product_name, candidate_name)
        
        # Skip candidates with very low text similarity early
        min_text_threshold = 0.2 if source == 'kogift' else 0.3
        if text_sim < min_text_threshold:
            logging.debug(f"   ❌ Text similarity too low: {text_sim:.3f} < {min_text_threshold:.3f} for '{candidate_name[:30]}...'")
            continue
            
        candidates_text_passed += 1
            
        # Calculate image similarity if images are available
        img_sim = 0
        haereum_img_path = haereum_product.get('image_path')
        candidate_img_path = candidate.get('image_path')
        
        if haereum_img_path and candidate_img_path:
            img_sim = matcher.calculate_image_similarity(haereum_img_path, candidate_img_path)
            
            # Apply 0.8 image similarity threshold as requested
            if img_sim < img_threshold:
                logging.debug(f"   ❌ Image similarity too low: {img_sim:.3f} < {img_threshold:.3f} for '{candidate_name[:30]}...'")
                continue
                
            candidates_image_passed += 1
            logging.debug(f"   ✅ Candidate passed both filters: text={text_sim:.3f}, image={img_sim:.3f} for '{candidate_name[:30]}...'")
        else:
            # If no images available, skip this candidate (since image matching is crucial)
            logging.debug(f"   ❌ Missing image paths - Haereum: {bool(haereum_img_path)}, Candidate: {bool(candidate_img_path)}")
            continue
            
        # Calculate combined score with source-based weights
        if source == 'kogift':
            text_weight = 0.6
            img_weight = 0.4
        else:  # naver
            text_weight = 0.7
            img_weight = 0.3
            
        combined_score = (text_sim * text_weight) + (img_sim * img_weight)
        
        # Track best match
        if combined_score > best_combined:
            best_combined = combined_score
            best_text_sim = text_sim
            best_img_sim = img_sim
            best_match = candidate
            logging.debug(f"   🎯 New best match: combined={combined_score:.3f} for '{candidate_name[:30]}...'")
            
    # Log matching statistics
    logging.info(f"📈 Matching stats for '{product_name[:30]}...': "
                f"Processed={candidates_processed}, Text passed={candidates_text_passed}, "
                f"Image passed={candidates_image_passed}")
    
    # Log the best match found
    if best_match:
        name_snippet = best_match.get('name', '')[:40]
        logging.info(f"🏆 Best {source} match for '{product_name[:30]}...': "
                    f"'{name_snippet}' (Text: {best_text_sim:.3f}, Image: {best_img_sim:.3f}, Combined: {best_combined:.3f})")
        
        # Enhanced verification for edge cases
        if source == 'naver':
            # Set minimum combined score threshold for Naver
            min_combined_threshold = 0.35
            
            # If combined score is too low, reject the match
            if best_combined < min_combined_threshold:
                logging.warning(f"❌ Rejecting Naver match '{name_snippet}' due to low combined score: {best_combined:.3f} < {min_combined_threshold:.3f}")
                return None
                
            # Check for price consistency if available
            haereum_price = haereum_product.get('price', 0)
            match_price = best_match.get('price', 0)
            
            if haereum_price > 0 and match_price > 0:
                # Calculate price difference percentage
                price_diff_pct = abs(match_price - haereum_price) / haereum_price * 100
                
                # If price difference is too large and similarity is borderline, reject match
                if price_diff_pct > 70 and best_combined < 0.55:
                    logging.warning(f"❌ Rejecting Naver match with large price difference ({price_diff_pct:.1f}%) and borderline similarity ({best_combined:.3f})")
                    return None
        
        # Return match info including scores
        return {
            'match_data': best_match,
            'text_similarity': best_text_sim,
            'image_similarity': best_img_sim,
            'combined_score': best_combined
        }
    else:
        logging.info(f"❌ No suitable {source} match found for '{product_name[:30]}...' after processing {candidates_processed} candidates")
    
    return None

# Wrapper for ProcessPoolExecutor compatibility
def _match_single_product_wrapper(i: int, haoreum_row_dict: Dict, kogift_data: Optional[List[Dict]], naver_data: Optional[List[Dict]], product_type: str, haoreum_img_path: Optional[str]) -> Tuple[int, Optional[Dict]]:
    """Wrapper to call _match_single_product using the global worker instance."""
    global worker_matcher_instance
    if worker_matcher_instance is None:
        # Initialize matcher if not available
        try:
            from configparser import ConfigParser
            config = ConfigParser()
            
            # 중요: UTF-8 인코딩을 명시적으로 지정
            try:
                config.read('config.ini', encoding=DEFAULT_ENCODING)
                logging.info(f"Config file loaded with {DEFAULT_ENCODING} encoding in worker {os.getpid()}")
            except Exception as config_err:
                logging.error(f"Error reading config file with {DEFAULT_ENCODING} encoding: {config_err}")
                # 폴백: 인코딩 미지정
                try:
                    config.read('config.ini')
                    logging.warning(f"Falling back to default encoding in worker {os.getpid()}")
                except Exception as fallback_err:
                    logging.error(f"Failed to read config in any encoding: {fallback_err}")
                    
            worker_matcher_instance = ProductMatcher(config)
            logging.info(f"Initialized matcher in worker {os.getpid()}")
        except Exception as e:
            logging.error(f"Failed to initialize matcher in worker {os.getpid()}: {e}")
            return i, None

    try:
        # Call the actual matching logic
        return _match_single_product(
            i, haoreum_row_dict, kogift_data, naver_data, 
            product_type, worker_matcher_instance, haoreum_img_path
        )
    except Exception as e:
        logging.error(f"Error in _match_single_product_wrapper for index {i}: {e}", exc_info=True)
        return i, None

def process_matching(
    haoreum_df: pd.DataFrame,
    kogift_map: Dict[str, List[Dict]],
    naver_map: Dict[str, List[Dict]],
    input_file_image_map: Dict[Any, str],
    config: configparser.ConfigParser,
    gpu_available: bool,
    progress_queue=None,
    max_workers: Optional[int] = None
) -> pd.DataFrame:
    """
    Process product matching using multiple workers and enhanced matching logic
    """
    start_time = time.time()
    
    # Validate input data
    if not isinstance(haoreum_df, pd.DataFrame) or haoreum_df.empty:
        logging.error("Invalid or empty Haoreum DataFrame")
        return pd.DataFrame()
        
    if not isinstance(kogift_map, dict) or not isinstance(naver_map, dict):
        logging.error("Invalid Kogift or Naver map")
        return haoreum_df
        
    # Determine the number of CPU cores to use
    if max_workers is None:
        max_workers = config.getint('Concurrency', 'max_match_workers', 
                                   fallback=max(1, os.cpu_count() // 2))
    
    logging.info(f"Starting product matching with {max_workers} workers")
    if progress_queue:
        progress_queue.emit("status", f"상품 매칭 시작 (GPU: {gpu_available}, 작업자: {max_workers})")
    
    # Initialize matcher and multiprocessing
    total_products = len(haoreum_df)
    
    # Download all images before starting matching process
    # if progress_queue:
    #     progress_queue.emit("status", "이미지 다운로드 시작...")
    # 
    # try:
    #     import asyncio
    #     from image_downloader import download_all_images
    #     
    #     # Create event loop for async operations
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)
    #     
    #     # Collect all image URLs from all sources
    #     all_products = []
    #     
    #     # Add Haoreum products
    #     for _, row in haoreum_df.iterrows():
    #         product = row.to_dict()
    #         if 'Code' in product and product['Code'] in input_file_image_map:
    #             product['해오름이미지경로'] = input_file_image_map[product['Code']]
    #         elif '상품코드' in product and product['상품코드'] in input_file_image_map:
    #             product['해오름이미지경로'] = input_file_image_map[product['상품코드']]
    #         all_products.append(product)
    #     
    #     # Add Kogift products
    #     for product_name, products in kogift_map.items():
    #         for product in products:
    #             if isinstance(product, dict):
    #                 all_products.append(product)
    #     
    #     # Add Naver products
    #     for product_name, products in naver_map.items():
    #         for product in products:
    #             if isinstance(product, dict):
    #                 all_products.append(product)
    #     
    #     # Download all images
    #     image_paths = loop.run_until_complete(download_all_images(all_products))
    #     loop.close()
    #     
    #     if progress_queue:
    #         progress_queue.emit("status", f"이미지 다운로드 완료 ({len(image_paths)}개)")
    # except Exception as e:
    #     logging.error(f"Error downloading images: {e}")
    #     if progress_queue:
    #         progress_queue.emit("status", "이미지 다운로드 중 오류 발생")
    # 
    # # --- IMPORTANT: Update candidate maps with downloaded local paths --- 
    # logging.info("Updating candidate data with local image paths...")
    # # Update Kogift map
    # for product_name, products in kogift_map.items():
    #     if isinstance(products, list):
    #         for product_dict in products:
    #             if isinstance(product_dict, dict):
    #                 # Get original URL (might be under different keys)
    #                 original_url = product_dict.get('image_url') or product_dict.get('image') or product_dict.get('src')
    #                 if original_url and original_url in image_paths:
    #                     product_dict['image_path'] = image_paths[original_url]
    #                 elif original_url:
    #                      logging.warning(f"Kogift image URL {original_url} not found in downloaded paths map.")
    # 
    # # Update Naver map
    # for product_name, products in naver_map.items():
    #     if isinstance(products, list):
    #         for product_dict in products:
    #             if isinstance(product_dict, dict):
    #                 # Get original URL (might be under different keys)
    #                 original_url = product_dict.get('image_url') or product_dict.get('image') or product_dict.get('src') or product_dict.get('네이버 이미지')
    #                 if original_url and original_url in image_paths:
    #                     product_dict['image_path'] = image_paths[original_url]
    #                 elif original_url:
    #                      logging.warning(f"Naver image URL {original_url} not found in downloaded paths map.")
    # logging.info("Finished updating candidate data with local image paths.")
    # ----------------------------------------------------------------

    # Initialize a multiprocessing pool
    try:
        logging.info(f"Initializing process pool for matching with {max_workers} workers")
        pool = multiprocessing.Pool(
            processes=max_workers,
            initializer=_init_worker_matcher,
            initargs=(config,)
        )
        
        # Get product type
        product_type = config.get('Processing', 'product_type', fallback='default')

        # Prepare arguments for parallel processing
        tasks = []
        for i, row in enumerate(haoreum_df.to_dict('records')):
            product_name = row.get('상품명', '')
            
            # Skip empty product names
            if not product_name:
                logging.warning(f"Skipping row {i} due to empty product name")
                continue
                
            # Get lists of candidates with validation
            kogift_candidates = []
            if product_name in kogift_map:
                candidates = kogift_map[product_name]
                if isinstance(candidates, list):
                    kogift_candidates = [c for c in candidates if isinstance(c, dict)]
                else:
                    logging.warning(f"Invalid Kogift candidates format for product {product_name}")
            
            naver_candidates = []
            if product_name in naver_map:
                candidates = naver_map[product_name]
                if isinstance(candidates, list):
                    naver_candidates = [c for c in candidates if isinstance(c, dict)]
                else:
                    logging.warning(f"Invalid Naver candidates format for product {product_name}")
            
            # Get Haoreum image path
            haoreum_img_path = None
            if 'Code' in row and row['Code'] in input_file_image_map:
                haoreum_img_path = input_file_image_map[row['Code']]
            elif '상품코드' in row and row['상품코드'] in input_file_image_map:
                haoreum_img_path = input_file_image_map[row['상품코드']]
                
            # Log task details
            logging.debug(f"Task {i+1}/{total_products}: '{product_name}' - " 
                        f"Kogift: {len(kogift_candidates)}, Naver: {len(naver_candidates)}, "
                        f"Image: {'Yes' if haoreum_img_path else 'No'}")
                
            tasks.append((i, row, kogift_candidates, naver_candidates, product_type, haoreum_img_path))
        
        # Process in batches to avoid overwhelming the queue
        batch_size = min(100, total_products)  # Adjust batch size based on total
        results = []

        for batch_start in range(0, total_products, batch_size):
            batch_end = min(batch_start + batch_size, total_products)
            batch_tasks = tasks[batch_start:batch_end]
            
            if not batch_tasks:
                continue
                
            logging.info(f"Processing batch {batch_start//batch_size + 1}: items {batch_start+1}-{batch_end} of {total_products}")
            
            # Use optimized wrapper function to reduce memory overhead
            batch_results = pool.starmap(_match_single_product_wrapper, batch_tasks)
            results.extend([r for r in batch_results if r[1] is not None])
            
            # Update progress
            progress_pct = min(100, int((batch_end / total_products) * 100))
            if progress_queue:
                progress_queue.emit("status", f"상품 매칭 진행 중: {progress_pct}% ({batch_end}/{total_products})")
            
            logging.info(f"Batch {batch_start//batch_size + 1} completed: {len([r for r in batch_results if r[1] is not None])} matches found")
            
        # Close pool
        pool.close()
        pool.join()
        
        # Create DataFrame from results
        result_df = haoreum_df.copy()
        
        # Initialize new columns
        result_df['매칭_여부'] = 'N'
        result_df['매칭_정확도'] = 0.0
        result_df['텍스트_유사도'] = 0.0
        result_df['이미지_유사도'] = 0.0
        result_df['제안_가격'] = None
        result_df['매칭_URL'] = None
        result_df['매칭_이미지'] = None
        result_df['매칭_상품명'] = None
        result_df['매칭_사이트'] = None
        result_df['가격차이'] = None
        result_df['매칭_품질'] = None

        # --- Add columns for detailed match results --- 
        # Kogift
        result_df['기본수량(2)'] = None
        result_df['판매단가(V포함)(2)'] = None
        result_df['고려기프트 상품링크'] = None
        result_df['고려기프트 이미지'] = None
        result_df['가격차이(2)'] = None # Calculated later if possible
        result_df['가격차이(2)(%)'] = None # Calculated later
        # Naver
        result_df['기본수량(3)'] = None
        result_df['판매단가(V포함)(3)'] = None
        result_df['공급사명'] = None
        result_df['네이버 쇼핑 링크'] = None
        result_df['공급사 상품링크'] = None
        result_df['네이버 이미지'] = None
        result_df['가격차이(3)'] = None # Calculated later
        result_df['가격차이(3)(%)'] = None # Calculated later

        # Placeholder for specific error messages if needed
        result_df['매칭_오류메시지'] = None 

        # Update matched products
        for idx, result in results:
            if result and isinstance(result, dict):
                result_df.at[idx, '매칭_여부'] = 'Y' # Mark as potentially matched

                # Copy basic matching metadata
                for field in ['매칭_정확도', '텍스트_유사도', '이미지_유사도', '매칭_사이트', '매칭_품질']:
                    if field in result:
                        result_df.at[idx, field] = result.get(field)

                # --- Populate detailed match information based on source --- 
                match_source = result.get('매칭_사이트')
                is_error_message = isinstance(result.get('price'), str) # Check if price is an error string

                if match_source == 'Kogift':
                    if not is_error_message:
                        result_df.at[idx, '기본수량(2)'] = result.get('수량') # Assuming '수량' is the key
                        result_df.at[idx, '판매단가(V포함)(2)'] = result.get('price')
                        result_df.at[idx, '고려기프트 상품링크'] = result.get('link')
                        result_df.at[idx, '고려기프트 이미지'] = result.get('image_path')
                    else:
                        # Store error message
                        result_df.at[idx, '매칭_오류메시지'] = result.get('price') # Or dedicated error field
                        # Optionally clear other Kogift fields or leave as None
                        result_df.at[idx, '고려기프트 상품링크'] = result.get('link') # Keep link if available
                        result_df.at[idx, '고려기프트 이미지'] = result.get('image_path') # Keep image if available

                elif match_source == 'Naver':
                    if not is_error_message:
                        result_df.at[idx, '기본수량(3)'] = result.get('수량') # Assuming '수량' is the key
                        result_df.at[idx, '판매단가(V포함)(3)'] = result.get('price')
                        result_df.at[idx, '공급사명'] = result.get('mallName') # Assuming 'mallName' is the key
                        result_df.at[idx, '네이버 쇼핑 링크'] = result.get('link') # Assuming 'link' is the key
                        result_df.at[idx, '공급사 상품링크'] = result.get('originallink') # Check actual key
                        
                        # Handle Naver image data - ensure it's in dictionary format
                        image_data = result.get('image_path')
                        if isinstance(image_data, dict):
                            # Already in dictionary format, use as is
                            result_df.at[idx, '네이버 이미지'] = image_data
                        elif isinstance(image_data, str):
                            # Convert string path to dictionary format
                            if image_data.startswith('http'):
                                # It's a URL
                                result_df.at[idx, '네이버 이미지'] = {
                                    'url': image_data,
                                    'source': 'naver'
                                }
                            else:
                                # It's a local path
                                result_df.at[idx, '네이버 이미지'] = {
                                    'local_path': image_data,
                                    'source': 'naver'
                                }
                        else:
                            # No valid image data
                            result_df.at[idx, '네이버 이미지'] = None
                    else:
                         # Store error message
                        result_df.at[idx, '매칭_오류메시지'] = result.get('price')
                        # Optionally clear other Naver fields or leave as None
                        result_df.at[idx, '네이버 쇼핑 링크'] = result.get('link') # Keep link if available
                        
                        # Handle Naver image data even in error case
                        image_data = result.get('image_path')
                        if isinstance(image_data, dict):
                            result_df.at[idx, '네이버 이미지'] = image_data
                        elif isinstance(image_data, str):
                            if image_data.startswith('http'):
                                result_df.at[idx, '네이버 이미지'] = {
                                    'url': image_data,
                                    'source': 'naver'
                                }
                            else:
                                result_df.at[idx, '네이버 이미지'] = {
                                    'local_path': image_data,
                                    'source': 'naver'
                                }
                        else:
                            result_df.at[idx, '네이버 이미지'] = None
                else:
                    # Handle cases where source is missing or different
                    logging.warning(f"Row {idx}: Match found but source ('{match_source}') is unknown or missing.")
                    if is_error_message:
                        result_df.at[idx, '매칭_오류메시지'] = result.get('price', '알 수 없는 매칭 오류')

            else:
                # Handle cases where matching failed entirely for the product
                result_df.at[idx, '매칭_여부'] = 'N'
                result_df.at[idx, '매칭_품질'] = '실패'
                result_df.at[idx, '매칭_오류메시지'] = '매칭 결과 없음' # Or a more specific error if available

        # --- Post-processing: Calculate Price Differences ---
        # Ensure base price column exists and is numeric
        if '판매단가(V포함)' in result_df.columns:  # Check if the base column exists
            base_price_col = pd.to_numeric(result_df['판매단가(V포함)'], errors='coerce')

            # Calculate for Kogift
            if '판매단가(V포함)(2)' in result_df.columns:
                kogift_price_col = pd.to_numeric(result_df['판매단가(V포함)(2)'], errors='coerce')
                # Only calculate if base price is valid
                valid_base = base_price_col.notna() & (base_price_col != 0)
                valid_kogift = kogift_price_col.notna()
                calculate_mask = valid_base & valid_kogift

                if calculate_mask.any(): # Proceed only if there are valid prices to compare
                   diff = kogift_price_col.where(calculate_mask) - base_price_col.where(calculate_mask)
                   result_df['가격차이(2)'] = diff
                   result_df['가격차이(2)(%)'] = np.where(
                       calculate_mask, # Use the combined mask
                       np.rint((diff / base_price_col.where(calculate_mask)) * 100).astype(int),
                       None
                   )
                else:
                   logging.debug("No valid base or Kogift prices found for difference calculation (Kogift).")
                   # Ensure columns exist even if calculation is skipped
                   if '가격차이(2)' not in result_df.columns: result_df['가격차이(2)'] = None
                   if '가격차이(2)(%)' not in result_df.columns: result_df['가격차이(2)(%)'] = None


            # Calculate for Naver
            if '판매단가(V포함)(3)' in result_df.columns:
                naver_price_col = pd.to_numeric(result_df['판매단가(V포함)(3)'], errors='coerce')
                 # Only calculate if base price is valid
                valid_base = base_price_col.notna() & (base_price_col != 0)
                valid_naver = naver_price_col.notna()
                calculate_mask = valid_base & valid_naver

                if calculate_mask.any(): # Proceed only if there are valid prices to compare
                   diff = naver_price_col.where(calculate_mask) - base_price_col.where(calculate_mask)
                   result_df['가격차이(3)'] = diff
                   result_df['가격차이(3)(%)'] = np.where(
                       calculate_mask, # Use the combined mask
                       np.rint((diff / base_price_col.where(calculate_mask)) * 100).astype(int),
                       None
                   )
                else:
                   logging.debug("No valid base or Naver prices found for difference calculation (Naver).")
                   # Ensure columns exist even if calculation is skipped
                   if '가격차이(3)' not in result_df.columns: result_df['가격차이(3)'] = None
                   if '가격차이(3)(%)' not in result_df.columns: result_df['가격차이(3)(%)'] = None

        else:
            # This case is hit if '판매단가(V포함)' is missing entirely
            logging.warning("Base price column '판매단가(V포함)' not found in input haoreum_df. Skipping all price difference calculations.")
            # Ensure difference columns still exist, filled with None
            result_df['가격차이(2)'] = None
            result_df['가격차이(2)(%)'] = None
            result_df['가격차이(3)'] = None
            result_df['가격차이(3)(%)'] = None

        # Log completion
        elapsed_time = time.time() - start_time
        logging.info(f"Product matching completed in {elapsed_time:.2f} seconds")
        logging.info(f"Total matches processed: {len(results)}")
        
        # Clean up the final DataFrame (replace NaN with suitable placeholders like '-')
        # result_df.fillna('-', inplace=True) # Apply this before formatting in excel_utils

        return result_df

    except Exception as e:
        logging.error(f"Error in product matching: {str(e)}", exc_info=True)
        if progress_queue:
            progress_queue.emit("error", f"상품 매칭 중 오류 발생: {str(e)}")
        
        # Return original dataframe if error occurs
        result_df = haoreum_df.copy()
        return result_df

def _filter_candidates_by_text(product_name: str, candidates: List[Dict], matcher: Optional[ProductMatcher] = None, config: Optional[configparser.ConfigParser] = None) -> List[Dict]:
    """텍스트 유사도로 후보군 필터링"""
    try:
        # 일관된 임계값 사용
        if matcher is not None:
            # ProductMatcher 인스턴스가 제공된 경우 해당 임계값 사용
            text_threshold = matcher.text_similarity_threshold
            text_sim_func = matcher.calculate_text_similarity
        elif config is not None:
            # 설정에서 일관된 이름의 임계값 사용
            text_threshold = config.getfloat('Matching', 'text_threshold', fallback=0.5)
            
            # calculate_text_similarity 함수 정의 (ProductMatcher 없을 때 사용)
            def text_sim_func(text1: str, text2: str) -> float:
                try:
                    # 가능하면 koSBERT 모듈 사용
                    from koSBERT_text_similarity import calculate_text_similarity
                    return calculate_text_similarity(text1, text2)
                except ImportError:
                    # 간단한 fallback 구현
                    from sentence_transformers import SentenceTransformer, util
                    model_name = config.get('Paths', 'text_model_path', 
                                           fallback='sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
                    model = SentenceTransformer(model_name)
                    embedding1 = model.encode(text1, convert_to_tensor=True)
                    embedding2 = model.encode(text2, convert_to_tensor=True)
                    return util.pytorch_cos_sim(embedding1, embedding2).item()
                except Exception as e:
                    logging.error(f"텍스트 유사도 계산 중 오류: {e}")
                    return 0.0
        else:
            # 둘 다 없는 경우 기본값 사용
            text_threshold = 0.5
            return candidates[:5]  # 최소한의 처리만 수행
        
        text_matches = []
        
        for candidate in candidates:
            candidate_name = candidate.get('name', '')
            if not candidate_name:
                continue
                
            # 텍스트 유사도 계산
            text_sim = text_sim_func(product_name, candidate_name)
            if text_sim >= text_threshold:
                text_matches.append((text_sim, candidate))
        
        # 유사도 순으로 정렬
        text_matches.sort(key=lambda x: x[0], reverse=True)
        return [item[1] for item in text_matches]
        
    except Exception as e:
        logging.error(f"Error in text filtering for {product_name}: {e}")
        return candidates[:5]  # 오류 발생 시 상위 5개만 반환

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

# Alias for backward compatibility
match_products = process_matching

def post_process_matching_results(df, config):
    """Cleans, formats, and conditionally clears competitor data in the matched DataFrame.
    IMPORTANT: This function does NOT filter rows, only modifies column values.
    """
    if df is None:
        logging.error("Input DataFrame is None for post-processing")
        return pd.DataFrame()
        
    if not isinstance(df, pd.DataFrame):
        logging.error(f"Input is not a DataFrame: {type(df)}")
        return pd.DataFrame()
        
    if df.empty:
        logging.warning("DataFrame is empty before filtering.")
        return df

    initial_rows = len(df)
    logging.info(f"Starting filtering of {initial_rows} matched results...")

    # IMPORTANT: Save the original input index to ensure we don't lose any products
    original_indices = df.index.tolist()
    logging.info(f"Preserved {len(original_indices)} original product indices")

    try:
        df_filtered = df.copy() # Work on a copy

        # --- 1. Data Cleaning and Numeric Conversion ---
        # Define columns expected to be numeric (use original names before potential rename)
        numeric_cols = ['가격차이(2)', '가격차이(3)', '가격차이(2)(%)', '가격차이(3)(%)', '가격차이 비율(3)',
                        '판매단가(V포함)', '판매단가(V포함)(2)', '판매단가(V포함)(3)']
        # Add similarity scores if they exist and should be numeric
        numeric_cols.extend([col for col in df_filtered.columns if '_Sim' in col or '_Combined' in col])

        # Clean percentage strings first (handle both '%', ' %', and potential extra spaces)
        percent_cols = ['가격차이(2)(%)', '가격차이(3)(%)', '가격차이 비율(3)']
        for col in percent_cols:
            if col in df_filtered.columns:
                df_filtered[col] = df_filtered[col].astype(str).str.replace(r'\s*%\s*$', '', regex=True).str.strip()

        # Clean price difference strings (remove commas)
        price_diff_cols = ['가격차이(2)', '가격차이(3)']
        for col in price_diff_cols:
            if col in df_filtered.columns:
                df_filtered[col] = df_filtered[col].astype(str).str.replace(r',', '', regex=True).str.strip()

        # Convert to numeric, coercing errors
        for col in numeric_cols:
            if col in df_filtered.columns:
                # Replace potential placeholders like '-' before conversion
                df_filtered[col] = df_filtered[col].replace(['-', ''], np.nan, regex=False)
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')
                logging.debug(f"Converted column '{col}' to numeric.")

        # --- 2. Initial Price Difference Filter ---
        if '가격차이(2)' in df_filtered.columns:
            negative_price2 = df_filtered['가격차이(2)'].lt(0)
            logging.info(f"Identificados {negative_price2.sum()} registros com preço Kogift menor")
            
        if '가격차이(3)' in df_filtered.columns:
            negative_price3 = df_filtered['가격차이(3)'].lt(0)
            logging.info(f"Identificados {negative_price3.sum()} registros com preço Naver menor")

        # --- 3. Conditional Clearing / Removal of Data ---
        # Define columns for Goryeo and Naver processing
        original_goryeo_cols = ['기본수량(2)', '판매가(V포함)(2)', '판매단가(V포함)(2)', '가격차이(2)', '가격차이(2)(%)', 
                              '고려기프트 상품링크', '고려기프트 이미지']
        original_naver_cols = ['기본수량(3)', '판매단가(V포함)(3)', '가격차이(3)', '가격차이(3)(%)', '가격차이 비율(3)',
                             '공급사명', '공급사 상품링크', '네이버 쇼핑 링크', '네이버 이미지']

        # Get existing columns to avoid errors
        existing_goryeo_clear = [col for col in original_goryeo_cols if col in df_filtered.columns]
        existing_naver_clear = [col for col in original_naver_cols if col in df_filtered.columns]

        # 3a. Clear Goryeo Data if Price Diff >= 0 OR Price Diff % > -1%
        goryeo_cleared_count = 0
        goryeo_clear_cond = pd.Series(False, index=df_filtered.index)
        if '가격차이(2)' in df_filtered.columns:
            goryeo_clear_cond = goryeo_clear_cond | (df_filtered['가격차이(2)'].notna() & df_filtered['가격차이(2)'].ge(0))
        
        # Check for both possible column name formats
        if '가격차이(2)(%)' in df_filtered.columns:
            goryeo_clear_cond = goryeo_clear_cond | (df_filtered['가격차이(2)(%)'].notna() & df_filtered['가격차이(2)(%)'].gt(-1.0))
        elif '가격차이(2)%' in df_filtered.columns:
            goryeo_clear_cond = goryeo_clear_cond | (df_filtered['가격차이(2)%'].notna() & df_filtered['가격차이(2)%'].gt(-1.0))
        elif '가격차이(2) %' in df_filtered.columns:
            goryeo_clear_cond = goryeo_clear_cond | (df_filtered['가격차이(2) %'].notna() & df_filtered['가격차이(2) %'].gt(-1.0))
        # Removed duplicate check for '가격차이(2)(%)'

        # Changed to avoid potential pandas version incompatibility
        rows_to_clear_goryeo = goryeo_clear_cond.fillna(False)
        if rows_to_clear_goryeo.any() and existing_goryeo_clear:
            df_filtered.loc[rows_to_clear_goryeo, existing_goryeo_clear] = np.nan 
            goryeo_cleared_count = rows_to_clear_goryeo.sum()
            logging.debug(f"Cleared Goryeo data for {goryeo_cleared_count} rows based on price diff >= 0 or % > -1.")

        # 3b. Clear Naver Data if Price Diff >= 0 OR Price Diff % > -1%
        naver_cleared_count1 = 0
        naver_clear_cond1 = pd.Series(False, index=df_filtered.index)
        if '가격차이(3)' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이(3)'].notna() & df_filtered['가격차이(3)'].ge(0))
        
        # Check for all possible column name variants
        if '가격차이 비율(3)' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이 비율(3)'].notna() & df_filtered['가격차이 비율(3)'].gt(-1.0))
        elif '가격차이(3)(%)' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이(3)(%)'].notna() & df_filtered['가격차이(3)(%)'].gt(-1.0))
        elif '가격차이(3)%' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이(3)%'].notna() & df_filtered['가격차이(3)%'].gt(-1.0))
        elif '가격차이(3) %' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이(3) %'].notna() & df_filtered['가격차이(3) %'].gt(-1.0))

        # Changed to avoid potential pandas version incompatibility
        rows_to_clear_naver1 = naver_clear_cond1.fillna(False)
        if rows_to_clear_naver1.any() and existing_naver_clear:
            df_filtered.loc[rows_to_clear_naver1, existing_naver_clear] = np.nan
            naver_cleared_count1 = rows_to_clear_naver1.sum()
            logging.debug(f"Cleared Naver data for {naver_cleared_count1} rows based on price diff >= 0 or % > -1.")

        # --- 4. IMPORTANT: DO NOT drop any rows, even if they have no comparison data ---
        all_comparison_cols_original = list(set(existing_goryeo_clear + existing_naver_clear))
        if all_comparison_cols_original:
            empty_comparison_mask = df_filtered[all_comparison_cols_original].isna().all(axis=1)
            empty_rows_count = empty_comparison_mask.sum()
            logging.info(f"Encontrados {empty_rows_count} produtos sem dados de comparação (seriam removidos no filtro original)")

        # --- 5. Final Formatting before Renaming ---
        percent_cols_to_format = ['가격차이(2)(%)', '가격차이 비율(3)', '가격차이(3)(%)']
        for key in percent_cols_to_format:
            if key in df_filtered.columns:
                numeric_series = pd.to_numeric(df_filtered[key], errors='coerce')
                mask = numeric_series.notna()
                df_filtered.loc[mask, key] = numeric_series[mask].apply(lambda x: f"{int(x)} %")

        price_diff_cols_to_format = ['가격차이(2)', '가격차이(3)']
        for key in price_diff_cols_to_format:
            if key in df_filtered.columns:
                numeric_series = pd.to_numeric(df_filtered[key], errors='coerce')
                mask = numeric_series.notna()
                df_filtered.loc[mask, key] = numeric_series[mask].apply(lambda x: f"{x:,.0f}")

        # Verify we haven't lost any rows
        final_rows = len(df_filtered)
        if final_rows != initial_rows:
            logging.error(f"Row count mismatch! Started with {initial_rows} rows but now have {final_rows} rows. Attempting to restore missing rows.")
            current_indices = df_filtered.index.tolist()
            missing_indices = [idx for idx in original_indices if idx not in current_indices]
            
            if missing_indices:
                logging.warning(f"Found {len(missing_indices)} missing rows. Restoring original rows.")
                missing_rows = df.loc[missing_indices].copy()
                df_filtered = pd.concat([df_filtered, missing_rows])
                logging.info(f"Restored missing rows. New row count: {len(df_filtered)}")

        logging.info(f"Finished filtering. {len(df_filtered)}/{initial_rows} rows maintained (no rows dropped).")
        return df_filtered

    except Exception as e:
        logging.error(f"Error in filter_dataframe: {e}", exc_info=True)
        # Return original DataFrame on error to ensure no data loss
        return df