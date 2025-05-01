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

def _match_single_product(i: int, haoreum_row_dict: Dict, kogift_data: List[Dict], naver_data: List[Dict], product_type: str, matcher: ProductMatcher, input_file_image_map: Dict[Any, Dict[str, str]]) -> Tuple[int, Optional[Dict]]:
    """Matches a single Haoreum product against Kogift and Naver data."""
    if not matcher:
        logging.error(f"Matcher object missing for index {i}.")
        return i, None

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
            logging.error(f"Invalid product name for index {i}: {product_name}")
            return i, None
            
        logging.debug(f"Matching product index {i}: {product_name}")
        
        # Validate and prepare Haoreum product data
        try:
            # --- Get Haoreum image data ---
            haoreum_code = haoreum_row_dict.get('Code')
            # Changed: Get the dictionary from input_file_image_map
            haoreum_img_data = input_file_image_map.get(haoreum_code) if input_file_image_map else None 
            # -----------------------------

            haoreum_product = {
                'name': product_name,
                'price': pd.to_numeric(haoreum_row_dict.get('판매단가(V포함)'), errors='coerce'),
                'link': haoreum_row_dict.get('본사상품링크'),
                # 'image_path': haoreum_img_path, # Deprecated
                'image_data': haoreum_img_data, # Store the dictionary
                'code': haoreum_code,
                '담당자': haoreum_row_dict.get('담당자'),
                '업체명': haoreum_row_dict.get('업체명'),
                '업체코드': haoreum_row_dict.get('업체코드'),
                '공급사명': haoreum_row_dict.get('공급사명'),
                '공급처코드': haoreum_row_dict.get('공급처코드'),
                '상품코드': haoreum_row_dict.get('상품코드'),
                '카테고리(중분류)': haoreum_row_dict.get('카테고리(중분류)'),
                '본사 기본수량': haoreum_row_dict.get('본사 기본수량')
            }
        except Exception as e:
            logging.error(f"Error preparing Haoreum product data for index {i}: {e}")
            return i, None

        # Validate required fields
        if not haoreum_product['name'] or pd.isna(haoreum_product['name']):
            logging.warning(f"Missing product name for index {i}")
            return i, None

        # Find best matches with improved error handling
        best_kogift_match = None
        best_naver_match = None
        
        try:
            if kogift_data:  # Only attempt matching if we have candidates
                # Pass the Haoreum product dictionary (which now contains image_data)
                best_kogift_match = _find_best_match(haoreum_product, kogift_data, matcher, 'kogift')
        except Exception as e:
            logging.error(f"Error finding Kogift match for product {haoreum_product['name']}: {e}")
            
        try:
            if naver_data:  # Only attempt matching if we have candidates
                 # Pass the Haoreum product dictionary (which now contains image_data)
                best_naver_match = _find_best_match(haoreum_product, naver_data, matcher, 'naver')
        except Exception as e:
            logging.error(f"Error finding Naver match for product {haoreum_product['name']}: {e}")

        # Combine results if matches found
        result = {**haoreum_row_dict}  # Start with original fields
        # Add temporary image data columns, initialize to None
        result['_temp_haoreum_image_data'] = haoreum_img_data
        result['_temp_kogift_image_data'] = None
        result['_temp_naver_image_data'] = None

        if best_kogift_match or best_naver_match:
            try:
                # Update common fields first
                result.update({
                    '구분(승인관리:A/가격관리:P)': product_type,
                    'name': haoreum_product['name'],
                    '본사링크': haoreum_product['link'],
                    '판매단가(V포함)': haoreum_product['price'],
                    # '해오름이미지경로': haoreum_product['image_data'], # Keep original haoreum_row_dict value if exists
                })

                # Add Kogift data if available
                if best_kogift_match:
                    kogift_match_data = best_kogift_match['match_data'] # This is the full candidate dict
                    result.update({
                        '고려 링크': kogift_match_data.get('link'),
                        # Store the full image dictionary for Kogift
                        '_temp_kogift_image_data': kogift_match_data.get('image_data'), 
                        '판매단가2(VAT포함)': kogift_match_data.get('price'),
                        '_고려_TextSim': best_kogift_match['text_similarity'],
                        '_해오름_고려_ImageSim': best_kogift_match['image_similarity'],
                        '_고려_Combined': best_kogift_match['combined_score'],
                        '고려 기본수량': kogift_match_data.get('quantity', '-')
                    })
                # else: _temp_kogift_image_data remains None

                # Add Naver data if available
                if best_naver_match:
                    naver_match_data = best_naver_match['match_data'] # This is the full candidate dict
                    naver_image_data = naver_match_data.get('image_data') # Should be dict 
                    
                    result.update({
                        '매칭_사이트': 'Naver',
                        '공급사명': naver_match_data.get('mallName', naver_match_data.get('seller', '')),
                        '네이버 쇼핑 링크': naver_match_data.get('link'),
                        '공급사 상품링크': naver_match_data.get('mallProductUrl', naver_match_data.get('originallink')),
                        # Store the full image dictionary for Naver
                        '_temp_naver_image_data': naver_image_data, 
                        '판매단가(V포함)(3)': naver_match_data.get('price'),
                        '텍스트_유사도': best_naver_match['text_similarity'],
                        '이미지_유사도': best_naver_match['image_similarity'],
                        '매칭_정확도': best_naver_match['combined_score'],
                        '기본수량(3)': naver_match_data.get('quantity', '1'),
                        '매칭_여부': 'Y',
                        '매칭_품질': '상' if best_naver_match['combined_score'] > 0.8 else '중' if best_naver_match['combined_score'] > 0.6 else '하'
                    })
                # else: _temp_naver_image_data remains None
                
                # Update match status if only Kogift matched
                if best_kogift_match and not best_naver_match:
                     result['매칭_여부'] = 'Y'
                     # Optionally set Kogift specific quality if needed

                logging.debug(f"Successfully matched product {haoreum_product['name']}")
                return i, result
            except Exception as e:
                logging.error(f"Error combining match results for product {haoreum_product['name']}: {e}")
                # Return original row data + None for temp image cols on error during combination
                return i, result # result already initialized with original + None temp cols
        else:
            # No match found
            logging.debug(f"No sufficient match found for product {haoreum_product['name']}")
            result['매칭_여부'] = 'N' # Explicitly set to N
            return i, result # Return original data + None temp cols

    except Exception as e:
        logging.error(f"Error in _match_single_product for index {i}: {e}", exc_info=True)
        # Return original data + None temp cols on major error
        result = {**haoreum_row_dict} 
        result['_temp_haoreum_image_data'] = None
        result['_temp_kogift_image_data'] = None
        result['_temp_naver_image_data'] = None
        return i, result

# Wrapper for ProcessPoolExecutor compatibility
def _match_single_product_wrapper(i: int, haoreum_row_dict: Dict, kogift_data: Optional[List[Dict]], naver_data: Optional[List[Dict]], product_type: str, input_file_image_map: Dict[Any, Dict[str, str]]) -> Tuple[int, Optional[Dict]]:
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
            product_type, worker_matcher_instance, input_file_image_map
        )
    except Exception as e:
        logging.error(f"Error in _match_single_product_wrapper for index {i}: {e}", exc_info=True)
        return i, None

def process_matching(
    haoreum_df: pd.DataFrame,
    kogift_map: Dict[str, List[Dict]],
    naver_map: Dict[str, List[Dict]],
    # Changed: Expect dictionary values here
    input_file_image_map: Dict[Any, Dict[str, str]], 
    config: configparser.ConfigParser,
    gpu_available: bool,
    progress_queue=None,
    max_workers: Optional[int] = None
) -> pd.DataFrame:
    """
    Process product matching using multiple workers and enhanced matching logic
    """
    start_time = time.time()
    debug_mode = config.getboolean('Debug', 'enabled', fallback=False)

    # Validate input data
    if not isinstance(haoreum_df, pd.DataFrame) or haoreum_df.empty:
        logging.error("Invalid or empty Haoreum DataFrame")
        return pd.DataFrame()
        
    if not isinstance(kogift_map, dict) or not isinstance(naver_map, dict):
        logging.error("Invalid Kogift or Naver map")
        return haoreum_df # Return original df if maps are invalid
        
    # Validate input_file_image_map structure (optional, basic check)
    if input_file_image_map and not all(isinstance(v, dict) for v in input_file_image_map.values() if v is not None):
         logging.warning("input_file_image_map contains non-dictionary values. Image data might be incomplete.")

    # Determine the number of CPU cores to use
    if max_workers is None:
        max_workers = config.getint('Concurrency', 'max_match_workers', 
                                   fallback=max(1, os.cpu_count() // 2))
    
    logging.info(f"Starting product matching with {max_workers} workers")
    if progress_queue:
        progress_queue.emit("status", f"상품 매칭 시작 (GPU: {gpu_available}, 작업자: {max_workers})")
    
    # Initialize matcher and multiprocessing
    total_products = len(haoreum_df)
    # Create a copy of the DataFrame to avoid modifying the original
    result_df = haoreum_df.copy()
    results = []

    # --- Prepare arguments for workers --- 
    args_list = []
    for i, row in haoreum_df.iterrows():
        product_name = row.get('상품명')
        if not product_name:
            logging.warning(f"Skipping row index {i} due to missing 상품명")
            continue

        # Get candidate lists
        kogift_candidates = kogift_map.get(product_name, [])
        naver_candidates = naver_map.get(product_name, [])

        # Assign product type (logic might need adjustment)
        product_type = 'A' # Default or determine based on row data
        
        # Convert row to dictionary
        haoreum_row_dict = row.to_dict()
        
        # Add arguments for the worker function
        # Changed: Pass input_file_image_map instead of haoreum_img_path
        args_list.append((
            i, 
            haoreum_row_dict, 
            kogift_candidates, 
            naver_candidates, 
            product_type, 
            input_file_image_map # Pass the whole map
        ))

    # --- Run matching in parallel --- 
    processed_count = 0
    timeout_seconds = config.getint('Concurrency', 'match_timeout_per_product', fallback=300) # 5 minutes default

    try:
        # Initialize worker pool with the initializer function and config path
        config_path = config.get('Paths', 'config_file_path', fallback='config.ini') # Get config path
        
        # Select appropriate executor context based on OS and needs
        # Note: 'fork' context is generally faster but less safe, not available on Windows.
        # 'spawn' is safer and works cross-platform.
        mp_context = multiprocessing.get_context('spawn') 

        with ProcessPoolExecutor(max_workers=max_workers, 
                                 mp_context=mp_context, # Use spawn context
                                 initializer=_init_worker_matcher, 
                                 initargs=(config_path,)) as executor:
            
            # Submit tasks
            # Changed: _match_single_product_wrapper now takes input_file_image_map
            futures = {executor.submit(_match_single_product_wrapper, *args): args[0] for args in args_list} # Map future to index
            
            for future in as_completed(futures):
                processed_count += 1
                original_index = futures[future] # Get the original DataFrame index
                try:
                    # Timeout argument removed from future.result() as it's handled by future itself or as_completed
                    result_index, result_data = future.result() 
                    # Ensure the result index matches the original index
                    if result_index != original_index:
                        logging.warning(f"Worker result index ({result_index}) mismatch with submission index ({original_index}). Attempting to use submission index.")
                        result_index = original_index
                    results.append((result_index, result_data))
                except TimeoutError:
                    logging.error(f"Timeout occurred processing product index {original_index}")
                    results.append((original_index, {'매칭_오류메시지': "Timeout"})) # Store error info
                except Exception as e:
                    logging.error(f"Error processing product index {original_index}: {e}", exc_info=True)
                    results.append((original_index, {'매칭_오류메시지': f"Worker Error: {str(e)[:100]}"})) # Store error info

                # Update progress
                if progress_queue and processed_count % 10 == 0:
                    progress = (processed_count / total_products) * 100
                    progress_queue.emit("progress", int(progress))
                    progress_queue.emit("status", f"상품 매칭 중... ({processed_count}/{total_products})")
            
            # Final progress update
            if progress_queue:
                progress_queue.emit("progress", 100)
                progress_queue.emit("status", "상품 매칭 완료. 결과 집계 중...")

    except Exception as pool_error:
        logging.error(f"Error during parallel matching execution: {pool_error}", exc_info=True)
        if progress_queue:
            progress_queue.emit("error", f"매칭 풀 실행 오류: {pool_error}")
        # Return the original DataFrame if pool fails catastrophically
        return haoreum_df 

    logging.info(f"Parallel matching finished. Aggregating {len(results)} results...")

    # --- Aggregate results --- 
    if not results:
        logging.warning("No results returned from matching workers.")
        return result_df # Return the copied df with no matches
        
    try:
        # Ensure all required columns exist before aggregation
        # These include the new temporary image data columns
        required_cols = [
            '_temp_haoreum_image_data', '_temp_kogift_image_data', '_temp_naver_image_data',
            '매칭_여부', '매칭_정확도', '텍스트_유사도', '이미지_유사도', '매칭_사이트', '매칭_품질', '매칭_오류메시지',
            '기본수량(2)', '판매단가(V포함)(2)', '고려기프트 상품링크', 
            '기본수량(3)', '판매단가(V포함)(3)', '공급사명', '네이버 쇼핑 링크', '공급사 상품링크'
        ]
        for col in required_cols:
             if col not in result_df.columns:
                  # Initialize object type for dictionaries, others with None/NA
                  dtype = object if '_temp_' in col else None
                  result_df[col] = pd.Series(dtype=dtype) 

        # Update matched products
        match_found_count = 0
        error_count = 0
        for idx, result_data in results:
            # Basic validation of the result tuple structure
            if idx is None:
                logging.warning("Skipping invalid result from worker (None index)")
                error_count += 1
                continue
            
            if result_data is None:
                logging.warning(f"Received None data for index {idx} from worker. Marking as no match.")
                result_data = {'매칭_여부': 'N', '매칭_품질': '실패', '매칭_오류메시지': 'Worker returned None'}
                error_count += 1
            elif not isinstance(result_data, dict):
                logging.warning(f"Skipping invalid result format for index {idx}: type {type(result_data)}")
                result_data = {'매칭_여부': 'N', '매칭_품질': '실패', '매칭_오류메시지': f"Invalid Worker Result Type: {type(result_data)}"}
                error_count += 1

            # Check if index exists in the DataFrame
            if idx not in result_df.index:
                logging.warning(f"Index {idx} from worker result not found in DataFrame. Skipping.")
                error_count += 1
                continue

            # Update row using .loc for potentially multiple columns
            # Only update columns present in the result_data AND the DataFrame
            update_dict = {k: v for k, v in result_data.items() if k in result_df.columns}
            if update_dict:
                 try:
                     # Use loc for robust assignment, especially with mixed types
                     for k, v in update_dict.items():
                          result_df.loc[idx, k] = v 
                     if update_dict.get('매칭_여부') == 'Y':
                          match_found_count += 1
                 except Exception as update_err:
                      logging.error(f"Error updating DataFrame at index {idx} with data {update_dict}: {update_err}")
                      result_df.loc[idx, '매칭_오류메시지'] = f"Update Error: {update_err}"
                      error_count += 1
            else:
                 logging.warning(f"No columns to update for index {idx} from result: {list(result_data.keys())}")
                 # Ensure default 'N' and '실패' are set if no match info came back
                 if '매칭_여부' not in result_data:
                      result_df.loc[idx, '매칭_여부'] = 'N'
                 if '매칭_품질' not in result_data:
                      result_df.loc[idx, '매칭_품질'] = '실패'

        logging.info(f"Result aggregation complete. Found matches for {match_found_count} products. Errors/Timeouts: {error_count}")

        # --- Post-processing: Calculate Price Differences --- 
        # (This part seems okay, ensure columns exist)
        if '판매단가(V포함)' in result_df.columns:
            base_price_col = pd.to_numeric(result_df['판매단가(V포함)'], errors='coerce')

            # Calculate for Kogift
            if '판매단가(V포함)(2)' in result_df.columns:
                kogift_price_col = pd.to_numeric(result_df['판매단가(V포함)(2)'], errors='coerce')
                valid_base = base_price_col.notna() & (base_price_col != 0)
                valid_kogift = kogift_price_col.notna()
                calculate_mask = valid_base & valid_kogift

                if calculate_mask.any():
                    diff = kogift_price_col.where(calculate_mask) - base_price_col.where(calculate_mask)
                    result_df['가격차이(2)'] = diff
                    # Use np.rint for rounding to nearest int, then safely convert
                    percent_diff = ((diff / base_price_col.where(calculate_mask)) * 100)
                    # Use loc for assignment to handle potential index alignment issues
                    result_df.loc[calculate_mask, '가격차이(2)(%)'] = np.rint(percent_diff[calculate_mask]).astype(pd.Int64Dtype()) 
                    result_df.loc[~calculate_mask, '가격차이(2)(%)'] = pd.NA
                else:
                    logging.debug("No valid base or Kogift prices found for difference calculation.")
                    if '가격차이(2)' not in result_df.columns: result_df['가격차이(2)'] = pd.NA
                    if '가격차이(2)(%)' not in result_df.columns: result_df['가격차이(2)(%)'] = pd.NA

            # Calculate for Naver
            if '판매단가(V포함)(3)' in result_df.columns:
                naver_price_col = pd.to_numeric(result_df['판매단가(V포함)(3)'], errors='coerce')
                valid_base = base_price_col.notna() & (base_price_col != 0)
                valid_naver = naver_price_col.notna()
                calculate_mask = valid_base & valid_naver

                if calculate_mask.any():
                    diff = naver_price_col.where(calculate_mask) - base_price_col.where(calculate_mask)
                    result_df['가격차이(3)'] = diff
                    percent_diff = ((diff / base_price_col.where(calculate_mask)) * 100)
                    # Use loc for assignment
                    result_df.loc[calculate_mask, '가격차이(3)(%)'] = np.rint(percent_diff[calculate_mask]).astype(pd.Int64Dtype()) 
                    result_df.loc[~calculate_mask, '가격차이(3)(%)'] = pd.NA
                else:
                    logging.debug("No valid base or Naver prices found for difference calculation.")
                    if '가격차이(3)' not in result_df.columns: result_df['가격차이(3)'] = pd.NA
                    if '가격차이(3)(%)' not in result_df.columns: result_df['가격차이(3)(%)'] = pd.NA
        else:
            logging.warning("Base price column '판매단가(V포함)' not found. Skipping price difference calculations.")
            result_df['가격차이(2)'] = pd.NA
            result_df['가격차이(2)(%)'] = pd.NA
            result_df['가격차이(3)'] = pd.NA
            result_df['가격차이(3)(%)'] = pd.NA

        # Log completion
        elapsed_time = time.time() - start_time
        logging.info(f"Product matching process completed in {elapsed_time:.2f} seconds")
        logging.info(f"Total rows processed: {len(haoreum_df)}, Matches found: {match_found_count}")
        
        # Log sample of the final result_df including temp image columns
        if not result_df.empty and debug_mode:
             temp_cols = ['_temp_haoreum_image_data', '_temp_kogift_image_data', '_temp_naver_image_data']
             cols_to_log = [col for col in temp_cols if col in result_df.columns]
             if cols_to_log:
                  logging.debug(f"Sample result_df with temp image data (first 5 rows):")
                  # Handle potential errors during logging complex objects
                  try:
                       log_str = result_df[cols_to_log].head().to_string()
                       logging.debug(log_str)
                  except Exception as log_ex:
                       logging.warning(f"Could not fully log sample data due to error: {log_ex}")

        return result_df

    except Exception as agg_err:
        logging.error(f"Error during result aggregation: {agg_err}", exc_info=True)
        if progress_queue:
            progress_queue.emit("error", f"결과 집계 중 오류 발생: {agg_err}")
        # Return the DataFrame state before the error if possible, or original
        return result_df if 'result_df' in locals() else haoreum_df

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

def _find_best_match(haoreum_product: Dict, candidates: List[Dict], matcher: ProductMatcher, source: str) -> Optional[Dict]:
    """
    Find the best matching product from a list of candidates.
    
    Args:
        haoreum_product: The reference product (Haereum), expected to have image_data dict
        candidates: List of candidate products to match against, expected to have image data dicts
        matcher: The ProductMatcher instance to use
        source: Source of candidates ('kogift' or 'naver')
        
    Returns:
        Dict with match data (including image data dict) and similarity scores, or None
    """
    if not candidates or not haoreum_product:
        return None

    # Get product name and category for thresholds
    product_name = haoreum_product.get('name')
    category = haoreum_product.get('카테고리(중분류)')
    
    if not product_name:
        logging.warning(f"Haoreum product missing name")
        return None
        
    # Get appropriate thresholds (may be category-specific)
    text_threshold, image_threshold = matcher.get_thresholds_for_category(category)
    
    best_match = None
    best_text_sim = 0.0
    best_image_sim = 0.0
    best_combined = 0.0
    
    # First filter by text similarity to reduce candidates
    filtered_candidates = _filter_candidates_by_text(product_name, candidates, matcher)
    
    # Get Haoreum image data
    haoreum_img_data = haoreum_product.get('image_data')
    
    for candidate in filtered_candidates:
        try:
            # Calculate text similarity
            candidate_name = candidate.get('name', '')
            if not candidate_name:
                continue
                
            text_sim = matcher.calculate_text_similarity(product_name, candidate_name)
            
            # Skip if text similarity is too low
            if text_sim < text_threshold:
                continue
                
            # Calculate image similarity if we have both images
            image_sim = 0.0
            if haoreum_img_data and candidate.get('image_data'):
                image_sim = matcher.calculate_image_similarity(
                    haoreum_img_data,  # Should be dict with local_path
                    candidate['image_data']  # Should be dict with local_path
                )
            
            # Calculate combined score
            # Weight text similarity more heavily for Naver matches
            text_weight = 0.7 if source == 'naver' else 0.6
            image_weight = 1.0 - text_weight
            
            combined_score = (text_sim * text_weight) + (image_sim * image_weight)
            
            # Update best match if this is better
            if combined_score > best_combined:
                best_match = {
                    'match_data': candidate,  # Store the full candidate dict
                    'text_similarity': text_sim,
                    'image_similarity': image_sim,
                    'combined_score': combined_score
                }
                best_text_sim = text_sim
                best_image_sim = image_sim
                best_combined = combined_score
                
        except Exception as e:
            logging.error(f"Error matching {product_name} with candidate {candidate.get('name', 'Unknown')}: {e}")
            continue
    
    # Log match details
    if best_match:
        match_name = best_match['match_data'].get('name', 'Unknown')
        logging.debug(f"Best {source} match for {product_name}: {match_name} "
                     f"(Text: {best_text_sim:.3f}, Image: {best_image_sim:.3f}, "
                     f"Combined: {best_combined:.3f})")
    else:
        logging.debug(f"No suitable {source} match found for {product_name}")
    
    return best_match