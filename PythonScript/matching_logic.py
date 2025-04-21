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
import torch

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
        self.use_category_thresholds = config.getboolean('Matching', 'use_category_thresholds', fallback=False)
        self.category_thresholds = {}
        if self.use_category_thresholds:
            try:
                thresholds = config.get('Matching', 'category_thresholds', fallback='{}')
                self.category_thresholds = json.loads(thresholds)
            except Exception as e:
                logging.warning(f"Failed to load category thresholds: {e}")
                self.use_category_thresholds = False
        
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
        return cat_thresholds['text'], cat_thresholds['image']

    def calculate_text_similarity(self, text1: Optional[str], text2: Optional[str]) -> float:
        """
        두 텍스트 간의 유사도를 계산합니다.
        개선된 텍스트 유사도 계산 로직을 사용합니다.
        """
        if not text1 or not text2:
            return 0.0
        
        try:
            # koSBERT_text_similarity의 향상된 기능 활용
            from koSBERT_text_similarity import (
                preprocess_text, encode_text, count_common_tokens, 
                MODEL_NAME, BOOST_EXACT_MATCH, NAME_SPLIT_MATCHING, EXACT_MATCH_BONUS
            )
            
            # 텍스트 전처리
            text1 = preprocess_text(text1)
            text2 = preprocess_text(text2)
            
            if not text1 or not text2:
                return 0.0
            
            # 임베딩 계산
            embedding1 = encode_text(text1, model_name=self.text_model_path)
            embedding2 = encode_text(text2, model_name=self.text_model_path)
            
            if embedding1 is None or embedding2 is None:
                return 0.0
            
            # 코사인 유사도 계산
            similarity = torch.cosine_similarity(embedding1.unsqueeze(0), embedding2.unsqueeze(0), dim=1).item()
            
            # 토큰 기반 보너스 적용 (설정에 따라)
            if BOOST_EXACT_MATCH and NAME_SPLIT_MATCHING:
                common_count = count_common_tokens(text1, text2)
                if common_count > 0:
                    # 공통 토큰이 많을수록 더 큰 보너스
                    token_boost = min(common_count * 0.05, EXACT_MATCH_BONUS)
                    similarity = min(1.0, similarity + token_boost)
                    
                # 정확히 일치하는 경우 추가 보너스
                if text1 == text2:
                    similarity = min(1.0, similarity + EXACT_MATCH_BONUS)
            
            return similarity
            
        except Exception as e:
            logging.error(f"텍스트 유사도 계산 중 오류 발생: {e}", exc_info=True)
            
            # 모듈 임포트 실패 또는 다른 오류 발생 시 기존 방식으로 처리
            return self._fallback_text_similarity(text1, text2)
        
    def _fallback_text_similarity(self, text1: str, text2: str) -> float:
        """기존 텍스트 유사도 계산 방식 (fallback)"""
        try:
            if self.text_model is None:
                self._initialize_text_model()
                if self.text_model is None:
                    logging.error("텍스트 모델 로드 실패")
                    return 0.0
                
            # 기존 모델 사용 로직
            embedding1 = self.text_model.encode(text1, convert_to_tensor=True)
            embedding2 = self.text_model.encode(text2, convert_to_tensor=True)
            
            similarity = util.pytorch_cos_sim(embedding1, embedding2).item()
            return similarity
            
        except Exception as e:
            logging.error(f"기본 텍스트 유사도 계산 중 오류 발생: {e}")
            return 0.0

    def calculate_image_similarity(self, img_path1: Optional[str], img_path2: Optional[str]) -> float:
        """
        두 이미지 간의 유사도를 계산합니다.
        enhanced_image_matcher.py의 향상된 로직을 사용합니다.
        """
        if not img_path1 or not img_path2:
            return 0.0
        
        if not os.path.exists(img_path1) or not os.path.exists(img_path2):
            logging.warning(f"이미지 파일이 존재하지 않습니다: {img_path1} 또는 {img_path2}")
            return 0.0
        
        try:
            # EnhancedImageMatcher 활용
            from enhanced_image_matcher import EnhancedImageMatcher, WEIGHTS
            
            # EnhancedImageMatcher 인스턴스 캐싱 (성능 향상)
            if not hasattr(self, 'image_matcher') or self.image_matcher is None:
                self.image_matcher = EnhancedImageMatcher(use_gpu=self.use_gpu)
                logging.info("EnhancedImageMatcher 초기화 완료")
            
            # 향상된 이미지 매칭 로직으로 유사도 계산
            combined_similarity, scores = self.image_matcher.calculate_combined_similarity(
                img_path1, img_path2, weights=WEIGHTS
            )
            
            # 로깅
            logging.debug(f"향상된 이미지 유사도: {combined_similarity:.4f} (SIFT={scores['sift']:.2f}, "
                        f"AKAZE={scores['akaze']:.2f}, Deep={scores['deep']:.2f})")
            
            return combined_similarity
        
        except Exception as e:
            logging.error(f"향상된 이미지 유사도 계산 중 오류 발생: {e}", exc_info=True)
            logging.info("기본 이미지 유사도 계산 방식으로 전환합니다")
            
            # 실패 시 기존 로직으로 폴백
            return self._fallback_image_similarity(img_path1, img_path2)
        
    def _fallback_image_similarity(self, img_path1: str, img_path2: str) -> float:
        """기존 이미지 유사도 계산 방식 (fallback)"""
        try:
            # 캐시에서 특징값 확인
            features1 = self.feature_cache.get(img_path1)
            features2 = self.feature_cache.get(img_path2)
            
            # 캐시에 없는 경우 계산
            if features1 is None:
                features1 = self._extract_image_features(img_path1)
                if features1 is not None:
                    self.feature_cache.put(img_path1, features1)
            
            if features2 is None:
                features2 = self._extract_image_features(img_path2)
                if features2 is not None:
                    self.feature_cache.put(img_path2, features2)
            
            # 특징값 추출 실패 시
            if features1 is None or features2 is None:
                return 0.0
            
            # 코사인 유사도 계산
            similarity = np.dot(features1, features2) / (np.linalg.norm(features1) * np.linalg.norm(features2))
            
            return float(np.clip(similarity, 0.0, 1.0))
        
        except Exception as e:
            logging.error(f"기본 이미지 유사도 계산 중 오류: {e}")
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

def _match_single_product(i: int, haoreum_row_dict: Dict, kogift_data: List[Dict], naver_data: List[Dict], product_type: str, matcher: ProductMatcher, haoreum_img_path: Optional[str]) -> Tuple[int, Optional[Dict]]:
    """Matches a single Haoreum product against Kogift and Naver data."""
    if not matcher:
        logging.error(f"Matcher object missing for index {i}.")
        return i, None

    try:
        logging.debug(f"Matching product index {i}: {haoreum_row_dict.get('상품명')}")
        
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
            '카테고리(중분류)': haoreum_row_dict.get('카테고리(중분류)'),
            '본사 기본수량': haoreum_row_dict.get('본사 기본수량')
        }

        # Validate required fields
        if not haoreum_product['name'] or pd.isna(haoreum_product['name']):
            logging.warning(f"Missing product name for index {i}")
            return i, None

        # Find best matches with improved error handling
        best_kogift_match = None
        best_naver_match = None
        
        try:
            best_kogift_match = _find_best_match(haoreum_product, kogift_data, matcher, 'kogift')
        except Exception as e:
            logging.error(f"Error finding Kogift match for product {haoreum_product['name']}: {e}")
            
        try:
            best_naver_match = _find_best_match(haoreum_product, naver_data, matcher, 'naver')
        except Exception as e:
            logging.error(f"Error finding Naver match for product {haoreum_product['name']}: {e}")

        # Combine results if matches found
        if best_kogift_match or best_naver_match:
            result = {**haoreum_row_dict}  # Copy all original fields
            
            # Add or update fields
            result.update({
                '구분(승인관리:A/가격관리:P)': product_type,
                'name': haoreum_product['name'],
                '본사링크': haoreum_product['link'],
                '판매단가(V포함)': haoreum_product['price'],
                '해오름이미지경로': haoreum_product['image_path'],
            })

            # Add Kogift data if available
            if best_kogift_match:
                result.update({
                    '고려 링크': best_kogift_match['match_data'].get('link'),
                    '고려기프트(이미지링크)': best_kogift_match['match_data'].get('image_path'),
                    '판매단가2(VAT포함)': best_kogift_match['match_data'].get('price'),
                    '_고려_TextSim': best_kogift_match['text_similarity'],
                    '_해오름_고려_ImageSim': best_kogift_match['image_similarity'],
                    '_고려_Combined': best_kogift_match['combined_score'],
                    '고려 기본수량': best_kogift_match['match_data'].get('quantity', '-')
                })
            else:
                result.update({
                    '고려 링크': None,
                    '고려기프트(이미지링크)': None,
                    '판매단가2(VAT포함)': None,
                    '_고려_TextSim': 0.0,
                    '_해오름_고려_ImageSim': 0.0,
                    '_고려_Combined': None,
                    '고려 기본수량': '-'
                })

            # Add Naver data if available
            if best_naver_match:
                result.update({
                    '네이버 공급사명': best_naver_match['match_data'].get('seller'),
                    '네이버 링크': best_naver_match['match_data'].get('link'),
                    '네이버쇼핑(이미지링크)': best_naver_match['match_data'].get('image_path'),
                    '판매단가3 (VAT포함)': best_naver_match['match_data'].get('price'),
                    '_네이버_TextSim': best_naver_match['text_similarity'],
                    '_해오름_네이버_ImageSim': best_naver_match['image_similarity'],
                    '_네이버_Combined': best_naver_match['combined_score'],
                    '네이버 기본수량': best_naver_match['match_data'].get('quantity', '-')
                })
            else:
                result.update({
                    '네이버 공급사명': None,
                    '네이버 링크': None,
                    '네이버쇼핑(이미지링크)': None,
                    '판매단가3 (VAT포함)': None,
                    '_네이버_TextSim': 0.0,
                    '_해오름_네이버_ImageSim': 0.0,
                    '_네이버_Combined': None,
                    '네이버 기본수량': '-'
                })

            logging.debug(f"Successfully matched product {haoreum_product['name']}")
            return i, result
        else:
            logging.debug(f"No sufficient match found for product {haoreum_product['name']}")
            return i, None

    except Exception as e:
        logging.error(f"Error in _match_single_product for index {i}: {e}", exc_info=True)
        return i, None

def _find_best_match(haoreum_product: Dict, target_data: List[Dict], matcher: ProductMatcher, data_type: str) -> Optional[Dict]:
    """Finds the best match for a Haoreum product in the target data."""
    if not haoreum_product or not target_data or not matcher:
        logging.error("Missing required parameters for _find_best_match")
        return None

    try:
        best_match = None
        best_score = 0.0
        threshold = 0.6  # Minimum similarity threshold

        for target_product in target_data:
            try:
                # Calculate text similarity
                text_sim = matcher.calculate_text_similarity(
                    haoreum_product['name'],
                    target_product.get('name', '')
                )
                
                # Skip if text similarity is too low
                if text_sim < threshold:
                    continue

                # Calculate image similarity if images are available
                image_sim = 0.0
                if haoreum_product.get('image_path') and target_product.get('image_path'):
                    try:
                        image_sim = matcher.calculate_image_similarity(
                            haoreum_product['image_path'],
                            target_product['image_path']
                        )
                    except Exception as e:
                        logging.warning(f"Error calculating image similarity: {e}")
                        image_sim = 0.0

                # Calculate combined score (weighted average)
                combined_score = (text_sim * 0.7) + (image_sim * 0.3)

                # Update best match if current score is higher
                if combined_score > best_score:
                    best_score = combined_score
                    best_match = {
                        'match_data': target_product,
                        'text_similarity': text_sim,
                        'image_similarity': image_sim,
                        'combined_score': combined_score
                    }

            except Exception as e:
                logging.error(f"Error processing target product: {e}")
                continue

        # Return best match if it meets the threshold
        if best_match and best_match['combined_score'] >= threshold:
            logging.debug(f"Found match with score {best_match['combined_score']:.2f} for {haoreum_product['name']}")
            return best_match
        else:
            logging.debug(f"No match found above threshold for {haoreum_product['name']}")
            return None

    except Exception as e:
        logging.error(f"Error in _find_best_match: {e}", exc_info=True)
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
            config.read('config.ini')
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
    Process matching between Haoreum products and Kogift/Naver products.
    """
    try:
        # Enhanced input validation
        if haoreum_df is None:
            logging.error("Haoreum DataFrame is None")
            return pd.DataFrame()
            
        if not isinstance(haoreum_df, pd.DataFrame):
            logging.error(f"Haoreum data is not a DataFrame: {type(haoreum_df)}")
            return pd.DataFrame()
            
        if haoreum_df.empty:
            logging.warning("Haoreum DataFrame is empty. No products to match.")
            return pd.DataFrame()

        # Validate required columns
        required_columns = ['상품명', 'Code']
        missing_columns = [col for col in required_columns if col not in haoreum_df.columns]
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()

        # Ensure index is unique and reset if needed
        if not haoreum_df.index.is_unique:
            logging.warning("Haoreum DataFrame index is not unique. Resetting index.")
            haoreum_df = haoreum_df.reset_index(drop=True)

        # Monitor memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024
        logging.info(f"Initial memory usage: {initial_memory:.2f} MB")

        # Start timing
        start_time = time.time()

        # Initialize results list
        results = []
        total_products = len(haoreum_df)
        processed_count = 0

        # Configure batch processing
        batch_size = config.getint('Matching', 'batch_size', fallback=50)
        if batch_size <= 0:
            batch_size = min(50, total_products)  # Default to 50 or total size if smaller

        # Configure executor
        executor_type = 'thread' if not gpu_available else config.get('Concurrency', 'matcher_executor_type', fallback='thread')
        max_workers = min(max_workers or os.cpu_count(), 4)  # Limit max workers
        timeout_per_task = config.getint('Concurrency', 'thread_pool_timeout_sec', fallback=300)

        # Process in batches
        for batch_start in range(0, total_products, batch_size):
            batch_end = min(batch_start + batch_size, total_products)
            batch_df = haoreum_df.iloc[batch_start:batch_end]

            with ThreadPoolExecutor(max_workers=max_workers) if executor_type == 'thread' else ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                
                # Submit batch tasks
                for i, row in batch_df.iterrows():
                    try:
                        haoreum_row_dict = row.to_dict()
                        product_name = haoreum_row_dict.get('상품명')
                        product_code = haoreum_row_dict.get('Code')
                        
                        if not product_name or pd.isna(product_name):
                            logging.warning(f"Skipping index {i}: Missing product name")
                            continue
                            
                        kogift_data = kogift_map.get(product_name, [])
                        naver_data = naver_map.get(product_name, [])
                        haoreum_img_path = input_file_image_map.get(product_code)
                        
                        # Log product details for debugging
                        logging.debug(f"Processing product {i}: {product_name}")
                        logging.debug(f"Kogift matches: {len(kogift_data)}, Naver matches: {len(naver_data)}")
                        
                        # Create task with all required arguments
                        task_args = (
                            i, haoreum_row_dict, kogift_data, naver_data,
                            haoreum_row_dict.get('구분', 'A'), haoreum_img_path
                        )
                        
                        if executor_type == 'process':
                            future = executor.submit(_match_single_product_wrapper, *task_args)
                        else:
                            future = executor.submit(_match_single_product, *task_args)
                            
                        futures.append(future)
                    except Exception as e:
                        logging.error(f"Error submitting task for product {product_name}: {e}")
                        continue

                # Process completed tasks
                for future in as_completed(futures):
                    try:
                        idx, result = future.result(timeout=timeout_per_task)
                        if result and isinstance(result, dict):
                            results.append(result)
                            logging.debug(f"Successfully matched product {idx}")
                        else:
                            logging.debug(f"No match found for product {idx}")
                    except TimeoutError:
                        logging.error(f"Task timeout after {timeout_per_task} seconds")
                    except Exception as e:
                        logging.error(f"Error processing task result: {e}")
                    finally:
                        processed_count += 1
                        # Update progress using emit instead of put
                        if progress_queue:
                            try:
                                progress_queue.emit("match", processed_count, total_products)
                            except Exception as e:
                                logging.warning(f"Error updating progress: {e}")

            # Clean up after batch
            if config.getboolean('Matching', 'gc_after_batch', fallback=True):
                import gc
                gc.collect()

            # Log memory usage
            current_memory = process.memory_info().rss / 1024 / 1024
            logging.info(f"After batch {batch_start//batch_size + 1}: Memory usage {current_memory:.2f} MB")

        # Create result DataFrame with validation
        if not results:
            logging.warning("No matches found. Creating empty result DataFrame with original data.")
            return _create_empty_result_df(haoreum_df, input_file_image_map)

        try:
            result_df = pd.DataFrame(results)
            if result_df.empty:
                logging.warning("Created empty result DataFrame. Returning original data.")
                return _create_empty_result_df(haoreum_df, input_file_image_map)
                
            # Validate result DataFrame
            if len(result_df) != total_products:
                logging.warning(f"Result DataFrame has {len(result_df)} rows, expected {total_products} rows")
        except Exception as e:
            logging.error(f"Error creating result DataFrame: {e}")
            return _create_empty_result_df(haoreum_df, input_file_image_map)
        
        # Add quality metrics
        try:
            result_df = _add_quality_metrics(result_df, config)
        except Exception as e:
            logging.error(f"Error adding quality metrics: {e}")

        # Log completion
        end_time = time.time()
        elapsed_time = end_time - start_time
        final_memory = process.memory_info().rss / 1024 / 1024
        memory_diff = final_memory - initial_memory

        logging.info(f"Matching completed in {elapsed_time:.2f} seconds")
        logging.info(f"Final memory usage: {final_memory:.2f} MB (Change: {memory_diff:+.2f} MB)")
        logging.info(f"Total products processed: {total_products}")
        logging.info(f"Products matched: {len(result_df)}")
        
        return result_df

    except Exception as e:
        logging.error(f"Error in process_matching: {e}", exc_info=True)
        return pd.DataFrame()  # Return empty DataFrame on error

def _create_empty_result_df(haoreum_df: pd.DataFrame, input_file_image_map: Dict[Any, str]) -> pd.DataFrame:
    """Create empty result DataFrame with original data structure."""
    results = []
    for _, row in haoreum_df.iterrows():
        row_dict = row.to_dict()
        product_code = row_dict.get('Code')
        
        base_data = {
            **row_dict,
            '구분(승인관리:A/가격관리:P)': row_dict.get('구분', 'A'),
            'name': row_dict.get('상품명'),
            '본사링크': row_dict.get('본사상품링크'),
            '판매단가(V포함)': row_dict.get('판매단가(V포함)'),
            '해오름이미지경로': input_file_image_map.get(product_code),
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
        }
        results.append(base_data)
    
    return pd.DataFrame(results)

def _add_quality_metrics(df: pd.DataFrame, config: configparser.ConfigParser) -> pd.DataFrame:
    """Add quality metrics to the result DataFrame."""
    try:
        # Convert price columns to numeric
        price_cols = ['판매단가(V포함)', '판매단가(V포함)(2)', '판매단가(V포함)(3)']
        for col in price_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Calculate price differences
        if '판매단가(V포함)(2)' in df.columns and '판매단가(V포함)' in df.columns:
            df['고려가격차이'] = df['판매단가(V포함)(2)'] - df['판매단가(V포함)']
            mask = df['판매단가(V포함)'].notna() & (df['판매단가(V포함)'] != 0)
            df.loc[mask, '고려가격비율(%)'] = (df.loc[mask, '고려가격차이'] / df.loc[mask, '판매단가(V포함)'] * 100).round(1)

        if '판매단가(V포함)(3)' in df.columns and '판매단가(V포함)' in df.columns:
            df['네이버가격차이'] = df['판매단가(V포함)(3)'] - df['판매단가(V포함)']
            mask = df['판매단가(V포함)'].notna() & (df['판매단가(V포함)'] != 0)
            df.loc[mask, '네이버가격비율(%)'] = (df.loc[mask, '네이버가격차이'] / df.loc[mask, '판매단가(V포함)'] * 100).round(1)

        # Add quality labels
        quality_evaluator = MatchQualityEvaluator(config)
        df = quality_evaluator.apply_quality_labels(df)

        # Log quality metrics
        kogift_matched = df['고려 링크'].notna().sum()
        naver_matched = df['네이버 링크'].notna().sum()
        both_matched = (df['고려 링크'].notna() & df['네이버 링크'].notna()).sum()
        
        logging.info(
            f"Match quality: {kogift_matched}/{len(df)} Kogift, "
            f"{naver_matched}/{len(df)} Naver, "
            f"{both_matched}/{len(df)} both"
        )

        return df
    except Exception as e:
        logging.error(f"Error adding quality metrics: {e}")
        return df

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

def filter_dataframe(df, config):
    """Filter and process the DataFrame with improved error handling."""
    if df is None:
        logging.error("Input DataFrame is None")
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
        if '가격차이(2)(%)' in df_filtered.columns:
            goryeo_clear_cond = goryeo_clear_cond | (df_filtered['가격차이(2)(%)'].notna() & df_filtered['가격차이(2)(%)'].gt(-1.0))

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
        if '가격차이 비율(3)' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이 비율(3)'].notna() & df_filtered['가격차이 비율(3)'].gt(-1.0))
        elif '가격차이(3)(%)' in df_filtered.columns:
            naver_clear_cond1 = naver_clear_cond1 | (df_filtered['가격차이(3)(%)'].notna() & df_filtered['가격차이(3)(%)'].gt(-1.0))

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
                df_filtered.loc[mask, key] = numeric_series[mask].apply(lambda x: f"{x:.1f} %")

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