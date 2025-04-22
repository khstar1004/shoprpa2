import csv
from sentence_transformers import SentenceTransformer, util
import torch
from typing import List, Dict, Union, Optional, Tuple
import logging
import os
import re
import configparser
import json
import pickle
import hashlib
import time
from pathlib import Path
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import Counter
from difflib import SequenceMatcher

# 기본 인코딩 설정 (한글 처리용)
DEFAULT_ENCODING = 'utf-8'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default values - Improved for accuracy
DEFAULT_MODEL_NAME = 'jhgan/ko-sroberta-multitask'
DEFAULT_THRESHOLD = 0.55  # Lowered to increase potential matches
DEFAULT_CACHE_SIZE = 1000
DEFAULT_CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cache', 'embeddings')
DEFAULT_BOOST_EXACT_MATCH = True
DEFAULT_NAME_SPLIT_MATCHING = True
DEFAULT_TOKEN_MATCH_WEIGHT = 0.3  # Increased weight for token matching
DEFAULT_ENSEMBLE_MODELS = True  # Use multiple models

# Ensemble model options
ADDITIONAL_MODELS = [
    'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2',  # Good for product names
    'jhgan/ko-sbert-nli'  # Additional Korean model
]

# Load config
config = configparser.ConfigParser()
try:
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding=DEFAULT_ENCODING)
    logger.info(f"Successfully loaded config from {config_path}")
    
    # Load text matching settings
    MODEL_NAME = config.get('Matching', 'text_model_name', fallback=DEFAULT_MODEL_NAME)
    TEXT_THRESHOLD = config.getfloat('Matching', 'text_threshold', fallback=DEFAULT_THRESHOLD)
    MAX_CACHE_SIZE = config.getint('Matching', 'max_cache_size', fallback=DEFAULT_CACHE_SIZE)
    BOOST_EXACT_MATCH = config.getboolean('Matching', 'boost_exact_word_match', fallback=DEFAULT_BOOST_EXACT_MATCH)
    NAME_SPLIT_MATCHING = config.getboolean('Matching', 'name_split_matching', fallback=DEFAULT_NAME_SPLIT_MATCHING)
    EXACT_MATCH_BONUS = config.getfloat('Matching', 'exact_match_bonus', fallback=0.2)
    TOKEN_MATCH_WEIGHT = config.getfloat('Matching', 'token_match_weight', fallback=DEFAULT_TOKEN_MATCH_WEIGHT)
    ENSEMBLE_MODELS = config.getboolean('Matching', 'use_ensemble_models', fallback=DEFAULT_ENSEMBLE_MODELS)
    FUZZY_MATCH_THRESHOLD = config.getfloat('Matching', 'fuzzy_match_threshold', fallback=0.8)
    USE_TFIDF = config.getboolean('Matching', 'use_tfidf', fallback=True)
    
    # Configure embedding cache
    CACHE_DIR = config.get('Matching', 'cached_features_dir', fallback=DEFAULT_CACHE_DIR)
    USE_CACHE = config.getboolean('Matching', 'use_persistent_cache', fallback=True)
    if USE_CACHE and not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)
        logger.info(f"Created embedding cache directory: {CACHE_DIR}")
    
    # 경로 변수 설정
    INPUT_PATH = os.path.join(config.get('Paths', 'temp_dir', fallback="C:\\RPA\\Temp"), "text_input.csv")
    OUTPUT_PATH = os.path.join(config.get('Paths', 'output_dir', fallback="C:\\RPA\\Output"), "text_similarity.csv")
except Exception as e:
    logger.warning(f"Could not load config file. Using default values. Error: {e}")
    MODEL_NAME = DEFAULT_MODEL_NAME
    TEXT_THRESHOLD = DEFAULT_THRESHOLD
    MAX_CACHE_SIZE = DEFAULT_CACHE_SIZE
    CACHE_DIR = DEFAULT_CACHE_DIR
    USE_CACHE = False
    BOOST_EXACT_MATCH = DEFAULT_BOOST_EXACT_MATCH
    NAME_SPLIT_MATCHING = DEFAULT_NAME_SPLIT_MATCHING
    EXACT_MATCH_BONUS = 0.2
    TOKEN_MATCH_WEIGHT = DEFAULT_TOKEN_MATCH_WEIGHT
    ENSEMBLE_MODELS = DEFAULT_ENSEMBLE_MODELS
    FUZZY_MATCH_THRESHOLD = 0.8
    USE_TFIDF = True
    INPUT_PATH = "C:\\RPA\\Image\\Target\\input.csv"
    OUTPUT_PATH = "C:\\RPA\\Image\\Target\\output.csv"

# Log settings
logger.info(f"Text Similarity Settings:")
logger.info(f"  - MODEL_NAME: {MODEL_NAME}")
logger.info(f"  - TEXT_THRESHOLD: {TEXT_THRESHOLD}")
logger.info(f"  - MAX_CACHE_SIZE: {MAX_CACHE_SIZE}")
logger.info(f"  - CACHE_DIR: {CACHE_DIR}")
logger.info(f"  - USE_CACHE: {USE_CACHE}")
logger.info(f"  - BOOST_EXACT_MATCH: {BOOST_EXACT_MATCH}")
logger.info(f"  - NAME_SPLIT_MATCHING: {NAME_SPLIT_MATCHING}")
logger.info(f"  - EXACT_MATCH_BONUS: {EXACT_MATCH_BONUS}")
logger.info(f"  - TOKEN_MATCH_WEIGHT: {TOKEN_MATCH_WEIGHT}")
logger.info(f"  - ENSEMBLE_MODELS: {ENSEMBLE_MODELS}")
logger.info(f"  - USE_TFIDF: {USE_TFIDF}")
logger.info(f"  - INPUT_PATH: {INPUT_PATH}")
logger.info(f"  - OUTPUT_PATH: {OUTPUT_PATH}")

# Cache for the model to avoid reloading
_model_cache = {}
_embedding_cache = {}
_cache_info = {}

# TFIDF Vectorizer for keyword importance
_tfidf_vectorizer = None
_tfidf_initialized = False

def initialize_tfidf_vectorizer(sample_texts=None):
    """Initialize TF-IDF vectorizer with sample texts if available"""
    global _tfidf_vectorizer, _tfidf_initialized
    
    try:
        if _tfidf_initialized:
            return _tfidf_vectorizer
            
        # Create vectorizer with Korean-specific settings
        _tfidf_vectorizer = TfidfVectorizer(
            analyzer='char_wb',  # Character n-grams with word boundaries
            ngram_range=(2, 4),  # 2-4 character n-grams
            min_df=2,            # Minimum document frequency
            max_df=0.9,          # Maximum document frequency
            sublinear_tf=True    # Apply sublinear tf scaling
        )
        
        # If sample texts provided, fit the vectorizer
        if sample_texts and len(sample_texts) > 10:
            preprocessed_texts = [preprocess_text(text) for text in sample_texts if text]
            _tfidf_vectorizer.fit(preprocessed_texts)
            logger.info(f"TF-IDF vectorizer initialized with {len(preprocessed_texts)} texts")
        else:
            logger.info("TF-IDF vectorizer initialized without fitting")
            
        _tfidf_initialized = True
        return _tfidf_vectorizer
        
    except Exception as e:
        logger.error(f"Error initializing TF-IDF vectorizer: {e}")
        _tfidf_initialized = False
        return None

class EmbeddingCache:
    """
    Cache for text embeddings to avoid recomputation
    """
    def __init__(self, cache_dir=CACHE_DIR, max_items=MAX_CACHE_SIZE, enabled=USE_CACHE):
        self.cache_dir = cache_dir
        self.max_items = max_items
        self.enabled = enabled
        self.memory_cache = {}
        self.cache_info = {}
        
        if self.enabled:
            os.makedirs(self.cache_dir, exist_ok=True)
            
    def _get_cache_key(self, text: str, model_name: str) -> str:
        """Generate a cache key based on text and model name"""
        # Use hash of text to ensure uniqueness
        text_hash = hashlib.md5(text.encode()).hexdigest()
        model_hash = hashlib.md5(model_name.encode()).hexdigest()[:8]
        return f"{text_hash}_{model_hash}"
    
    def _get_cache_path(self, cache_key: str) -> str:
        """Get the path to the cache file for a given key"""
        return os.path.join(self.cache_dir, f"{cache_key}.pkl")
    
    def get(self, text: str, model_name: str) -> Optional[torch.Tensor]:
        """Get embedding from cache if it exists"""
        if not self.enabled:
            return None
            
        cache_key = self._get_cache_key(text, model_name)
        
        # Check memory cache first
        if cache_key in self.memory_cache:
            # Update last access time
            self.cache_info[cache_key] = time.time()
            return self.memory_cache[cache_key]
            
        # Check disk cache
        cache_path = self._get_cache_path(cache_key)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    embedding = pickle.load(f)
                    
                # Add to memory cache (with LRU eviction if needed)
                if len(self.memory_cache) >= self.max_items:
                    # Find least recently used item
                    oldest_key = min(self.cache_info, key=self.cache_info.get) if self.cache_info else None
                    if oldest_key:
                        # Remove it from memory cache and cache info
                        del self.memory_cache[oldest_key]
                        del self.cache_info[oldest_key]
                    
                # Add to memory cache
                self.memory_cache[cache_key] = embedding
                self.cache_info[cache_key] = time.time()
                
                return embedding
            except Exception as e:
                logger.warning(f"Error loading cache for text: {e}")
                return None
        
        return None
        
    def put(self, text: str, model_name: str, embedding: torch.Tensor) -> None:
        """Save embedding to cache"""
        if not self.enabled:
            return
            
        cache_key = self._get_cache_key(text, model_name)
        
        # Add to memory cache (with LRU eviction if needed)
        if len(self.memory_cache) >= self.max_items:
            # Find least recently used item
            oldest_key = min(self.cache_info, key=self.cache_info.get) if self.cache_info else None
            if oldest_key:
                # Remove it from memory cache and cache info
                del self.memory_cache[oldest_key]
                del self.cache_info[oldest_key]
            
        # Add to memory cache
        self.memory_cache[cache_key] = embedding
        self.cache_info[cache_key] = time.time()
        
        # Save to disk
        try:
            cache_path = self._get_cache_path(cache_key)
            with open(cache_path, 'wb') as f:
                pickle.dump(embedding, f)
        except Exception as e:
            logger.warning(f"Error saving cache for text: {e}")

# Initialize embedding cache
embedding_cache = EmbeddingCache()

def preprocess_text(text: str) -> str:
    """
    전처리 함수: 텍스트 정규화 및 불필요한 문자 제거
    
    Args:
        text: 전처리할 텍스트
        
    Returns:
        전처리된 텍스트
    """
    if not text:
        return ""
    
    # 텍스트를 소문자로 변환 (영문의 경우)
    text = text.lower()
    
    # 괄호 안의 내용 분리하여 처리 (옵션 정보 등이 괄호 안에 있는 경우가 많음)
    bracket_content = []
    bracket_regex = r'\(([^)]+)\)'
    matches = re.findall(bracket_regex, text)
    if matches:
        for match in matches:
            bracket_content.append(match)
        # 괄호와 내용 제거
        text = re.sub(r'\([^)]+\)', ' ', text)
    
    # 특수 문자 제거 (하이픈, 슬래시 등은 공백으로 대체)
    text = re.sub(r'[^\w\s가-힣]', ' ', text)
    
    # 불필요한 공백 제거
    text = re.sub(r'\s+', ' ', text)
    
    # 앞뒤 공백 제거
    text = text.strip()
    
    # 괄호 내용이 중요하다면 뒤에 추가
    if bracket_content and len(bracket_content) > 0:
        text += " " + " ".join(bracket_content)
    
    return text

def extract_numbers(text: str) -> List[str]:
    """Extract all numbers from text"""
    if not text:
        return []
    return re.findall(r'\d+', text)
    
def get_number_match_score(text1: str, text2: str) -> float:
    """Calculate similarity based on matching numbers"""
    nums1 = set(extract_numbers(text1))
    nums2 = set(extract_numbers(text2))
    
    if not nums1 and not nums2:
        return 0.5  # Neutral if no numbers
        
    if not nums1 or not nums2:
        return 0.0  # No match if one has numbers and other doesn't
    
    # Calculate Jaccard similarity
    intersection = len(nums1.intersection(nums2))
    union = len(nums1.union(nums2))
    
    return intersection / union if union > 0 else 0.0

def split_product_name(name: str) -> List[str]:
    """
    상품명을 토큰으로 분리
    
    Args:
        name: 상품명
        
    Returns:
        토큰 리스트
    """
    if not name:
        return []
        
    # 전처리
    name = preprocess_text(name)
    
    # 공백으로 분리
    tokens = name.split()
    
    # 숫자+단위 패턴 처리 (예: 1kg, 500ml 등)
    result = []
    for token in tokens:
        # 숫자+단위 패턴 확인
        unit_match = re.match(r'(\d+)([a-z가-힣]+)', token)
        if unit_match:
            # 숫자와 단위 분리
            number, unit = unit_match.groups()
            result.append(number)
            result.append(unit)
        else:
            result.append(token)
    
    return result

def count_common_tokens(name1: str, name2: str) -> int:
    """
    두 상품명 간의 공통 토큰 수 계산
    
    Args:
        name1: 첫 번째 상품명
        name2: 두 번째 상품명
        
    Returns:
        공통 토큰 수
    """
    # 각 상품명을 토큰으로 분리
    tokens1 = split_product_name(name1)
    tokens2 = split_product_name(name2)
    
    # 공통 토큰 찾기
    common = set(tokens1) & set(tokens2)
    
    return len(common)

def calculate_token_similarity(name1: str, name2: str) -> float:
    """Calculate similarity based on token overlap with weighting"""
    tokens1 = split_product_name(name1)
    tokens2 = split_product_name(name2)
    
    if not tokens1 or not tokens2:
        return 0.0
    
    # Count common tokens
    common_tokens = set(tokens1) & set(tokens2)
    
    # Calculate Jaccard similarity
    jaccard = len(common_tokens) / len(set(tokens1) | set(tokens2))
    
    # Calculate coverage (proportion of tokens matched)
    coverage1 = len(common_tokens) / len(tokens1) if tokens1 else 0
    coverage2 = len(common_tokens) / len(tokens2) if tokens2 else 0
    
    # Weight longer token lists more heavily
    if len(tokens1) > len(tokens2):
        weighted_coverage = 0.7 * coverage1 + 0.3 * coverage2
    else:
        weighted_coverage = 0.3 * coverage1 + 0.7 * coverage2
    
    # Combine scores
    return 0.5 * jaccard + 0.5 * weighted_coverage

def calculate_fuzzy_similarity(text1: str, text2: str) -> float:
    """Calculate fuzzy string similarity using SequenceMatcher"""
    if not text1 or not text2:
        return 0.0
        
    # Use SequenceMatcher for fuzzy matching
    matcher = SequenceMatcher(None, text1, text2)
    return matcher.ratio()

def calculate_tfidf_similarity(text1: str, text2: str) -> float:
    """Calculate similarity using TF-IDF vectors"""
    global _tfidf_vectorizer
    
    if not text1 or not text2 or not USE_TFIDF:
        return 0.0
        
    try:
        # Initialize vectorizer if needed
        if not _tfidf_initialized:
            initialize_tfidf_vectorizer([text1, text2])
            
        # Transform texts to TF-IDF vectors
        tfidf_matrix = _tfidf_vectorizer.transform([text1, text2])
        
        # Calculate cosine similarity
        from sklearn.metrics.pairwise import cosine_similarity
        sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
        return float(sim)
    except Exception as e:
        logger.warning(f"Error calculating TF-IDF similarity: {e}")
        return 0.0

def get_sbert_model(model_name: str = MODEL_NAME) -> SentenceTransformer:
    """
    모델 로드 또는 캐시에서 가져오기
    
    Args:
        model_name: 모델 이름
        
    Returns:
        SentenceTransformer 모델
    """
    global _model_cache
    
    if model_name in _model_cache:
        return _model_cache[model_name]
        
    try:
        # 모델을 처음 로드하는 경우
        logger.info(f"Loading SentenceTransformer model: {model_name}")
        model = SentenceTransformer(model_name)
        _model_cache[model_name] = model
        return model
    except Exception as e:
        logger.error(f"Error loading model {model_name}: {e}")
        # 기본 모델로 폴백
        if model_name != DEFAULT_MODEL_NAME and DEFAULT_MODEL_NAME not in _model_cache:
            logger.info(f"Falling back to default model: {DEFAULT_MODEL_NAME}")
            return get_sbert_model(DEFAULT_MODEL_NAME)
        raise e

def encode_text(text: str, model_name: str = MODEL_NAME) -> Optional[torch.Tensor]:
    """
    텍스트를 임베딩 벡터로 인코딩
    
    Args:
        text: 인코딩할 텍스트
        model_name: 사용할 모델 이름
        
    Returns:
        임베딩 텐서
    """
    if not text:
        return None
        
    # 전처리
    text = preprocess_text(text)
    if not text:
        return None
        
    # 캐시 확인
    cached_embedding = embedding_cache.get(text, model_name)
    if cached_embedding is not None:
        return cached_embedding
        
    try:
        # 모델 로드
        model = get_sbert_model(model_name)
        
        # 인코딩
        embedding = model.encode(text, convert_to_tensor=True)
        
        # 캐시 저장
        embedding_cache.put(text, model_name, embedding)
        
        return embedding
    except Exception as e:
        logger.error(f"Error encoding text: {e}")
        return None

def encode_with_ensemble(text: str) -> Dict[str, torch.Tensor]:
    """Encode text using multiple models for ensemble"""
    if not text or not ENSEMBLE_MODELS:
        # Default single model encoding
        embedding = encode_text(text, MODEL_NAME)
        return {'primary': embedding} if embedding is not None else None
    
    results = {}
    
    # Primary model
    primary = encode_text(text, MODEL_NAME)
    if primary is None:
        return None
    
    results['primary'] = primary
    
    # Additional models
    for model_name in ADDITIONAL_MODELS:
        try:
            embedding = encode_text(text, model_name)
            if embedding is not None:
                results[model_name] = embedding
        except Exception as e:
            logger.warning(f"Error encoding with model {model_name}: {e}")
    
    return results if len(results) > 0 else None

def calculate_ensemble_similarity(text1: str, text2: str) -> float:
    """Calculate similarity using ensemble of models"""
    if not text1 or not text2:
        return 0.0
    
    if not ENSEMBLE_MODELS:
        # Use single model if ensemble is disabled
        embedding1 = encode_text(text1, MODEL_NAME)
        embedding2 = encode_text(text2, MODEL_NAME)
        
        if embedding1 is None or embedding2 is None:
            return 0.0
            
        return float(util.pytorch_cos_sim(embedding1, embedding2).item())
    
    # Encode with ensemble
    embeddings1 = encode_with_ensemble(text1)
    embeddings2 = encode_with_ensemble(text2)
    
    if embeddings1 is None or embeddings2 is None:
        return 0.0
    
    # Calculate similarity for each model
    similarities = {}
    
    # Primary model similarity
    if 'primary' in embeddings1 and 'primary' in embeddings2:
        similarities['primary'] = float(util.pytorch_cos_sim(
            embeddings1['primary'], embeddings2['primary']).item())
    else:
        return 0.0  # Primary model is required
    
    # Calculate similarity for each additional model
    for model_name in ADDITIONAL_MODELS:
        if model_name in embeddings1 and model_name in embeddings2:
            similarities[model_name] = float(util.pytorch_cos_sim(
                embeddings1[model_name], embeddings2[model_name]).item())
    
    # Combine similarities with weights
    weight_primary = 0.6
    weight_secondary = 0.4 / max(1, len(similarities) - 1)
    
    final_sim = similarities['primary'] * weight_primary
    
    for model_name, sim in similarities.items():
        if model_name != 'primary':
            final_sim += sim * weight_secondary
    
    return final_sim

def calculate_similarity(query: str, candidates: List[str], 
                         model_name: str = MODEL_NAME, 
                         threshold: float = TEXT_THRESHOLD) -> List[Tuple[str, float]]:
    """
    후보 텍스트 중에서 쿼리와 가장 유사한 텍스트 찾기
    
    Args:
        query: 쿼리 텍스트
        candidates: 후보 텍스트 리스트
        model_name: 사용할 모델 이름
        threshold: 유사도 임계값
        
    Returns:
        (텍스트, 유사도) 튜플 리스트
    """
    if not query or not candidates:
        return []
        
    # 전처리
    query = preprocess_text(query)
    if not query:
        return []
        
    # TF-IDF 벡터라이저 초기화
    if USE_TFIDF and not _tfidf_initialized:
        initialize_tfidf_vectorizer([query] + candidates)
        
    # 결과 저장
    results = []
    
    # 토큰 기반 필터링 (옵션)
    if NAME_SPLIT_MATCHING:
        # 쿼리 토큰
        query_tokens = set(split_product_name(query))
        if not query_tokens:
            return []
            
        # 각 후보에 대해 처리
        for candidate in candidates:
            # 전처리
            candidate = preprocess_text(candidate)
            if not candidate:
                continue
                
            # 1. 토큰 기반 유사도
            token_sim = calculate_token_similarity(query, candidate)
            
            # 2. 숫자 매칭 점수
            number_sim = get_number_match_score(query, candidate)
            
            # 3. 퍼지 매칭
            fuzzy_sim = calculate_fuzzy_similarity(query, candidate)
            
            # 4. TF-IDF 유사도
            tfidf_sim = calculate_tfidf_similarity(query, candidate)
            
            # 5. 인코딩 기반 유사도 (앙상블)
            if ENSEMBLE_MODELS:
                model_sim = calculate_ensemble_similarity(query, candidate)
            else:
                # 개별 인코딩
                query_embedding = encode_text(query, model_name)
                candidate_embedding = encode_text(candidate, model_name)
                
                # 코사인 유사도
                if query_embedding is None or candidate_embedding is None:
                    model_sim = 0.0
                else:
                    model_sim = float(util.pytorch_cos_sim(query_embedding, candidate_embedding).item())
            
            # 가중치 적용하여 최종 점수 계산
            final_sim = (
                0.55 * model_sim +    # 인코딩 기반 (앙상블)
                0.20 * token_sim +    # 토큰 기반
                0.10 * fuzzy_sim +    # 퍼지 매칭
                0.10 * tfidf_sim +    # TF-IDF
                0.05 * number_sim     # 숫자 매칭
            )
            
            # 정확히 일치하는 경우 보너스
            if query.lower() == candidate.lower():
                final_sim = min(1.0, final_sim + EXACT_MATCH_BONUS)
                
            # 임계값 이상인 경우 결과에 추가
            if final_sim >= threshold:
                results.append((candidate, final_sim))
    else:
        # 토큰 매칭 없이 순수 인코딩 기반
        # 쿼리 인코딩
        query_embedding = encode_text(query, model_name)
        if query_embedding is None:
            return []
            
        # 각 후보에 대해 처리
        for candidate in candidates:
            # 전처리
            candidate = preprocess_text(candidate)
            if not candidate:
                continue
                
            # 인코딩
            candidate_embedding = encode_text(candidate, model_name)
            if candidate_embedding is None:
                continue
                
            # 유사도 계산
            similarity = float(util.pytorch_cos_sim(query_embedding, candidate_embedding).item())
            
            # 정확히 일치하는 경우 보너스
            if query.lower() == candidate.lower():
                similarity = min(1.0, similarity + EXACT_MATCH_BONUS)
                
            # 임계값 이상인 경우 결과에 추가
            if similarity >= threshold:
                results.append((candidate, similarity))
    
    # 유사도 기준 내림차순 정렬
    results.sort(key=lambda x: x[1], reverse=True)
    
    return results

def calculate_text_similarity(text1: str, text2: str, model_name: str = MODEL_NAME) -> float:
    """
    두 텍스트 간의 유사도 계산
    
    Args:
        text1: 첫 번째 텍스트
        text2: 두 번째 텍스트
        model_name: 사용할 모델 이름
        
    Returns:
        유사도 (0.0 ~ 1.0)
    """
    if not text1 or not text2:
        return 0.0
        
    # 전처리
    text1 = preprocess_text(text1)
    text2 = preprocess_text(text2)
    
    if not text1 or not text2:
        return 0.0
    
    # 완전히 동일한 경우
    if text1 == text2:
        return 1.0
    
    # 토큰 기반 유사도
    token_sim = calculate_token_similarity(text1, text2)
    
    # 숫자 매칭 점수
    number_sim = get_number_match_score(text1, text2)
    
    # 퍼지 매칭
    fuzzy_sim = calculate_fuzzy_similarity(text1, text2)
    
    # TF-IDF 유사도
    tfidf_sim = calculate_tfidf_similarity(text1, text2)
    
    # 인코딩 기반 유사도 (앙상블)
    if ENSEMBLE_MODELS:
        model_sim = calculate_ensemble_similarity(text1, text2)
    else:
        # 개별 인코딩
        embedding1 = encode_text(text1, model_name)
        embedding2 = encode_text(text2, model_name)
        
        # 코사인 유사도
        if embedding1 is None or embedding2 is None:
            model_sim = 0.0
        else:
            model_sim = float(util.pytorch_cos_sim(embedding1, embedding2).item())
    
    # 가중치 적용하여 최종 점수 계산
    final_sim = (
        0.55 * model_sim +    # 인코딩 기반 (앙상블)
        0.20 * token_sim +    # 토큰 기반
        0.10 * fuzzy_sim +    # 퍼지 매칭
        0.10 * tfidf_sim +    # TF-IDF
        0.05 * number_sim     # 숫자 매칭
    )
    
    # 정확히 일치하는 경우 보너스
    if text1 == text2:
        final_sim = min(1.0, final_sim + EXACT_MATCH_BONUS)
    
    logger.debug(f"Text similarity: {final_sim:.4f} (model={model_sim:.4f}, token={token_sim:.4f}, fuzzy={fuzzy_sim:.4f}, tfidf={tfidf_sim:.4f}, number={number_sim:.4f})")
    
    return final_sim

def find_most_similar_product(query: str, product_names: List[str], 
                              model_name: str = MODEL_NAME,
                              threshold: float = TEXT_THRESHOLD) -> Optional[Tuple[str, float]]:
    """
    Find the most similar product name to the query.
    
    Args:
        query: The product name to search for
        product_names: List of product names to search within
        model_name: Name of the SBERT model to use
        threshold: Minimum similarity score to consider a match
        
    Returns:
        Tuple of (most_similar_product_name, similarity_score) or None if no match found
    """
    if not query or not product_names:
        return None
    
    similarities = calculate_similarity(query, product_names, model_name, threshold)
    
    if similarities:
        return similarities[0]  # Return the highest similarity match
    return None

def batch_similarity_matrix(texts: List[str], model_name: str = MODEL_NAME) -> torch.Tensor:
    """
    Compute similarity matrix for a list of texts.
    
    Args:
        texts: List of texts to compare with each other
        model_name: Name of the SBERT model to use
        
    Returns:
        Tensor containing pairwise similarities (n×n matrix)
    """
    if not texts:
        return torch.tensor([])
    
    # 텍스트 전처리
    processed_texts = [preprocess_text(t) for t in texts]
    valid_texts = [t for t in processed_texts if t]
    
    if not valid_texts:
        return torch.tensor([])
    
    # 인코딩 (캐싱 활용)
    embeddings = []
    for text in valid_texts:
        embedding = encode_text(text, model_name)
        if embedding is not None:
            embeddings.append(embedding)
        else:
            # 임베딩 실패한 경우 0 벡터 (차원은 다른 임베딩과 맞춰야 함)
            if embeddings:
                embeddings.append(torch.zeros_like(embeddings[0]))
            else:
                logger.warning("First embedding failed, cannot create empty tensor")
                return torch.tensor([])
    
    # 임베딩을 하나의 텐서로 결합
    if embeddings:
        embeddings_tensor = torch.stack(embeddings)
        # 유사도 행렬 계산
        return util.pytorch_cos_sim(embeddings_tensor, embeddings_tensor)
    else:
        return torch.tensor([])

def legacy_calculate_similarity_csv():
    """Original function preserved for backward compatibility."""
    # Ko-Sentence-BERT 모델 로드
    model = SentenceTransformer(MODEL_NAME)

    # CSV 파일에서 문장 리스트 불러오기
    with open(INPUT_PATH, 'r', encoding='utf-8') as file:
        sentences = [line.strip() for line in file.readlines()]

    # 첫 번째 라인을 메인 문장으로 설정하고 임베딩 계산
    main_sentence = sentences[0]
    embedding_main = model.encode(main_sentence, convert_to_tensor=True)

    similarities = []

    # 나머지 라인들과의 유사도 계산
    for sentence in sentences[1:]:
        embedding_sentence = model.encode(sentence, convert_to_tensor=True)
        
        cosine_sim = util.pytorch_cos_sim(embedding_main, embedding_sentence)
        similarities.append(cosine_sim.item())

    # 유사도를 CSV 파일에 저장
    save_to_csv(similarities)

def save_to_csv(data):
    with open(OUTPUT_PATH, 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        for value in data:
            writer.writerow([value])

# Test function
def test_similarity():
    """Test the similarity functions with sample Korean product names."""
    test_query = "777쓰리쎄븐 TS-6500C 손톱깎이 13P세트"
    test_candidates = [
        "쓰리쎄븐 777 TS-6500C 손톱깎이 세트", 
        "777 쓰리쎄븐 손톱깎이 세트",
        "쓰리쎄븐 손톱깎이 13P세트",
        "TS-6500C 손톱깎이",
        "손톱깎이 세트 고급형",
        "삼성 갤럭시 S22"
    ]
    
    print(f"Query: {test_query}")
    print("===== Similarity Results =====")
    results = calculate_similarity(test_query, test_candidates)
    for cand, score in results:
        print(f"{score:.4f} - {cand}")
    
    print("\n===== Most Similar Product =====")
    best_match = find_most_similar_product(test_query, test_candidates)
    if best_match:
        print(f"Best match: {best_match[0]} (Score: {best_match[1]:.4f})")
    else:
        print("No good match found.")
        
    print("\n===== Token Analysis =====")
    for candidate in test_candidates:
        tokens_query = split_product_name(test_query)
        tokens_candidate = split_product_name(candidate)
        common = set(tokens_query) & set(tokens_candidate)
        print(f"'{candidate}'")
        print(f"  Query tokens: {tokens_query}")
        print(f"  Candidate tokens: {tokens_candidate}")
        print(f"  Common tokens: {common} ({len(common)})")
        print()

if __name__ == "__main__":
    # 기존 CSV 처리
    if os.path.exists(INPUT_PATH):
        legacy_calculate_similarity_csv()
        print(f"CSV processing complete. Results saved to {OUTPUT_PATH}")
    
    # 테스트 실행
    test_similarity()