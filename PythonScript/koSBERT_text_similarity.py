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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default values
DEFAULT_MODEL_NAME = 'jhgan/ko-sroberta-multitask'
DEFAULT_THRESHOLD = 0.6
DEFAULT_CACHE_SIZE = 1000
DEFAULT_CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cache', 'embeddings')
DEFAULT_BOOST_EXACT_MATCH = True
DEFAULT_NAME_SPLIT_MATCHING = True

# Load config
config = configparser.ConfigParser()
try:
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    logger.info(f"Successfully loaded config from {config_path}")
    
    # Load text matching settings
    MODEL_NAME = config.get('Matching', 'text_model_name', fallback=DEFAULT_MODEL_NAME)
    TEXT_THRESHOLD = config.getfloat('Matching', 'text_threshold', fallback=DEFAULT_THRESHOLD)
    MAX_CACHE_SIZE = config.getint('Matching', 'max_cache_size', fallback=DEFAULT_CACHE_SIZE)
    BOOST_EXACT_MATCH = config.getboolean('Matching', 'boost_exact_word_match', fallback=DEFAULT_BOOST_EXACT_MATCH)
    NAME_SPLIT_MATCHING = config.getboolean('Matching', 'name_split_matching', fallback=DEFAULT_NAME_SPLIT_MATCHING)
    EXACT_MATCH_BONUS = config.getfloat('Matching', 'exact_match_bonus', fallback=0.2)
    
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
logger.info(f"  - INPUT_PATH: {INPUT_PATH}")
logger.info(f"  - OUTPUT_PATH: {OUTPUT_PATH}")

# Cache for the model to avoid reloading
_model_cache = {}
_embedding_cache = {}
_cache_info = {}

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
    
    # 여러 개의 공백을 하나로 변환
    text = re.sub(r'\s+', ' ', text)
    
    # 앞뒤 공백 제거
    text = text.strip()
    
    # 특수문자 제거 또는 변환 (옵션)
    # text = re.sub(r'[^\w\s]', '', text)  # 모든 특수문자 제거
    
    return text

def split_product_name(name: str) -> List[str]:
    """
    제품명을 의미 있는 단위(토큰)로 분리
    
    Args:
        name: 제품명
        
    Returns:
        분리된 토큰 목록
    """
    if not name:
        return []
    
    # 먼저, 일반적인 구분자로 분리
    tokens = re.split(r'[\s,_\-/]+', name)
    
    # 빈 토큰 제거
    tokens = [t for t in tokens if t]
    
    # 숫자와 단위 결합 패턴 (예: 13P, 500ml 등)
    # 이 부분은 필요에 따라 조정 가능
    
    # 결과 반환
    return tokens

def count_common_tokens(name1: str, name2: str) -> int:
    """
    두 제품명 간의 공통 토큰 수 계산
    
    Args:
        name1: 첫 번째 제품명
        name2: 두 번째 제품명
        
    Returns:
        공통 토큰 수
    """
    tokens1 = set(split_product_name(name1))
    tokens2 = set(split_product_name(name2))
    
    return len(tokens1.intersection(tokens2))

def get_sbert_model(model_name: str = MODEL_NAME) -> SentenceTransformer:
    """Get or load a SentenceTransformer model with caching."""
    global _model_cache
    
    if model_name not in _model_cache:
        logger.info(f"Loading SBERT model: {model_name}")
        try:
            _model_cache[model_name] = SentenceTransformer(model_name)
            logger.info(f"SBERT model {model_name} loaded successfully")
        except Exception as e:
            logger.error(f"Error loading SBERT model {model_name}: {e}")
            raise
    
    return _model_cache[model_name]

def encode_text(text: str, model_name: str = MODEL_NAME) -> torch.Tensor:
    """
    텍스트를 인코딩하여 임베딩 벡터 반환
    
    Args:
        text: 인코딩할 텍스트
        model_name: 사용할 모델 이름
        
    Returns:
        텍스트 임베딩
    """
    if not text:
        return None
    
    # 텍스트 전처리
    preprocessed_text = preprocess_text(text)
    if not preprocessed_text:
        return None
    
    # 캐시에서 임베딩 확인
    cached_embedding = embedding_cache.get(preprocessed_text, model_name)
    if cached_embedding is not None:
        return cached_embedding
    
    # 모델 가져오기
    model = get_sbert_model(model_name)
    
    # 텍스트 인코딩
    embedding = model.encode(preprocessed_text, convert_to_tensor=True)
    
    # 캐시에 저장
    embedding_cache.put(preprocessed_text, model_name, embedding)
    
    return embedding

def calculate_similarity(query: str, candidates: List[str], 
                         model_name: str = MODEL_NAME, 
                         threshold: float = TEXT_THRESHOLD) -> List[Tuple[str, float]]:
    """
    Calculate semantic similarity between a query and multiple candidate texts.
    
    Args:
        query: The query text to compare against candidates
        candidates: List of candidate texts to be compared with the query
        model_name: Name of the SBERT model to use
        threshold: Minimum similarity score to include in results (0-1)
        
    Returns:
        List of (candidate, score) tuples sorted by descending similarity score,
        filtered by threshold
    """
    if not query or not candidates:
        return []
    
    try:
        # 전처리
        query = preprocess_text(query)
        candidates = [preprocess_text(c) for c in candidates]
        
        # 빈 텍스트 필터링
        valid_candidates = [(c, i) for i, c in enumerate(candidates) if c]
        if not valid_candidates:
            return []
        
        valid_texts, indices = zip(*valid_candidates)
        
        # 인코딩
        query_embedding = encode_text(query, model_name)
        if query_embedding is None:
            return []
        
        # 후보 텍스트들을 개별적으로 인코딩 (캐싱 활용)
        valid_embeddings = []
        for text in valid_texts:
            embedding = encode_text(text, model_name)
            if embedding is not None:
                valid_embeddings.append(embedding)
            else:
                valid_embeddings.append(torch.zeros_like(query_embedding))
        
        # 임베딩을 하나의 텐서로 결합
        if valid_embeddings:
            candidate_embeddings = torch.stack(valid_embeddings)
        else:
            return []
        
        # 코사인 유사도 계산
        similarities = util.pytorch_cos_sim(query_embedding, candidate_embeddings)[0]
        
        # 토큰 기반 보너스 추가 (if enabled)
        results = []
        for i, (text, sim) in enumerate(zip(valid_texts, similarities)):
            score = sim.item()
            
            # 정확한 단어 매칭 보너스 적용
            if BOOST_EXACT_MATCH and NAME_SPLIT_MATCHING:
                common_count = count_common_tokens(query, text)
                if common_count > 0:
                    # 공통 토큰이 많을수록 더 큰 보너스
                    token_boost = min(common_count * 0.05, EXACT_MATCH_BONUS)
                    score = min(1.0, score + token_boost)
                
                # 정확히 일치하는 경우 추가 보너스
                if query == text:
                    score = min(1.0, score + EXACT_MATCH_BONUS)
            
            orig_candidate = candidates[indices[i]]
            results.append((orig_candidate, score))
        
        # 결과 정렬 및 임계값 필터링
        results.sort(key=lambda x: x[1], reverse=True)
        filtered_results = [(cand, score) for cand, score in results if score >= threshold]
        
        return filtered_results
    
    except Exception as e:
        logger.error(f"Error calculating text similarity: {e}")
        return []

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