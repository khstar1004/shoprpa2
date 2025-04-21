import csv
from sentence_transformers import SentenceTransformer, util
import torch
from typing import List, Dict, Union, Optional, Tuple
import logging
import os

# Default model name
DEFAULT_MODEL_NAME = 'jhgan/ko-sroberta-multitask'

# 경로 변수 설정
INPUT_PATH = "C:\\RPA\\Image\\Target\\input.csv"
OUTPUT_PATH = "C:\\RPA\\Image\\Target\\output.csv"

# Cache for the model to avoid reloading
_model_cache = {}

def get_sbert_model(model_name: str = DEFAULT_MODEL_NAME) -> SentenceTransformer:
    """Get or load a SentenceTransformer model with caching."""
    global _model_cache
    
    if model_name not in _model_cache:
        logging.info(f"Loading SBERT model: {model_name}")
        try:
            _model_cache[model_name] = SentenceTransformer(model_name)
            logging.info(f"SBERT model {model_name} loaded successfully")
        except Exception as e:
            logging.error(f"Error loading SBERT model {model_name}: {e}")
            raise
    
    return _model_cache[model_name]

def calculate_similarity(query: str, candidates: List[str], 
                         model_name: str = DEFAULT_MODEL_NAME, 
                         threshold: float = 0.6) -> List[Tuple[str, float]]:
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
        # Get the model (cached if already loaded)
        model = get_sbert_model(model_name)
        
        # Encode the query
        query_embedding = model.encode(query, convert_to_tensor=True)
        
        # Encode all candidates at once for efficiency
        candidate_embeddings = model.encode(candidates, convert_to_tensor=True)
        
        # Calculate similarities
        similarities = util.pytorch_cos_sim(query_embedding, candidate_embeddings)[0]
        
        # Create (candidate, score) pairs and sort by score
        results = [(cand, score.item()) for cand, score in zip(candidates, similarities)]
        results.sort(key=lambda x: x[1], reverse=True)
        
        # Filter by threshold
        filtered_results = [(cand, score) for cand, score in results if score >= threshold]
        
        return filtered_results
    
    except Exception as e:
        logging.error(f"Error calculating text similarity: {e}")
        return []

def find_most_similar_product(query: str, product_names: List[str], 
                              model_name: str = DEFAULT_MODEL_NAME,
                              threshold: float = 0.6) -> Optional[Tuple[str, float]]:
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

def batch_similarity_matrix(texts: List[str], model_name: str = DEFAULT_MODEL_NAME) -> torch.Tensor:
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
    
    model = get_sbert_model(model_name)
    embeddings = model.encode(texts, convert_to_tensor=True)
    return util.pytorch_cos_sim(embeddings, embeddings)

def legacy_calculate_similarity_csv():
    """Original function preserved for backward compatibility."""
    # Ko-Sentence-BERT 모델 로드
    model = SentenceTransformer(DEFAULT_MODEL_NAME)

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

if __name__ == "__main__":
    # 기존 CSV 처리
    if os.path.exists(INPUT_PATH):
        legacy_calculate_similarity_csv()
        print(f"CSV processing complete. Results saved to {OUTPUT_PATH}")
    
    # 테스트 실행
    test_similarity()