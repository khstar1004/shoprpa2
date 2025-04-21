import csv
from transformers import AutoTokenizer
import re
from typing import List, Dict, Optional, Set, Tuple
import os
import logging

def tokenize_product_name(product_name: str, tokenizer_name: str = "jhgan/ko-sroberta-multitask") -> List[str]:
    """
    Tokenize a single product name and clean up the tokens.
    
    Args:
        product_name: The product name to tokenize
        tokenizer_name: The name of the pre-trained tokenizer model to use
        
    Returns:
        List of cleaned tokens
    """
    if not product_name:
        return []
        
    # Load tokenizer (consider caching for repeated use)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
    
    # Tokenize product name
    tokens = tokenizer.tokenize(product_name)
    
    # Clean tokens (remove ## and other artifacts)
    cleaned_tokens = [token.replace("##", "") for token in tokens]
    
    return cleaned_tokens

def extract_meaningful_keywords(product_name: str, max_keywords: int = 3) -> List[str]:
    """
    Extract most meaningful keywords from a product name for search.
    Uses a combination of rules rather than just tokenization.
    
    Args:
        product_name: Product name to process
        max_keywords: Maximum number of keywords to extract
        
    Returns:
        List of extracted keywords in order of importance
    """
    if not product_name:
        return []
    
    # 1. Remove product codes using regex
    product_code_pattern = r'\b([A-Z]{1,}[-]?\d{1,}[-]?[A-Za-z0-9]*)\b'
    cleaned_name = re.sub(product_code_pattern, '', product_name, flags=re.IGNORECASE)
    cleaned_name = ' '.join(cleaned_name.split()).strip()  # Clean up extra spaces
    
    # If cleaning removed everything, use original
    if not cleaned_name:
        cleaned_name = product_name
    
    # 2. Split into parts and extract most meaningful ones
    parts = cleaned_name.split()
    
    # If very few parts, just return them all up to max_keywords
    if len(parts) <= max_keywords:
        return parts
    
    # 3. For longer names, extract key components:
    # - First part (usually brand)
    # - Last part (usually product type or unit)
    # - Middle parts based on length/uniqueness
    keywords = []
    
    # Always include brand (first word) if it's not a common word
    common_words = {"더", "신", "새", "구", "및", "용", "품"}
    if parts[0] not in common_words and len(parts[0]) > 1:
        keywords.append(parts[0])
    
    # Add meaningful middle parts - prioritize longer words that may be descriptive
    middle_parts = parts[1:-1]
    middle_parts.sort(key=len, reverse=True)  # Sort by length, longest first
    
    # Add middle parts until we're one short of max_keywords
    for part in middle_parts:
        if len(keywords) >= max_keywords - 1:
            break
        if len(part) > 1 and part not in common_words:
            keywords.append(part)
    
    # Add last part if it's meaningful (product type usually)
    if parts[-1] not in common_words and len(parts[-1]) > 1:
        keywords.append(parts[-1])
    
    # Ensure we don't exceed max_keywords
    return keywords[:max_keywords]

def tokenize_product_names_from_csv(
    input_file: str = "C:\\RPA\\Image\\Target\\input.csv",
    output_file: str = "C:\\RPA\\Image\\Target\\output.csv",
    tokenizer_name: str = "jhgan/ko-sroberta-multitask",
    use_optimized: bool = True
):
    """
    Process a CSV file of product names, tokenizing each one.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        tokenizer_name: The name of the pre-trained tokenizer model to use
        use_optimized: Whether to use optimized keyword extraction instead of tokenization
    """
    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return
    
    try:
        # Load product names from CSV
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            product_names = [row[0] for row in reader]
        
        processed_results = []
        
        # Process each product name
        for name in product_names:
            if use_optimized:
                # Use the optimized keyword extraction
                processed_results.append(extract_meaningful_keywords(name))
            else:
                # Use the traditional tokenization
                processed_results.append(tokenize_product_name(name, tokenizer_name))
        
        # Save processed results to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for tokens in processed_results:
                writer.writerow(tokens)
                
        logging.info(f"Processed {len(product_names)} product names. Results saved to {output_file}")
    except Exception as e:
        logging.error(f"Error processing product names: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Process with optimized keyword extraction by default
    tokenize_product_names_from_csv(use_optimized=True)
    
    # Example of direct use
    test_product = "777쓰리쎄븐 TS-6500C 손톱깎이 13P세트"
    keywords = extract_meaningful_keywords(test_product)
    print(f"Product: {test_product}")
    print(f"Extracted keywords: {keywords}")