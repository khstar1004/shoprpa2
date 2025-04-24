import os
import sys
import configparser
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Import the ProductMatcher class
try:
    from matching_logic import ProductMatcher
    logging.info("Successfully imported ProductMatcher")
except ImportError as e:
    logging.error(f"Failed to import ProductMatcher: {e}")
    sys.exit(1)

def main():
    """Test ProductMatcher initialization"""
    logging.info("Starting ProductMatcher initialization test")
    
    # Create a configuration
    config = configparser.ConfigParser()
    
    # Try to load from config.ini
    if os.path.exists('../config.ini'):
        config.read('../config.ini', encoding='utf-8')
        logging.info("Loaded configuration from ../config.ini")
    else:
        logging.warning("config.ini not found, using default configuration")
        
        # Set up minimal configuration
        config['Matching'] = {
            'text_threshold': '0.45',
            'image_threshold': '0.42',
            'combined_threshold': '0.48',
            'text_weight': '0.65',
            'image_weight': '0.35',
            'token_match_weight': '0.35',
            'fuzzy_match_threshold': '0.8',
            'use_ensemble_models': 'True',
            'use_gpu': 'False',
            'image_resize_dimension': '256'
        }
        
        config['Paths'] = {
            'text_model_path': 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
        }
        
        config['ImageMatching'] = {
            'use_multiple_models': 'False'
        }
    
    # Initialize ProductMatcher
    try:
        matcher = ProductMatcher(config)
        logging.info("ProductMatcher initialized successfully")
        
        # Log important attributes
        logging.info(f"token_match_weight: {matcher.token_match_weight}")
        logging.info(f"ensemble_models: {matcher.ensemble_models}")
        logging.info(f"image_ensemble: {matcher.image_ensemble}")
        logging.info(f"text_model_path: {matcher.text_model_path}")
        logging.info(f"use_gpu: {matcher.use_gpu}")
        
        return True
    except Exception as e:
        logging.error(f"Error initializing ProductMatcher: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 