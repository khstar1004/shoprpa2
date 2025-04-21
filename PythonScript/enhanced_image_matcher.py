"""
Enhanced Image Matcher - Combines traditional CV techniques with deep learning

This module provides improved image matching capabilities by combining:
1. SIFT (Scale-Invariant Feature Transform) for local feature matching
2. AKAZE (Accelerated-KAZE) for handling non-linear transformations
3. EfficientNetB0 for deep feature extraction and similarity

The combined approach provides better accuracy for product image matching.
"""

import os
import cv2
import numpy as np
import tensorflow as tf
import logging
from typing import Tuple, Dict, List, Optional, Union
import time
from PIL import Image
from urllib.parse import urlparse
import configparser
import json
import pickle
from pathlib import Path
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default Constants
DEFAULT_IMG_SIZE = (224, 224)
DEFAULT_FEATURE_MATCH_THRESHOLD = 10
DEFAULT_SIFT_RATIO_THRESHOLD = 0.75
DEFAULT_AKAZE_DISTANCE_THRESHOLD = 50
DEFAULT_COMBINED_THRESHOLD = 0.65
DEFAULT_WEIGHTS = {'sift': 0.3, 'akaze': 0.2, 'deep': 0.5}
DEFAULT_CACHE_DIR = 'C:\\RPA\\Temp\\feature_cache'

def _load_config() -> configparser.ConfigParser:
    """Load configuration from config.ini"""
    config = configparser.ConfigParser()
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
        config.read(config_path, encoding='utf-8')
        logger.info(f"Successfully loaded config from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return None

def _get_config_values(config: configparser.ConfigParser) -> Dict:
    """Get configuration values with fallbacks"""
    if not config:
        return None

    try:
        return {
            'FEATURE_MATCH_THRESHOLD': config.getint('ImageMatching', 'feature_match_threshold', 
                                                   fallback=DEFAULT_FEATURE_MATCH_THRESHOLD),
            'SIFT_RATIO_THRESHOLD': config.getfloat('ImageMatching', 'sift_ratio_threshold', 
                                                   fallback=DEFAULT_SIFT_RATIO_THRESHOLD),
            'AKAZE_DISTANCE_THRESHOLD': config.getint('ImageMatching', 'akaze_distance_threshold', 
                                                     fallback=DEFAULT_AKAZE_DISTANCE_THRESHOLD),
            'COMBINED_THRESHOLD': config.getfloat('ImageMatching', 'combined_threshold', 
                                                fallback=DEFAULT_COMBINED_THRESHOLD),
            'WEIGHTS': {
                'sift': config.getfloat('ImageMatching', 'sift_weight', 
                                      fallback=DEFAULT_WEIGHTS['sift']),
                'akaze': config.getfloat('ImageMatching', 'akaze_weight', 
                                       fallback=DEFAULT_WEIGHTS['akaze']),
                'deep': config.getfloat('ImageMatching', 'deep_weight', 
                                      fallback=DEFAULT_WEIGHTS['deep'])
            },
            'USE_BACKGROUND_REMOVAL': config.getboolean('ImageMatching', 'use_background_removal_before_matching', 
                                                       fallback=False),
            'CACHE_FEATURES': config.getboolean('ImageMatching', 'cache_extracted_features', 
                                              fallback=True),
            'MAX_CACHE_ITEMS': config.getint('ImageMatching', 'max_feature_cache_items', 
                                           fallback=1000),
            'FEATURE_CACHE_DIR': config.get('ImageMatching', 'feature_cache_dir', 
                                          fallback=DEFAULT_CACHE_DIR)
        }
    except Exception as e:
        logger.error(f"Error getting config values: {e}")
        return None

# Load initial configuration
CONFIG = _load_config()
SETTINGS = _get_config_values(CONFIG) or {
    'FEATURE_MATCH_THRESHOLD': DEFAULT_FEATURE_MATCH_THRESHOLD,
    'SIFT_RATIO_THRESHOLD': DEFAULT_SIFT_RATIO_THRESHOLD,
    'AKAZE_DISTANCE_THRESHOLD': DEFAULT_AKAZE_DISTANCE_THRESHOLD,
    'COMBINED_THRESHOLD': DEFAULT_COMBINED_THRESHOLD,
    'WEIGHTS': DEFAULT_WEIGHTS,
    'USE_BACKGROUND_REMOVAL': False,
    'CACHE_FEATURES': True,
    'MAX_CACHE_ITEMS': 1000,
    'FEATURE_CACHE_DIR': DEFAULT_CACHE_DIR
}

# Log settings
logger.info("EnhancedImageMatcher Settings:")
for key, value in SETTINGS.items():
    logger.info(f"  - {key}: {value}")


class FeatureCache:
    """
    Cache for features extracted from images to avoid recomputation
    """
    def __init__(self, cache_dir=SETTINGS['FEATURE_CACHE_DIR'], max_items=SETTINGS['MAX_CACHE_ITEMS'], enabled=SETTINGS['CACHE_FEATURES']):
        self.cache_dir = cache_dir
        self.max_items = max_items
        self.enabled = enabled
        self.memory_cache = {}
        self.cache_info = {}
        
        if self.enabled:
            os.makedirs(self.cache_dir, exist_ok=True)
            
    def _get_cache_key(self, img_path: str, feature_type: str) -> str:
        """Generate a cache key based on image path and feature type"""
        # Use hash of absolute path to ensure uniqueness and avoid path issues
        img_hash = hashlib.md5(os.path.abspath(img_path).encode()).hexdigest()
        return f"{img_hash}_{feature_type}"
    
    def _get_cache_path(self, cache_key: str) -> str:
        """Get the path to the cache file for a given key"""
        return os.path.join(self.cache_dir, f"{cache_key}.pkl")
    
    def get(self, img_path: str, feature_type: str) -> Optional[np.ndarray]:
        """Get features from cache if they exist"""
        if not self.enabled:
            return None
            
        cache_key = self._get_cache_key(img_path, feature_type)
        
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
                    features = pickle.load(f)
                    
                # Add to memory cache (with LRU eviction if needed)
                if len(self.memory_cache) >= self.max_items:
                    # Find least recently used item
                    oldest_key = min(self.cache_info, key=self.cache_info.get)
                    # Remove it from memory cache and cache info
                    del self.memory_cache[oldest_key]
                    del self.cache_info[oldest_key]
                    
                # Add to memory cache
                self.memory_cache[cache_key] = features
                self.cache_info[cache_key] = time.time()
                
                return features
            except Exception as e:
                logger.warning(f"Error loading cache for {img_path}: {e}")
                return None
        
        return None
        
    def put(self, img_path: str, feature_type: str, features: np.ndarray) -> None:
        """Save features to cache"""
        if not self.enabled:
            return
            
        cache_key = self._get_cache_key(img_path, feature_type)
        
        # Add to memory cache (with LRU eviction if needed)
        if len(self.memory_cache) >= self.max_items:
            # Find least recently used item
            oldest_key = min(self.cache_info, key=self.cache_info.get)
            # Remove it from memory cache and cache info
            del self.memory_cache[oldest_key]
            del self.cache_info[oldest_key]
            
        # Add to memory cache
        self.memory_cache[cache_key] = features
        self.cache_info[cache_key] = time.time()
        
        # Save to disk
        try:
            cache_path = self._get_cache_path(cache_key)
            with open(cache_path, 'wb') as f:
                pickle.dump(features, f)
        except Exception as e:
            logger.warning(f"Error saving cache for {img_path}: {e}")


class EnhancedImageMatcher:
    """
    Class that combines multiple image matching techniques for better accuracy
    """
    
    def __init__(self, config: Optional[configparser.ConfigParser] = None, use_gpu: bool = False):
        """
        Initialize the matcher with necessary models and parameters.
        
        Args:
            config: ConfigParser instance with settings (optional)
            use_gpu: Whether to use GPU for TensorFlow operations
        """
        # Load config if not provided
        if config is None:
            self.settings = SETTINGS
        else:
            self.settings = _get_config_values(config) or SETTINGS
        
        # Configure TensorFlow for GPU if needed
        if not use_gpu:
            try:
                tf.config.set_visible_devices([], 'GPU')
                logger.info("GPU disabled for TensorFlow operations")
            except Exception as e:
                logger.warning(f"Could not configure GPU settings: {e}")
        
        # Initialize OpenCV feature detectors
        self.sift = cv2.SIFT_create()
        self.akaze = cv2.AKAZE_create()
        
        # Initialize matchers
        FLANN_INDEX_KDTREE = 1
        index_params = dict(algorithm=FLANN_INDEX_KDTREE, trees=5)
        search_params = dict(checks=50)
        self.flann = cv2.FlannBasedMatcher(index_params, search_params)
        self.bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
        
        # Load EfficientNetB0 model
        logger.info("Loading EfficientNetB0 model...")
        self.efficientnet_model = None
        try:
            base_model = tf.keras.applications.EfficientNetB0(
                weights='imagenet', 
                include_top=False,
                input_shape=(DEFAULT_IMG_SIZE[0], DEFAULT_IMG_SIZE[1], 3)
            )
            global_avg_layer = tf.keras.layers.GlobalAveragePooling2D()(base_model.output)
            self.efficientnet_model = tf.keras.Model(inputs=base_model.input, outputs=global_avg_layer)
            logger.info("EfficientNetB0 model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load EfficientNetB0 model: {e}")

        # Initialize feature cache
        self.feature_cache = FeatureCache(
            cache_dir=self.settings['FEATURE_CACHE_DIR'],
            max_items=self.settings['MAX_CACHE_ITEMS'],
            enabled=self.settings['CACHE_FEATURES']
        )
        
        # Create cache directory if needed
        if self.settings['CACHE_FEATURES']:
            os.makedirs(self.settings['FEATURE_CACHE_DIR'], exist_ok=True)
            logger.info(f"Created/verified feature cache directory: {self.settings['FEATURE_CACHE_DIR']}")

    def _load_and_prepare_image(self, image_path: str) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Load image from path and prepare for both CV and deep learning
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Tuple of (OpenCV image, TensorFlow preprocessed image) or (None, None) if failed
        """
        if not os.path.exists(image_path):
            logger.warning(f"Image file not found: {image_path}")
            return None, None
            
        try:
            # Load for OpenCV processing (grayscale)
            cv_image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if cv_image is None:
                raise ValueError(f"OpenCV could not read image: {image_path}")
                
            # Load for TensorFlow processing (color)
            tf_img = tf.keras.preprocessing.image.load_img(
                image_path, 
                target_size=DEFAULT_IMG_SIZE, 
                color_mode='rgb'
            )
            tf_array = tf.keras.preprocessing.image.img_to_array(tf_img)
            tf_batch = tf.expand_dims(tf_array, 0)
            tf_preprocessed = tf.keras.applications.efficientnet.preprocess_input(tf_batch)
            
            return cv_image, tf_preprocessed
        except Exception as e:
            logger.error(f"Error loading image {image_path}: {e}")
            return None, None

    def _remove_background(self, img_path: str) -> Optional[str]:
        """
        Remove background from image if requested
        
        Args:
            img_path: Path to the image
            
        Returns:
            Path to the processed image, or original path if not processed
        """
        if not self.settings['USE_BACKGROUND_REMOVAL']:
            return img_path
            
        try:
            # Import rembg here to not require it if not used
            from rembg import remove, new_session
            
            # Create output path
            output_path = os.path.join(os.path.dirname(img_path), 
                                     f"nobg_{os.path.basename(img_path)}")
            output_path = output_path.replace('.jpg', '.png').replace('.jpeg', '.png')
            
            # Process image if output doesn't already exist
            if not os.path.exists(output_path):
                # Read image
                with open(img_path, 'rb') as f:
                    img_data = f.read()
                
                # Remove background
                session = new_session("u2net")
                output_data = remove(img_data, session=session)
                
                # Save result
                with open(output_path, 'wb') as f:
                    f.write(output_data)
                    
                logger.info(f"Background removed from {img_path}, saved to {output_path}")
            
            return output_path
        except Exception as e:
            logger.warning(f"Error removing background from {img_path}: {e}")
            return img_path

    def calculate_sift_similarity(self, img_path1: str, img_path2: str) -> float:
        """
        Calculate SIFT feature similarity between two images
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        # Check if result is in cache
        cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "sift_similarity")
        if cached_result is not None:
            return float(cached_result)
        
        # Remove background if requested
        if self.settings['USE_BACKGROUND_REMOVAL']:
            img_path1 = self._remove_background(img_path1)
            img_path2 = self._remove_background(img_path2)
        
        # Load images
        img1, _ = self._load_and_prepare_image(img_path1)
        img2, _ = self._load_and_prepare_image(img_path2)
        if img1 is None or img2 is None:
            return 0.0
            
        # Extract SIFT keypoints and descriptors
        try:
            kp1, des1 = self.sift.detectAndCompute(img1, None)
            kp2, des2 = self.sift.detectAndCompute(img2, None)
            
            if des1 is None or des2 is None or len(des1) < 2 or len(des2) < 2:
                logger.debug(f"Not enough SIFT features for {img_path1} or {img_path2}")
                return 0.0
                
            # Match features
            matches = self.flann.knnMatch(des1, des2, k=2)
            
            # Apply Lowe's ratio test to get good matches
            good_matches = []
            for match_pair in matches:
                if len(match_pair) == 2:
                    m, n = match_pair
                    if m.distance < self.settings['SIFT_RATIO_THRESHOLD'] * n.distance:
                        good_matches.append(m)
            
            # Extract match points for homography
            if len(good_matches) > self.settings['FEATURE_MATCH_THRESHOLD']:
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography with RANSAC
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
                inliers = np.sum(mask) if mask is not None else 0
                
                # Calculate similarity based on ratio of inliers to all good matches
                if len(good_matches) > 0:
                    similarity = inliers / len(good_matches)
                    
                    # Cache result
                    self.feature_cache.put(f"{img_path1}|{img_path2}", "sift_similarity", np.array([similarity]))
                    
                    return float(similarity)
            
            return 0.0
        except Exception as e:
            logger.error(f"Error in SIFT similarity calculation between {img_path1} and {img_path2}: {e}")
            return 0.0
            
    def calculate_akaze_similarity(self, img_path1: str, img_path2: str) -> float:
        """
        Calculate AKAZE feature similarity between two images
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        # Check if result is in cache
        cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "akaze_similarity")
        if cached_result is not None:
            return float(cached_result)
        
        # Remove background if requested
        if self.settings['USE_BACKGROUND_REMOVAL']:
            img_path1 = self._remove_background(img_path1)
            img_path2 = self._remove_background(img_path2)
        
        # Load images
        img1, _ = self._load_and_prepare_image(img_path1)
        img2, _ = self._load_and_prepare_image(img_path2)
        if img1 is None or img2 is None:
            return 0.0
            
        # Extract AKAZE keypoints and descriptors
        try:
            kp1, des1 = self.akaze.detectAndCompute(img1, None)
            kp2, des2 = self.akaze.detectAndCompute(img2, None)
            
            if des1 is None or des2 is None or len(des1) < 2 or len(des2) < 2:
                logger.debug(f"Not enough AKAZE features for {img_path1} or {img_path2}")
                return 0.0
                
            # Match features with Brute Force matcher
            matches = self.bf.match(des1, des2)
            
            # Sort by distance
            matches = sorted(matches, key=lambda x: x.distance)
            
            # Count good matches (low distance)
            good_matches = [m for m in matches if m.distance < self.settings['AKAZE_DISTANCE_THRESHOLD']]
            
            if len(matches) > 0:
                # Calculate similarity based on number and quality of matches
                num_good_matches = len(good_matches)
                avg_distance = np.mean([m.distance for m in matches[:min(len(matches), 30)]]) if len(matches) > 0 else float('inf')
                
                # Normalize average distance (lower is better)
                norm_dist = max(0, 1 - (avg_distance / 100))
                
                # Combine metrics
                if num_good_matches > self.settings['FEATURE_MATCH_THRESHOLD']:
                    similarity = 0.5 + (0.5 * norm_dist)
                elif num_good_matches > 0:
                    similarity = 0.3 * (num_good_matches / self.settings['FEATURE_MATCH_THRESHOLD']) + (0.2 * norm_dist)
                else:
                    similarity = 0.0
                    
                # Cache result
                self.feature_cache.put(f"{img_path1}|{img_path2}", "akaze_similarity", np.array([similarity]))
                    
                return float(similarity)
            
            return 0.0
        except Exception as e:
            logger.error(f"Error in AKAZE similarity calculation between {img_path1} and {img_path2}: {e}")
            return 0.0

    def calculate_deep_similarity(self, img_path1: str, img_path2: str) -> float:
        """
        Calculate deep feature similarity using EfficientNetB0
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            
        Returns:
            Cosine similarity between feature vectors (0.0 to 1.0)
        """
        # Check if result is in cache
        cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "deep_similarity")
        if cached_result is not None:
            return float(cached_result)
        
        # Remove background if requested
        if self.settings['USE_BACKGROUND_REMOVAL']:
            img_path1 = self._remove_background(img_path1)
            img_path2 = self._remove_background(img_path2)
        
        if self.efficientnet_model is None:
            logger.warning("Deep learning model not loaded, cannot calculate deep similarity")
            return 0.0
        
        # Load and prepare images
        _, tf_image1 = self._load_and_prepare_image(img_path1)
        _, tf_image2 = self._load_and_prepare_image(img_path2)
        
        if tf_image1 is None or tf_image2 is None:
            return 0.0
            
        try:
            # Check if features are cached individually
            features1 = self.feature_cache.get(img_path1, "deep_features")
            if features1 is None:
                # Extract features
                features1 = self.efficientnet_model(tf_image1).numpy().flatten()
                # Cache features
                self.feature_cache.put(img_path1, "deep_features", features1)
                
            features2 = self.feature_cache.get(img_path2, "deep_features")
            if features2 is None:
                # Extract features
                features2 = self.efficientnet_model(tf_image2).numpy().flatten()
                # Cache features
                self.feature_cache.put(img_path2, "deep_features", features2)
            
            # Calculate cosine similarity
            norm1 = np.linalg.norm(features1)
            norm2 = np.linalg.norm(features2)
            
            if norm1 == 0 or norm2 == 0:
                logger.warning(f"Zero norm detected in deep features: {img_path1}, {img_path2}")
                return 0.0
                
            similarity = np.dot(features1, features2) / (norm1 * norm2)
            
            # Clip to valid range
            similarity = float(max(0.0, min(1.0, similarity)))
            
            # Cache result
            self.feature_cache.put(f"{img_path1}|{img_path2}", "deep_similarity", np.array([similarity]))
            
            return similarity
        except Exception as e:
            logger.error(f"Error calculating deep similarity between {img_path1} and {img_path2}: {e}")
            return 0.0
            
    def calculate_combined_similarity(self, img_path1: str, img_path2: str, 
                                      weights: Optional[Dict[str, float]] = None) -> Tuple[float, Dict[str, float]]:
        """
        Calculate combined similarity score using all methods
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            weights: Optional dictionary with weights for each method
                     (default: values from config)
            
        Returns:
            Tuple of (combined similarity score, individual scores dictionary)
        """
        # Use weights from config if not provided
        if weights is None:
            weights = self.settings['WEIGHTS']
            
        # Check if files exist
        if not os.path.exists(img_path1) or not os.path.exists(img_path2):
            logger.warning(f"One or both image paths don't exist: {img_path1}, {img_path2}")
            return 0.0, {'sift': 0.0, 'akaze': 0.0, 'deep': 0.0}
            
        # Calculate individual similarities
        start_time = time.time()
        
        sift_similarity = self.calculate_sift_similarity(img_path1, img_path2)
        akaze_similarity = self.calculate_akaze_similarity(img_path1, img_path2)
        deep_similarity = self.calculate_deep_similarity(img_path1, img_path2)
        
        # Record individual scores
        scores = {
            'sift': sift_similarity,
            'akaze': akaze_similarity,
            'deep': deep_similarity
        }
        
        # Calculate weighted combination
        combined = (
            sift_similarity * weights['sift'] +
            akaze_similarity * weights['akaze'] +
            deep_similarity * weights['deep']
        )
        
        logger.debug(
            f"Image similarity for {os.path.basename(img_path1)} and {os.path.basename(img_path2)}: "
            f"SIFT={sift_similarity:.4f}, AKAZE={akaze_similarity:.4f}, Deep={deep_similarity:.4f}, "
            f"Combined={combined:.4f} (took {time.time() - start_time:.2f}s)"
        )
        
        return combined, scores
    
    def is_match(self, img_path1: str, img_path2: str, threshold: Optional[float] = None) -> Tuple[bool, float, Dict[str, float]]:
        """
        Determine if two images match based on combined similarity
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            threshold: Optional custom threshold (default: from config)
            
        Returns:
            Tuple of (is_match, similarity_score, individual_scores_dict)
        """
        if threshold is None:
            threshold = self.settings['COMBINED_THRESHOLD']
            
        combined, scores = self.calculate_combined_similarity(img_path1, img_path2)
        is_match = combined >= threshold
        
        return is_match, combined, scores

    def clear_cache(self):
        """Clear the feature cache to free memory"""
        self.feature_cache = FeatureCache()
        logger.info("Feature cache cleared")


# Helper function to get the common file extension
def get_file_extension(path: str) -> str:
    """Extract file extension from path or URL"""
    parsed = urlparse(path)
    _, ext = os.path.splitext(parsed.path)
    return ext.lower() or '.jpg'
    

# Function to match product images with batch processing capability
def match_product_images(haoreum_paths: List[str], 
                        kogift_paths: List[str],
                        threshold: float = None,
                        custom_weights: Optional[Dict[str, float]] = None) -> List[Dict]:
    """
    Match Haoreum product images with Kogift product images
    
    Args:
        haoreum_paths: List of paths to Haoreum product images
        kogift_paths: List of paths to Kogift product images
        threshold: Similarity threshold for matches (default: from config)
        custom_weights: Optional custom weights for similarity calculations
        
    Returns:
        List of dictionaries with match results
    """
    # Use threshold from config if not provided
    if threshold is None:
        threshold = SETTINGS['COMBINED_THRESHOLD']
    
    # Use custom_weights from config if not provided
    if custom_weights is None:
        custom_weights = SETTINGS['WEIGHTS']
    
    matcher = EnhancedImageMatcher()
    results = []
    
    # Check all inputs exist
    haoreum_valid = [p for p in haoreum_paths if os.path.exists(p)]
    kogift_valid = [p for p in kogift_paths if os.path.exists(p)]
    
    if len(haoreum_valid) == 0 or len(kogift_valid) == 0:
        logger.warning(f"No valid images found: Haoreum {len(haoreum_valid)}/{len(haoreum_paths)}, "
                      f"Kogift {len(kogift_valid)}/{len(kogift_paths)}")
        return results
        
    logger.info(f"Matching {len(haoreum_valid)} Haoreum images with {len(kogift_valid)} Kogift images")
    
    # Process each Haoreum image
    for h_idx, haoreum_path in enumerate(haoreum_valid):
        best_match = None
        best_similarity = 0
        best_scores = {}
        
        # Find best matching Kogift image
        for kogift_path in kogift_valid:
            is_match, similarity, scores = matcher.is_match(haoreum_path, kogift_path, threshold)
            
            if is_match and similarity > best_similarity:
                best_match = kogift_path
                best_similarity = similarity
                best_scores = scores
                
        # Add result
        result = {
            'haoreum_image': haoreum_path,
            'kogift_image': best_match,
            'similarity': best_similarity,
            'scores': best_scores,
            'is_match': best_match is not None
        }
        results.append(result)
        
        # Log progress
        if (h_idx + 1) % 10 == 0 or h_idx == len(haoreum_valid) - 1:
            logger.info(f"Processed {h_idx + 1}/{len(haoreum_valid)} Haoreum images")
    
    # Clean up
    matcher.clear_cache()
    
    return results


# Test function to run independently
def main():
    """Test function to demonstrate the enhanced image matcher"""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Enhanced Image Matcher Test")
    parser.add_argument("--haoreum", type=str, help="Path to Haoreum images directory")
    parser.add_argument("--kogift", type=str, help="Path to Kogift images directory")
    parser.add_argument("--output", type=str, default="match_results.json", help="Output JSON file path")
    parser.add_argument("--threshold", type=float, help="Match threshold (default: from config)")
    args = parser.parse_args()
    
    # Use directories from config if not provided
    if args.haoreum is None:
        args.haoreum = CONFIG.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    
    if args.kogift is None:
        args.kogift = CONFIG.get('Matching', 'images_dir', fallback='C:\\RPA\\Image\\Target')
    
    # Get image lists
    haoreum_images = [os.path.join(args.haoreum, f) for f in os.listdir(args.haoreum) 
                     if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif'))]
    kogift_images = [os.path.join(args.kogift, f) for f in os.listdir(args.kogift) 
                    if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif'))]
    
    # Run matching
    results = match_product_images(haoreum_images, kogift_images, threshold=args.threshold)
    
    # Convert to serializable format
    output_results = []
    for r in results:
        if r['is_match']:
            output_results.append({
                'haoreum_image': os.path.basename(r['haoreum_image']),
                'kogift_image': os.path.basename(r['kogift_image']),
                'similarity': r['similarity'],
                'sift_score': r['scores']['sift'],
                'akaze_score': r['scores']['akaze'],
                'deep_score': r['scores']['deep'],
            })
    
    # Save results
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(output_results, f, indent=2, ensure_ascii=False)
    
    # Print summary
    matched = len([r for r in results if r['is_match']])
    print(f"Results: {matched}/{len(results)} images matched")
    print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main() 