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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DEFAULT_IMG_SIZE = (224, 224)
FEATURE_MATCH_THRESHOLD = 10  # Minimum good matches for SIFT/AKAZE
SIFT_RATIO_THRESHOLD = 0.75   # Lowe's ratio test threshold
AKAZE_DISTANCE_THRESHOLD = 50 # Maximum distance for AKAZE matches
COMBINED_THRESHOLD = 0.65     # Threshold for combined similarity score

# Try to load config if available
config = configparser.ConfigParser()
try:
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
    config.read(config_path, encoding='utf-8')
    # Load thresholds from config if available
    FEATURE_MATCH_THRESHOLD = config.getint('ImageMatching', 'feature_match_threshold', fallback=FEATURE_MATCH_THRESHOLD)
    SIFT_RATIO_THRESHOLD = config.getfloat('ImageMatching', 'sift_ratio_threshold', fallback=SIFT_RATIO_THRESHOLD)
    AKAZE_DISTANCE_THRESHOLD = config.getint('ImageMatching', 'akaze_distance_threshold', fallback=AKAZE_DISTANCE_THRESHOLD)
    COMBINED_THRESHOLD = config.getfloat('ImageMatching', 'combined_threshold', fallback=COMBINED_THRESHOLD)
except Exception as e:
    logger.warning(f"Could not load config file. Using default values. Error: {e}")


class EnhancedImageMatcher:
    """
    Class that combines multiple image matching techniques for better accuracy
    """
    
    def __init__(self, use_gpu: bool = False):
        """
        Initialize the matcher with necessary models and parameters.
        
        Args:
            use_gpu: Whether to use GPU for TensorFlow operations
        """
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

        # Feature cache to avoid recomputation
        self.feature_cache = {}
        
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

    def _get_cached_deep_features(self, image_path: str) -> Optional[np.ndarray]:
        """
        Get deep features from cache or compute and cache them
        
        Args:
            image_path: Path to the image
            
        Returns:
            Feature vector or None if computation failed
        """
        # Check cache first
        cache_key = f"deep_{image_path}"
        if cache_key in self.feature_cache:
            return self.feature_cache[cache_key]
            
        # Load image and compute features
        _, tf_image = self._load_and_prepare_image(image_path)
        if tf_image is None or self.efficientnet_model is None:
            return None
            
        try:
            features = self.efficientnet_model(tf_image).numpy().flatten()
            self.feature_cache[cache_key] = features
            return features
        except Exception as e:
            logger.error(f"Error computing deep features for {image_path}: {e}")
            return None
            
    def calculate_sift_similarity(self, img_path1: str, img_path2: str) -> float:
        """
        Calculate SIFT feature similarity between two images
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
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
            for m, n in matches:
                if m.distance < SIFT_RATIO_THRESHOLD * n.distance:
                    good_matches.append(m)
            
            # Extract match points for homography
            if len(good_matches) > 4:
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography with RANSAC
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
                inliers = np.sum(mask) if mask is not None else 0
                
                # Calculate similarity based on ratio of inliers to all good matches
                if len(good_matches) > 0:
                    similarity = inliers / len(good_matches)
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
            good_matches = [m for m in matches if m.distance < AKAZE_DISTANCE_THRESHOLD]
            
            if len(matches) > 0:
                # Calculate similarity based on number and quality of matches
                num_good_matches = len(good_matches)
                avg_distance = np.mean([m.distance for m in matches[:min(len(matches), 30)]]) if len(matches) > 0 else float('inf')
                
                # Normalize average distance (lower is better)
                norm_dist = max(0, 1 - (avg_distance / 100))
                
                # Combine metrics
                if num_good_matches > FEATURE_MATCH_THRESHOLD:
                    similarity = 0.5 + (0.5 * norm_dist)
                elif num_good_matches > 0:
                    similarity = 0.3 * (num_good_matches / FEATURE_MATCH_THRESHOLD) + (0.2 * norm_dist)
                else:
                    similarity = 0.0
                    
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
        if self.efficientnet_model is None:
            logger.warning("Deep learning model not loaded, cannot calculate deep similarity")
            return 0.0
            
        # Get features (from cache or compute)
        features1 = self._get_cached_deep_features(img_path1)
        features2 = self._get_cached_deep_features(img_path2)
        
        if features1 is None or features2 is None:
            return 0.0
            
        try:
            # Calculate cosine similarity
            norm1 = np.linalg.norm(features1)
            norm2 = np.linalg.norm(features2)
            
            if norm1 == 0 or norm2 == 0:
                logger.warning(f"Zero norm detected in deep features: {img_path1}, {img_path2}")
                return 0.0
                
            similarity = np.dot(features1, features2) / (norm1 * norm2)
            # Clip to valid range
            return float(max(0.0, min(1.0, similarity)))
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
                     (default: {'sift': 0.3, 'akaze': 0.2, 'deep': 0.5})
            
        Returns:
            Tuple of (combined similarity score, individual scores dictionary)
        """
        # Default weights
        if weights is None:
            weights = {'sift': 0.3, 'akaze': 0.2, 'deep': 0.5}
            
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
            threshold: Optional custom threshold (default: COMBINED_THRESHOLD)
            
        Returns:
            Tuple of (is_match, similarity_score, individual_scores_dict)
        """
        if threshold is None:
            threshold = COMBINED_THRESHOLD
            
        combined, scores = self.calculate_combined_similarity(img_path1, img_path2)
        is_match = combined >= threshold
        
        return is_match, combined, scores

    def clear_cache(self):
        """Clear the feature cache to free memory"""
        self.feature_cache.clear()
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
                        threshold: float = COMBINED_THRESHOLD,
                        custom_weights: Optional[Dict[str, float]] = None) -> List[Dict]:
    """
    Match Haoreum product images with Kogift product images
    
    Args:
        haoreum_paths: List of paths to Haoreum product images
        kogift_paths: List of paths to Kogift product images
        threshold: Similarity threshold for matches
        custom_weights: Optional custom weights for similarity calculations
        
    Returns:
        List of dictionaries with match results
    """
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
    parser.add_argument("--haoreum", type=str, required=True, help="Path to Haoreum images directory")
    parser.add_argument("--kogift", type=str, required=True, help="Path to Kogift images directory")
    parser.add_argument("--output", type=str, default="match_results.json", help="Output JSON file path")
    parser.add_argument("--threshold", type=float, default=COMBINED_THRESHOLD, help="Match threshold")
    args = parser.parse_args()
    
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