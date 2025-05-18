"""
Enhanced Image Matcher - Combines traditional CV techniques with deep learning

This module provides improved image matching capabilities by combining:
1. SIFT (Scale-Invariant Feature Transform) for local feature matching
2. AKAZE (Accelerated-KAZE) for handling non-linear transformations
3. EfficientNetB0 for deep feature extraction and similarity
4. ORB (Oriented FAST and Rotated BRIEF) for additional feature matching
5. RANSAC-based homography for geometric verification

The combined approach provides better accuracy for product image matching.
"""

import os
# Set TensorFlow GPU memory growth before importing TensorFlow
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'

import cv2
import numpy as np
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
import pandas as pd

# Import TensorFlow for deep learning features
try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    logging.warning("TensorFlow not available. Deep learning features will be disabled.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default Constants - Optimized for accuracy
DEFAULT_IMG_SIZE = (299, 299)  # Larger size for better feature extraction
DEFAULT_FEATURE_MATCH_THRESHOLD = 10  # Minimum matches needed
DEFAULT_SIFT_RATIO_THRESHOLD = 0.80  # More strict (was 0.85)
DEFAULT_AKAZE_DISTANCE_THRESHOLD = 45  # More strict (was 60)
DEFAULT_COMBINED_THRESHOLD = 0.05  # More balanced threshold to prevent very poor matches
DEFAULT_WEIGHTS = {'sift': 0.40, 'akaze': 0.30, 'deep': 0.20, 'orb': 0.10}  # Reweighted to favor traditional CV methods
DEFAULT_CACHE_DIR = 'C:\\RPA\\Temp\\feature_cache'

# Enhanced parameters
DEFAULT_SIFT_FEATURES = 3500  # Higher number of SIFT features for better accuracy
DEFAULT_AKAZE_FEATURES = 3500  # Higher number of AKAZE features
DEFAULT_ORB_FEATURES = 3500    # Higher number of ORB features

# Add quality check parameters
DEFAULT_MIN_MATCH_COUNT = 8   # Increased minimum number of matches (was 10, then 5)
DEFAULT_INLIER_THRESHOLD = 3.5  # More strict RANSAC reprojection error threshold (was 5.0)

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
        # Get use_gpu from Matching section
        use_gpu = config.getboolean('Matching', 'use_gpu', fallback=False)
        logger.info(f"GPU usage setting from config: {use_gpu}")

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
                                      fallback=DEFAULT_WEIGHTS['deep']),
                'orb': config.getfloat('ImageMatching', 'orb_weight',
                                     fallback=DEFAULT_WEIGHTS['orb']),
            },
            'USE_BACKGROUND_REMOVAL': config.getboolean('ImageMatching', 'use_background_removal_before_matching', 
                                                       fallback=True),
            'CACHE_FEATURES': config.getboolean('ImageMatching', 'cache_extracted_features', 
                                              fallback=True),
            'MAX_CACHE_ITEMS': config.getint('ImageMatching', 'max_feature_cache_items', 
                                           fallback=1000),
            'FEATURE_CACHE_DIR': config.get('ImageMatching', 'feature_cache_dir', 
                                          fallback=DEFAULT_CACHE_DIR),
            'SIFT_FEATURES': config.getint('ImageMatching', 'sift_features',
                                        fallback=DEFAULT_SIFT_FEATURES),
            'AKAZE_FEATURES': config.getint('ImageMatching', 'akaze_features',
                                         fallback=DEFAULT_AKAZE_FEATURES),
            'ORB_FEATURES': config.getint('ImageMatching', 'orb_features',
                                       fallback=DEFAULT_ORB_FEATURES),
            'MIN_MATCH_COUNT': config.getint('ImageMatching', 'min_match_count',
                                          fallback=DEFAULT_MIN_MATCH_COUNT),
            'INLIER_THRESHOLD': config.getfloat('ImageMatching', 'inlier_threshold',
                                             fallback=DEFAULT_INLIER_THRESHOLD),
            'APPLY_CLAHE': config.getboolean('ImageMatching', 'apply_clahe',
                                          fallback=True),
            'USE_MULTIPLE_MODELS': config.getboolean('ImageMatching', 'use_multiple_models',
                                                  fallback=True),
            'USE_GPU': use_gpu
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
    'USE_BACKGROUND_REMOVAL': True,
    'CACHE_FEATURES': True,
    'MAX_CACHE_ITEMS': 1000,
    'FEATURE_CACHE_DIR': DEFAULT_CACHE_DIR,
    'SIFT_FEATURES': DEFAULT_SIFT_FEATURES,
    'AKAZE_FEATURES': DEFAULT_AKAZE_FEATURES,
    'ORB_FEATURES': DEFAULT_ORB_FEATURES,
    'MIN_MATCH_COUNT': DEFAULT_MIN_MATCH_COUNT,
    'INLIER_THRESHOLD': DEFAULT_INLIER_THRESHOLD,
    'APPLY_CLAHE': True,
    'USE_MULTIPLE_MODELS': True,
    'USE_GPU': False  # Default to False if not specified
}

def _is_gpu_available():
    """Check if GPU is truly available for TensorFlow to use"""
    try:
        # Check using nvidia-smi first
        import subprocess
        try:
            nvidia_smi = subprocess.run(["nvidia-smi"], capture_output=True, text=True, check=False)
            if nvidia_smi.returncode == 0:
                logger.info("GPU detected via nvidia-smi")
                return True
        except (FileNotFoundError, subprocess.SubprocessError):
            logger.debug("nvidia-smi check failed, trying other methods")
        
        # Try importing CUDA runtime directly
        try:
            from ctypes import cdll
            cdll.LoadLibrary("nvcuda.dll")
            logger.info("CUDA runtime is accessible")
            return True
        except:
            logger.debug("Could not load CUDA runtime directly")
            
        # Last resort: check environment
        cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        if cuda_visible and cuda_visible != "-1":
            logger.info(f"CUDA_VISIBLE_DEVICES is set to: {cuda_visible}")
            return True
            
        return False
    except Exception as e:
        logger.warning(f"Error checking GPU availability: {e}")
        return False

def check_gpu_status():
    """Standalone utility function to check GPU status"""
    try:
        logger.info("=== GPU Status Check ===")
        
        # Check environment variables
        cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        logger.info(f"CUDA_VISIBLE_DEVICES: {cuda_visible}")
        
        # Try nvidia-smi
        try:
            import subprocess
            nvidia_smi = subprocess.run(["nvidia-smi"], capture_output=True, text=True, check=False)
            if nvidia_smi.returncode == 0:
                logger.info("nvidia-smi available and returned success")
                # Print the first line (driver version)
                if nvidia_smi.stdout:
                    first_line = nvidia_smi.stdout.splitlines()[0] if nvidia_smi.stdout.splitlines() else "No output"
                    logger.info(f"nvidia-smi first line: {first_line}")
            else:
                logger.warning("nvidia-smi command failed")
        except Exception as e:
            logger.warning(f"Error running nvidia-smi: {e}")
        
        # Check CUDA_PATH
        cuda_path = os.environ.get("CUDA_PATH", "Not set")
        logger.info(f"CUDA_PATH: {cuda_path}")
        
        logger.info("=== End GPU Status Check ===")
        return True
    except Exception as e:
        logger.error(f"Error checking GPU status: {e}")
        return False

# Run a GPU status check before configuring TensorFlow
gpu_check_result = check_gpu_status()

# Configure TensorFlow based on the use_gpu setting
use_gpu = SETTINGS.get('USE_GPU', False)

if use_gpu:
    logger.info("Configuring TensorFlow to use GPU")
    
    # Check if GPU is actually available
    gpu_available = _is_gpu_available()
    
    if gpu_available:
        logger.info("GPU appears to be available, enabling for TensorFlow")
        # Allow TensorFlow to use the GPU - don't set any restrictions
        if "CUDA_VISIBLE_DEVICES" in os.environ:
            del os.environ["CUDA_VISIBLE_DEVICES"]
    else:
        logger.warning("GPU was requested but no CUDA-capable device was detected")
        logger.warning("Falling back to CPU mode")
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
        # Update setting
        SETTINGS['USE_GPU'] = False
else:
    logger.info("Configuring TensorFlow to use CPU only (by configuration)")
    # Force TensorFlow to use CPU only
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1'

# Complete GPU status check after TensorFlow initialization
def check_tf_gpu_status():
    """Check GPU status after TensorFlow has been imported"""
    try:
        logger.info("=== TensorFlow GPU Status ===")
        
        # List physical GPUs
        gpus = tf.config.list_physical_devices('GPU')
        logger.info(f"Physical GPUs detected by TensorFlow: {len(gpus)}")
        for i, gpu in enumerate(gpus):
            logger.info(f"  GPU {i}: {gpu}")
            
        # List logical GPUs
        logical_gpus = tf.config.list_logical_devices('GPU')
        logger.info(f"Logical GPUs available: {len(logical_gpus)}")
        
        # Run a simple GPU test if available
        if len(gpus) > 0:
            try:
                with tf.device('/GPU:0'):
                    a = tf.constant([[1.0, 2.0], [3.0, 4.0]])
                    b = tf.constant([[1.0, 1.0], [1.0, 1.0]])
                    c = tf.matmul(a, b)
                    logger.info(f"Simple GPU test succeeded: {c.numpy()}")
            except Exception as e:
                logger.warning(f"Simple GPU test failed: {e}")
                
        logger.info("=== End TensorFlow GPU Status ===")
        return len(gpus) > 0
    except Exception as e:
        logger.error(f"Error checking TensorFlow GPU status: {e}")
        return False

# Run the TensorFlow GPU status check
tf_gpu_available = check_tf_gpu_status()

# Update the global settings if needed
if tf_gpu_available and not SETTINGS.get('USE_GPU', False):
    logger.info("GPU is available but was disabled in settings - consider enabling it")
elif not tf_gpu_available and SETTINGS.get('USE_GPU', False):
    logger.warning("GPU was enabled in settings but is not available - updating setting")
    SETTINGS['USE_GPU'] = False

# Check if GPU is available (compatible with all TF versions)
try:
    gpu_available_tf = len(tf.config.list_physical_devices('GPU')) > 0
    logger.info(f"TensorFlow GPU available: {gpu_available_tf}")
except Exception as e:
    logger.warning(f"Error checking TensorFlow GPU availability: {e}")
    try:
        # Fallback for older TF versions
        gpu_available_tf = tf.test.is_gpu_available()
        logger.info(f"TensorFlow GPU available (legacy check): {gpu_available_tf}")
    except Exception as e2:
        logger.warning(f"Error in legacy GPU check: {e2}")
        gpu_available_tf = False

# Log GPU devices after TensorFlow initialization
try:
    gpus = tf.config.list_physical_devices('GPU')
    logger.info(f"{len(gpus)} physical GPU devices visible to TensorFlow")
    
    # Apply memory growth for GPUs if available
    if len(gpus) > 0 and use_gpu:
        for gpu in gpus:
            try:
                tf.config.experimental.set_memory_growth(gpu, True)
                logger.info(f"Set memory growth for GPU: {gpu}")
            except Exception as e:
                logger.warning(f"Could not set memory growth for GPU: {e}")
    
    logical_gpus = tf.config.list_logical_devices('GPU')
    logger.info(f"{len(logical_gpus)} logical GPUs available after setting visible devices.")
except Exception as e:
    logger.error(f"Error checking GPU devices: {e}")
    logger.info("Continuing with CPU mode")

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
    
    def __init__(self, config: Optional[configparser.ConfigParser] = None, use_gpu: bool = None):
        """
        Initialize the matcher with necessary models and parameters.
        
        Args:
            config: ConfigParser instance with settings (optional)
            use_gpu: Whether to use GPU for TensorFlow operations (if None, use config setting)
        """
        self.config = config
        
        # Load configuration values
        if config:
            global SETTINGS
            new_settings = _get_config_values(config)
            if new_settings:
                SETTINGS = new_settings
                logger.debug("Updated settings from config")
                
        # Set up properties
        # If use_gpu is explicitly provided, use that value
        # Otherwise use the value from SETTINGS
        if use_gpu is None:
            self.use_gpu = SETTINGS.get('USE_GPU', False)
        else:
            self.use_gpu = use_gpu
            
        # Log GPU usage setting
        logger.info(f"EnhancedImageMatcher initialized with GPU usage set to: {self.use_gpu}")
        
        # Add USE_BACKGROUND_REMOVAL attribute 
        self.USE_BACKGROUND_REMOVAL = SETTINGS.get('USE_BACKGROUND_REMOVAL', True)
        
        self.sift_features = SETTINGS['SIFT_FEATURES']
        self.akaze_features = SETTINGS['AKAZE_FEATURES']
        self.orb_features = SETTINGS['ORB_FEATURES']
        self.min_match_count = SETTINGS['MIN_MATCH_COUNT']
        self.inlier_threshold = SETTINGS['INLIER_THRESHOLD']
        self.apply_clahe = SETTINGS['APPLY_CLAHE']
        self.use_multiple_models = SETTINGS['USE_MULTIPLE_MODELS']
        
        # Initialize feature cache
        self.feature_cache = FeatureCache()
        
        # Initialize OpenCV feature detectors
        self.sift = cv2.SIFT_create(nfeatures=self.sift_features)
        self.akaze = cv2.AKAZE_create()
        self.orb = cv2.ORB_create(nfeatures=self.orb_features)
        
        # Initialize matchers
        self.flann = cv2.FlannBasedMatcher(dict(algorithm=1, trees=5), dict(checks=50))
        self.bf_akaze = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=False)
        self.bf_orb = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=False)
        
        # Set up CLAHE
        if self.apply_clahe:
            self.clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        
        # Initialize deep learning models
        try:
            self._initialize_deep_models()
            
        except Exception as e:
            logger.error(f"Error initializing EnhancedImageMatcher: {e}")
            # Set minimal defaults to avoid method not found errors
            self.model = None
            self.models = []
            
        # Log initialization
        logger.info(f"Initialized EnhancedImageMatcher (GPU: {self.use_gpu}, SIFT features: {self.sift_features}, AKAZE features: {self.akaze_features}, ORB features: {self.orb_features})")
        
        # Verify that critical methods exist and are callable
        try:
            if not hasattr(self, 'calculate_similarity'):
                logger.error("calculate_similarity method missing, restoring default implementation")
                # Add default implementation if missing
                setattr(self, 'calculate_similarity', self._default_calculate_similarity)
        except Exception as attr_error:
            logger.error(f"Error verifying methods: {attr_error}")
            
    def _default_calculate_similarity(self, img_path1: str, img_path2: str, 
                                      weights: Optional[Dict[str, float]] = None) -> float:
        """Default implementation of calculate_similarity if missing"""
        logger.warning("Using default calculate_similarity implementation")
        try:
            # Calculate individual similarities
            sift_score = self.calculate_sift_similarity(img_path1, img_path2)
            akaze_score = self.calculate_akaze_similarity(img_path1, img_path2)
            
            # Simple average
            return (sift_score + akaze_score) / 2.0
        except Exception as e:
            logger.error(f"Error in default_calculate_similarity: {e}")
            return 0.0
    
    def _initialize_deep_models(self):
        """Initialize deep learning models for feature extraction with GPU if available"""
        try:
            # Check if TensorFlow is available
            if not TENSORFLOW_AVAILABLE:
                logger.warning("TensorFlow is not available. Deep learning features will be disabled.")
                self.model = None
                self.models = []
                return

            # Configure GPU usage
            if self.use_gpu:
                # Ensure the global settings are applied
                if not SETTINGS.get('USE_GPU', False):
                    logger.warning("Global GPU settings disabled but instance requested GPU")
                    logger.warning("Using global setting, disabling GPU for this instance")
                    self.use_gpu = False
                
            # Display status
            if self.use_gpu:
                logger.info("Attempting to configure GPU for deep models")
                # GPU configuration should already be set up globally
                gpus = tf.config.list_physical_devices('GPU')
                if gpus:
                    logger.info(f"Using GPU: {len(gpus)} physical devices available")
                else:
                    logger.warning("No GPU devices found despite GPU flag being set")
                    logger.info("Falling back to CPU mode for deep models")
                    self.use_gpu = False  # Fall back to CPU
            else:
                logger.info("GPU disabled for deep models by configuration")
                
            # Initialize primary model (EfficientNetB0)
            logger.info("Initializing EfficientNetB0 model...")
            self.model = tf.keras.applications.EfficientNetB0(
                include_top=False, 
                weights='imagenet',
                pooling='avg'
            )
            logger.info("EfficientNetB0 model initialized successfully")
            
            # Initialize additional models for ensemble if enabled
            self.models = []
            if self.use_multiple_models:
                # Add MobileNetV2 (faster but still good features)
                try:
                    logger.info("Initializing MobileNetV2 model...")
                    mobilenet = tf.keras.applications.MobileNetV2(
                        include_top=False,
                        weights='imagenet',
                        pooling='avg'
                    )
                    self.models.append(('mobilenet', mobilenet))
                    logger.info("MobileNetV2 model loaded successfully")
                except Exception as e:
                    logger.warning(f"Failed to load MobileNetV2: {e}")
                
                # Add ResNet50 (better accuracy, slower)
                try:
                    logger.info("Initializing ResNet50 model...")
                    resnet = tf.keras.applications.ResNet50(
                        include_top=False,
                        weights='imagenet',
                        pooling='avg'
                    )
                    self.models.append(('resnet', resnet))
                    logger.info("ResNet50 model loaded successfully")
                except Exception as e:
                    logger.warning(f"Failed to load ResNet50: {e}")
            
            # Standard preprocessing functions
            self.efficient_preprocess = tf.keras.applications.efficientnet.preprocess_input
            self.mobilenet_preprocess = tf.keras.applications.mobilenet_v2.preprocess_input
            self.resnet_preprocess = tf.keras.applications.resnet.preprocess_input
            
            logger.info(f"Deep learning models initialized. Using ensemble: {self.use_multiple_models}, GPU: {self.use_gpu}")
            
        except Exception as e:
            logger.error(f"Error initializing deep learning models: {e}")
            self.model = None
            self.models = []
    
    def _load_and_prepare_image(self, image_path: str) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Load and prepare an image for processing with preprocessing and contrast enhancement
        Returns color and grayscale versions
        Uses cv2.imdecode for robust Unicode path handling on Windows.
        """
        try:
            # Check if image exists
            if not os.path.exists(image_path):
                logger.warning(f"Image does not exist: {image_path}")
                return None, None
            
            # Handle problematic file extensions before reading
            _, file_ext = os.path.splitext(image_path)
            if file_ext.lower() in ['.asp', '.aspx', '.php', '.jsp']:
                # Create a copy with .jpg extension
                new_path = os.path.splitext(image_path)[0] + '.jpg'
                try:
                    if not os.path.exists(new_path):
                        import shutil
                        shutil.copy2(image_path, new_path)
                    logger.info(f"Copied {file_ext} file to .jpg for processing: {new_path}")
                    image_path = new_path # Use the new path for reading
                except Exception as copy_err:
                    logger.error(f"Error copying potentially problematic file {image_path} to {new_path}: {copy_err}")
                    # Proceed with original path, but decoding might fail

            # Read the file into a numpy array
            np_arr = np.fromfile(image_path, np.uint8)
            if np_arr.size == 0:
                logger.warning(f"Failed to read image file (empty): {image_path}")
                return None, None

            # Decode the image using cv2.imdecode
            img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            if img is None:
                logger.warning(f"Unable to decode image: {image_path}")
                return None, None
                
            # Get grayscale version
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Apply contrast enhancement if enabled
            if self.apply_clahe:
                # Make sure clahe is initialized
                if not hasattr(self, 'clahe') or self.clahe is None:
                    self.clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                    logger.debug("Initialized CLAHE within _load_and_prepare_image")
                gray = self.clahe.apply(gray)
            
            return img, gray
            
        except Exception as e:
            logger.error(f"Error loading image {image_path}: {e}")
            return None, None
                
    def calculate_sift_similarity(self, img_path1: str, img_path2: str) -> float:
        """Calculate SIFT feature similarity with more lenient geometric verification"""
        try:
            # Try to get from cache first
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "sift_similarity")
            if cached_result is not None:
                return cached_result
            
            # Load images
            _, gray1 = self._load_and_prepare_image(img_path1)
            _, gray2 = self._load_and_prepare_image(img_path2)
            
            if gray1 is None or gray2 is None:
                return 0.0
                
            # Extract keypoints and descriptors
            kp1, des1 = self.sift.detectAndCompute(gray1, None)
            kp2, des2 = self.sift.detectAndCompute(gray2, None)
            
            if des1 is None or des2 is None or len(des1) < 2 or len(des2) < 2:
                return 0.0
                
            # Match features
            matches = self.flann.knnMatch(des1, des2, k=2)
            
            # Store good matches using Lowe's ratio test with more lenient threshold
            good_matches = []
            for m, n in matches:
                if m.distance < SETTINGS['SIFT_RATIO_THRESHOLD'] * n.distance:
                    good_matches.append(m)
                    
            num_good_matches = len(good_matches)
            logger.debug(f"SIFT: {num_good_matches} good matches found")
            
            # Calculate normalized score
            max_possible_matches = min(len(kp1), len(kp2))
            match_score = len(good_matches) / max(1, max_possible_matches)
            
            # Apply geometric verification if we have enough matches
            # Reduced min_match_count to 5 (was previously self.min_match_count which is typically 10)
            inlier_score = 0.0
            if num_good_matches >= 5:
                # Get matched keypoints
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography matrix with higher ransacReprojThreshold
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 10.0)  # Increased from self.inlier_threshold (5.0)
                
                if H is not None:
                    # Count inliers
                    inliers = mask.ravel().sum()
                    inlier_score = inliers / max(1, num_good_matches)
                    logger.debug(f"SIFT: Homography inliers: {inliers}/{num_good_matches}")
                    
                    # Combine raw match score with inlier ratio with higher weight for raw matches
                    match_score = 0.6 * match_score + 0.4 * inlier_score  # Changed from 0.4/0.6 to 0.6/0.4
            
            # Normalize and scale the final score with higher multiplier
            final_score = min(1.0, match_score * 2.0)  # Increased from 1.5 to 2.0
            
            # Cache the result
            self.feature_cache.put(f"{img_path1}|{img_path2}", "sift_similarity", final_score)
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating SIFT similarity: {e}")
            return 0.0
    
    def calculate_akaze_similarity(self, img_path1: str, img_path2: str) -> float:
        """Calculate AKAZE feature similarity with more lenient geometric verification"""
        try:
            # Try to get from cache first
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "akaze_similarity")
            if cached_result is not None:
                return cached_result
            
            # Load images
            _, gray1 = self._load_and_prepare_image(img_path1)
            _, gray2 = self._load_and_prepare_image(img_path2)
            
            if gray1 is None or gray2 is None:
                return 0.0
                
            # Extract keypoints and descriptors
            kp1, des1 = self.akaze.detectAndCompute(gray1, None)
            kp2, des2 = self.akaze.detectAndCompute(gray2, None)
            
            if des1 is None or des2 is None or len(des1) < 2 or len(des2) < 2:
                return 0.0
                
            # Match features
            matches = self.bf_akaze.knnMatch(des1, des2, k=2)
            
            # Filter matches with more lenient threshold
            good_matches = []
            for match in matches:
                if len(match) == 2:
                    m, n = match
                    if m.distance < (SETTINGS['AKAZE_DISTANCE_THRESHOLD'] / 100.0) * n.distance:
                        good_matches.append(m)
                        
            num_good_matches = len(good_matches)
            logger.debug(f"AKAZE: {num_good_matches} good matches found")
            
            # Calculate base score
            max_possible_matches = min(len(kp1), len(kp2))
            match_score = len(good_matches) / max(1, max_possible_matches)
            
            # Apply geometric verification if we have enough matches
            # Reduced min_match_count to 5 (was previously self.min_match_count which is typically 10)
            inlier_score = 0.0
            if num_good_matches >= 5:
                # Get matched keypoints
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography matrix with higher ransacReprojThreshold
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 10.0)  # Increased from self.inlier_threshold (5.0)
                
                if H is not None:
                    # Count inliers
                    inliers = mask.ravel().sum()
                    inlier_score = inliers / max(1, num_good_matches)
                    logger.debug(f"AKAZE: Homography inliers: {inliers}/{num_good_matches}")
                    
                    # Combine raw match score with inlier ratio with higher weight for raw matches
                    match_score = 0.6 * match_score + 0.4 * inlier_score  # Changed from 0.4/0.6 to 0.6/0.4
            
            # Normalize and scale with higher multiplier
            final_score = min(1.0, match_score * 2.0)  # Increased from 1.5 to 2.0
            
            # Cache the result
            self.feature_cache.put(f"{img_path1}|{img_path2}", "akaze_similarity", final_score)
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating AKAZE similarity: {e}")
            return 0.0
            
    def calculate_orb_similarity(self, img_path1: str, img_path2: str) -> float:
        """Calculate ORB feature similarity with more lenient matching criteria"""
        try:
            # Try to get from cache first
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "orb_similarity")
            if cached_result is not None:
                return cached_result
            
            # Load images
            _, gray1 = self._load_and_prepare_image(img_path1)
            _, gray2 = self._load_and_prepare_image(img_path2)
            
            if gray1 is None or gray2 is None:
                return 0.0
                
            # Extract keypoints and descriptors
            kp1, des1 = self.orb.detectAndCompute(gray1, None)
            kp2, des2 = self.orb.detectAndCompute(gray2, None)
            
            if des1 is None or des2 is None or len(des1) < 2 or len(des2) < 2:
                return 0.0
                
            # Match features
            matches = self.bf_orb.knnMatch(des1, des2, k=2)
            
            # Filter matches with more lenient ratio
            good_matches = []
            for match in matches:
                if len(match) == 2:
                    m, n = match
                    if m.distance < 0.85 * n.distance:  # More lenient ratio for ORB (was 0.75)
                        good_matches.append(m)
                        
            num_good_matches = len(good_matches)
            logger.debug(f"ORB: {num_good_matches} good matches found")
            
            # Calculate base score
            max_possible_matches = min(len(kp1), len(kp2))
            match_score = len(good_matches) / max(1, max_possible_matches)
            
            # Apply geometric verification if we have enough matches (reduced threshold)
            inlier_score = 0.0
            if num_good_matches >= 5:  # Reduced from self.min_match_count (10)
                # Get matched keypoints
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography matrix with more lenient threshold
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 10.0)  # Increased from self.inlier_threshold
                
                if H is not None:
                    # Count inliers
                    inliers = mask.ravel().sum()
                    inlier_score = inliers / max(1, num_good_matches)
                    logger.debug(f"ORB: Homography inliers: {inliers}/{num_good_matches}")
                    
                    # Combine raw match score with inlier ratio with higher weight for raw matches
                    match_score = 0.6 * match_score + 0.4 * inlier_score  # Changed from 0.4/0.6 to 0.6/0.4
            
            # Normalize and scale with higher multiplier
            final_score = min(1.0, match_score * 2.0)  # Increased scaling factor from 1.5 to 2.0
            
            # Cache the result
            self.feature_cache.put(f"{img_path1}|{img_path2}", "orb_similarity", final_score)
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating ORB similarity: {e}")
            return 0.0
    
    def calculate_deep_similarity(self, img_path1: str, img_path2: str) -> float:
        """
        Calculate deep learning feature similarity using ensemble of models
        """
        try:
            # Check if TensorFlow is available
            if not TENSORFLOW_AVAILABLE:
                logger.warning("Deep similarity calculation skipped - TensorFlow not available")
                return 0.0
                
            # Try to get from cache first
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "deep_similarity")
            if cached_result is not None:
                return cached_result
                
            # Check if model is available
            if self.model is None:
                logger.warning("Deep model not available")
                return 0.0
                
            # Try to get cached features
            features1 = self.feature_cache.get(img_path1, "deep_features")
            features2 = self.feature_cache.get(img_path2, "deep_features")
            
            # Extract features if not in cache
            if features1 is None:
                features1 = self._extract_deep_features(img_path1)
                if features1 is not None:
                    self.feature_cache.put(img_path1, "deep_features", features1)
                    
            if features2 is None:
                features2 = self._extract_deep_features(img_path2)
                if features2 is not None:
                    self.feature_cache.put(img_path2, "deep_features", features2)
                    
            # Check if we got valid features
            if features1 is None or features2 is None:
                return 0.0
                
            # Calculate cosine similarity
            similarity = np.dot(features1['primary'], features2['primary']) / (
                np.linalg.norm(features1['primary']) * np.linalg.norm(features2['primary']))
                
            # If we have ensemble features, combine them
            ensemble_similarity = similarity
            if self.use_multiple_models and 'ensemble' in features1 and 'ensemble' in features2:
                for model_name in features1['ensemble'].keys():
                    if model_name in features2['ensemble']:
                        model_sim = np.dot(features1['ensemble'][model_name], features2['ensemble'][model_name]) / (
                            np.linalg.norm(features1['ensemble'][model_name]) * np.linalg.norm(features2['ensemble'][model_name]))
                        # Weight by model importance
                        if model_name == 'mobilenet':
                            model_weight = 0.3
                        elif model_name == 'resnet':
                            model_weight = 0.4
                        else:
                            model_weight = 0.2
                            
                        ensemble_similarity = ensemble_similarity * 0.7 + model_sim * 0.3
                
            # Cache final similarity
            self.feature_cache.put(f"{img_path1}|{img_path2}", "deep_similarity", float(ensemble_similarity))
            
            return float(ensemble_similarity)
        
        except Exception as e:
            logger.error(f"Error calculating deep similarity: {e}")
            return 0.0
            
    def _extract_deep_features(self, img_path: str) -> Optional[Dict[str, np.ndarray]]:
        """Extract deep features from image using ensemble of models"""
        try:
            # Check if TensorFlow is available
            if not TENSORFLOW_AVAILABLE:
                logger.warning("Deep feature extraction skipped - TensorFlow not available")
                return None
                
            # Check if model is available
            if self.model is None:
                logger.warning("Deep model not available for feature extraction")
                return None
            
            # Check if image exists
            if not os.path.exists(img_path):
                logger.warning(f"Image does not exist: {img_path}")
                return None
                
            # Handle problematic file extensions before reading
            _, file_ext = os.path.splitext(img_path)
            if file_ext.lower() in ['.asp', '.aspx', '.php', '.jsp']:
                # Create a copy with .jpg extension
                new_path = os.path.splitext(img_path)[0] + '.jpg'
                try:
                    if not os.path.exists(new_path):
                        import shutil
                        shutil.copy2(img_path, new_path)
                    logger.info(f"Copied {file_ext} file to .jpg for deep feature extraction: {new_path}")
                    img_path = new_path # Use the new path
                except Exception as copy_err:
                    logger.error(f"Error copying {img_path} to {new_path}: {copy_err}")
                
            # Load image using cv2.imdecode for robust path handling
            try:
                np_arr = np.fromfile(img_path, np.uint8)
                if np_arr.size == 0:
                    logger.warning(f"Failed to read image file for deep features (empty): {img_path}")
                    return None
                img_cv = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                if img_cv is None:
                    logger.warning(f"Unable to decode image for deep features: {img_path}")
                    return None
                # Convert BGR (cv2) to RGB (TensorFlow/PIL)
                img_rgb = cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB)
                # Resize using cv2 for consistency
                img_resized = cv2.resize(img_rgb, DEFAULT_IMG_SIZE, interpolation=cv2.INTER_AREA)
                x = np.expand_dims(img_resized, axis=0)
                x = self.efficient_preprocess(x)
            except Exception as img_err:
                logger.error(f"Error loading/preprocessing image {img_path} for deep features: {img_err}")
                return None
            
            # Extract primary features using EfficientNet
            features = self.model.predict(x, verbose=0)
            features = features.flatten()
            
            # Normalize features
            features = features / np.linalg.norm(features)
            
            result = {'primary': features}
            
            # Extract ensemble features if enabled
            if self.use_multiple_models and self.models:
                ensemble_features = {}
                for model_name, model in self.models:
                    try:
                        # Preprocess according to model
                        if model_name == 'mobilenet':
                            x_model = self.mobilenet_preprocess(np.expand_dims(img_resized, axis=0))
                        elif model_name == 'resnet':
                            x_model = self.resnet_preprocess(np.expand_dims(img_resized, axis=0))
                        else:
                            x_model = x
                            
                        # Extract features
                        model_features = model.predict(x_model, verbose=0).flatten()
                        model_features = model_features / np.linalg.norm(model_features)
                        ensemble_features[model_name] = model_features
                    except Exception as e:
                        logger.warning(f"Error extracting {model_name} features: {e}")
                
                result['ensemble'] = ensemble_features
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting deep features: {e}")
            return None
    
    def calculate_combined_similarity(self, img_path1: str, img_path2: str, 
                                      weights: Optional[Dict[str, float]] = None) -> Tuple[float, Dict[str, float]]:
        """
        Calculate combined similarity using all methods
        Returns the weighted score and individual scores
        
        Implements tiered confidence levels:
        - High confidence: score >= 0.6
        - Medium confidence: 0.3 <= score < 0.6
        - Low confidence: 0.1 <= score < 0.3
        - Very low confidence: score < 0.1
        """
        if not weights:
            weights = SETTINGS['WEIGHTS'].copy()  # Make a copy to avoid modifying the original
            
        # Calculate similarities
        sift_score = self.calculate_sift_similarity(img_path1, img_path2)
        akaze_score = self.calculate_akaze_similarity(img_path1, img_path2)
        orb_score = self.calculate_orb_similarity(img_path1, img_path2)
        
        # Only calculate deep score if TensorFlow is available
        if TENSORFLOW_AVAILABLE and self.model is not None:
            deep_score = self.calculate_deep_similarity(img_path1, img_path2)
        else:
            deep_score = 0.0
            # Adjust weights if deep features are not available
            if 'deep' in weights and weights['deep'] > 0:
                remaining_weight = weights.pop('deep')
                # Distribute the deep weight to other methods
                if sum(weights.values()) > 0:
                    for k in weights:
                        weights[k] += remaining_weight * (weights[k] / sum(weights.values()))
                else:
                    # If all other weights are 0, distribute evenly
                    remaining_keys = list(weights.keys())
                    for k in remaining_keys:
                        weights[k] = remaining_weight / len(remaining_keys)
        
        # Store individual scores
        scores = {
            'sift': sift_score,
            'akaze': akaze_score,
            'deep': deep_score,
            'orb': orb_score
        }
        
        # Calculate weighted sum
        if sum(weights.values()) == 0:
            logger.warning("All weights are zero, defaulting to equal weights")
            weights = {k: 1.0/len(weights) for k in weights}
            
        combined_score = 0.0
        weight_sum = 0.0
        
        for method, score in scores.items():
            if method in weights:
                combined_score += score * weights[method]
                weight_sum += weights[method]
                
        # Normalize by weight sum
        if weight_sum > 0:
            combined_score /= weight_sum
            
        # Determine confidence tier based on scores
        high_scores = sum(1 for score in scores.values() if score >= 0.6)
        medium_scores = sum(1 for score in scores.values() if 0.3 <= score < 0.6)
        low_scores = sum(1 for score in scores.values() if 0.1 <= score < 0.3)
        
        # Apply confidence tier-based boosts
        if high_scores >= 2:
            # At least two methods have high confidence - strong match
            combined_score = max(combined_score, 0.8 * max(scores.values()))
            logger.debug(f"High confidence match: {high_scores} methods with score >= 0.6")
        elif high_scores == 1 and medium_scores >= 1:
            # One high confidence and at least one medium confidence - good match
            combined_score = max(combined_score, 0.7 * max(scores.values()))
            logger.debug(f"Good confidence match: {high_scores} high, {medium_scores} medium methods")
        elif medium_scores >= 2:
            # At least two medium confidence methods - moderate match
            combined_score = max(combined_score, 0.6 * max(scores.values()))
            logger.debug(f"Moderate confidence match: {medium_scores} methods with score >= 0.3")
        elif medium_scores == 1 and low_scores >= 1:
            # One medium confidence and at least one low confidence - acceptable match
            combined_score = max(combined_score, 0.5 * max(scores.values()))
            logger.debug(f"Acceptable confidence match: {medium_scores} medium, {low_scores} low methods")
        elif low_scores >= 2:
            # Multiple low confidence methods - possible match, needs review
            combined_score = max(combined_score, 0.3 * max(scores.values()))
            logger.debug(f"Low confidence match: {low_scores} methods with score >= 0.1")
            
        # Increase confidence if SIFT and AKAZE agree strongly (traditional CV validation)
        if sift_score >= 0.4 and akaze_score >= 0.4:
            logger.debug(f"Strong agreement between SIFT and AKAZE: {sift_score:.2f}, {akaze_score:.2f}")
            combined_score = min(1.0, combined_score * 1.15)  # 15% boost when traditional methods agree
            
        # Calculate a consensus score based on standard deviation (lower std = higher consensus)
        non_zero_scores = [s for s in scores.values() if s > 0.01]
        if len(non_zero_scores) >= 2:
            score_std = np.std(non_zero_scores)
            consensus_factor = max(0, 1 - score_std)  # Higher when methods agree
            
            # Apply consensus boost (up to 10%)
            consensus_boost = min(0.1, consensus_factor * 0.2)
            combined_score = min(1.0, combined_score * (1 + consensus_boost))
            logger.debug(f"Consensus boost: {consensus_boost:.2f} (std={score_std:.2f})")
            
        logger.debug(f"Combined similarity: {combined_score:.4f} (SIFT={sift_score:.2f}, AKAZE={akaze_score:.2f}, Deep={deep_score:.2f}, ORB={orb_score:.2f})")
        
        return combined_score, scores
        
    def calculate_similarity(self, img_path1: str, img_path2: str, 
                           weights: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate combined similarity between two images
        This is a wrapper around calculate_combined_similarity that returns just the score
        """
        try:
            # Try to get from cache first
            cache_key = f"{img_path1}|{img_path2}"
            cached_result = self.feature_cache.get(cache_key, "combined_similarity")
            if cached_result is not None:
                return float(cached_result)
                
            # Calculate similarity
            combined_score, _ = self.calculate_combined_similarity(img_path1, img_path2, weights)
            
            # Cache the result
            self.feature_cache.put(cache_key, "combined_similarity", combined_score)
            
            return combined_score
        except Exception as e:
            logger.error(f"Error calculating similarity between {img_path1} and {img_path2}: {e}")
            return 0.0
            
    def is_match(self, img_path1: str, img_path2: str, threshold: Optional[float] = None) -> Tuple[bool, float, Dict[str, float]]:
        """
        Determine if two images match based on similarity score
        
        Args:
            img_path1: Path to first image
            img_path2: Path to second image
            threshold: Similarity threshold (default: from config)
            
        Returns:
            Tuple of (is_match, similarity_score, individual_scores)
        """
        if threshold is None:
            threshold = SETTINGS.get('COMBINED_THRESHOLD', 0.05)
            
        try:
            # Check if images exist
            if not os.path.exists(img_path1) or not os.path.exists(img_path2):
                if not os.path.exists(img_path1):
                    logger.warning(f"First image does not exist: {img_path1}")
                if not os.path.exists(img_path2):
                    logger.warning(f"Second image does not exist: {img_path2}")
                return False, 0.0, {'sift': 0.0, 'akaze': 0.0, 'deep': 0.0, 'orb': 0.0}
            
            # Calculate combined similarity
            similarity, scores = self.calculate_combined_similarity(img_path1, img_path2)
            
            # Determine match confidence level
            confidence_level = "Very Low"
            if similarity >= 0.60:
                confidence_level = "High"
            elif similarity >= 0.30:
                confidence_level = "Medium"
            elif similarity >= 0.10:
                confidence_level = "Low"
            
            # Determine if it's a match
            is_match = similarity >= threshold
            
            # Log match information for debugging with appropriate level based on confidence
            if similarity >= 0.60:
                logger.info(f"HIGH CONFIDENCE MATCH: {os.path.basename(img_path1)} and {os.path.basename(img_path2)} with score {similarity:.4f}")
                logger.info(f"  - SIFT: {scores['sift']:.3f}, AKAZE: {scores['akaze']:.3f}, Deep: {scores['deep']:.3f}, ORB: {scores['orb']:.3f}")
            elif similarity >= 0.30:
                logger.info(f"MEDIUM CONFIDENCE MATCH: {os.path.basename(img_path1)} and {os.path.basename(img_path2)} with score {similarity:.4f}")
            elif similarity >= 0.10:
                logger.debug(f"LOW CONFIDENCE MATCH: {os.path.basename(img_path1)} and {os.path.basename(img_path2)} with score {similarity:.4f}")
            elif similarity >= threshold:
                logger.debug(f"VERY LOW CONFIDENCE MATCH (above threshold): {os.path.basename(img_path1)} and {os.path.basename(img_path2)} with score {similarity:.4f}")
            
            # Store confidence level in the scores dictionary
            scores['confidence_level'] = confidence_level
            
            return is_match, similarity, scores
        except Exception as e:
            logger.error(f"Error determining match between {img_path1} and {img_path2}: {e}")
            return False, 0.0, {'sift': 0.0, 'akaze': 0.0, 'deep': 0.0, 'orb': 0.0, 'confidence_level': 'Error'}
            
    def clear_cache(self):
        """Clear the feature cache"""
        if hasattr(self, 'feature_cache'):
            logger.debug("Clearing feature cache")
            # This is a no-op if cache is disabled
            # Actual cache files will be cleaned up based on cache settings


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


def match_naver_product_images(haoreum_paths: List[str], 
                             naver_results: pd.DataFrame,
                             threshold: Optional[float] = None,
                             custom_weights: Optional[Dict[str, float]] = None) -> Dict[str, Dict]:
    """
    Match Haoreum product images with Naver product images from crawled results
    
    Args:
        haoreum_paths: List of paths to Haoreum product images
        naver_results: DataFrame containing Naver crawl results
        threshold: Optional similarity threshold (default: from config)
        custom_weights: Optional custom weights for similarity calculations
        
    Returns:
        Dictionary mapping product names to match results
    """
    if threshold is None:
        threshold = SETTINGS['COMBINED_THRESHOLD']
    
    if custom_weights is None:
        custom_weights = SETTINGS['WEIGHTS']
    
    matcher = EnhancedImageMatcher()
    results = {}
    
    # Validate Haoreum paths
    haoreum_valid = [p for p in haoreum_paths if os.path.exists(p)]
    if not haoreum_valid:
        logger.warning("No valid Haoreum images found")
        return results
    
    # Extract Naver image paths from results
    naver_images = []
    product_names = []
    missing_images = 0
    valid_images = 0
    
    for _, row in naver_results.iterrows():
        try:
            if isinstance(row.get('original_row'), dict):
                product_name = row['original_row'].get('상품명')
                # Check both 네이버 이미지 and 네이버쇼핑(이미지링크) columns
                image_path = None
                for col in ['네이버 이미지', '네이버쇼핑(이미지링크)', 'image_path']:
                    if col in row and row[col] and row[col] != '-':
                        image_path = row[col]
                        break
                
                # Also check in the original_row
                if not image_path and 'original_row' in row:
                    for col in ['네이버 이미지', '네이버쇼핑(이미지링크)', 'image_path']:
                        if col in row['original_row'] and row['original_row'][col] and row['original_row'][col] != '-':
                            image_path = row['original_row'][col]
                            break
                
                if not image_path:
                    logger.warning(f"No image path found for product: {product_name}")
                    missing_images += 1
                    continue
                
                # Handle image path which could be a dictionary or string
                if isinstance(image_path, dict):
                    # If it's a dictionary (as used by excel_utils.py), extract the local path
                    if 'local_path' in image_path and image_path['local_path']:
                        actual_path = image_path['local_path']
                    elif 'url' in image_path:
                        # No local path but has URL - try to see if we can find the local file
                        img_url = image_path['url']
                        source = image_path.get('source', 'naver')
                        # Try to find the file by URL hash
                        try:
                            url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
                            naver_dir = 'C:\\RPA\\Image\\Main\\Naver'
                            matching_files = []
                            
                            if os.path.exists(naver_dir):
                                for f in os.listdir(naver_dir):
                                    if url_hash in f and os.path.isfile(os.path.join(naver_dir, f)):
                                        matching_files.append(f)
                            
                            if matching_files:
                                # Prefer original jpg over _nobg version
                                matching_files.sort(key=lambda x: '_nobg' in x)
                                actual_path = os.path.join(naver_dir, matching_files[0])
                                logger.debug(f"Found local file for URL by hash: {actual_path}")
                            else:
                                # Try both Main and Target directories
                                alt_dirs = ['C:\\RPA\\Image\\Main\\Naver', 'C:\\RPA\\Image\\Target\\Naver']
                                found = False
                                
                                for alt_dir in alt_dirs:
                                    if os.path.exists(alt_dir):
                                        # Try to find by product name in filename
                                        product_words = product_name.split()
                                        if product_words:
                                            for f in os.listdir(alt_dir):
                                                # Use first word of product in filename (more unique)
                                                if product_words[0] in f and os.path.isfile(os.path.join(alt_dir, f)):
                                                    actual_path = os.path.join(alt_dir, f)
                                                    found = True
                                                    logger.debug(f"Found local file for URL by product name: {actual_path}")
                                                    break
                                    if found:
                                        break
                                
                                if not found:
                                    logger.warning(f"Image dictionary has URL but no local path: {img_url} for product: {product_name}")
                                    missing_images += 1
                                    continue
                        except Exception as e:
                            logger.error(f"Error finding local file for URL: {e}")
                            missing_images += 1
                            continue
                    else:
                        logger.warning(f"Invalid image dictionary format for product: {product_name}")
                        missing_images += 1
                        continue
                else:
                    # It's a regular string path
                    actual_path = image_path
                
                # Check if the image file exists
                if os.path.exists(actual_path):
                    naver_images.append(actual_path)
                    product_names.append(product_name)
                    valid_images += 1
                else:
                    # Try to find _nobg version
                    base_path, ext = os.path.splitext(actual_path)
                    nobg_path = f"{base_path}_nobg.png"
                    if os.path.exists(nobg_path):
                        naver_images.append(nobg_path)
                        product_names.append(product_name)
                        valid_images += 1
                        logger.debug(f"Using _nobg version instead: {nobg_path}")
                    else:
                        logger.warning(f"Image file does not exist (both original and _nobg): {actual_path} for product: {product_name}")
                        missing_images += 1
        except Exception as e:
            logger.error(f"Error processing row in naver_results: {e}")
            continue
    
    logger.info(f"Found {valid_images} valid Naver images, {missing_images} missing or invalid")
    
    if not naver_images:
        logger.warning("No valid Naver images found in results")
        return results
    
    logger.info(f"Matching {len(haoreum_valid)} Haoreum images with {len(naver_images)} Naver images")
    
    # Process each Haoreum image
    for h_idx, haoreum_path in enumerate(haoreum_valid):
        best_match = None
        best_similarity = 0
        best_scores = {}
        matched_product = None
        
        # Get product name from Haoreum path
        haoreum_product = os.path.splitext(os.path.basename(haoreum_path))[0]
        
        # Find best matching Naver image
        for naver_url, product_name in zip(naver_images, product_names):
            try:
                if not os.path.exists(naver_url):
                    logger.warning(f"Skipping non-existent Naver image: {naver_url}")
                    continue
                    
                is_match, similarity, scores = matcher.is_match(haoreum_path, naver_url, threshold)
                
                if is_match and similarity > best_similarity:
                    best_match = naver_url
                    best_similarity = similarity
                    best_scores = scores
                    matched_product = product_name
            except Exception as e:
                logger.error(f"Error matching {haoreum_path} with {naver_url}: {e}")
                continue
        
        # Store result
        results[haoreum_product] = {
            'haoreum_image': haoreum_path,
            'naver_image': best_match,
            'matched_product': matched_product,
            'similarity': best_similarity,
            'scores': best_scores,
            'is_match': best_match is not None
        }
        
        # Log progress
        if (h_idx + 1) % 10 == 0 or h_idx == len(haoreum_valid) - 1:
            logger.info(f"Processed {h_idx + 1}/{len(haoreum_valid)} Haoreum images")
    
    # Log match results summary
    matched_count = sum(1 for result in results.values() if result['is_match'])
    logger.info(f"Matched {matched_count}/{len(results)} Haoreum images with Naver images")
    
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