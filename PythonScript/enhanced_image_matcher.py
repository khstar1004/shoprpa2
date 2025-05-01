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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default Constants - Optimized for accuracy
DEFAULT_IMG_SIZE = (299, 299)  # Larger size for better feature extraction
DEFAULT_FEATURE_MATCH_THRESHOLD = 15  # Increased from 10
DEFAULT_SIFT_RATIO_THRESHOLD = 0.70  # More strict (was 0.75)
DEFAULT_AKAZE_DISTANCE_THRESHOLD = 40  # More strict (was 50)
DEFAULT_COMBINED_THRESHOLD = 0.55  # Lower to allow more potential matches
DEFAULT_WEIGHTS = {'sift': 0.25, 'akaze': 0.20, 'deep': 0.40, 'orb': 0.15}  # New weights with ORB
DEFAULT_CACHE_DIR = 'C:\\RPA\\Temp\\feature_cache'

# Enhanced parameters
DEFAULT_SIFT_FEATURES = 2000  # Increased number of SIFT features
DEFAULT_AKAZE_FEATURES = 2000  # Increased number of AKAZE features
DEFAULT_ORB_FEATURES = 2000    # Number of ORB features

# Add quality check parameters
DEFAULT_MIN_MATCH_COUNT = 10   # Minimum number of matches for geometric verification
DEFAULT_INLIER_THRESHOLD = 5.0  # RANSAC reprojection error threshold

# Type hint for image input
ImageType = Union[str, Dict[str, Optional[str]]]

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

# Import TensorFlow after environment variable settings
import tensorflow as tf

# For debugging
logger.info(f"TensorFlow version: {tf.__version__}")

# Check for CUDA support in TensorFlow
logger.info(f"TensorFlow built with CUDA: {tf.test.is_built_with_cuda()}")

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
    
    def _get_local_path(self, img_input: ImageType) -> Optional[str]:
        """Extracts the local file path from either a string or a dict."""
        if isinstance(img_input, str):
            return img_input
        elif isinstance(img_input, dict) and 'local_path' in img_input and img_input['local_path']:
            return img_input['local_path']
        elif isinstance(img_input, dict):
            # Attempt to find path even if dict structure is slightly off
            for key in ['path', 'file', 'filePath']:
                 if key in img_input and img_input[key]:
                     return img_input[key]
            logger.warning(f"Could not find 'local_path' in image dictionary: {img_input}")
            return None
        else:
            logger.warning(f"Invalid image input type: {type(img_input)}")
            return None

    def _load_and_prepare_image(self, image_input: ImageType) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Load and prepare an image for processing from path or dict.
        Returns color and grayscale versions
        Uses cv2.imdecode for robust Unicode path handling on Windows.
        """
        image_path = self._get_local_path(image_input)
        if not image_path:
            return None, None

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
                
    def calculate_sift_similarity(self, img_input1: ImageType, img_input2: ImageType) -> float:
        """Calculate SIFT feature similarity with geometric verification"""
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            return 0.0
            
        try:
            # Try to get from cache first
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "sift_similarity")
            if cached_result is not None:
                return cached_result
            
            # Load images using paths
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
            
            # Store good matches using Lowe's ratio test
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
            inlier_score = 0.0
            if num_good_matches >= self.min_match_count:
                # Get matched keypoints
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography matrix
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, self.inlier_threshold)
                
                if H is not None:
                    # Count inliers
                    inliers = mask.ravel().sum()
                    inlier_score = inliers / max(1, num_good_matches)
                    logger.debug(f"SIFT: Homography inliers: {inliers}/{num_good_matches}")
                    
                    # Combine raw match score with inlier ratio
                    match_score = 0.4 * match_score + 0.6 * inlier_score
            
            # Normalize and scale the final score
            final_score = min(1.0, match_score * 1.5)  # Scale up to better differentiate
            
            # Cache the result using paths
            self.feature_cache.put(f"{img_path1}|{img_path2}", "sift_similarity", final_score)
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating SIFT similarity: {e}")
            return 0.0
    
    def calculate_akaze_similarity(self, img_input1: ImageType, img_input2: ImageType) -> float:
        """Calculate AKAZE feature similarity with geometric verification"""
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            return 0.0

        try:
            # Try to get from cache first using paths
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "akaze_similarity")
            if cached_result is not None:
                return cached_result
            
            # Load images using paths
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
            
            # Filter matches
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
            inlier_score = 0.0
            if num_good_matches >= self.min_match_count:
                # Get matched keypoints
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography matrix
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, self.inlier_threshold)
                
                if H is not None:
                    # Count inliers
                    inliers = mask.ravel().sum()
                    inlier_score = inliers / max(1, num_good_matches)
                    logger.debug(f"AKAZE: Homography inliers: {inliers}/{num_good_matches}")
                    
                    # Combine raw match score with inlier ratio
                    match_score = 0.4 * match_score + 0.6 * inlier_score
            
            # Normalize and scale
            final_score = min(1.0, match_score * 1.5)
            
            # Cache the result using paths
            self.feature_cache.put(f"{img_path1}|{img_path2}", "akaze_similarity", final_score)
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating AKAZE similarity: {e}")
            return 0.0
            
    def calculate_orb_similarity(self, img_input1: ImageType, img_input2: ImageType) -> float:
        """Calculate ORB feature similarity with geometric verification"""
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            return 0.0

        try:
            # Try to get from cache first using paths
            cached_result = self.feature_cache.get(f"{img_path1}|{img_path2}", "orb_similarity")
            if cached_result is not None:
                return cached_result
            
            # Load images using paths
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
            
            # Filter matches
            good_matches = []
            for match in matches:
                if len(match) == 2:
                    m, n = match
                    if m.distance < 0.75 * n.distance:  # Standard ratio for ORB
                        good_matches.append(m)
                        
            num_good_matches = len(good_matches)
            logger.debug(f"ORB: {num_good_matches} good matches found")
            
            # Calculate base score
            max_possible_matches = min(len(kp1), len(kp2))
            match_score = len(good_matches) / max(1, max_possible_matches)
            
            # Apply geometric verification if we have enough matches
            inlier_score = 0.0
            if num_good_matches >= self.min_match_count:
                # Get matched keypoints
                src_pts = np.float32([kp1[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                dst_pts = np.float32([kp2[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
                
                # Find homography matrix
                H, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, self.inlier_threshold)
                
                if H is not None:
                    # Count inliers
                    inliers = mask.ravel().sum()
                    inlier_score = inliers / max(1, num_good_matches)
                    logger.debug(f"ORB: Homography inliers: {inliers}/{num_good_matches}")
                    
                    # Combine raw match score with inlier ratio
                    match_score = 0.4 * match_score + 0.6 * inlier_score
            
            # Normalize and scale
            final_score = min(1.0, match_score * 1.5)
            
            # Cache the result using paths
            self.feature_cache.put(f"{img_path1}|{img_path2}", "orb_similarity", final_score)
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating ORB similarity: {e}")
            return 0.0
    
    def calculate_deep_similarity(self, img_input1: ImageType, img_input2: ImageType) -> float:
        """
        Calculate deep learning feature similarity using ensemble of models
        """
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            return 0.0

        try:
            # Try to get from cache first using paths
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
                        # Handle potential NaN/Inf values before normalization
                        if np.any(np.isnan(model_features)) or np.any(np.isinf(model_features)):
                             logger.warning(f"NaN or Inf detected in {model_name} features for {img_path}. Replacing with zeros.")
                             model_features = np.nan_to_num(model_features, nan=0.0, posinf=0.0, neginf=0.0)
                        
                        # Calculate norm, handle zero norm case
                        norm = np.linalg.norm(model_features)
                        if norm > 1e-6: # Avoid division by zero
                            model_features = model_features / norm
                        else:
                            model_features = np.zeros_like(model_features) # Set to zero vector if norm is near zero
                            
                        ensemble_features[model_name] = model_features
                    except Exception as e:
                        logger.warning(f"Error extracting {model_name} features for {img_path}: {e}")
                
                result['ensemble'] = ensemble_features
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting deep features: {e}")
            return None
    
    def calculate_combined_similarity(self, img_input1: ImageType, img_input2: ImageType, 
                                      weights: Optional[Dict[str, float]] = None) -> Tuple[float, Dict[str, float]]:
        """
        Calculate combined similarity using all methods
        Returns the weighted score and individual scores
        """
        # Get local paths for calculations
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            logger.warning("Could not get valid paths for combined similarity calculation.")
            return 0.0, {'sift': 0.0, 'akaze': 0.0, 'deep': 0.0, 'orb': 0.0}

        if not weights:
            weights = SETTINGS['WEIGHTS']
            
        # Calculate similarities using paths
        sift_score = self.calculate_sift_similarity(img_path1, img_path2)
        akaze_score = self.calculate_akaze_similarity(img_path1, img_path2)
        deep_score = self.calculate_deep_similarity(img_path1, img_path2)
        orb_score = self.calculate_orb_similarity(img_path1, img_path2)
        
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
            
        # Boost score if multiple methods agree
        high_scores = sum(1 for score in scores.values() if score > 0.65)
        if high_scores >= 3:
            combined_score = min(1.0, combined_score * 1.1)  # +10% boost if 3+ methods agree
        elif high_scores >= 2:
            combined_score = min(1.0, combined_score * 1.05)  # +5% boost if 2+ methods agree
            
        logger.debug(f"Combined similarity: {combined_score:.4f} (SIFT={sift_score:.2f}, AKAZE={akaze_score:.2f}, Deep={deep_score:.2f}, ORB={orb_score:.2f})")
        
        return combined_score, scores
        
    def calculate_similarity(self, img_input1: ImageType, img_input2: ImageType, 
                           weights: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate combined similarity between two images
        This is a wrapper around calculate_combined_similarity that returns just the score
        """
        # Get local paths for caching and calculation
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            return 0.0

        try:
            # Try to get from cache first using paths
            cache_key = f"{img_path1}|{img_path2}"
            cached_result = self.feature_cache.get(cache_key, "combined_similarity")
            if cached_result is not None:
                return float(cached_result)
                
            # Calculate similarity using paths
            combined_score, _ = self.calculate_combined_similarity(img_path1, img_path2, weights)
            
            # Cache the result using paths
            self.feature_cache.put(cache_key, "combined_similarity", combined_score)
            
            return combined_score
        except Exception as e:
            logger.error(f"Error calculating similarity between {img_path1} and {img_path2}: {e}")
            return 0.0
            
    def is_match(self, img_input1: ImageType, img_input2: ImageType, threshold: Optional[float] = None) -> Tuple[bool, float, Dict[str, float]]:
        """
        Determine if two images match based on similarity score
        
        Args:
            img_input1: Path or dict for first image
            img_input2: Path or dict for second image
            threshold: Similarity threshold (default: from config)
            
        Returns:
            Tuple of (is_match, similarity_score, individual_scores)
        """
        # Get local paths for calculation
        img_path1 = self._get_local_path(img_input1)
        img_path2 = self._get_local_path(img_input2)
        if not img_path1 or not img_path2:
            return False, 0.0, {'sift': 0.0, 'akaze': 0.0, 'deep': 0.0, 'orb': 0.0}

        if threshold is None:
            threshold = SETTINGS['COMBINED_THRESHOLD']
            
        try:
            # Calculate combined similarity using paths
            similarity, scores = self.calculate_combined_similarity(img_path1, img_path2)
            
            # Determine if it's a match
            is_match = similarity >= threshold
            
            return is_match, similarity, scores
        except Exception as e:
            logger.error(f"Error determining match between {img_path1} and {img_path2}: {e}")
            return False, 0.0, {'sift': 0.0, 'akaze': 0.0, 'deep': 0.0, 'orb': 0.0}
            
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
def match_product_images(haoreum_image_data: List[Dict[str, Optional[str]]], 
                        kogift_image_data: List[Dict[str, Optional[str]]],
                        threshold: float = None,
                        custom_weights: Optional[Dict[str, float]] = None) -> List[Dict]:
    """
    Match Haoreum product images with Kogift product images
    
    Args:
        haoreum_image_data: List of dictionaries for Haoreum product images ({local_path, url, source})
        kogift_image_data: List of dictionaries for Kogift product images ({local_path, url, source})
        threshold: Similarity threshold for matches (default: from config)
        custom_weights: Optional custom weights for similarity calculations
        
    Returns:
        List of dictionaries with match results, containing the full image data dicts.
    """
    # Use threshold from config if not provided
    if threshold is None:
        threshold = SETTINGS['COMBINED_THRESHOLD']
    
    # Use custom_weights from config if not provided
    if custom_weights is None:
        custom_weights = SETTINGS['WEIGHTS']
    
    matcher = EnhancedImageMatcher()
    results = []
    
    # Validate image data dictionaries (check for local_path)
    haoreum_valid = [data for data in haoreum_image_data if data and data.get('local_path') and os.path.exists(data['local_path'])]
    kogift_valid = [data for data in kogift_image_data if data and data.get('local_path') and os.path.exists(data['local_path'])]
    
    if len(haoreum_valid) == 0 or len(kogift_valid) == 0:
        logger.warning(f"No valid image data found: Haoreum {len(haoreum_valid)}/{len(haoreum_image_data)}, "
                      f"Kogift {len(kogift_valid)}/{len(kogift_image_data)}")
        return results
        
    logger.info(f"Matching {len(haoreum_valid)} Haoreum images with {len(kogift_valid)} Kogift images")
    
    # Process each Haoreum image
    for h_idx, h_data in enumerate(haoreum_valid):
        best_match_data = None
        best_similarity = 0
        best_scores = {}
        
        # Find best matching Kogift image
        for k_data in kogift_valid:
            # Pass the dictionaries directly to is_match
            is_match, similarity, scores = matcher.is_match(h_data, k_data, threshold)
            
            if is_match and similarity > best_similarity:
                best_match_data = k_data # Store the full dictionary
                best_similarity = similarity
                best_scores = scores
                
        # Add result, storing the full dictionaries
        result = {
            'haoreum_image_data': h_data,
            'kogift_image_data': best_match_data, # Can be None if no match
            'similarity': best_similarity,
            'scores': best_scores,
            'is_match': best_match_data is not None
        }
        results.append(result)
        
        # Log progress
        if (h_idx + 1) % 10 == 0 or h_idx == len(haoreum_valid) - 1:
            logger.info(f"Processed {h_idx + 1}/{len(haoreum_valid)} Haoreum images")
    
    # Clean up
    matcher.clear_cache()
    
    return results


def match_naver_product_images(haoreum_image_data: List[Dict[str, Optional[str]]], 
                             naver_results: pd.DataFrame, # Keep as DataFrame for now
                             threshold: Optional[float] = None,
                             custom_weights: Optional[Dict[str, float]] = None) -> Dict[str, Dict]:
    """
    Match Haoreum product images with Naver product images from crawled results
    
    Args:
        haoreum_image_data: List of Haoreum image data dictionaries
        naver_results: DataFrame containing Naver crawl results (expected to have image data dicts)
        threshold: Optional similarity threshold (default: from config)
        custom_weights: Optional custom weights for similarity calculations
        
    Returns:
        Dictionary mapping product names (from Haoreum data) to match results (containing image data dicts).
    """
    if threshold is None:
        threshold = SETTINGS['COMBINED_THRESHOLD']
    
    if custom_weights is None:
        custom_weights = SETTINGS['WEIGHTS']
    
    matcher = EnhancedImageMatcher()
    results = {}
    
    # Validate Haoreum image data
    haoreum_valid = [data for data in haoreum_image_data if data and data.get('local_path') and os.path.exists(data['local_path'])]
    if not haoreum_valid:
        logger.warning("No valid Haoreum image data found for Naver matching")
        return results
    
    # Extract Naver image data dictionaries and product names from results DataFrame
    naver_image_data_list = []
    naver_product_names = []
    missing_or_invalid_images = 0
    valid_images = 0

    # --- Standardize on 'image_data' column --- 
    # Assume this column holds the dictionary: {'url': ..., 'local_path': ..., 'source': ...}
    required_image_col = 'image_data' 

    for _, row in naver_results.iterrows():
        try:
            # --- Get Original Product Name (from potential columns) --- 
            original_product_name = None
            if isinstance(row.get('original_row'), dict):
                 original_product_name = row['original_row'].get('상품명')
            if not original_product_name: 
                 # Try common alternative names
                 potential_name_cols = ['original_product_name', '상품명', '네이버 상품명']
                 for name_col in potential_name_cols:
                      if name_col in row and pd.notna(row[name_col]):
                           original_product_name = row[name_col]
                           break

            if not original_product_name:
                 logger.warning(f"Could not determine original product name for Naver result row index {_}")
                 missing_or_invalid_images += 1
                 continue

            # --- Get Image Data Dictionary --- 
            image_data_dict = None
            if required_image_col in row and isinstance(row[required_image_col], dict):
                image_data_dict = row[required_image_col]
            # Fallback: Check other potential columns if 'image_data' is missing/not a dict
            elif required_image_col not in row or not isinstance(row.get(required_image_col), dict):
                 logger.debug(f"Column '{required_image_col}' missing or not a dict for {original_product_name}. Checking alternates.")
                 fallback_cols = ['네이버 이미지', 'image', '네이버쇼핑(이미지링크)', 'image_path']
                 for col in fallback_cols:
                      if col in row and isinstance(row[col], dict):
                           image_data_dict = row[col]
                           logger.debug(f"Found image dict in fallback column '{col}'")
                           break
                      # Basic check if it's a string representing a dict (less ideal)
                      elif col in row and isinstance(row[col], str) and row[col].startswith('{'):
                           try:
                                potential_dict = eval(row[col]) # Use eval cautiously
                                if isinstance(potential_dict, dict):
                                     image_data_dict = potential_dict
                                     logger.warning(f"Evaluated image dict string from fallback column '{col}'")
                                     break
                           except Exception as eval_err:
                                logger.warning(f"Failed to eval string from '{col}': {eval_err}")
            
            # --- Validate the Dictionary and Local Path --- 
            if isinstance(image_data_dict, dict) and \
               image_data_dict.get('local_path') and \
               isinstance(image_data_dict['local_path'], str) and \
               os.path.exists(image_data_dict['local_path']):
                
                # Ensure 'source' is present
                if 'source' not in image_data_dict:
                    image_data_dict['source'] = 'naver'
                    
                naver_image_data_list.append(image_data_dict)
                naver_product_names.append(original_product_name)
                valid_images += 1
            else:
                # Log why it was invalid
                if not isinstance(image_data_dict, dict):
                     logger.warning(f"No valid image_data dictionary found for Naver product: {original_product_name}")
                elif not image_data_dict.get('local_path') or not isinstance(image_data_dict.get('local_path'), str):
                     logger.warning(f"Image dictionary missing or invalid 'local_path' for Naver product: {original_product_name} (Dict: {str(image_data_dict)[:100]}...)")
                elif not os.path.exists(image_data_dict['local_path']):
                     logger.warning(f"Naver image local_path does not exist: {image_data_dict['local_path']} for product: {original_product_name}")
                missing_or_invalid_images += 1

        except Exception as e:
            logger.error(f"Error processing Naver result row index {_} for product '{original_product_name or "Unknown"}': {e}", exc_info=True)
            missing_or_invalid_images += 1
            continue

    logger.info(f"Found {valid_images} valid Naver image data entries, {missing_or_invalid_images} missing or invalid.")

    if not naver_image_data_list:
        logger.warning("No valid Naver image data found in results after processing.")
        return results

    logger.info(f"Matching {len(haoreum_valid)} Haoreum images with {len(naver_image_data_list)} Naver images")
    
    # Process each Haoreum image
    for h_idx, h_data in enumerate(haoreum_valid):
        best_match_data = None
        best_similarity = 0
        best_scores = {}
        matched_product_name = None # Store the name of the matched Naver product
        
        # Get product name from Haoreum data (use 'name' if present, else derive from path)
        haoreum_product_name = h_data.get('name')
        if not haoreum_product_name and h_data.get('local_path'):
            haoreum_product_name = os.path.splitext(os.path.basename(h_data['local_path']))[0]
        
        if not haoreum_product_name:
             logger.warning(f"Could not determine Haoreum product name from data: {h_data}")
             continue # Skip if we can't identify the Haoreum product

        # Find best matching Naver image
        for n_data, n_product_name in zip(naver_image_data_list, naver_product_names):
            try:
                # Pass dictionaries directly to is_match
                is_match, similarity, scores = matcher.is_match(h_data, n_data, threshold)
                
                if is_match and similarity > best_similarity:
                    best_match_data = n_data # Store the full dictionary
                    best_similarity = similarity
                    best_scores = scores
                    matched_product_name = n_product_name # Store the associated Naver product name
            except Exception as e:
                logger.error(f"Error matching {h_data.get('local_path')} with {n_data.get('local_path')}: {e}")
                continue
        
        # Store result using Haoreum product name as key
        results[haoreum_product_name] = {
            'haoreum_image_data': h_data,
            'naver_image_data': best_match_data, # Can be None
            'matched_product_name': matched_product_name, # Name of the matched Naver product
            'similarity': best_similarity,
            'scores': best_scores,
            'is_match': best_match_data is not None
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

    # Load global config for test paths
    # Need to ensure CONFIG is loaded globally for the test function
    global CONFIG
    if CONFIG is None:
         CONFIG = _load_config()
         if CONFIG is None:
              logger.error("Failed to load config for test function. Exiting.")
              return

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
    
    # Convert paths to dictionaries for testing match_product_images
    haoreum_data = [{'local_path': p, 'url': Path(p).as_uri(), 'source': 'haoreum'} for p in haoreum_images]
    kogift_data = [{'local_path': p, 'url': Path(p).as_uri(), 'source': 'kogift'} for p in kogift_images]

    # Run matching
    results = match_product_images(haoreum_data, kogift_data, threshold=args.threshold)

    # Convert to serializable format
    output_results = []
    for r in results:
        if r['is_match']:
            output_results.append({
                'haoreum_image': os.path.basename(r['haoreum_image_data']['local_path']),
                'kogift_image': os.path.basename(r['kogift_image_data']['local_path']),
                'similarity': r['similarity'],
                'sift_score': r['scores'].get('sift', 0.0), # Use .get for safety
                'akaze_score': r['scores'].get('akaze', 0.0),
                'deep_score': r['scores'].get('deep', 0.0),
                'orb_score': r['scores'].get('orb', 0.0)
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