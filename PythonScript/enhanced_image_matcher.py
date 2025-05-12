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
os.environ['TF_GPU_ALLOCATOR'] = 'cuda_malloc_async'  # Enable async memory allocator
os.environ['TF_GPU_THREAD_MODE'] = 'gpu_private'  # Enable GPU thread mode
os.environ['TF_USE_CUDNN'] = '1'  # Enable cuDNN

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
import concurrent.futures
from functools import partial
import threading
from queue import Queue
import tensorflow as tf

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

# GPU Configuration
DEFAULT_GPU_MEMORY_FRACTION = 0.7  # Default GPU memory fraction to use
DEFAULT_BATCH_SIZE = 32  # Default batch size for GPU processing

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
        # Get use_gpu from Matching section with improved detection
        use_gpu = config.getboolean('Matching', 'use_gpu', fallback=None)
        if use_gpu is None:
            # If not explicitly set, try to detect GPU
            use_gpu = _is_gpu_available()
            logger.info(f"GPU auto-detection result: {use_gpu}")
        else:
            logger.info(f"GPU usage setting from config: {use_gpu}")

        # Get GPU specific settings
        gpu_memory_fraction = config.getfloat('Matching', 'gpu_memory_fraction', 
                                            fallback=DEFAULT_GPU_MEMORY_FRACTION)
        batch_size = config.getint('Matching', 'gpu_batch_size', 
                                 fallback=DEFAULT_BATCH_SIZE)

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
            'USE_GPU': use_gpu,
            'GPU_MEMORY_FRACTION': gpu_memory_fraction,
            'BATCH_SIZE': batch_size
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
    'USE_GPU': False,  # Default to False if not specified
    'GPU_MEMORY_FRACTION': DEFAULT_GPU_MEMORY_FRACTION,
    'BATCH_SIZE': DEFAULT_BATCH_SIZE
}

def _is_gpu_available():
    """Check if GPU is truly available for TensorFlow to use"""
    try:
        # First try TensorFlow's built-in GPU detection
        gpus = tf.config.list_physical_devices('GPU')
        if gpus:
            logger.info(f"TensorFlow detected {len(gpus)} GPU(s)")
            # Configure TensorFlow to use memory growth
            for gpu in gpus:
                try:
                    tf.config.experimental.set_memory_growth(gpu, True)
                    logger.info(f"Enabled memory growth for GPU: {gpu}")
                except RuntimeError as e:
                    logger.warning(f"Memory growth setting failed for GPU {gpu}: {e}")
            return True

        # Check using nvidia-smi as backup
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
        
        # Check TensorFlow GPU devices
        gpus = tf.config.list_physical_devices('GPU')
        logger.info(f"TensorFlow GPU devices: {gpus}")
        
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
            
        # Check CUDA runtime
        try:
            from ctypes import cdll
            cdll.LoadLibrary("nvcuda.dll")
            logger.info("CUDA runtime is accessible")
        except Exception as e:
            logger.warning(f"CUDA runtime check failed: {e}")
            
        # Check TensorFlow CUDA build
        logger.info(f"TensorFlow CUDA build: {tf.test.is_built_with_cuda()}")
        logger.info(f"TensorFlow using GPU: {tf.test.is_built_with_gpu_support()}")
        
        return _is_gpu_available()
    except Exception as e:
        logger.error(f"Error during GPU status check: {e}")
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
    """Enhanced image matcher that combines multiple matching techniques"""
    
    def __init__(self, config: Optional[configparser.ConfigParser] = None, use_gpu: bool = None):
        """Initialize the matcher with optional config"""
        self.config = config
        self.settings = _get_config_values(config) if config else SETTINGS
        
        # Override GPU setting if explicitly provided
        if use_gpu is not None:
            self.settings['USE_GPU'] = use_gpu
            
        # Initialize GPU settings
        if self.settings['USE_GPU']:
            try:
                # Configure TensorFlow for GPU
                gpus = tf.config.list_physical_devices('GPU')
                if gpus:
                    # Set memory growth and memory limit
                    for gpu in gpus:
                        tf.config.experimental.set_memory_growth(gpu, True)
                        tf.config.experimental.set_virtual_device_configuration(
                            gpu,
                            [tf.config.experimental.VirtualDeviceConfiguration(
                                memory_limit=int(self.settings['GPU_MEMORY_FRACTION'] * 1024))]
                        )
                    logger.info(f"GPU enabled with memory fraction: {self.settings['GPU_MEMORY_FRACTION']}")
                else:
                    logger.warning("GPU requested but not available, falling back to CPU")
                    self.settings['USE_GPU'] = False
            except Exception as e:
                logger.error(f"Error configuring GPU: {e}")
                self.settings['USE_GPU'] = False
        
        # Initialize feature cache
        self.cache = FeatureCache(
            cache_dir=self.settings['FEATURE_CACHE_DIR'],
            max_items=self.settings['MAX_CACHE_ITEMS'],
            enabled=self.settings['CACHE_FEATURES']
        )
        
        # Initialize deep learning models
        self.models = {}
        self._initialize_deep_models()
        
        # Initialize thread pool for parallel processing
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=4,  # Adjust based on your needs
            thread_name_prefix="ImageMatcher"
        )
        
        # Initialize batch processing queue
        self.batch_queue = Queue()
        self.batch_results = {}
        self.batch_lock = threading.Lock()
        
        logger.info(f"Enhanced Image Matcher initialized (GPU: {self.settings['USE_GPU']})")
    
    def _initialize_deep_models(self):
        """Initialize deep learning models with GPU support"""
        try:
            import tensorflow as tf
            from tensorflow.keras.applications import EfficientNetB0, ResNet50V2
            
            # Configure mixed precision for better GPU performance
            if self.settings['USE_GPU']:
                policy = tf.keras.mixed_precision.Policy('mixed_float16')
                tf.keras.mixed_precision.set_global_policy(policy)
                logger.info("Enabled mixed precision training")
            
            # Load models with proper GPU configuration
            with tf.device('/GPU:0' if self.settings['USE_GPU'] else '/CPU:0'):
                # Primary model (EfficientNetB0)
                self.models['efficientnet'] = EfficientNetB0(
                    weights='imagenet',
                    include_top=False,
                    pooling='avg'
                )
                
                # Additional model for ensemble (if enabled)
                if self.settings['USE_MULTIPLE_MODELS']:
                    self.models['resnet'] = ResNet50V2(
                        weights='imagenet',
                        include_top=False,
                        pooling='avg'
                    )
                
            logger.info(f"Deep learning models initialized on {'GPU' if self.settings['USE_GPU'] else 'CPU'}")
            
            # Warm up the models
            if self.settings['USE_GPU']:
                dummy_input = tf.random.normal([1, 224, 224, 3])
                for model_name, model in self.models.items():
                    _ = model(dummy_input)
                logger.info("Models warmed up on GPU")
                
        except Exception as e:
            logger.error(f"Error initializing deep learning models: {e}")
            self.models = {}
    
    def _process_batch(self, batch_images: List[np.ndarray]) -> List[np.ndarray]:
        """Process a batch of images using GPU acceleration"""
        if not self.settings['USE_GPU'] or not self.models:
            return []
            
        try:
            # Convert batch to tensor
            batch_tensor = tf.convert_to_tensor(batch_images)
            
            # Process with primary model
            features_primary = self.models['efficientnet'](batch_tensor)
            
            # Process with secondary model if available
            if 'resnet' in self.models:
                features_secondary = self.models['resnet'](batch_tensor)
                # Combine features (simple concatenation for now)
                features = tf.concat([features_primary, features_secondary], axis=1)
            else:
                features = features_primary
                
            return features.numpy()
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return []
    
    def _batch_worker(self):
        """Background worker for processing image batches"""
        while True:
            try:
                # Get batch of images
                batch = []
                batch_keys = []
                
                # Collect images for the batch
                while len(batch) < self.settings['BATCH_SIZE']:
                    try:
                        key, image = self.batch_queue.get_nowait()
                        if key is None:  # Shutdown signal
                            return
                        batch.append(image)
                        batch_keys.append(key)
                    except Queue.Empty:
                        break
                
                if not batch:
                    continue
                
                # Process batch
                features = self._process_batch(batch)
                
                # Store results
                with self.batch_lock:
                    for key, feature in zip(batch_keys, features):
                        self.batch_results[key] = feature
                        
            except Exception as e:
                logger.error(f"Error in batch worker: {e}")
    
    def calculate_similarity(self, img_path1: str, img_path2: str, 
                           weights: Optional[Dict[str, float]] = None) -> float:
        """Calculate similarity between two images with GPU acceleration"""
        if not weights:
            weights = self.settings['WEIGHTS']
            
        try:
            # Start batch processing thread if using GPU
            if self.settings['USE_GPU'] and not hasattr(self, '_batch_thread'):
                self._batch_thread = threading.Thread(
                    target=self._batch_worker,
                    daemon=True
                )
                self._batch_thread.start()
            
            # Calculate traditional feature similarities in parallel
            future_sift = self.thread_pool.submit(
                self.calculate_sift_similarity, img_path1, img_path2)
            future_akaze = self.thread_pool.submit(
                self.calculate_akaze_similarity, img_path1, img_path2)
            future_orb = self.thread_pool.submit(
                self.calculate_orb_similarity, img_path1, img_path2)
            
            # Calculate deep features (GPU accelerated if available)
            deep_sim = self.calculate_deep_similarity(img_path1, img_path2)
            
            # Get results from parallel computations
            sift_sim = future_sift.result()
            akaze_sim = future_akaze.result()
            orb_sim = future_orb.result()
            
            # Combine similarities
            combined_sim = (
                weights['sift'] * sift_sim +
                weights['akaze'] * akaze_sim +
                weights['deep'] * deep_sim +
                weights['orb'] * orb_sim
            )
            
            return min(combined_sim, 1.0)  # Ensure score doesn't exceed 1.0
            
        except Exception as e:
            logger.error(f"Error calculating similarity: {e}")
            return 0.0
    
    def calculate_deep_similarity(self, img_path1: str, img_path2: str) -> float:
        """Calculate deep learning based similarity with GPU acceleration"""
        try:
            # Extract features (GPU accelerated if available)
            features1 = self._extract_deep_features(img_path1)
            features2 = self._extract_deep_features(img_path2)
            
            if features1 is None or features2 is None:
                return 0.0
            
            # Calculate cosine similarity
            similarity = np.dot(features1, features2) / (
                np.linalg.norm(features1) * np.linalg.norm(features2)
            )
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error calculating deep similarity: {e}")
            return 0.0
    
    def _extract_deep_features(self, img_path: str) -> Optional[np.ndarray]:
        """Extract deep features with GPU acceleration"""
        try:
            # Check cache first
            cached_features = self.cache.get(img_path, 'deep')
            if cached_features is not None:
                return cached_features
            
            # Load and preprocess image
            img = self._load_and_prepare_image(img_path)[0]
            if img is None:
                return None
            
            # Add batch dimension
            img = np.expand_dims(img, axis=0)
            
            # Use GPU acceleration if available
            if self.settings['USE_GPU']:
                # Add to batch queue
                key = hashlib.md5(img_path.encode()).hexdigest()
                self.batch_queue.put((key, img))
                
                # Wait for result
                max_wait = 30  # Maximum wait time in seconds
                start_time = time.time()
                while time.time() - start_time < max_wait:
                    with self.batch_lock:
                        if key in self.batch_results:
                            features = self.batch_results.pop(key)
                            # Cache the features
                            self.cache.put(img_path, 'deep', features)
                            return features
                    time.sleep(0.1)
                
                logger.warning(f"Timeout waiting for GPU batch processing: {img_path}")
                
            # Fall back to CPU processing if GPU not available or timeout
            features = self.models['efficientnet'].predict(img, verbose=0)
            if 'resnet' in self.models:
                features_secondary = self.models['resnet'].predict(img, verbose=0)
                features = np.concatenate([features, features_secondary], axis=1)
            
            # Flatten features
            features = features.flatten()
            
            # Cache the features
            self.cache.put(img_path, 'deep', features)
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting deep features: {e}")
            return None
    
    def __del__(self):
        """Cleanup resources"""
        try:
            # Stop batch processing
            if hasattr(self, 'batch_queue'):
                self.batch_queue.put((None, None))  # Shutdown signal
            
            # Close thread pool
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=True)
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


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