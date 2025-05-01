import os
# Set TensorFlow GPU memory growth before importing TensorFlow
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'

import logging
import numpy as np
from PIL import Image
from rembg import remove, new_session
import tensorflow as tf
from typing import Union, Optional, Dict, List, Tuple, Any
import asyncio
from pathlib import Path
import requests
from io import BytesIO
import configparser
import tempfile
import aiohttp
from image_downloader import download_image as async_download_image

# Load config
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.ini')
try:
    config.read(config_path, encoding='utf-8')
    # Get image directory from config
    IMAGE_DIR = config.get('Matching', 'images_dir', fallback='C:\\RPA\\Image\\Target')
    logging.info(f"Using image directory from config: {IMAGE_DIR}")
    # Create the directory if it doesn't exist
    os.makedirs(IMAGE_DIR, exist_ok=True)
    
    # Create a temp directory within the image directory
    TEMP_DIR = os.path.join(IMAGE_DIR, 'temp')
    os.makedirs(TEMP_DIR, exist_ok=True)
except Exception as e:
    logging.error(f"Error loading config or creating image directories: {e}")
    # Fallback to default locations
    IMAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'images')
    TEMP_DIR = os.path.join(tempfile.gettempdir(), 'image_utils_temp')
    os.makedirs(IMAGE_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

# Configure TensorFlow GPU - use a safer approach that won't conflict with other modules
try:
    # We already set TF_FORCE_GPU_ALLOW_GROWTH at the beginning
    # This is just to check if a GPU is available
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        logging.info(f"Found {len(gpus)} GPU(s) available for image processing")
    else:
        logging.info("No GPUs available for image processing, using CPU")
except Exception as e:
    logging.warning(f"Error checking GPU availability: {e}")

# --- Global Cache for Models ---
# Avoid reloading models repeatedly
IMAGE_MODEL_CACHE = {
    "efficientnetb0": None
}
TEXT_MODEL_CACHE = {
    "ko-sroberta-multitask": None
}

# Global session cache for rembg
rembg_session = None

# --- Global Variables ---
ENHANCED_MATCHER_INSTANCE = None

# --- Utility Functions ---
def initialize_rembg_session(model_name="u2net"):
    """Initializes the rembg session if it's not already initialized."""
    global rembg_session
    if rembg_session is None:
        try:
            rembg_session = new_session(model_name)
            logging.info(f"rembg session initialized successfully with model {model_name}.")
        except Exception as e:
            logging.error(f"Failed to initialize rembg session: {e}")
            rembg_session = None # Ensure session remains None if initialization fails
    return rembg_session

async def get_image_from_url(session: aiohttp.ClientSession, image_url: str) -> Union[Image.Image, None]:
    """Downloads an image from a URL and returns it as a PIL Image object."""
    try:
        if not image_url:
            logging.error("Empty image URL provided to get_image_from_url")
            return None
            
        # Fix URL format - ensure forward slashes and proper scheme
        if isinstance(image_url, str):
            # Fix backslashes in URLs (this is a common issue with crawled URLs)
            if "\\" in image_url:
                image_url = image_url.replace("\\", "/")
            
            # Now normalize URLs with proper scheme
            if not image_url.startswith(('http://', 'https://')):
                if any(domain in image_url.lower() for domain in ["kogift", "koreagift", "adpanchok", "jclgift"]):
                    # Add proper scheme based on URL format
                    if image_url.startswith('//'):
                        image_url = f"https:{image_url}"
                    elif ":" in image_url and not image_url.startswith(('http:', 'https:')):
                        # Handle case where URL is like 'https:\www...' - split at colon and fix scheme
                        parts = image_url.split(':', 1)
                        if len(parts) == 2:
                            scheme = parts[0].lower()
                            path = parts[1].lstrip('/').lstrip('\\')
                            image_url = f"{scheme}://{path}"
                    else:
                        # Simple case - just prefix with https://
                        image_url = f"https://{image_url}"
                    
                    logging.debug(f"Normalized image URL: {image_url}")
                else:
                    logging.error(f"Invalid image URL scheme: {image_url}")
                    return None

        # Use the async downloader - IMPORTANT: This needs the session and save_directory potentially
        # This function might need redesign if it's intended to directly use the downloader's logic
        # For now, let's assume it relies on a pre-downloaded path or needs its own download logic
        
        # Simplified direct download for this utility function
        async with session.get(image_url, timeout=15) as response:
            if response.status == 200:
                try:
                    content = await response.read()
                    image = Image.open(BytesIO(content))
                    return image.copy()
                except Exception as e:
                    logging.error(f"Error reading image data from {image_url}: {e}")
                    return None
            else:
                 logging.error(f"Failed to download image from {image_url}. Status: {response.status}")
                 return None

    except Exception as e:
        logging.error(f"An unexpected error occurred while getting image from {image_url}: {e}")
        return None

# --- Background Removal ---

def remove_background(input_path: Union[str, Path, Image.Image, bytes], output_path: Union[str, Path], model_name="u2net") -> bool:
    """
    Removes the background from an image using the rembg library.

    Args:
        input_path: Path to the input image file, PIL Image object, or image bytes.
        output_path: Path to save the output image file (PNG format).
        model_name: The model to use for background removal (default: "u2net").

    Returns:
        True if background removal was successful, False otherwise.
    """
    global rembg_session
    if rembg_session is None:
        logging.warning("rembg session not initialized. Attempting to initialize now.")
        initialize_rembg_session(model_name)
        if rembg_session is None:
            logging.error("Failed to remove background: rembg session could not be initialized.")
            return False

    # Convert Path to string if needed
    if isinstance(output_path, Path):
        output_path = str(output_path)
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    
    # Save original input_path for logging and tracking
    original_input_path = input_path if isinstance(input_path, (str, Path)) else "PIL_Image_or_bytes"
    original_output_path = output_path

    input_data = None
    try:
        if isinstance(input_path, (str, Path)):
            # Convert to string if Path object
            if isinstance(input_path, Path):
                input_path = str(input_path)
                
            if not os.path.exists(input_path):
                logging.error(f"Input file not found: {input_path}")
                return False
            
            # Handle problematic file extensions
            _, file_ext = os.path.splitext(input_path)
            if file_ext.lower() in ['.asp', '.aspx', '.php', '.jsp', '.html', '.htm']:
                try:
                    # Try to open the file with PIL and save to a temporary file with a valid image extension
                    temp_img = Image.open(input_path)
                    temp_path = input_path + '.png'  # Keep original path for tracking but add valid extension
                    temp_img.save(temp_path)
                    # Update input_path to use the converted image
                    input_path = temp_path
                    logging.info(f"Converted {file_ext} file to PNG format: {temp_path}")
                except Exception as e:
                    logging.error(f"Error converting {file_ext} file to valid image format: {e}")
                    return False
            
            # Read input file
            try:
                with open(input_path, 'rb') as i:
                    input_data = i.read()
            except IOError as e:
                logging.error(f"Error reading input file {input_path}: {e}")
                return False
                
        elif isinstance(input_path, Image.Image):
            # Handle PIL Image object
            try:
                buffer = BytesIO()
                input_path.save(buffer, format="PNG") # Save PIL image to buffer
                buffer.seek(0)
                input_data = buffer.read()
            except Exception as e:
                logging.error(f"Error processing PIL Image object: {e}")
                return False
                
        elif isinstance(input_path, bytes):
            # Handle raw bytes
            try:
                img = Image.open(BytesIO(input_path))
                # Convert to PNG format for better compatibility
                buffer = BytesIO()
                img.save(buffer, format="PNG")
                buffer.seek(0)
                input_data = buffer.read()
            except Exception as e:
                logging.error(f"Error processing image bytes: {e}")
                return False
                
        else:
            logging.error(f"Unsupported input type: {type(input_path)}")
            return False

        if input_data is None:
            logging.error("Input data could not be processed.")
            return False

        # Use the global session for removal
        result_bytes = remove(input_data, session=rembg_session)

        # Ensure the output is saved as PNG
        if not output_path.lower().endswith('.png'):
            output_path = os.path.splitext(output_path)[0] + '.png'
            
        # Always add _nobg suffix if not already present
        if not output_path.lower().endswith('_nobg.png'):
            output_path = os.path.splitext(output_path)[0] + '_nobg.png'

        with open(output_path, 'wb') as o:
            o.write(result_bytes)
            
        # Verify the output file exists and has content
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logging.info(f"Background removed successfully. Input: {original_input_path}, Output: {output_path}")
            return True
        else:
            logging.error(f"Background removal failed: Output file is empty or missing: {output_path}")
            return False

    except Exception as e:
        logging.error(f"Error during background removal process: {e}")
        # Attempt to remove potentially corrupted output file
        if output_path and os.path.exists(output_path):
            try:
                os.remove(output_path)
                logging.info(f"Removed potentially corrupted output file: {output_path}")
            except OSError as rm_err:
                logging.error(f"Error removing output file {output_path}: {rm_err}")
        return False

# New async wrapper for background removal
async def remove_background_async(input_path: Union[str, Path, Image.Image, bytes], output_path: Union[str, Path], model_name="u2net") -> bool:
    """
    Asynchronously removes the background from an image by running the synchronous
    remove_background function in a separate thread.

    Args:
        input_path: Path to the input image file, PIL Image object, or image bytes.
        output_path: Path to save the output image file (PNG format).
        model_name: The model to use for background removal (default: "u2net").

    Returns:
        True if background removal was successful, False otherwise.
    """
    loop = asyncio.get_running_loop()
    # Run the synchronous remove_background function in the default executor (thread pool)
    try:
        # Ensure the session is initialized before calling the sync function within the executor
        # This might initialize it multiple times if called concurrently before the first initialization finishes,
        # but initialize_rembg_session handles the check `if rembg_session is None`.
        initialize_rembg_session(model_name)
        if rembg_session is None:
             logging.error("Cannot run remove_background_async: rembg session failed to initialize.")
             return False

        success = await loop.run_in_executor(
            None,  # Use the default executor
            remove_background,
            input_path,
            output_path,
            model_name # Pass model_name explicitly
        )
        return success
    except Exception as e:
        logging.error(f"Error executing remove_background in executor: {e}")
        return False

# --- Image Similarity ---

def load_image_model(model_name: str = 'efficientnetb0') -> Union[tf.keras.Model, None]:
    """Loads the specified image model (currently only EfficientNetB0).
       Uses a cache to avoid reloading.
    """
    if model_name != 'efficientnetb0':
        logging.error(f"Unsupported image model requested: {model_name}")
        return None

    if IMAGE_MODEL_CACHE[model_name] is None:
        logging.info(f"Loading image similarity model ({model_name})...")
        try:
            # Load EfficientNetB0 without the top classification layer
            base_model = tf.keras.applications.EfficientNetB0(
                weights='imagenet', include_top=False, input_shape=(224, 224, 3)
            )
            # Add a global pooling layer to get a feature vector
            global_avg_layer = tf.keras.layers.GlobalAveragePooling2D()(base_model.output)
            # Create the final model
            model = tf.keras.Model(inputs=base_model.input, outputs=global_avg_layer)
            IMAGE_MODEL_CACHE[model_name] = model
            logging.info(f"Image similarity model ({model_name}) loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to load image model '{model_name}': {e}", exc_info=True)
            IMAGE_MODEL_CACHE[model_name] = None # Ensure cache reflects failure

    return IMAGE_MODEL_CACHE[model_name]

def preprocess_image(img_path: str) -> Union[tf.Tensor, None]:
    """Loads and preprocesses an image for the EfficientNetB0 model."""
    if not os.path.exists(img_path):
        logging.warning(f"Image file not found for preprocessing: {img_path}")
        return None
    try:
        # Load image, ensuring it's RGB
        img = tf.keras.preprocessing.image.load_img(img_path, target_size=(224, 224), color_mode='rgb')
        img_array = tf.keras.preprocessing.image.img_to_array(img)
        # Expand dimensions to create a batch of 1
        img_batch = tf.expand_dims(img_array, 0)
        # Preprocess using the specific function for EfficientNet
        img_preprocessed = tf.keras.applications.efficientnet.preprocess_input(img_batch)
        return img_preprocessed
    except FileNotFoundError:
         logging.error(f"Preprocessing failed: File not found at {img_path}")
         return None
    except Exception as e:
        logging.error(f"Error preprocessing image {img_path}: {e}", exc_info=True)
        return None

def get_enhanced_matcher(config: configparser.ConfigParser) -> Any:
    """Get or create an EnhancedImageMatcher instance."""
    global ENHANCED_MATCHER_INSTANCE
    if ENHANCED_MATCHER_INSTANCE is None:
        # 원형 의존성 방지를 위해 지연 임포트 사용
        from enhanced_image_matcher import EnhancedImageMatcher
        ENHANCED_MATCHER_INSTANCE = EnhancedImageMatcher(config)
    return ENHANCED_MATCHER_INSTANCE

def calculate_image_similarity(img_path1: str, img_path2: str, config: configparser.ConfigParser) -> float:
    """Calculate image similarity using EnhancedImageMatcher."""
    try:
        matcher = get_enhanced_matcher(config)
        similarity, _ = matcher.calculate_combined_similarity(img_path1, img_path2)
        return similarity
    except Exception as e:
        logging.error(f"Error calculating image similarity: {e}", exc_info=True)
        # 오류 발생 시 fallback으로 기본 이미지 유사도 계산 시도
        try:
            model = load_image_model()
            if model is None:
                return 0.0
                
            # 이미지 전처리
            img1_tensor = preprocess_image(img_path1)
            img2_tensor = preprocess_image(img_path2)
            
            if img1_tensor is None or img2_tensor is None:
                return 0.0
                
            # 특징 추출
            features1 = model.predict(img1_tensor, verbose=0).flatten()
            features2 = model.predict(img2_tensor, verbose=0).flatten()
            
            # 코사인 유사도 계산
            similarity = np.dot(features1, features2) / (np.linalg.norm(features1) * np.linalg.norm(features2))
            return float(similarity)
        except Exception as fallback_err:
            logging.error(f"Fallback image similarity calculation also failed: {fallback_err}")
            return 0.0

# Example Usage (Optional - for testing)
if __name__ == "__main__":
    # Ensure the rembg session is initialized before any async operations that might use it
    initialize_rembg_session()
    
    # Log the directories used
    logging.info(f"Main image directory: {IMAGE_DIR}")
    logging.info(f"Temporary directory: {TEMP_DIR}")

    # --- Test get_image_from_url ---
    test_image_url = "https://via.placeholder.com/150" # Example URL
    print(f"Testing get_image_from_url with URL: {test_image_url}")
    pil_image = get_image_from_url(test_image_url)
    if pil_image:
        print("Successfully downloaded and opened image.")
        # pil_image.show() # Uncomment to display the image
    else:
        print("Failed to get image from URL.")

    # --- Test remove_background (using downloaded image) ---
    if pil_image:
        input_img_for_sync = pil_image
        output_sync_path = Path(os.path.join(TEMP_DIR, "test_output_sync.png"))
        print(f"Testing synchronous remove_background with PIL Image. Output: {output_sync_path}")
        success_sync = remove_background(input_img_for_sync, output_sync_path)
        if success_sync:
            print(f"Synchronous background removal successful: {output_sync_path}")
        else:
            print("Synchronous background removal failed.")

    # --- Test remove_background_async (using downloaded image) ---
    async def run_async_test():
        if pil_image:
            input_img_for_async = pil_image
            output_async_path = Path(os.path.join(TEMP_DIR, "test_output_async.png"))
            print(f"Testing asynchronous remove_background_async with PIL Image. Output: {output_async_path}")
            success_async = await remove_background_async(input_img_for_async, output_async_path)
            if success_async:
                print(f"Asynchronous background removal successful: {output_async_path}")
            else:
                print("Asynchronous background removal failed.")
        else:
             print("Skipping async test as image download failed.")

    # Run the async test function
    print("Running async test...")
    # Ensure the rembg session is initialized before starting the event loop
    # (It should be initialized already by the call above, but good practice)
    if rembg_session is None:
         initialize_rembg_session()

    if rembg_session: # Only run async test if session is valid
        asyncio.run(run_async_test())
    else:
        print("Skipping async test because rembg session failed to initialize.")

    # --- Test with a local file (create a dummy file if needed) ---
    # Create a dummy input file for testing local file path input
    dummy_input_path = Path(os.path.join(TEMP_DIR, "dummy_input.png"))
    if pil_image and not dummy_input_path.exists():
        try:
            pil_image.save(dummy_input_path)
            print(f"Created dummy input file: {dummy_input_path}")
        except Exception as e:
            print(f"Failed to save dummy input file: {e}")

    if dummy_input_path.exists():
        output_local_path = Path(os.path.join(TEMP_DIR, "test_output_local.png"))
        print(f"Testing synchronous remove_background with local file: {dummy_input_path}. Output: {output_local_path}")
        success_local = remove_background(str(dummy_input_path), output_local_path) # Pass path as string or Path object
        if success_local:
            print(f"Synchronous background removal successful for local file: {output_local_path}")
        else:
            print("Synchronous background removal failed for local file.")

        # Clean up dummy file
        # try:
        #     os.remove(dummy_input_path)
        #     print(f"Cleaned up dummy input file: {dummy_input_path}")
        # except OSError as e:
        #     print(f"Error cleaning up dummy file: {e}")
    else:
        print("Skipping local file test as dummy input file could not be created.")

    print("image_utils tests completed.") 