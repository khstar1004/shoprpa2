import os
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

# Disable GPU usage explicitly for TensorFlow if needed
# os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
try:
    tf.config.set_visible_devices([], 'GPU')
    logical_gpus = tf.config.list_logical_devices('GPU')
    logging.info(f"{len(logical_gpus)} Logical GPUs available after setting visible devices.")
except RuntimeError as e:
    # Virtual devices must be set before GPUs have been initialized
    logging.warning(f"Could not explicitly disable GPUs, might already be initialized: {e}")
except Exception as e:
     logging.error(f"Error configuring TensorFlow GPU visibility: {e}")

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
            
        # URL 정규화
        if not image_url.startswith(('http://', 'https://')):
            if "kogift" in image_url.lower() or "koreagift" in image_url.lower() or "adpanchok" in image_url.lower() or "jclgift" in image_url.lower():
                image_url = f"https://{image_url}" if not image_url.startswith('//') else f"https:{image_url}"
                logging.debug(f"Normalized image URL: {image_url}")
            else:
                logging.error(f"Invalid image URL scheme: {image_url}")
                return None

        # Use the async downloader
        result = await async_download_image(session, image_url)
        
        # Check if result is a tuple with the expected structure
        if not isinstance(result, tuple) or len(result) != 3:
            logging.error(f"Unexpected result format from async_download_image: {result}")
            return None
            
        url, success, image_path = result

        if success and image_path and os.path.exists(image_path):
            try:
                image = Image.open(image_path)
                # Return a copy in case the temporary file is deleted later
                return image.copy()
            except IOError as e:
                logging.error(f"Error opening downloaded image {image_path}: {e}")
                return None
        else:
            logging.error(f"Failed to download image from {image_url} or file not found. Path: {image_path}, Success: {success}")
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

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True) # Ensure output directory exists

    input_data = None
    if isinstance(input_path, (str, Path)):
        input_path = Path(input_path)
        if not input_path.exists():
            logging.error(f"Input file not found: {input_path}")
            return False
        try:
            with open(input_path, 'rb') as i:
                input_data = i.read()
        except IOError as e:
            logging.error(f"Error reading input file {input_path}: {e}")
            return False
    elif isinstance(input_path, Image.Image):
        try:
            buffer = BytesIO()
            input_path.save(buffer, format="PNG") # Save PIL image to buffer
            buffer.seek(0)
            input_data = buffer.read()
        except Exception as e:
            logging.error(f"Error processing PIL Image object: {e}")
            return False
    elif isinstance(input_path, bytes):
        input_data = input_path
    else:
        logging.error(f"Unsupported input type: {type(input_path)}")
        return False

    if input_data is None:
        logging.error("Input data could not be processed.")
        return False

    try:
        # Use the global session for removal
        result_bytes = remove(input_data, session=rembg_session)

        # Ensure the output is saved as PNG
        output_path = output_path.with_suffix('.png')

        with open(output_path, 'wb') as o:
            o.write(result_bytes)
        logging.info(f"Background removed successfully. Output saved to: {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error during background removal process: {e}")
        # Attempt to remove potentially corrupted output file
        if output_path.exists():
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