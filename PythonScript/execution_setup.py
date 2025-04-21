import os
import sys
import logging
import json
import shutil
import tensorflow as tf
import configparser # Import configparser
from typing import Union, Optional, Dict, List, Tuple, Any

from utils import load_config # Assuming load_config is in utils.py

# Internal function to load and perform basic validation
def _load_and_validate_config(config_path: str) -> Union[configparser.ConfigParser, None]:
    """Loads config using utils.load_config and performs basic validation."""
    config = load_config(config_path)
    
    # Basic check: Does the parser have any sections?
    if not config.sections():
         print(f"CRITICAL: Config file {config_path} loaded, but contains no sections or is invalid. Cannot proceed.")
         logging.critical(f"Config file {config_path} loaded, but contains no sections or is invalid. Cannot proceed.")
         return None
         
    # Example: Check if a crucial section exists
    if not config.has_section('Paths'):
        print(f"CRITICAL: Config file {config_path} is missing the required [Paths] section.")
        logging.critical(f"Config file {config_path} is missing the required [Paths] section.")
        return None
        
    # Add more checks as needed (e.g., essential keys within sections)
    if not config.get('Paths', 'input_dir', fallback=None):
         print(f"CRITICAL: Config file {config_path} is missing 'input_dir' in [Paths].")
         logging.critical(f"Config file {config_path} is missing 'input_dir' in [Paths].")
         return None
         
    return config

def setup_logging(config: configparser.ConfigParser):
    """Configures logging based on the provided ConfigParser configuration."""
    try:
        log_file_path = config.get('Paths', 'log_file', fallback='shoprpa_log.txt')
        log_level_str = config.get('Logging', 'log_level', fallback='INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        if not isinstance(log_level, int): # Fallback if getattr fails
            log_level = logging.INFO
            logging.warning(f"Invalid log_level '{log_level_str}' in config [Logging]. Defaulting to INFO.")
            
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"Error reading logging config: {e}. Using defaults.") # Print as logging not set yet
        log_file_path = 'shoprpa_log.txt'
        log_level = logging.INFO

    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError as e:
            print(f"Error creating log directory {log_dir}: {e}")
            
    # Remove existing handlers before adding new ones
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(threadName)s - [%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info(f"--- Logging Initialized (Level: {logging.getLevelName(log_level)}) ---")

def detect_gpu():
    """Attempts to detect GPU using TensorFlow and returns a boolean flag."""
    gpu_available = False
    try:
        gpus = tf.config.list_physical_devices('GPU')
        if gpus:
            gpu_available = True
            logging.info(f"TensorFlow detected GPU(s): {gpus}")
            # Optional: Configure memory growth (might be needed earlier depending on usage)
            # for gpu in gpus:
            #     tf.config.experimental.set_memory_growth(gpu, True)
        else:
            logging.info("TensorFlow did not detect any physical GPUs.")
    except Exception as e:
        # Log specific error during device listing but don't crash
        logging.error(f"Error during TensorFlow GPU device listing: {e}", exc_info=True)
    return gpu_available

def ensure_directories(config: configparser.ConfigParser) -> bool:
    """Creates core directories defined in the [Paths] section if they don't exist and validates write permissions."""
    all_dirs_ok = True
    if not config.has_section('Paths'):
        logging.error("Configuration Error: [Paths] section is missing.")
        return False
        
    # Keys within [Paths] that represent directories
    dir_keys = ['input_dir', 'temp_dir', 'output_dir', 'image_main_dir', 'image_target_dir']
    
    # Store any fallback directories applied
    fallbacks_applied = {}
    
    for key in dir_keys:
        directory = config.get('Paths', key, fallback=None)
        if not directory:
            logging.error(f"Configuration Error: Required directory path for '{key}' is missing in [Paths].")
            
            # Create a fallback for critical image directories
            if key in ['image_main_dir', 'image_target_dir']:
                fallback_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), key.split('_')[1])
                logging.warning(f"Using fallback for {key}: {fallback_dir}")
                directory = fallback_dir
                fallbacks_applied[key] = fallback_dir
                # Update config with fallback
                if not config.has_section('Paths'):
                    config.add_section('Paths')
                config.set('Paths', key, fallback_dir)
            else:
                all_dirs_ok = False
                continue
        
        try:
            # Try to create the directory
            os.makedirs(directory, exist_ok=True)
            logging.debug(f"Ensured directory exists: {directory}")
            
            # Check if directory is writable by attempting to create a test file
            test_file_path = os.path.join(directory, '.write_test')
            try:
                with open(test_file_path, 'w') as f:
                    f.write('test')
                os.remove(test_file_path)  # Clean up
                logging.debug(f"Verified write permission for directory: {directory}")
            except (IOError, PermissionError) as e:
                logging.error(f"Directory exists but is not writable: {directory} - {e}")
                
                # For image directories, create an alternative if permission denied
                if key in ['image_main_dir', 'image_target_dir']:
                    fallback_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), key.split('_')[1])
                    logging.warning(f"Using fallback due to permission error for {key}: {fallback_dir}")
                    
                    try:
                        os.makedirs(fallback_dir, exist_ok=True)
                        # Verify we can write to the fallback
                        test_fallback_path = os.path.join(fallback_dir, '.write_test')
                        with open(test_fallback_path, 'w') as f:
                            f.write('test')
                        os.remove(test_fallback_path)
                        
                        # Update the config with fallback
                        config.set('Paths', key, fallback_dir)
                        fallbacks_applied[key] = fallback_dir
                        logging.info(f"Successfully created and verified fallback directory: {fallback_dir}")
                    except Exception as fallback_err:
                        logging.error(f"Failed to create fallback directory {fallback_dir}: {fallback_err}")
                        all_dirs_ok = False
                else:
                    all_dirs_ok = False
        except OSError as e:
            logging.error(f"Failed to create directory {directory} (for key '{key}'): {e}. This might be critical.")
            
            # For image directories, create an alternative if access error
            if key in ['image_main_dir', 'image_target_dir']:
                fallback_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), key.split('_')[1])
                logging.warning(f"Using fallback due to access error for {key}: {fallback_dir}")
                
                try:
                    os.makedirs(fallback_dir, exist_ok=True)
                    # Update the config with fallback
                    config.set('Paths', key, fallback_dir)
                    fallbacks_applied[key] = fallback_dir
                    logging.info(f"Successfully created fallback directory: {fallback_dir}")
                except Exception as fallback_err:
                    logging.error(f"Failed to create fallback directory {fallback_dir}: {fallback_err}")
                    all_dirs_ok = False
            else:
                all_dirs_ok = False
    
    # Log summary of fallbacks applied
    if fallbacks_applied:
        logging.warning("The following directory fallbacks were applied due to missing paths or permission issues:")
        for key, path in fallbacks_applied.items():
            logging.warning(f"  - {key} → {path}")
    
    return all_dirs_ok

def clear_temp_files(config: configparser.ConfigParser):
    """Clears temporary directories specified in [Paths]."""
    if not config.has_section('Paths'):
         logging.error("Cannot clear temp files: [Paths] section missing in config.")
         return
         
    temp_dir_keys = ['temp_dir', 'image_main_dir', 'image_target_dir']
    temp_dirs_to_clear = [config.get('Paths', key, fallback=None) for key in temp_dir_keys]
    
    logging.info(f"Attempting to clear temporary directories: {[d for d in temp_dirs_to_clear if d]}")
    for temp_dir in temp_dirs_to_clear:
        if temp_dir and os.path.exists(temp_dir) and os.path.isdir(temp_dir):
            try:
                for filename in os.listdir(temp_dir):
                    file_path = os.path.join(temp_dir, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        logging.error(f'Failed to delete {file_path}. Reason: {e}')
                logging.info(f"Cleared contents of {temp_dir}")
            except Exception as e:
                logging.error(f"Error clearing directory {temp_dir}: {e}")
        elif not temp_dir:
             logging.warning(f"Skipping temp directory clear: Path not defined in config for one of {temp_dir_keys}.")
        elif not os.path.exists(temp_dir):
            logging.warning(f"Temporary directory not found, cannot clear: {temp_dir}")
        elif not os.path.isdir(temp_dir):
             logging.warning(f"Temporary path is not a directory, cannot clear: {temp_dir}")

def validate_program_operation(config: configparser.ConfigParser) -> bool:
    """Basic checks using ConfigParser to ensure the program can likely run."""
    logging.info("Performing pre-run validation checks...")
    checks_passed = True

    # 1. Check Core Directories (Existence is handled by ensure_directories)
    #    Check essential keys were present during ensure_directories implicitly
    if not config.has_section('Paths'):
        logging.error("Validation Failed: Missing [Paths] section in config.")
        return False # Critical failure
        
    required_path_keys = ['input_dir', 'output_dir', 'temp_dir', 'image_main_dir', 'image_target_dir']
    missing_path_keys = [key for key in required_path_keys if not config.get('Paths', key, fallback=None)]
    if missing_path_keys:
        logging.error(f"Validation Failed: Missing essential directory paths in [Paths]: {missing_path_keys}")
        checks_passed = False

    # 2. Check API Keys (Warn if missing)
    if not config.get('API_Keys', 'naver_client_id', fallback=None) or \
       not config.get('API_Keys', 'naver_client_secret', fallback=None):
        logging.warning("Validation Hint: Naver API Client ID or Secret missing in [API_Keys]. Naver search will fail.")
        # Decide if this should be fatal: checks_passed = False

    # 3. Check External Dependencies (Basic Import Checks)
    try:
        import playwright.sync_api
    except ImportError:
        logging.error("Validation Failed: Playwright library seems missing. Kogift/Haereum scraping will fail.")
        checks_passed = False

    try:
        import sentence_transformers
    except ImportError:
        logging.error("Validation Failed: sentence-transformers library seems missing. Text matching will fail.")
        checks_passed = False

    if checks_passed:
        logging.info("Pre-run validation checks passed.")
    else:
        logging.error("One or more critical pre-run validation checks failed.")

    return checks_passed

def initialize_environment(config_path):
    """Loads config using configparser, sets up logging, detects GPU, ensures dirs, and validates."""
    # 1. Load Config using internal helper
    config = _load_and_validate_config(config_path)
    if config is None:
        # Errors already printed/logged by _load_and_validate_config
        sys.exit(1)

    # 2. Setup Logging (Needs ConfigParser object)
    setup_logging(config)
    # Log config summary (be careful not to log secrets)
    try:
        config_summary = {}
        for section in config.sections():
            config_summary[section] = {}
            for key, value in config.items(section):
                if 'secret' in key.lower() or 'password' in key.lower() or 'key' in key.lower() or 'id' in key.lower():
                    config_summary[section][key] = '******' if value else 'Not Set'
                else:
                    config_summary[section][key] = value
        logging.info(f"Using Configuration from {config_path}: {json.dumps(config_summary, indent=2)}")
    except Exception as log_e:
        logging.error(f"Could not log config summary: {log_e}")

    # 3. Ensure Directories (Needs ConfigParser object)
    if not ensure_directories(config):
        logging.error("Failed to create essential directories. Exiting.")
        sys.exit(1)

    # 4. Detect GPU (Needs TensorFlow potentially)
    gpu_available = detect_gpu()

    # 5. Validate Operation (Needs ConfigParser object)
    validation_passed = validate_program_operation(config)

    return config, gpu_available, validation_passed 