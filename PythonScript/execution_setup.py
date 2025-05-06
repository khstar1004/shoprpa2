import os
# Set TensorFlow GPU memory growth before importing any TensorFlow-related code
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'

import sys
import logging
from typing import Union, Optional, Dict, List, Tuple, Any
import configparser  
import subprocess
import traceback
import json
import platform
import shutil
import time
from pathlib import Path

# Import TensorFlow first to ensure GPU settings are applied
try:
    import tensorflow as tf
    TF_AVAILABLE = True
except ImportError:
    TF_AVAILABLE = False
    logging.warning("TensorFlow not available, GPU check will be limited")

# Now import remaining modules
import requests
from PIL import Image
import pandas as pd
import numpy as np

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
    
    if not TF_AVAILABLE:
        logging.warning("TensorFlow not available, cannot detect GPU properly")
        return False
        
    try:
        gpus = tf.config.list_physical_devices('GPU')
        if gpus:
            gpu_available = True
            logging.info(f"TensorFlow detected GPU(s): {gpus}")
            try:
                # Just verify GPU devices are visible
                logical_gpus = tf.config.list_logical_devices('GPU')
                logging.info(f"{len(logical_gpus)} Logical GPUs configured.")
                # Memory growth is now handled via TF_FORCE_GPU_ALLOW_GROWTH
            except RuntimeError as e:
                # Virtual devices must be set before GPUs have been initialized
                logging.error(f"Error configuring GPU devices: {e}")
                gpu_available = False # Don't use GPU if configuration failed
        else:
            logging.info("TensorFlow did not detect any physical GPUs.")
    except Exception as e:
        # Log specific error during device listing but don't crash
        logging.error(f"Error during TensorFlow GPU device listing: {e}", exc_info=True)
    
    # Fallback to nvidia-smi check if TensorFlow didn't detect a GPU
    if not gpu_available:
        try:
            if platform.system() == "Windows":
                result = subprocess.run(['nvidia-smi'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
                if result.returncode == 0:
                    logging.info("GPU detected via nvidia-smi, but not visible to TensorFlow")
                    gpu_available = True
            else:
                # For Linux/Mac
                result = subprocess.run(['which', 'nvidia-smi'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
                if result.returncode == 0:
                    result = subprocess.run(['nvidia-smi'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
                    if result.returncode == 0:
                        logging.info("GPU detected via nvidia-smi, but not visible to TensorFlow")
                        gpu_available = True
        except Exception as e:
            logging.warning(f"Error checking for GPU via nvidia-smi: {e}")
    
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
    """Clear temporary directories specified in the [Paths] section of the config file."""
    try:
        # Get temp directory from config
        temp_dir = config.get('Paths', 'temp_dir', fallback=None)
        if temp_dir and os.path.exists(temp_dir):
            logging.info(f"Clearing temporary directory: {temp_dir}")
            try:
                # Remove all files in temp directory
                for filename in os.listdir(temp_dir):
                    file_path = os.path.join(temp_dir, filename)
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            logging.debug(f"Removed temporary file: {file_path}")
                    except Exception as e:
                        logging.error(f"Error removing file {file_path}: {e}")
            except Exception as e:
                logging.error(f"Error clearing temp directory {temp_dir}: {e}")

        # Get image directories from config
        image_main_dir = config.get('Paths', 'image_main_dir', fallback=None)
        image_target_dir = config.get('Paths', 'image_target_dir', fallback=None)
        
        # Note: We are no longer clearing image directories as they are now permanent storage
        logging.info("Image directories are now permanent storage and will not be cleared")
        
    except Exception as e:
        logging.error(f"Error in clear_temp_files: {e}")
        raise

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

    # 3. Check Email Configuration (when enabled)
    if config.has_section('Email') and config.getboolean('Email', 'enabled', fallback=False):
        logging.info("Email functionality is enabled. Validating email configuration...")
        try:
            # Import the validation function if email is enabled
            from email_sender import validate_email_config
            
            # Validate email configuration
            if validate_email_config(config):
                logging.info("Email configuration validated successfully.")
            else:
                logging.warning("Email configuration is invalid or incomplete. Email sending will be disabled.")
                # Don't fail the entire application for email config issues
                if config.has_section('Email'):
                    config.set('Email', 'enabled', 'false')
                    logging.info("Email functionality has been automatically disabled due to configuration issues.")
        except ImportError:
            logging.warning("Email sender module not available. Email functionality will be disabled.")
            if config.has_section('Email'):
                config.set('Email', 'enabled', 'false')
        except Exception as e:
            logging.error(f"Error during email configuration validation: {e}")
            if config.has_section('Email'):
                config.set('Email', 'enabled', 'false')

    # 4. Check External Dependencies (Basic Import Checks)
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