#!/usr/bin/env python3
"""
이미지 경로 대소문자 수정 스크립트
This script fixes path capitalization issues by ensuring all references use the correct case.
"""

import os
import shutil
import logging
import sys
from pathlib import Path
import configparser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('path_fix.log')
    ]
)
logger = logging.getLogger('path_fix')

def get_config():
    """Load configuration from config.ini file"""
    config = configparser.ConfigParser()
    config_path = Path(__file__).resolve().parent.parent / 'config.ini'
    
    try:
        config.read(config_path, encoding='utf-8')
        logger.info(f"Successfully loaded configuration from {config_path}")
    except Exception as e:
        logger.error(f"Error loading config from {config_path}: {e}")
        # Create minimal default config
        config['Paths'] = {
            'image_main_dir': 'C:\\RPA\\Image\\Main',
            'output_dir': 'C:\\RPA\\Output'
        }
    
    return config

def fix_image_directories():
    """
    Ensures all image directories use proper capitalization and
    moves any images from incorrectly cased directories.
    """
    # Get config
    config = get_config()
    base_img_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    base_dir = Path(base_img_dir)
    
    # Define the correct capitalization for directories
    correct_dirs = {
        'haereum': 'Haereum',
        'kogift': 'Kogift',
        'naver': 'Naver',
        'other': 'Other'
    }
    
    # Create properly capitalized directories if they don't exist
    for _, correct_name in correct_dirs.items():
        correct_path = base_dir / correct_name
        correct_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {correct_path}")
    
    # Find and fix incorrectly capitalized directories
    for dir_path in base_dir.glob('*'):
        if not dir_path.is_dir():
            continue
            
        dir_name = dir_path.name
        dir_name_lower = dir_name.lower()
        
        # Check if this is an incorrectly cased version of our directories
        if dir_name_lower in correct_dirs and dir_name != correct_dirs[dir_name_lower]:
            correct_path = base_dir / correct_dirs[dir_name_lower]
            logger.info(f"Found incorrectly cased directory: {dir_path}")
            
            # Move files to the correctly cased directory
            for file_path in dir_path.glob('*'):
                if file_path.is_file():
                    target_path = correct_path / file_path.name
                    try:
                        # Copy instead of move to avoid permission issues
                        shutil.copy2(file_path, target_path)
                        logger.info(f"Copied file: {file_path} -> {target_path}")
                    except Exception as e:
                        logger.error(f"Error copying file {file_path}: {e}")
            
            # Only attempt to delete the directory if it's empty or we successfully moved all files
            try:
                # Remove the incorrectly cased directory if empty
                if not any(dir_path.glob('*')):
                    dir_path.rmdir()
                    logger.info(f"Removed empty directory: {dir_path}")
                else:
                    logger.warning(f"Cannot remove directory that still has files: {dir_path}")
            except Exception as e:
                logger.error(f"Error removing directory {dir_path}: {e}")

def update_env_variables():
    """
    Update environment variables to ensure all code uses the correct paths.
    """
    # Get config
    config = get_config()
    base_img_dir = config.get('Paths', 'image_main_dir', fallback='C:\\RPA\\Image\\Main')
    
    # Set environment variables with correct paths
    os.environ['RPA_IMAGE_DIR'] = str(Path(base_img_dir).parent)  # C:\RPA\Image
    os.environ['RPA_IMAGE_MAIN_DIR'] = str(Path(base_img_dir))    # C:\RPA\Image\Main
    os.environ['RPA_KOGIFT_DIR'] = str(Path(base_img_dir) / 'Kogift')
    os.environ['RPA_HAEREUM_DIR'] = str(Path(base_img_dir) / 'Haereum')
    os.environ['RPA_NAVER_DIR'] = str(Path(base_img_dir) / 'Naver')
    
    # Log the set environment variables
    logger.info(f"Set RPA_IMAGE_DIR = {os.environ['RPA_IMAGE_DIR']}")
    logger.info(f"Set RPA_IMAGE_MAIN_DIR = {os.environ['RPA_IMAGE_MAIN_DIR']}")
    logger.info(f"Set RPA_KOGIFT_DIR = {os.environ['RPA_KOGIFT_DIR']}")
    logger.info(f"Set RPA_HAEREUM_DIR = {os.environ['RPA_HAEREUM_DIR']}")
    logger.info(f"Set RPA_NAVER_DIR = {os.environ['RPA_NAVER_DIR']}")

def main():
    """Main function"""
    logger.info("Starting image path capitalization fix")
    
    # Fix directories
    fix_image_directories()
    
    # Update environment variables
    update_env_variables()
    
    logger.info("Image path capitalization fix complete")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 