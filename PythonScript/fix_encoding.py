#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging
import codecs
import argparse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def convert_file_to_utf8(file_path):
    """
    Convert file encoding from CP949 to UTF-8
    
    Args:
        file_path: Path to the file to convert
        
    Returns:
        bool: True if conversion successful, False otherwise
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
            
        # Try to detect encoding
        # First try UTF-8
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.info(f"File {file_path} is already in UTF-8 encoding")
                return True
        except UnicodeDecodeError:
            logger.info(f"File {file_path} is not in UTF-8 encoding, attempting CP949")
            
        # Try CP949 (commonly used encoding in Korean Windows)
        try:
            with open(file_path, 'r', encoding='cp949') as f:
                content = f.read()
                
            # Write back as UTF-8
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logger.info(f"Successfully converted {file_path} from CP949 to UTF-8")
            return True
        except UnicodeDecodeError:
            logger.warning(f"Could not decode {file_path} as CP949, trying with other encodings")
            
        # Try other common encodings
        encodings = ['euc-kr', 'latin1', 'iso-8859-1']
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read()
                    
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                    
                logger.info(f"Successfully converted {file_path} from {encoding} to UTF-8")
                return True
            except UnicodeDecodeError:
                continue
                
        logger.error(f"Could not convert {file_path} to UTF-8, no working encoding found")
        return False
    except Exception as e:
        logger.error(f"Error converting {file_path}: {str(e)}")
        return False

def main():
    # 현재 디렉토리 확인
    current_dir = os.getcwd()
    
    parser = argparse.ArgumentParser(description='Convert files to UTF-8 encoding')
    parser.add_argument('--path', '-p', help='Path to file or directory to convert', default=current_dir)
    parser.add_argument('--recursive', '-r', action='store_true', help='Process directories recursively')
    parser.add_argument('--file', '-f', help='Specific file to convert')
    args = parser.parse_args()
    
    target_path = args.path
    
    if args.file:
        # Convert a specific file
        file_path = os.path.join(target_path, args.file) if not os.path.isabs(args.file) else args.file
        if os.path.exists(file_path):
            convert_file_to_utf8(file_path)
        else:
            logger.error(f"File not found: {file_path}")
    else:
        # Check if target_path is a directory
        if os.path.isdir(target_path):
            # Define target file patterns (commonly having encoding issues)
            target_extensions = ['.ini', '.txt', '.csv', '.properties', '.cfg']
            
            if args.recursive:
                # Process directories recursively
                for root, dirs, files in os.walk(target_path):
                    for file in files:
                        _, ext = os.path.splitext(file)
                        if ext.lower() in target_extensions:
                            file_path = os.path.join(root, file)
                            convert_file_to_utf8(file_path)
            else:
                # Process only files in the target directory
                for file in os.listdir(target_path):
                    file_path = os.path.join(target_path, file)
                    if os.path.isfile(file_path):
                        _, ext = os.path.splitext(file)
                        if ext.lower() in target_extensions:
                            convert_file_to_utf8(file_path)
        else:
            # Single file
            convert_file_to_utf8(target_path)
    
    logger.info("Conversion process completed")

if __name__ == "__main__":
    main() 