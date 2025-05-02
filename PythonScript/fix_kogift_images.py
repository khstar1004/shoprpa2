#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Fix Kogift Images in Excel Files
--------------------------------
This script fixes issues with Kogift images in Excel files by:
1. Scanning for and finding local image files that match URLs
2. Replacing URL text with actual embedded images in Excel
3. Preserving hyperlinks to the original URLs

Usage:
    python fix_kogift_images.py --input [input_excel_file] --output [output_excel_file]
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fix_kogift_images.log')
    ]
)
logger = logging.getLogger('fix_kogift_images')

def main():
    """Standalone script to fix Kogift images in Excel files"""
    parser = argparse.ArgumentParser(description='Fix Kogift images in Excel files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output Excel file path (optional)')
    parser.add_argument('--scan-dirs', '-d', nargs='+', help='Additional image directories to scan')
    
    args = parser.parse_args()
    
    # Validate input file
    input_file = args.input
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return 1
    
    # Set output file if not specified
    output_file = args.output
    if not output_file:
        base_name = os.path.basename(input_file)
        file_name, ext = os.path.splitext(base_name)
        output_file = os.path.join(os.path.dirname(input_file), f"{file_name}_fixed{ext}")
    
    logger.info(f"Starting Kogift image fix process")
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file}")
    
    # Import kogift_image_fix here to avoid adding dependencies to this script
    try:
        sys.path.append(str(Path(__file__).resolve().parent))
        from kogift_image_fix import fix_excel_kogift_images
        
        # Call the fix function with additional scan directories if provided
        result = fix_excel_kogift_images(input_file, output_file)
        
        if result:
            logger.info(f"Successfully fixed Kogift images. Output saved to: {result}")
            print(f"✅ Successfully fixed Kogift images in Excel file.")
            print(f"✅ Output saved to: {result}")
            return 0
        else:
            logger.error("Failed to fix Kogift images")
            print("❌ Failed to fix Kogift images. Check the log for details.")
            return 1
            
    except ImportError:
        logger.error("Failed to import kogift_image_fix module. Make sure it's in the same directory.")
        print("❌ Failed to import necessary modules. Make sure kogift_image_fix.py is in the same directory.")
        return 1
    except Exception as e:
        logger.error(f"Error fixing Kogift images: {e}")
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 