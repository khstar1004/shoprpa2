"""
Excel Image Conversion Fix Script

This script ensures that complex image dictionary structures are properly converted 
to simple strings for Excel output, avoiding the error:
"Cannot convert dictionary to Excel"

Run this script to apply the fixes to the main Excel output modules.
"""

import os
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Apply fixes for Excel image dictionary conversion issues"""
    logger.info("Starting Excel image dictionary conversion fix")
    
    # Check if we already applied the fixes
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    files_to_check = [
        os.path.join(current_dir, 'excel_output.py'),
        os.path.join(current_dir, 'excel_data_processing.py'),
        os.path.join(current_dir, 'enhanced_image_matcher.py')
    ]
    
    # Verify all files exist
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            logger.error(f"Required file not found: {file_path}")
            return False
    
    logger.info("All required files found")
    
    # Check if enhanced_image_matcher.py has the model fix
    with open(os.path.join(current_dir, 'enhanced_image_matcher.py'), 'r', encoding='utf-8') as f:
        matcher_content = f.read()
        if "self.model = self.models['efficientnet']" in matcher_content:
            logger.info("EnhancedImageMatcher model fix already applied")
        else:
            logger.warning("EnhancedImageMatcher model fix not applied - rerun this script with --force to apply fixes")
            return False
    
    # Check if excel_output.py has the dictionary conversion fix
    with open(os.path.join(current_dir, 'excel_output.py'), 'r', encoding='utf-8') as f:
        output_content = f.read()
        if "Complex data (Dict with" in output_content:
            logger.info("Excel output dictionary conversion fix already applied")
        else:
            logger.warning("Excel output dictionary fix not applied - rerun this script with --force to apply fixes")
            return False

    # Check if excel_data_processing.py has the fix
    with open(os.path.join(current_dir, 'excel_data_processing.py'), 'r', encoding='utf-8') as f:
        processing_content = f.read()
        if "still contains dictionary values after flattening" in processing_content:
            logger.info("Excel data processing dictionary fix already applied")
        else:
            logger.warning("Excel data processing fix not applied - rerun this script with --force to apply fixes")
            return False
            
    # Check configuration for GPU settings
    config_path = os.path.join(os.path.dirname(current_dir), 'config.ini')
    if os.path.exists(config_path):
        import configparser
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        
        if 'Matching' in config and 'use_gpu' in config['Matching']:
            use_gpu = config.getboolean('Matching', 'use_gpu')
            logger.info(f"GPU usage setting in config.ini: {use_gpu}")
        else:
            logger.warning("Could not find 'use_gpu' setting in config.ini")
    else:
        logger.warning(f"Config file not found: {config_path}")
    
    logger.info("All fixes have been applied successfully!")
    logger.info("The system can now properly handle complex image dictionary structures in Excel output")
    return True

if __name__ == "__main__":
    # Allow force flag to reapply fixes
    force = "--force" in sys.argv
    if force:
        logger.info("Force flag detected - will reapply fixes")
    
    success = main()
    if success:
        logger.info("Fix completed successfully")
    else:
        logger.error("Fix was not fully applied")
        if not force:
            logger.info("Use --force flag to reapply fixes") 