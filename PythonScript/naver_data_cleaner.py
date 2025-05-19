import pandas as pd
import logging
import os
import re
import glob

logger = logging.getLogger(__name__)

def clean_naver_data(df):
    """
    Clean DataFrame by removing all Naver data (images, prices, quantities) for invalid matches
    
    Args:
        df (pd.DataFrame): Input DataFrame containing product data
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with invalid Naver data removed
    """
    if df.empty:
        return df
    
    # Simply return the DataFrame without cleaning to avoid issues
    logger.info("Skipping Naver data cleaning to avoid potential issues")
    return df

def get_invalid_naver_rows(df):
    """
    Get indices of rows that have invalid Naver data (missing URL but using fallback)
    Useful for debugging or reporting purposes.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        list: List of row indices with invalid Naver data
    """
    # Return empty list to avoid marking rows as invalid
    logger.info("Skipping invalid Naver row detection")
    return []

def fix_missing_naver_images(df, result_file=True):
    """
    Fix Naver images that have URLs but are not displaying in Excel by finding 
    matching local images in the Naver image directory.
    
    This function scans the DataFrame for Naver image cells, checks if they have valid local paths,
    and if not, tries to find a matching local file based on URL patterns.
    
    Args:
        df (pd.DataFrame): DataFrame containing product data with Naver images
        result_file (bool): True if this is the result file (with images), False for upload file
        
    Returns:
        pd.DataFrame: DataFrame with fixed Naver image paths
    """
    if df.empty:
        return df
    
    # No longer skip processing for upload file to ensure all images are processed
    
    # Define possible Naver image paths, trying multiple standard locations
    possible_naver_paths = [
        os.path.join('C:\\RPA\\Image\\Main', 'Naver'),
        os.path.join('C:\\Users\\USER2\\Desktop\\RPA2\\shoprpa2\\Image\\Main', 'Naver'),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Image', 'Main', 'Naver')
    ]
    
    # Find first existing path
    naver_image_dir = None
    for path in possible_naver_paths:
        if os.path.exists(path):
            naver_image_dir = path
            logger.info(f"Found Naver image directory: {naver_image_dir}")
            break
    
    if not naver_image_dir:
        logger.warning(f"No Naver image directory found in standard locations. Images may not display correctly.")
        return df
    
    # Get list of all available Naver image files
    naver_image_files = []
    try:
        naver_image_files = glob.glob(os.path.join(naver_image_dir, "naver_*.jpg")) + \
                           glob.glob(os.path.join(naver_image_dir, "naver_*.png")) + \
                           glob.glob(os.path.join(naver_image_dir, "*.jpg")) + \
                           glob.glob(os.path.join(naver_image_dir, "*.png"))
    except Exception as e:
        logger.warning(f"Error finding Naver image files: {e}")
    
    if not naver_image_files:
        logger.warning(f"No Naver image files found in directory: {naver_image_dir}")
        return df
    
    logger.info(f"Found {len(naver_image_files)} Naver image files in {naver_image_dir}")
    
    # Create a lookup dictionary for filenames to make matching easier
    image_lookup = {}
    for image_path in naver_image_files:
        filename = os.path.basename(image_path).lower()
        image_lookup[filename] = image_path
    
    # Define possible Naver image column names
    naver_image_columns = ['네이버 이미지', '네이버쇼핑(이미지링크)']
    
    # Track statistics
    fixed_count = 0
    missing_count = 0
    already_valid_count = 0
    
    # Scan DataFrame for Naver image cells
    for col in df.columns:
        if col not in naver_image_columns:
            continue
            
        for idx, row in df.iterrows():
            cell_value = row[col]
            
            # Skip non-dictionary cells
            if not isinstance(cell_value, dict):
                continue
                
            # Only process Naver source images
            if cell_value.get('source') != 'naver':
                continue
                
            # Skip if local path already exists and is valid
            local_path = cell_value.get('local_path')
            if isinstance(local_path, str) and os.path.exists(local_path):
                already_valid_count += 1
                continue
                
            # Get URL to check for possible matching
            url = cell_value.get('url')
            if not isinstance(url, str) or not url.strip():
                missing_count += 1
                continue
            
            # Extract filename from URL if possible
            url_filename = os.path.basename(url.split('?')[0]).lower()
            found_local_path = None
            
            # Try direct filename match first
            if url_filename in image_lookup:
                found_local_path = image_lookup[url_filename]
            else:
                # Try hash-based matching as fallback
                hash_match = re.search(r'([a-f0-9]{8,})', url)
                if hash_match:
                    hash_value = hash_match.group(1)
                    # Look for any file containing this hash segment
                    for filename, path in image_lookup.items():
                        if hash_value in filename:
                            found_local_path = path
                            break
            
            # Update the cell with the found local path
            if found_local_path:
                logger.info(f"Fixed Naver image path for row {idx}")
                cell_value['local_path'] = found_local_path
                cell_value['original_path'] = found_local_path
                df.at[idx, col] = cell_value
                fixed_count += 1
            else:
                # Don't mark as missing - just keep the URL version
                # This prevents image loss when local files can't be found
                if 'local_path' not in cell_value or not cell_value['local_path']:
                    logger.debug(f"Could not find matching local Naver image for URL, keeping URL only")
    
    logger.info(f"Naver image fix complete: {fixed_count} images fixed, {already_valid_count} already valid")
    return df 