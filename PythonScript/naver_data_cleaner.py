import pandas as pd
import logging
import os
import re
import glob
import configparser
from pathlib import Path

logger = logging.getLogger(__name__)

def get_config(config_path='../config.ini'):
    """Load configuration from config.ini file."""
    script_dir = Path(__file__).parent
    paths_to_try = [
        script_dir / config_path,
        script_dir.parent / 'config.ini'
    ]
    
    conf = configparser.ConfigParser()
    loaded_path = None
    for p_try in paths_to_try:
        if p_try.exists():
            conf.read(p_try, encoding='utf-8')
            loaded_path = p_try
            break
            
    if not loaded_path:
        project_root_config = Path(os.getcwd()).parent / 'config.ini'
        if project_root_config.exists():
             conf.read(project_root_config, encoding='utf-8')
             loaded_path = project_root_config
        else: 
            default_config_path = Path('config.ini')
            if default_config_path.exists():
                conf.read(default_config_path, encoding='utf-8')
                loaded_path = default_config_path
            else:
                 logger.error(f"Config file not found at {paths_to_try} or {project_root_config} or {default_config_path}")
                 raise FileNotFoundError(f"Config file not found.")
    logger.info(f"Loaded config from: {loaded_path}")
    return conf

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

    # Define Naver-related column names
    naver_image_columns = ['네이버 이미지', '네이버쇼핑(이미지링크)']
    naver_price_columns = ['판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)', '네이버 공급사명', '네이버 링크']
    
    # Track changes for logging
    rows_missing_image = 0
    rows_missing_price = 0
    rows_filtered = 0
    rows_fully_processed = 0

    # Process each row in the DataFrame
    for idx, row in df.iterrows():
        has_valid_naver_image = False
        has_naver_price_data = False
        
        # Check if any Naver image column has valid data
        for col in naver_image_columns:
            if col in df.columns and pd.notna(row.get(col)) and row.get(col) != '-':
                cell_value = row[col]
                
                # Check if it has an actual URL
                if isinstance(cell_value, dict) and 'url' in cell_value:
                    url = cell_value.get('url')
                    if isinstance(url, str) and url.strip():
                        has_valid_naver_image = True
                        break
                elif isinstance(cell_value, str) and cell_value.strip() and cell_value != '-':
                    has_valid_naver_image = True
                    break

        # Check if any Naver price column has data
        for col in naver_price_columns:
            if col in df.columns and pd.notna(row.get(col)) and row.get(col) != '-':
                has_naver_price_data = True
                break
        
        # Filter case: No valid Naver image but has price data
        if not has_valid_naver_image:
            # Clear all Naver price data
            for col in naver_price_columns:
                if col in df.columns:
                    df.at[idx, col] = '-'
            
            if has_naver_price_data:
                rows_filtered += 1
        
        # Track statistics
        if not has_valid_naver_image and has_naver_price_data:
            rows_missing_image += 1
        if has_valid_naver_image and not has_naver_price_data:
            rows_missing_price += 1
        if has_valid_naver_image and has_naver_price_data:
            rows_fully_processed += 1

    logger.info(f"Naver data analysis: {rows_missing_image} rows missing image URLs, {rows_missing_price} rows missing price data, {rows_fully_processed} rows with complete data")
    logger.info(f"Filtered {rows_filtered} rows with price data but no Naver image URL")
    
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
    # logger.info("Skipping invalid Naver row detection") # Keep this disabled for now
    logger.info("get_invalid_naver_rows called. Currently, it returns an empty list.")
    return []

def fix_missing_naver_images(df, result_file=True, config_obj=None):
    """
    Fix Naver images that have URLs but are not displaying in Excel by finding 
    matching local images in the Naver image directory.
    
    This function scans the DataFrame for Naver image cells, checks if they have valid local paths,
    and if not, tries to find a matching local file based on URL patterns.
    
    Args:
        df (pd.DataFrame): DataFrame containing product data with Naver images
        result_file (bool): True if this is the result file (with images), False for upload file
        config_obj (configparser.ConfigParser): Config object to use for Naver image directory
        
    Returns:
        pd.DataFrame: DataFrame with fixed Naver image paths
    """
    if df.empty:
        return df
    
    # No longer skip processing for upload file to ensure all images are processed
    
    if config_obj:
        config = config_obj
    else:
        try:
            config = get_config()
        except FileNotFoundError:
            logger.error("fix_missing_naver_images: Config file not found. Cannot determine Naver image directory.")
            return df # Return df as is if config cannot be loaded

    # Define possible Naver image paths, trying multiple standard locations
    image_main_dir_str = config.get('Paths', 'image_main_dir', fallback='C:\\\\RPA\\\\Image\\\\Main')
    user_home_dir_str = config.get('Paths', 'user_home_dir', fallback=str(Path.home())) # Get user home if specified
    
    # Construct paths using Path objects for reliability
    base_naver_image_dir = Path(image_main_dir_str) / 'Naver'
    
    # Alternative path based on a common user structure if needed (example)
    # desktop_rpa_path = Path(user_home_dir_str) / 'Desktop' / 'RPA2' / 'shoprpa2' / 'Image' / 'Main' / 'Naver'
    # script_relative_path = Path(__file__).resolve().parent.parent / 'Image' / 'Main' / 'Naver'
    
    possible_naver_paths = [
        base_naver_image_dir,
        # desktop_rpa_path, # Example, can be enabled if this is a common structure
        # script_relative_path # Relative to script location
    ]
    
    # Add paths from the original hardcoded list if they are different and might be relevant as fallbacks
    original_hardcoded_paths = [
        Path('C:\\\\RPA\\\\Image\\\\Main') / 'Naver', # Already covered by base_naver_image_dir if config matches
        Path('C:\\\\Users\\\\USER2\\\\Desktop\\\\RPA2\\\\shoprpa2\\\\Image\\\\Main') / 'Naver',
        Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / 'Image' / 'Main' / 'Naver' # Script relative, similar to script_relative_path
    ]
    for p in original_hardcoded_paths:
        if p not in possible_naver_paths:
            possible_naver_paths.append(p)

    # Find first existing path
    naver_image_dir = None
    for path in possible_naver_paths:
        if os.path.exists(path):
            naver_image_dir = path
            logger.info(f"Found Naver image directory: {naver_image_dir}")
            break
    
    if not naver_image_dir:
        logger.warning(f"No Naver image directory found in standard locations: {possible_naver_paths}. Images may not display correctly.")
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