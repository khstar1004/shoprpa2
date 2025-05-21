import pandas as pd
import logging
import configparser
import os
import openpyxl # Required for reading/writing .xlsx
import re
import aiohttp
import asyncio
from urllib.parse import urljoin
import datetime

# Import the naver_data_cleaner function with fallback options
try:
    from .naver_data_cleaner import clean_naver_data
except ImportError:
    try:
        # Try absolute import if relative import fails
        from PythonScript.naver_data_cleaner import clean_naver_data
    except ImportError:
        # Define a fallback function if import fails
        def clean_naver_data(df):
            logging.warning("Could not import clean_naver_data - using fallback implementation")
            if df.empty:
                return df
            
            # Simple fallback implementation
            naver_image_column = '네이버쇼핑(이미지링크)'
            naver_price_columns = ['네이버 기본수량', '판매단가3 (VAT포함)', '네이버 가격차이', '네이버가격차이(%)', '네이버 공급사명', '네이버 링크']
            
            if naver_image_column in df.columns:
                for idx, row in df.iterrows():
                    cell_value = row.get(naver_image_column)
                    if pd.isna(cell_value) or cell_value == '' or cell_value == '-':
                        for col in naver_price_columns:
                            if col in df.columns:
                                df.at[idx, col] = '-'
            return df

# Define expected column names from the final Excel output
# Adjust these if the actual column names in the generated Excel differ
KOREAGIFT_LINK_COL = '고려 링크'
NAVER_LINK_COL = '네이버 링크'
# Price difference columns to filter
KOREAGIFT_PRICE_DIFF_COL = '고려 가격차이'
NAVER_PRICE_DIFF_COL = '네이버 가격차이'
# Optional: Add other columns to check if links alone are insufficient
# KOREAGIFT_PRICE_COL = '판매단가2(VAT포함)'
# NAVER_PRICE_COL = '판매단가3 (VAT포함)'

async def check_image_url_exists(url, session=None):
    """Check if an image URL actually exists by performing a HEAD request.
    Returns True if the URL exists, False otherwise.
    """
    if not url:
        return False
    
    close_session = False
    try:
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.jclgift.com/'
        }
        
        async with session.head(url, headers=headers, timeout=5, ssl=False) as response:
            return response.status == 200
    except Exception as e:
        logging.debug(f"Error checking URL {url}: {e}")
        return False
    finally:
        if close_session and session:
            await session.close()

async def convert_haereum_url(url):
    """Convert Korean text Haereum URLs to proper product code URLs.
    Tries different formats (jpg, png) and returns the first working URL.
    """
    if not url or not isinstance(url, str):
        return None
        
    if '/upload/product/simg3/' not in url:
        return url  # Not a Haereum product URL
        
    # If URL already contains product code format (capital letters followed by numbers), keep it
    if re.search(r'\/[A-Z]{4}\d{7}', url):
        return url
    
    # For Korean text URLs, try standard product code patterns with different extensions
    base_url = 'https://www.jclgift.com'
    async with aiohttp.ClientSession() as session:
        # Extract the Korean keyword from the URL
        korean_text = url.split('/')[-1].split('.')[0]
        logging.info(f"Converting Korean URL for product: {korean_text}")
        
        # First try the pattern from successful image downloads in logs
        # These are actual product codes seen in successful downloads
        product_codes = [
            "DDAC0007399s", "CCCH0008576s_3", "BBCA0009349s", 
            "CCBK0001873s", "LLAC0009357s"
        ]
        
        # Try with different extensions
        for code in product_codes:
            for ext in ['.jpg', '.png', '.jpeg']:
                test_url = urljoin(base_url, f"/upload/product/simg3/{code}{ext}")
                
                if await check_image_url_exists(test_url, session):
                    logging.info(f"Korean URL converted using known product code: {url} → {test_url}")
                    return test_url
        
        # Try more systematically with brand codes and number ranges
        brand_codes = ["DDAC", "BBCA", "CCCH", "AACJ", "CCBK", "LLAC", "GGBJ", "AAZZ", "EEBB"]
        for prefix in brand_codes:
            # Try different number ranges - checking specific ranges
            for num_range in [range(7000, 9500, 100), range(1000, 3000, 100)]:
                for num in num_range:
                    # Try both with and without size suffix
                    for suffix in ["s", ""]:
                        # Try different extensions
                        for ext in ['.jpg', '.png', '.jpeg']:
                            product_code = f"{prefix}{str(num).zfill(7)}{suffix}"
                            test_url = urljoin(base_url, f"/upload/product/simg3/{product_code}{ext}")
                            
                            # Use head request to quickly check if URL exists
                            if await check_image_url_exists(test_url, session):
                                logging.info(f"Korean URL converted using systematic search: {url} → {test_url}")
                                return test_url
        
        # If no pattern matches found, try checking if original URL with different extension works
        url_without_ext = url.rsplit('.', 1)[0]
        for ext in ['.jpg', '.png', '.jpeg']:
            test_url = f"{url_without_ext}{ext}"
            if await check_image_url_exists(test_url, session):
                logging.info(f"Extension changed: {url} → {test_url}")
                return test_url
    
    logging.warning(f"Could not find working URL alternative for: {url}")
    # Return original if no working alternatives found
    return url

def _is_data_missing(series: pd.Series) -> pd.Series:
    """Checks if data in a pandas Series is missing.
    Considers '-', '', None, NaN as missing.
    """
    # Using vectorized operations for efficiency
    is_missing = series.isna() | series.isin(['-', ''])
    return is_missing

def filter_upload_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters DataFrame based on configurable criteria:
    1. Only keep rows with price differences < -1 (more negative)
    2. Only removes rows where BOTH Koreagift and Naver data are missing
    3. Uses a configurable price difference threshold (default -0.5%)
    4. Still keeps rows with valid data even if they have front URLs
    5. Removes rows where company image (해오름) is missing, empty or just "-"
    
    The price threshold can be adjusted by setting filter_upload_data._price_threshold 
    before calling the function.
    """
    # Get the current threshold setting or use default
    price_threshold = getattr(filter_upload_data, '_price_threshold', -0.5)
    
    # Fixed threshold to filter out rows with price difference >= -1
    FIXED_PRICE_THRESHOLD = -1.0
    
    if df.empty:
        logging.warning("Input DataFrame for upload filtering is empty. Returning empty DataFrame.")
        return df.copy() # Return a copy

    # Check if necessary link columns exist
    required_cols = [KOREAGIFT_LINK_COL, NAVER_LINK_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.warning(f"Required columns for upload filtering missing: {missing_cols}. Using available columns for filtering.")
        # Continue with available columns rather than skip completely
    
    # Make a copy to avoid modifying the original
    filtered_df = df.copy()
    initial_rows = len(filtered_df)
    
    # --- 1. Filter based on missing links ---
    # Determine missing status based on link columns
    kogift_col = KOREAGIFT_LINK_COL if KOREAGIFT_LINK_COL in filtered_df.columns else None
    naver_col = NAVER_LINK_COL if NAVER_LINK_COL in filtered_df.columns else None
    
    is_kogift_missing = pd.Series(True, index=filtered_df.index)  # Default to True if column missing
    is_naver_missing = pd.Series(True, index=filtered_df.index)   # Default to True if column missing
    
    if kogift_col:
        is_kogift_missing = _is_data_missing(filtered_df[kogift_col])
    if naver_col:
        is_naver_missing = _is_data_missing(filtered_df[naver_col])
    
    # Identify rows where BOTH sources are missing - this is the primary filter
    missing_links_mask = is_kogift_missing & is_naver_missing
    
    # --- 2. Price difference filtering with strict threshold (-1) ---
    # Log the current price threshold
    logging.info(f"Using price difference threshold: < {FIXED_PRICE_THRESHOLD} (more negative)")
    
    # Initialize masks for rows to keep - rows must have price difference < -1 to be included
    kogift_price_valid = pd.Series(False, index=filtered_df.index)
    naver_price_valid = pd.Series(False, index=filtered_df.index)
    
    # Only apply price filtering if we have both price column and link column
    if KOREAGIFT_PRICE_DIFF_COL in filtered_df.columns and kogift_col:
        # Convert to numeric, coercing errors to NaN
        kogift_price_diff = pd.to_numeric(filtered_df[KOREAGIFT_PRICE_DIFF_COL], errors='coerce')
        
        # Keep rows that have:
        # 1. Not missing Kogift link AND
        # 2. Have a valid price difference value AND
        # 3. Have a price difference < -1 (more negative than -1)
        kogift_price_valid = (
            ~is_kogift_missing &
            kogift_price_diff.notna() &
            (kogift_price_diff < FIXED_PRICE_THRESHOLD)
        )
    
    # Same for Naver
    if NAVER_PRICE_DIFF_COL in filtered_df.columns and naver_col:
        naver_price_diff = pd.to_numeric(filtered_df[NAVER_PRICE_DIFF_COL], errors='coerce')
        naver_price_valid = (
            ~is_naver_missing &
            naver_price_diff.notna() &
            (naver_price_diff < FIXED_PRICE_THRESHOLD)
        )
    
    # --- 3. Count front URLs but don't filter them out ---
    front_url_mask = pd.Series(False, index=filtered_df.index)
    if '네이버쇼핑(이미지링크)' in filtered_df.columns:
        # Create a mask to detect URLs with "front" pattern to count them
        front_url_mask = filtered_df['네이버쇼핑(이미지링크)'].astype(str).str.contains(
            'pstatic.net/front/', case=False, na=False
        )
        front_url_count = front_url_mask.sum()
        if front_url_count > 0:
            logging.warning(f"Found {front_url_count} rows with 'front/' URLs, but keeping them in the data.")
    
    # --- 4. NEW: Filter out rows where company image is missing, empty, or just "-" ---
    company_img_col = '해오름(이미지링크)'  # Upload file column name
    
    # For result file, column names are different
    if company_img_col not in filtered_df.columns and '본사 이미지' in filtered_df.columns:
        company_img_col = '본사 이미지'
    
    empty_company_img_mask = pd.Series(True, index=filtered_df.index)  # Default to True if column missing
    
    if company_img_col in filtered_df.columns:
        # Check if company image is empty or just "-"
        empty_company_img_mask = filtered_df[company_img_col].isna() | (filtered_df[company_img_col] == '') | (filtered_df[company_img_col] == '-')
        
        # Count rows to be removed due to this condition
        rows_to_remove_empty_company = empty_company_img_mask.sum()
        if rows_to_remove_empty_company > 0:
            logging.info(f"Will remove {rows_to_remove_empty_company} rows where company image is missing, empty, or just '-'")
    
    # Combine filtering conditions:
    # 1. Remove rows where both Kogift and Naver links are missing
    # 2. Keep rows that have EITHER Kogift price < -1 OR Naver price < -1
    # 3. Remove rows where company image is missing, empty, or just "-"
    rows_to_keep_mask = (
        # Don't keep row if both links are missing
        (~missing_links_mask) &
        # AND only keep row if either Kogift or Naver has a price difference < -1
        (kogift_price_valid | naver_price_valid) &
        # AND don't keep row if company image is missing, empty, or just "-"
        ~empty_company_img_mask
    )
    
    # Apply the filter
    filtered_df = filtered_df[rows_to_keep_mask].copy()
    
    # Log filtering results
    removed_count = initial_rows - len(filtered_df)
    if removed_count > 0:
        logging.info(f"Upload filter: Removed {removed_count} rows total:")
        logging.info(f"  - {missing_links_mask.sum()} rows identified with both Koreagift and Naver links missing")
        logging.info(f"  - {(~kogift_price_valid & ~is_kogift_missing).sum()} rows with Koreagift price difference >= {FIXED_PRICE_THRESHOLD}")
        logging.info(f"  - {(~naver_price_valid & ~is_naver_missing).sum()} rows with Naver price difference >= {FIXED_PRICE_THRESHOLD}")
        if company_img_col in filtered_df.columns:
            logging.info(f"  - {empty_company_img_mask.sum()} rows with missing, empty, or '-' company image")
        logging.info(f"  - {front_url_mask.sum()} rows had 'front/' URLs (but were not removed for this reason)")
    else:
        logging.info("Upload filter: No rows removed.")
    
    return filtered_df

def filter_front_urls_from_upload_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes rows with unreliable "front" URLs in the Naver image column.
    Instead of removing these URLs, it replaces them with placeholder values.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with processed front URLs
    """
    if df.empty:
        logging.warning("Input DataFrame for front URL processing is empty. Returning empty DataFrame.")
        return df.copy()
    
    # Check if the Naver image column exists
    naver_img_col = '네이버쇼핑(이미지링크)'
    if naver_img_col not in df.columns:
        logging.warning(f"Column '{naver_img_col}' not found in DataFrame. Cannot process front URLs.")
        return df.copy()
    
    # Create a copy of the DataFrame
    result_df = df.copy()
    
    # Process each value to handle complex structures
    front_url_count = 0
    placeholder_url = "https://placeholder-image.com/product_image.jpg"
    
    for idx, value in enumerate(result_df[naver_img_col]):
        processed = False
        
        # Handle dictionary format
        if isinstance(value, dict) and 'url' in value:
            url = value.get('url')
            if url and isinstance(url, str) and 'pstatic.net/front/' in url.lower():
                # Keep the dictionary structure but replace the problematic URL
                value['original_url'] = url  # Save original URL
                value['url'] = placeholder_url
                result_df.at[idx, naver_img_col] = value
                front_url_count += 1
                processed = True
        
        # Handle string format
        elif isinstance(value, str) and 'pstatic.net/front/' in value.lower():
            # Create a dictionary with both original and placeholder URL
            result_df.at[idx, naver_img_col] = {
                'url': placeholder_url,
                'original_url': value
            }
            front_url_count += 1
            processed = True
            
        if processed:
            logging.debug(f"Row {idx}: Replaced front URL with placeholder value")
    
    if front_url_count > 0:
        logging.info(f"Processed {front_url_count} 'front/' URLs by replacing them with placeholders")
    else:
        logging.info("No 'front/' URLs found in the data")
    
    return result_df

def apply_filter_to_upload_excel(upload_file_path: str, config: configparser.ConfigParser) -> bool:
    """Reads the upload Excel, applies filtering with configurable strictness, and saves it back, overwriting the original.
    
    This function uses different filtering approaches based on the 'upload_filter_strictness' setting in the Debug section:
    - 'high': Uses strict filtering that may remove more rows
    - 'medium': Uses balanced filtering (default)
    - 'low': Uses lenient filtering to keep more products in the output
    - 'none': No filtering, just processes and saves the file

    Args:
        upload_file_path: Path to the upload .xlsx file.
        config: The configuration object

    Returns:
        True if filtering was applied successfully, False otherwise.
    """
    if not upload_file_path or not isinstance(upload_file_path, str):
        logging.error("Upload file path is invalid. Skipping filtering.")
        return False
    if not os.path.exists(upload_file_path):
        logging.error(f"Upload file does not exist: {upload_file_path}. Skipping filtering.")
        return False

    # Determine filtering strictness from config
    filter_strictness = 'medium'  # Default to medium strictness
    try:
        if config.has_section('Debug'):
            filter_strictness = config.get('Debug', 'upload_filter_strictness', fallback='medium').lower()
            logging.info(f"Using upload filter strictness level: {filter_strictness}")
    except Exception as e:
        logging.warning(f"Could not read filter strictness from config: {e}. Using default 'medium'.")
    
    logging.info(f"Applying {filter_strictness} strictness post-creation filter to upload file: {upload_file_path}")
    
    try:
        # Read the Excel file into a DataFrame
        import pandas as pd
        df = pd.read_excel(upload_file_path, engine='openpyxl')
        
        # Record original row count
        original_rows = len(df)
        
        if original_rows == 0:
            logging.warning(f"Upload file is empty: {upload_file_path}")
            # Save empty DataFrame back to file to ensure consistent format
            df.to_excel(upload_file_path, index=False, engine='openpyxl')
            return True
        
        # Apply filtering based on strictness setting
        if filter_strictness == 'none':
            logging.info("Upload filtering is disabled (strictness=none). No rows will be filtered.")
            df_filtered = df.copy()
        else:
            # We no longer need to adjust price thresholds here since the filter_upload_data function
            # now uses a fixed FIXED_PRICE_THRESHOLD of -1.0
            # Just apply the filter directly
            logging.info("Applying filtering - only keeping rows with price differences < -1")
            df_filtered = filter_upload_data(df)
        
        # Process front URLs without removing rows
        df_filtered = filter_front_urls_from_upload_data(df_filtered)
        
        # Apply clean_naver_data to ensure no price data exists for rows without Naver images
        logging.info("Applying Naver data cleaning to ensure price data is cleared for rows without images")
        df_filtered = clean_naver_data(df_filtered)
        
        # Calculate rows removed
        rows_removed = original_rows - len(df_filtered)
        
        if rows_removed > 0:
            logging.info(f"{filter_strictness.capitalize()} upload filtering removed {rows_removed} rows in total. Original: {original_rows}, Final: {len(df_filtered)}")
            
            # Calculate the percentage of remaining rows
            keep_percentage = (len(df_filtered) / original_rows) * 100
            if keep_percentage < 25 and original_rows > 5:
                # If less than 25% of rows remain and we started with a significant number
                logging.warning(f"Warning: Filtering removed {100-keep_percentage:.1f}% of rows! This may indicate an issue with the data.")
                
                # For extreme cases (>90% removed), add some rows back
                if keep_percentage < 10 and original_rows > 5:
                    logging.warning("Extreme filtering detected! Adding back some rows to prevent empty result.")
                    # Take a sample of filtered rows to add back
                    filtered_indices = set(df.index) - set(df_filtered.index)
                    add_back_count = min(5, len(filtered_indices))
                    if add_back_count > 0:
                        # Add back some of the best rows from filtered set
                        # This is a simplistic approach - in practice you'd want more sophisticated selection
                        add_back_indices = list(filtered_indices)[:add_back_count]
                        add_back_rows = df.loc[add_back_indices].copy()
                        df_filtered = pd.concat([df_filtered, add_back_rows])
                        logging.info(f"Added back {len(add_back_rows)} rows to prevent empty results.")
        else:
            logging.info("No rows were removed during upload filtering.")
        
        # Add comment about filtering process to the DataFrame
        # (Not actually adding to the DataFrame, just logging)
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Upload file filtered at {timestamp}. Original rows: {original_rows}, Final rows: {len(df_filtered)}")

        # Save the filtered data back to the same file path, overwriting it.
        # Use 'openpyxl' engine to ensure compatibility with .xlsx format features.
        df_filtered.to_excel(upload_file_path, index=False, engine='openpyxl')
        
        logging.info(f"Successfully filtered and overwrote upload file: {upload_file_path}")
        return True
    except FileNotFoundError:
         logging.error(f"Upload file not found during filtering read: {upload_file_path}")
         return False
    except ImportError:
         logging.error("`openpyxl` is required to read/write Excel .xlsx files. Please install it (`pip install openpyxl`).")
         return False
    except Exception as e:
        logging.error(f"Error filtering or saving upload Excel file {upload_file_path}: {e}", exc_info=True)
        return False

async def test_haereum_urls(urls):
    """Test function to check and fix Haereum URLs.
    
    Args:
        urls: List of URLs to test and fix
        
    Returns:
        Dictionary mapping original URLs to fixed URLs
    """
    results = {}
    valid_count = 0
    
    print(f"Testing {len(urls)} Haereum URLs...")
    print("-" * 80)
    
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Testing: {url}")
        fixed_url = await convert_haereum_url(url)
        results[url] = fixed_url
        
        # Check if the fixed URL is valid
        is_valid = await check_image_url_exists(fixed_url)
        status = "✅ VALID" if is_valid else "❌ INVALID"
        if is_valid:
            valid_count += 1
            
        print(f"{status}: {url}")
        print(f"→ {fixed_url}")
        print("-" * 80)
    
    print(f"\nSummary: {valid_count}/{len(urls)} URLs valid after conversion")
    return results

async def fix_haereum_urls_in_excel(excel_path, config=None):
    """Fix Haereum URLs with Korean text in a single Excel file.
    
    Args:
        excel_path: Path to the Excel file
        config: ConfigParser object (optional)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info(f"Processing Excel file: {excel_path}")
        
        # Read the Excel file
        df = pd.read_excel(excel_path, engine='openpyxl')
        
        if KOREAGIFT_LINK_COL not in df.columns:
            logging.warning(f"No Haereum link column found in {excel_path}")
            return False
            
        # Find rows with Korean text in Haereum URLs
        korean_url_mask = df[KOREAGIFT_LINK_COL].astype(str).str.contains(
            r'/upload/product/simg3/.*[가-힣].*\.gif', 
            regex=True, 
            na=False
        )
        
        korean_url_count = korean_url_mask.sum()
        if korean_url_count == 0:
            logging.info(f"No Korean Haereum URLs found in {excel_path}")
            return True
            
        logging.info(f"Found {korean_url_count} Haereum URLs with Korean text that need fixing")
        
        # Get the URLs that need fixing
        korean_urls = df.loc[korean_url_mask, KOREAGIFT_LINK_COL].tolist()
        
        # Process the URLs
        results = {}
        for url in korean_urls:
            fixed_url = await convert_haereum_url(url)
            results[url] = fixed_url
        
        # Update the DataFrame
        fixed_count = 0
        for old_url, new_url in results.items():
            if old_url != new_url and new_url is not None:
                logging.info(f"Replacing URL: {old_url} → {new_url}")
                df[KOREAGIFT_LINK_COL] = df[KOREAGIFT_LINK_COL].replace(old_url, new_url)
                fixed_count += 1
        
        # Save the updated DataFrame
        df.to_excel(excel_path, index=False, engine='openpyxl')
        logging.info(f"Fixed {fixed_count} URLs in {excel_path}")
        
        return True
    except Exception as e:
        logging.error(f"Error processing {excel_path}: {e}")
        return False

async def batch_fix_haereum_urls(directory, pattern="*.xlsx", config=None):
    """Fix Haereum URLs with Korean text in all Excel files in a directory.
    
    Args:
        directory: Directory containing Excel files
        pattern: File pattern to match (default: "*.xlsx")
        config: ConfigParser object (optional)
    
    Returns:
        Dict with results for each file
    """
    import glob
    import os
    
    results = {}
    
    # Find all Excel files in the directory
    excel_files = glob.glob(os.path.join(directory, pattern))
    
    if not excel_files:
        logging.warning(f"No Excel files found in {directory} matching pattern {pattern}")
        return results
    
    logging.info(f"Found {len(excel_files)} Excel files to process")
    
    # Process each file
    for excel_path in excel_files:
        success = await fix_haereum_urls_in_excel(excel_path, config)
        results[excel_path] = success
    
    # Print summary
    success_count = sum(1 for success in results.values() if success)
    logging.info(f"Successfully processed {success_count}/{len(excel_files)} Excel files")
    
    return results

def show_help():
    """Display help information for using this tool."""
    print("\nHaereum URL Converter Tool")
    print("========================\n")
    print("This tool helps fix Korean text URLs from Haereum by converting them to")
    print("proper product code URLs with supported file extensions.\n")
    print("Usage:")
    print("  python upload_filter.py                        - Test default URLs")
    print("  python upload_filter.py <url1> <url2> ...      - Test specific URLs")
    print("  python upload_filter.py --batch <directory>    - Process all Excel files in directory")
    print("  python upload_filter.py --batch <directory> <pattern>")
    print("                                                 - Process Excel files matching pattern")
    print("  python upload_filter.py --help                 - Show this help message\n")
    print("Examples:")
    print("  python upload_filter.py --batch C:\\RPA\\Excel")
    print("  python upload_filter.py --batch C:\\RPA\\Excel \"*Product*.xlsx\"\n")

if __name__ == "__main__":
    # This will run if the script is executed directly
    import sys
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Example URLs to test (or use command line arguments)
    test_urls = [
        "https://www.jclgift.com/upload/product/simg3/페이즐리반다나손수건스카프54cm대형.gif",
        "https://www.jclgift.com/upload/product/simg3/메쉬가방중형32X33X12비치가방망사.gif",
        "https://www.jclgift.com/upload/product/simg3/손톱깎이세트선물세트네일세트12p.gif",
        "https://www.jclgift.com/upload/product/simg3/고급3단자동양우산10k.gif",
        "https://www.jclgift.com/upload/product/simg3/메쉬가방대형비치백망사가방비치가방43X.gif"
    ]
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ["--help", "-h", "/?"]:
            show_help()
        elif sys.argv[1] == "--batch" and len(sys.argv) > 2:
            # Batch process all Excel files in the specified directory
            directory = sys.argv[2]
            pattern = sys.argv[3] if len(sys.argv) > 3 else "*.xlsx"
            print(f"Batch processing Excel files in {directory} matching {pattern}")
            asyncio.run(batch_fix_haereum_urls(directory, pattern))
        else:
            # Use command line arguments as URLs to test
            test_urls = sys.argv[1:]
            print(f"Starting URL conversion test for {len(test_urls)} Haereum URLs...")
            asyncio.run(test_haereum_urls(test_urls))
    else:
        # Run the test with default URLs
        print(f"Starting URL conversion test for {len(test_urls)} Haereum URLs...")
        asyncio.run(test_haereum_urls(test_urls)) 