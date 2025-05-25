import os
import logging
import pandas as pd
import traceback

def clear_kogift_data_if_missing_image(df):
    """
    Clear Kogift-related data in rows where the '고려기프트 이미지' column is empty or contains placeholder values.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame: Modified DataFrame with cleared Kogift data where appropriate
    """
    try:
        # Make a copy to avoid modifying the original DataFrame
        modified_df = df.copy()
        
        # Check if the Kogift image column exists
        if '고려기프트 이미지' not in modified_df.columns:
            logging.warning("Column '고려기프트 이미지' not found in DataFrame. Skipping Kogift data clearing.")
            return modified_df
            
        # Define placeholder values that indicate empty Kogift image
        placeholder_values = [None, '', '-', 'nan', 'NaN', 'none', 'None']
        
        # Define columns to clear when Kogift image is missing
        kogift_columns = [
            '기본수량(2)',
            '판매가(V포함)(2)',
            '가격차이(2)',
            '가격차이(2)(%)',
            '고려기프트 상품링크'
        ]
        
        # Filter columns that actually exist in the DataFrame
        existing_columns = [col for col in kogift_columns if col in modified_df.columns]
        
        if not existing_columns:
            logging.warning(f"None of the Kogift data columns exist in the DataFrame. Skipping Kogift data clearing.")
            return modified_df
            
        missing_columns = [col for col in kogift_columns if col not in modified_df.columns]
        if missing_columns:
            logging.warning(f"Some Kogift data columns are missing: {missing_columns}")
        
        # Count rows before filtering
        original_count = len(modified_df)
        
        # Create a mask for rows where Kogift image is missing or has placeholder value
        missing_image_mask = modified_df['고려기프트 이미지'].isna()  # Check for NaN/None
        for placeholder in placeholder_values:
            if placeholder is not None and isinstance(placeholder, str):
                missing_image_mask = missing_image_mask | (modified_df['고려기프트 이미지'] == placeholder)
        
        # Count rows with missing Kogift images
        missing_image_count = missing_image_mask.sum()
        
        if missing_image_count > 0:
            logging.info(f"Found {missing_image_count} rows with missing or placeholder Kogift images")
            
            # Clear the Kogift data columns for rows with missing images
            for col in existing_columns:
                modified_df.loc[missing_image_mask, col] = None
                
            logging.info(f"Cleared Kogift data in {missing_image_count} rows for columns: {existing_columns}")
        else:
            logging.info("No rows with missing Kogift images found")
            
        return modified_df
        
    except Exception as e:
        logging.error(f"Error clearing Kogift data: {e}")
        logging.debug(traceback.format_exc())
        # Return original DataFrame if error occurs
        return df

def apply_kogift_data_filter(file_path, config=None):
    """
    Apply Kogift data filter to clear Kogift data when image is missing.
    
    Args:
        file_path: Path to the Excel file
        config: Optional config object for future customization
        
    Returns:
        bool: True if filtering was successful, False otherwise
    """
    try:
        logging.info(f"Applying Kogift data filter to file: {file_path}")
        
        # Verify file exists
        if not os.path.exists(file_path):
            logging.error(f"Excel file not found: {file_path}")
            return False
            
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        # Apply Kogift data clearing
        filtered_df = clear_kogift_data_if_missing_image(df)
        
        # Save the modified DataFrame back to the Excel file
        filtered_df.to_excel(file_path, index=False)
        logging.info(f"Saved Kogift data filtered Excel file: {file_path}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error applying Kogift data filter: {e}")
        logging.debug(traceback.format_exc())
        return False

def filter_by_price_difference(file_path, config=None):
    """
    Filter the upload Excel file to remove rows where price difference columns
    have values >= -1. This helps remove products that have higher or equal prices
    compared to the reference price.
    
    Args:
        file_path: Path to the upload Excel file
        config: Optional config object for future customization
        
    Returns:
        bool: True if filtering was successful, False otherwise
    """
    try:
        logging.info(f"Applying price difference filtering to upload file: {file_path}")
        
        # Verify file exists
        if not os.path.exists(file_path):
            logging.error(f"Upload Excel file not found: {file_path}")
            return False
            
        # Check that it's an upload file (not a result file)
        filename = os.path.basename(file_path)
        if "_upload_" not in filename:
            logging.warning(f"File does not appear to be an upload file: {filename}. Skipping price difference filtering.")
            return False
            
        # Read the Excel file
        df = pd.read_excel(file_path)
        original_row_count = len(df)
        logging.info(f"Original row count before price difference filtering: {original_row_count}")
        
        # Define the price difference columns to check
        price_diff_columns = ['고려 가격차이', '네이버 가격차이']
        
        # Verify columns exist in the file
        columns_exist = all(col in df.columns for col in price_diff_columns)
        if not columns_exist:
            # Get the actual column names that exist
            existing_columns = [col for col in price_diff_columns if col in df.columns]
            missing_columns = [col for col in price_diff_columns if col not in df.columns]
            
            if not existing_columns:
                logging.warning(f"None of the required price difference columns {price_diff_columns} exist in the file. Skipping filtering.")
                return False
                
            logging.warning(f"Some price difference columns {missing_columns} do not exist. Will filter using only: {existing_columns}")
            price_diff_columns = existing_columns
        
        # Create a combined filter condition - keep rows where ALL price differences are < -1
        # This means removing rows where ANY price difference is >= -1
        filter_conditions = []
        for col in price_diff_columns:
            # Convert column to numeric, handling string values safely
            try:
                # Convert to numeric, coercing errors to NaN
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                # Create condition for each column: values must be < -1 to keep
                # Note: pd.NA and NaN values will be preserved (not filtered out)
                filter_conditions.append((numeric_col < -1) | numeric_col.isna())
                logging.debug(f"Successfully converted column '{col}' to numeric for filtering")
            except Exception as e:
                logging.warning(f"Error converting column '{col}' to numeric: {e}. Treating all values as valid.")
                # If conversion fails, keep all rows for this column
                filter_conditions.append(pd.Series([True] * len(df), index=df.index))
        
        # Combine all conditions with logical AND
        if filter_conditions:
            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = combined_filter & condition
                
            # Apply the filter
            filtered_df = df[combined_filter]
            removed_rows = original_row_count - len(filtered_df)
            
            # Only save if rows were actually removed
            if removed_rows > 0:
                logging.info(f"Removing {removed_rows} rows with price difference >= -1")
                filtered_df.to_excel(file_path, index=False)
                logging.info(f"Saved filtered upload file with {len(filtered_df)} remaining rows")
            else:
                logging.info("No rows needed to be removed by price difference filter")
                
            return True
        else:
            logging.warning("No valid filter conditions could be created. Skipping filtering.")
            return False
            
    except Exception as e:
        logging.error(f"Error filtering by price difference: {e}")
        logging.debug(traceback.format_exc())
        return False 