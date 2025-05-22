import os
import logging
import pandas as pd
import traceback

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
            # Create condition for each column: values must be < -1 to keep
            # Note: pd.NA and NaN values will be preserved (not filtered out)
            filter_conditions.append((df[col] < -1) | df[col].isna())
        
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