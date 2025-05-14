import os
import logging
from excel_utils import create_split_excel_outputs

def fix_excel_output(df, output_dir):
    """
    Fix and create Excel output files from the given DataFrame.
    
    Args:
        df: pandas DataFrame containing the data to be processed
        output_dir: Directory where the output files should be saved
        
    Returns:
        tuple: (result_file_path, upload_file_path)
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Create base output path
        output_path_base = os.path.join(output_dir, "result")
        
        # Use the existing create_split_excel_outputs function
        result_path, upload_path = create_split_excel_outputs(df, output_path_base)
        
        return result_path, upload_path
        
    except Exception as e:
        logging.error(f"Error in fix_excel_output: {str(e)}")
        raise 