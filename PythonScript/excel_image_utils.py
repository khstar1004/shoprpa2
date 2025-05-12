import os
import logging
import openpyxl
from openpyxl.drawing.image import Image
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment
from PIL import Image as PILImage
from io import BytesIO
import pandas as pd
import traceback
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple

# Import constants
from excel_constants import (
    IMAGE_COLUMNS, HAEREUM_DIR_NAME, KOGIFT_DIR_NAME, 
    NAVER_DIR_NAME, OTHER_DIR_NAME, IMAGE_MAIN_DIR,
    IMAGE_MAX_SIZE, IMAGE_STANDARD_SIZE, RESAMPLING_FILTER
)
from excel_style_constants import (
    RESULT_IMAGE_WIDTH, RESULT_IMAGE_HEIGHT
)

# Initialize logger
logger = logging.getLogger(__name__)

class ImageProcessor:
    """Class to handle all image-related operations"""
    
    @staticmethod
    def verify_image_data(img_value: Any, img_col_name: str) -> Dict[str, Any]:
        """Verify and format image data"""
        if ImageProcessor._is_empty_value(img_value):
            return '-'
            
        if isinstance(img_value, str):
            return ImageProcessor._process_string_value(img_value, img_col_name)
            
        if isinstance(img_value, dict):
            return ImageProcessor._process_dict_value(img_value, img_col_name)
            
        return '-'
    
    @staticmethod
    def _is_empty_value(value: Any) -> bool:
        """Check if value is empty"""
        return (
            value is None or
            pd.isna(value) or
            (isinstance(value, str) and value.strip() in ['', '-'])
        )
    
    @staticmethod
    def _process_string_value(value: str, col_name: str) -> Dict[str, Any]:
        """Process string type image value"""
        value = value.strip()
        
        # Handle JSON-like strings
        if value.startswith('{') and value.endswith('}'):
            try:
                import ast
                img_dict = ast.literal_eval(value)
                if isinstance(img_dict, dict):
                    return ImageProcessor._process_dict_value(img_dict, col_name)
            except (SyntaxError, ValueError):
                pass
        
        # Handle URLs
        if value.startswith(('http://', 'https://')):
            return {
                'url': value,
                'source': ImageProcessor._determine_source(col_name)
            }
            
        # Handle file paths
        return ImageProcessor._process_file_path(value, col_name)
    
    @staticmethod
    def _process_dict_value(value: Dict[str, Any], col_name: str) -> Dict[str, Any]:
        """Process dictionary type image value"""
        # Handle nested URL
        if 'url' in value and isinstance(value['url'], dict):
            value['original_nested_url'] = value['url']
            value['url'] = value['url'].get('url', '')
            
        # Verify local path
        if 'local_path' in value and value['local_path']:
            if os.path.exists(value['local_path']):
                return value
                
        # Keep URL only if valid
        if 'url' in value and value['url']:
            if isinstance(value['url'], str) and value['url'].startswith(('http://', 'https://')):
                return value
                
        return '-'
    
    @staticmethod
    def _determine_source(col_name: str) -> str:
        """Determine image source from column name"""
        source_map = {
            '본사': 'haereum',
            '고려': 'kogift',
            '네이버': 'naver'
        }
        
        for key, value in source_map.items():
            if key in col_name:
                return value
        return 'other'
    
    @staticmethod
    def _process_file_path(path: str, col_name: str) -> Dict[str, Any]:
        """Process file path and return appropriate image data"""
        # Normalize path
        path = path.replace('\\', '/')
        
        # Handle absolute paths
        if os.path.isabs(path) and os.path.exists(path):
            return {
                'url': f"file:///{path}",
                'local_path': path,
                'original_path': path,
                'source': ImageProcessor._determine_source(col_name)
            }
            
        # Handle relative paths
        source = ImageProcessor._determine_source(col_name)
        base_paths = ImageProcessor._get_base_paths(source)
        
        for base_path in base_paths:
            try:
                abs_path = (base_path / path).resolve()
                if abs_path.exists():
                    return {
                        'url': f"file:///{str(abs_path)}",
                        'local_path': str(abs_path),
                        'original_path': str(abs_path),
                        'source': source
                    }
            except Exception:
                continue
                
        return {'original_path': path, 'source': source}
    
    @staticmethod
    def _get_base_paths(source: str) -> List[Path]:
        """Get list of base paths for image source"""
        base_paths = []
        if source == 'haereum':
            base_paths = [
                IMAGE_MAIN_DIR / HAEREUM_DIR_NAME,
                IMAGE_MAIN_DIR / 'Target' / HAEREUM_DIR_NAME,
                IMAGE_MAIN_DIR
            ]
        elif source == 'kogift':
            base_paths = [
                IMAGE_MAIN_DIR / KOGIFT_DIR_NAME,
                IMAGE_MAIN_DIR / 'Target' / KOGIFT_DIR_NAME,
                IMAGE_MAIN_DIR
            ]
        elif source == 'naver':
            base_paths = [
                IMAGE_MAIN_DIR / NAVER_DIR_NAME,
                IMAGE_MAIN_DIR / 'Target' / NAVER_DIR_NAME,
                IMAGE_MAIN_DIR
            ]
        else:
            base_paths = [
                IMAGE_MAIN_DIR / OTHER_DIR_NAME,
                IMAGE_MAIN_DIR / 'Target' / OTHER_DIR_NAME,
                IMAGE_MAIN_DIR
            ]
        return base_paths

    def process_image_columns(self, worksheet: openpyxl.worksheet.worksheet.Worksheet, 
                            df: pd.DataFrame) -> int:
        """Process image columns in the worksheet"""
        try:
            successful_embeddings = 0
            columns_to_process = [col for col in IMAGE_COLUMNS if col in df.columns]
            
            if not columns_to_process:
                logger.debug("No image columns found in DataFrame")
                return 0
                
            for col_idx, column in enumerate(columns_to_process):
                excel_col = get_column_letter(df.columns.get_loc(column) + 1)
                
                for row_idx, cell_value in enumerate(df[column], 2):
                    try:
                        if self._is_empty_value(cell_value):
                            continue
                            
                        img_data = self.verify_image_data(cell_value, column)
                        if img_data == '-' or not isinstance(img_data, dict):
                            continue
                            
                        if 'local_path' not in img_data or not img_data['local_path']:
                            continue
                            
                        img_path = img_data['local_path']
                        if not os.path.exists(img_path) or os.path.getsize(img_path) == 0:
                            continue
                            
                        # Add image to worksheet
                        if self._add_image_to_worksheet(worksheet, img_path, excel_col, row_idx, img_data):
                            successful_embeddings += 1
                            
                    except Exception as e:
                        logger.error(f"Error processing image at row {row_idx}, column {column}: {e}")
                        continue
                        
            # Adjust dimensions after adding images
            self._adjust_dimensions_for_images(worksheet, df)
            
            return successful_embeddings
            
        except Exception as e:
            logger.error(f"Error in process_image_columns: {e}")
            return 0
    
    def _add_image_to_worksheet(self, worksheet, img_path: str, excel_col: str, 
                              row_idx: int, img_data: Dict[str, Any]) -> bool:
        """Add single image to worksheet"""
        try:
            # Resize image if needed
            resized_path = safe_load_image(img_path, 
                                         max_height=RESULT_IMAGE_HEIGHT,
                                         max_width=RESULT_IMAGE_WIDTH)
            if not resized_path:
                return False
                
            # Create and add image
            img = Image(resized_path)
            img.width = RESULT_IMAGE_WIDTH
            img.height = RESULT_IMAGE_HEIGHT
            img.anchor = f"{excel_col}{row_idx}"
            
            worksheet.add_image(img)
            
            # Clear cell content
            cell = worksheet.cell(row=row_idx, column=get_column_letter(excel_col))
            cell.value = ""
            
            # Add hyperlink if URL exists
            if 'url' in img_data and isinstance(img_data['url'], str):
                cell.hyperlink = img_data['url']
                cell.font = Font(color="0563C1", underline="single")
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding image to worksheet: {e}")
            return False
    
    def _adjust_dimensions_for_images(self, worksheet, df: pd.DataFrame) -> None:
        """Adjust worksheet dimensions for images"""
        try:
            # Adjust column widths
            image_cols = [col for col in df.columns if col in IMAGE_COLUMNS]
            for col in image_cols:
                col_letter = get_column_letter(df.columns.get_loc(col) + 1)
                worksheet.column_dimensions[col_letter].width = 85
            
            # Adjust row heights
            for row_idx in range(2, worksheet.max_row + 1):
                worksheet.row_dimensions[row_idx].height = 400
                
        except Exception as e:
            logger.error(f"Error adjusting dimensions: {e}")

def safe_load_image(path, max_height=150, max_width=150):
    """Safely load and resize an image for Excel."""
    try:
        img = PILImage.open(path)
        # Calculate new dimensions preserving aspect ratio
        width, height = img.size
        if width > max_width or height > max_height:
            ratio = min(max_width / width, max_height / height)
            new_width = int(width * ratio)
            new_height = int(height * ratio)
            img = img.resize((new_width, new_height), RESAMPLING_FILTER)
            
            # Save temporary resized version
            temp_dir = os.path.join(os.path.dirname(path), 'temp')
            os.makedirs(temp_dir, exist_ok=True)
            temp_path = os.path.join(temp_dir, f"resized_{os.path.basename(path)}")
            img.save(temp_path)
            return temp_path
        return path
    except Exception as e:
        logger.warning(f"Error loading/resizing image {path}: {e}")
        return None

def _process_image_columns(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame) -> int:
    """
    Process image columns in the DataFrame and add images to the worksheet.
    
    Args:
        worksheet: The worksheet to add images to
        df: DataFrame containing the data with image columns
        
    Returns:
        int: Number of successfully embedded images
    """
    # Initialize tracking variables
    successful_embeddings = 0
    attempted_embeddings = 0
    kogift_successful = 0
    kogift_attempted = 0
    naver_successful = 0
    naver_attempted = 0
    
    # Only handle image-specific columns
    columns_to_process = [col for col in IMAGE_COLUMNS if col in df.columns]
    
    if not columns_to_process:
        logger.debug("No image columns found in DataFrame")
        return 0
    
    # Track the count of images per column
    img_counts = {col: 0 for col in columns_to_process}
    err_counts = {col: 0 for col in columns_to_process}
    
    logger.debug(f"Processing {len(columns_to_process)} image columns")
    
    # For each image column in the DataFrame
    for col_idx, column in enumerate(columns_to_process):
        is_kogift_image = 'kogift' in column.lower() or '고려기프트' in column
        is_naver_image = 'naver' in column.lower() or '네이버' in column
        
        # Excel column letter for this column (e.g., 'A', 'B', ...)
        excel_col = get_column_letter(df.columns.get_loc(column) + 1)
        
        # For each row in the DataFrame
        for row_idx, cell_value in enumerate(df[column]):
            img_path = None  # Initialize image path
            
            # Skip empty cells (None, NaN, empty strings)
            if pd.isna(cell_value) or cell_value == "":
                continue
                
            # Skip cells with placeholder dash
            if cell_value == "-":
                continue
            
            # Handle dictionary format (most complete info)
            if isinstance(cell_value, dict):
                # Try local path first, then URL
                if 'local_path' in cell_value and cell_value['local_path']:
                    img_path = cell_value['local_path']
                    
                    # Special handling for Naver images
                    if is_naver_image:
                        logger.debug(f"Found Naver local_path: {img_path}")
                        
                        # Verify the path exists and is absolute
                        if not os.path.isabs(img_path):
                            abs_path = os.path.abspath(img_path)
                            logger.debug(f"Converting relative Naver path to absolute: {img_path} -> {abs_path}")
                            img_path = abs_path
                        
                        # Verify the file exists
                        if not os.path.exists(img_path):
                            logger.warning(f"Naver image path doesn't exist: {img_path}")
                            
                            # Try alternative extensions
                            base_path = os.path.splitext(img_path)[0]
                            for ext in ['.jpg', '.jpeg', '.png', '.gif']:
                                alt_path = f"{base_path}{ext}"
                                if os.path.exists(alt_path):
                                    logger.info(f"Found alternative Naver image path: {alt_path}")
                                    img_path = alt_path
                                    break
                            else:
                                # If no alternative found, try looking for _nobg version
                                nobg_path = f"{base_path}_nobg.png"
                                if os.path.exists(nobg_path):
                                    logger.info(f"Found _nobg version of Naver image: {nobg_path}")
                                    img_path = nobg_path
                    elif is_kogift_image:
                        logger.debug(f"Found Kogift local_path: {img_path}")
            
            # Handle string path
            elif isinstance(cell_value, str) and cell_value not in ['-', '']:
                if cell_value.startswith(('http://', 'https://')):
                    # Web URL - we would need a downloaded version
                    logger.debug(f"Found web URL, but need local version to embed: {cell_value[:50]}...")
                    continue
                elif cell_value.startswith('file:///'):
                    # Local file URL
                    img_path = cell_value.replace('file:///', '').replace('/', os.sep)
                elif os.path.exists(cell_value):
                    # Direct file path
                    img_path = cell_value
                else:
                    # Path-like string but file doesn't exist
                    logger.debug(f"Path-like string but file not found: {cell_value[:50]}...")
                    continue
            
            # Skip if no valid path was found
            if not img_path:
                continue
            
            # Add image to worksheet if file exists and has content
            try:
                attempted_embeddings += 1
                if is_kogift_image:
                    kogift_attempted += 1
                if is_naver_image:
                    naver_attempted += 1
                
                # Verify file exists and is not empty
                if not os.path.exists(img_path):
                    logger.warning(f"Image file not found: {img_path}")
                    continue
                
                if os.path.getsize(img_path) == 0:
                    logger.warning(f"Image file is empty: {img_path}")
                    continue
                
                # Create and resize the image
                try:
                    img = Image(img_path)
                    
                    # Set larger image size for better visibility
                    img.width = RESULT_IMAGE_WIDTH  # pixels - increased from 240
                    img.height = RESULT_IMAGE_HEIGHT  # pixels - increased from 240
                    
                    # Position image in the cell
                    img.anchor = f"{excel_col}{row_idx + 2}"  # +2 because DataFrame is 0-indexed but Excel rows start at 1, and row 1 is the header
                    
                    # Add image to worksheet
                    worksheet.add_image(img)
                    
                    # Clear text in cell to avoid showing both image and text
                    cell = worksheet.cell(row=row_idx + 2, column=df.columns.get_loc(column) + 1)
                    cell.value = ""
                    
                    successful_embeddings += 1
                    if is_kogift_image:
                        kogift_successful += 1
                    if is_naver_image:
                        naver_successful += 1
                    
                except Exception as img_err:
                    logger.warning(f"Failed to add image at row {row_idx + 2}, column {column}: {img_err}")
                    # If image fails, try to add the URL as a clickable link
                    if isinstance(cell_value, dict) and 'url' in cell_value and isinstance(cell_value['url'], str):
                        cell = worksheet.cell(row=row_idx + 2, column=df.columns.get_loc(column) + 1)
                        url = cell_value['url']
                        if url.startswith(('http://', 'https://')):
                            cell.value = url
                            cell.hyperlink = url
                            cell.font = Font(color="0563C1", underline="single")
                            logger.debug(f"Added URL as fallback for failed image: {url[:50]}...")
                    
            except Exception as e:
                logger.warning(f"Error processing image at row {row_idx + 2}, column {column}: {e}")
                # Keep cell value as is for reference
    
    logger.info(f"Image processing complete. Embedded {successful_embeddings}/{attempted_embeddings} images.")
    if kogift_attempted > 0:
        logger.info(f"Kogift image processing: {kogift_successful}/{kogift_attempted} images embedded successfully.")
    if naver_attempted > 0:
        logger.info(f"Naver image processing: {naver_successful}/{naver_attempted} images embedded successfully.")
    
    # Track image columns for dimension adjustment
    image_cols = [(df.columns.get_loc(col) + 1, col) for col in columns_to_process]
    
    # Adjust row heights where images are embedded
    for row_idx in range(2, worksheet.max_row + 1):
        has_image = False
        for col_idx, _ in image_cols:
            cell = worksheet.cell(row=row_idx, column=col_idx)
            if cell.value == "":  # Cell was cleared for image
                has_image = True
                break
        
        if has_image:
            # Set taller row height to accommodate larger images
            worksheet.row_dimensions[row_idx].height = 380  # Increased height for image rows
    
    return successful_embeddings

def _adjust_image_cell_dimensions(worksheet: openpyxl.worksheet.worksheet.Worksheet, df: pd.DataFrame):
    """Adjusts row heights and column widths for cells containing images."""
    from excel_constants import ERROR_MESSAGE_VALUES
    
    logger.debug("Adjusting dimensions for image cells...")
    
    # Get image column indices using the IMAGE_COLUMNS constant
    image_cols = {col: idx for idx, col in enumerate(df.columns, 1) if col in IMAGE_COLUMNS}
    
    if not image_cols:
        return

    # Increase column widths for image columns to accommodate larger images
    for col_name, col_idx in image_cols.items():
        try:
            col_letter = get_column_letter(col_idx)
            # Use larger column width for image columns
            worksheet.column_dimensions[col_letter].width = 85  # Increased from 80
        except Exception as e:
            logger.error(f"Error adjusting column width for {col_name}: {e}")
    
    # Create a set of rows that need height adjustment
    rows_with_images = set()
    
    try:
        # Find rows that have actual images (not error messages or empty cells)
        for row_idx in range(2, worksheet.max_row + 1):
            for col_name, col_idx in image_cols.items():
                try:
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    
                    # If the cell is empty, it likely has an image
                    if cell.value == "" or cell.value is None:
                        rows_with_images.add(row_idx)
                        break
                        
                    # Check for image data in dictionary format
                    cell_value = str(cell.value) if cell.value else ""
                    if cell_value and cell_value.startswith('{') and cell_value.endswith('}'):
                        try:
                            import ast
                            img_dict = ast.literal_eval(cell_value)
                            if isinstance(img_dict, dict) and ('local_path' in img_dict or 'url' in img_dict):
                                rows_with_images.add(row_idx)
                                break
                        except:
                            pass
                            
                    # Check for path-like strings
                    if (cell_value and cell_value != '-' and 
                        not any(err_msg in cell_value for err_msg in ERROR_MESSAGE_VALUES) and
                        ('\\' in cell_value or '/' in cell_value or '.jpg' in cell_value.lower() or 
                         '.png' in cell_value.lower() or '.jpeg' in cell_value.lower() or
                         'http' in cell_value.lower())):
                        rows_with_images.add(row_idx)
                        break
                except Exception as e:
                    logger.error(f"Error checking cell at row {row_idx}, column {col_idx}: {e}")
    except Exception as e:
        logger.error(f"Error finding rows with images: {e}")
    
    # Apply increased height to rows with images
    for row_idx in rows_with_images:
        try:
            # Set larger row height to accommodate bigger images
            worksheet.row_dimensions[row_idx].height = 400  # Increased from 380
            
            # Center-align all cells in this row for better appearance with images
            for col_idx in range(1, worksheet.max_column + 1):
                try:
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    # Preserve horizontal alignment, set vertical to center
                    current_alignment = cell.alignment
                    cell.alignment = Alignment(
                        horizontal=current_alignment.horizontal,
                        vertical="center",
                        wrap_text=current_alignment.wrap_text
                    )
                except Exception as e:
                    logger.error(f"Error adjusting cell alignment at row {row_idx}, column {col_idx}: {e}")
        except Exception as e:
            logger.error(f"Error adjusting row height for row {row_idx}: {e}")
    
    logger.debug(f"Adjusted dimensions for {len(rows_with_images)} rows with images") 