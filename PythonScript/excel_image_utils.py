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
    """이미지 처리 및 Excel 파일 내 이미지 관리를 위한 클래스"""
    
    def __init__(self):
        self.max_image_size = (160, 160)  # 최대 이미지 크기 (width, height)
    
    def process_image_for_excel(self, image_path: str) -> Optional[Dict[str, Any]]:
        """Excel에 삽입할 이미지를 처리합니다."""
        try:
            if not os.path.exists(image_path):
                logger.warning(f"Image file not found: {image_path}")
                return None
                
            # 이미지 크기 및 형식 검증
            with PILImage.open(image_path) as img:
                # 이미지 크기 조정
                img.thumbnail(self.max_image_size)
                
                # 이미지 정보 반환
                return {
                    'path': image_path,
                    'size': img.size,
                    'format': img.format
                }
                
        except Exception as e:
            logger.error(f"Error processing image {image_path}: {e}")
            return None
    
    def add_image_to_worksheet(self, 
                             worksheet: openpyxl.worksheet.worksheet.Worksheet,
                             image_path: str,
                             row: int,
                             col: int) -> bool:
        """워크시트에 이미지를 추가합니다."""
        try:
            if not os.path.exists(image_path):
                logger.warning(f"Image file not found: {image_path}")
                return False
                
            # 이미지 객체 생성
            img = Image(image_path)
            img.width, img.height = self.max_image_size
            
            # 이미지 위치 설정
            col_letter = get_column_letter(col)
            img.anchor = f"{col_letter}{row}"
            
            # 워크시트에 이미지 추가
            worksheet.add_image(img)
            
            # 행 높이 조정
            worksheet.row_dimensions[row].height = 120
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding image to worksheet: {e}")
            return False
    
    def extract_image_url(self, image_data: Dict[str, Any]) -> str:
        """이미지 데이터에서 URL을 추출합니다."""
        try:
            if isinstance(image_data, dict):
                return image_data.get('url', '')
            elif isinstance(image_data, str) and image_data.startswith(('http://', 'https://')):
                return image_data
            return ''
        except Exception as e:
            logger.error(f"Error extracting image URL: {e}")
            return ''

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
    Process and add images to Excel worksheet.
    
    Args:
        worksheet: The worksheet to add images to
        df: DataFrame containing image data
        
    Returns:
        int: Number of images successfully added
    """
    try:
        # Image validation function
        def validate_image(img_path: str) -> bool:
            try:
                if not os.path.exists(img_path):
                    logging.warning(f"Image file not found: {img_path}")
                    return False
                    
                # Check file size (10MB limit)
                file_size = os.path.getsize(img_path)
                if file_size > 10 * 1024 * 1024:
                    logging.warning(f"Image too large ({file_size/1024/1024:.1f}MB): {img_path}")
                    return False
                    
                # Verify image can be opened and is valid
                with Image.open(img_path) as img:
                    img.verify()
                    
                    # Check dimensions
                    if img.size[0] < 100 or img.size[1] < 100:
                        logging.warning(f"Image too small ({img.size}): {img_path}")
                        return False
                        
                    # Check format
                    if img.format.lower() not in ['jpeg', 'jpg', 'png', 'gif']:
                        logging.warning(f"Unsupported image format {img.format}: {img_path}")
                        return False
                        
                return True
            except Exception as e:
                logging.error(f"Invalid image {img_path}: {e}")
                return False
                
        # Track progress
        images_added = 0
        errors = 0
        
        # Get image columns
        image_cols = [col for col in df.columns if col in IMAGE_COLUMNS]
        if not image_cols:
            logging.info("No image columns found in DataFrame")
            return 0
            
        # Process each row
        for row_idx, row in df.iterrows():
            for col in image_cols:
                try:
                    img_data = row[col]
                    if pd.isna(img_data) or img_data == '' or img_data == '-':
                        continue
                        
                    # Extract image path
                    img_path = None
                    if isinstance(img_data, dict):
                        img_path = img_data.get('local_path')
                    elif isinstance(img_data, str):
                        if os.path.exists(img_data):
                            img_path = img_data
                            
                    if not img_path:
                        continue
                        
                    # Validate image
                    if not validate_image(img_path):
                        errors += 1
                        continue
                        
                    # Add image to worksheet
                    try:
                        # Calculate cell position
                        col_letter = get_column_letter(df.columns.get_loc(col) + 1)
                        cell_pos = f"{col_letter}{row_idx + 2}"  # +2 because Excel is 1-based and we have header
                        
                        # Load and resize image
                        img = Image.open(img_path)
                        max_height = 150
                        max_width = 150
                        
                        # Calculate aspect ratio
                        width, height = img.size
                        aspect = width / height
                        
                        if width > max_width or height > max_height:
                            if aspect > 1:
                                new_width = max_width
                                new_height = int(max_width / aspect)
                            else:
                                new_height = max_height
                                new_width = int(max_height * aspect)
                            img = img.resize((new_width, new_height), Image.LANCZOS)
                            
                        # Add image to worksheet
                        img_path_temp = f"temp_{os.path.basename(img_path)}"
                        img.save(img_path_temp)
                        
                        img = openpyxl.drawing.image.Image(img_path_temp)
                        img.anchor = cell_pos
                        worksheet.add_image(img)
                        
                        # Clean up temp file
                        try:
                            os.remove(img_path_temp)
                        except:
                            pass
                            
                        images_added += 1
                        
                    except Exception as e:
                        logging.error(f"Error adding image to cell {cell_pos}: {e}")
                        errors += 1
                        
                except Exception as e:
                    logging.error(f"Error processing image in row {row_idx}, column {col}: {e}")
                    errors += 1
                    
        logging.info(f"Added {images_added} images to worksheet (errors: {errors})")
        return images_added
        
    except Exception as e:
        logging.error(f"Error in _process_image_columns: {e}")
        logging.debug(traceback.format_exc())
        return 0

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

__all__ = [
    'ImageProcessor',
    'safe_load_image',
    '_process_image_columns',
    '_adjust_image_cell_dimensions'
] 