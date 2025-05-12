import os
import pandas as pd
import logging
import sys
from pathlib import Path
import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Import our Excel utilities
from excel_utils import excel_generator

def main():
    """Script to put all Haereum gift images into an Excel file"""
    logging.info("Starting Haereum images to Excel embedding...")
    
    # Path to Haereum image directory
    haereum_dir = Path("C:/RPA/Image/Main/Haereum")
    
    # Check if directory exists
    if not haereum_dir.exists():
        logging.error(f"Haereum image directory doesn't exist: {haereum_dir}")
        return
    
    # Find all Haereum images (jpg & png, excluding _nobg versions)
    jpg_images = sorted([f for f in haereum_dir.glob("*.jpg") if "_nobg" not in f.name])
    png_images = sorted([f for f in haereum_dir.glob("*.png") if "_nobg" not in f.name])
    haereum_images = jpg_images + png_images
    
    if not haereum_images:
        logging.error(f"No Haereum images found in {haereum_dir}")
        return
    
    logging.info(f"Found {len(haereum_images)} Haereum images (jpg: {len(jpg_images)}, png: {len(png_images)})")
    
    # Create data for Excel
    test_data = []
    
    # Create row for each image
    for i, img_path in enumerate(haereum_images):
        # Extract product name from filename
        product_name = img_path.stem
        if product_name.startswith('haereum_'):
            product_name = product_name[8:]  # Remove 'haereum_' prefix
        
        # Replace underscores with spaces
        product_name = product_name.replace('_', ' ')
        
        # Create image data dictionary
        image_data = {
            'url': f"file://{str(img_path)}",
            'local_path': str(img_path),
            'source': 'haereum',
            'product_name': product_name
        }
        
        # Create data row
        test_data.append({
            "번호": i+1,
            "상품명": product_name,
            "파일명": img_path.name,
            "본사 이미지": image_data
        })
    
    # Create DataFrame
    df = pd.DataFrame(test_data)
    
    # Output Excel file with current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("C:/RPA/Output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"해오름_이미지_{timestamp}.xlsx"
    
    # Create Excel file with images
    logging.info(f"Creating Excel file with {len(haereum_images)} Haereum images: {output_file}")
    success = excel_generator.create_excel_output(df, str(output_file))[0]
    
    if success:
        logging.info(f"Successfully created Excel file with Haereum images at: {output_file}")
    else:
        logging.error("Failed to create Excel file.")

if __name__ == "__main__":
    main() 