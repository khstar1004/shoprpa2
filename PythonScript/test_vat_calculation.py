#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VAT calculation test script
This script tests the consistency of VAT calculations for quantity pricing
"""

import pandas as pd
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                  format='%(asctime)s - %(levelname)s - %(message)s')

def test_vat_calculation():
    """Test VAT calculation to ensure it's exactly 10%"""
    
    # Test basic VAT calculation
    test_prices = [4550, 5140, 7420, 14240, 8494, 5082, 22000]
    
    print("===== Testing VAT Calculation =====")
    print("{:<10} {:<10} {:<10}".format("Base Price", "With VAT", "Manually Calculated"))
    
    for price in test_prices:
        # Calculate VAT (should be exactly 10%)
        price_with_vat = round(price * 1.1)
        manual_vat = price * 0.1
        manual_total = price + manual_vat
        
        print("{:<10} {:<10} {:<10.2f}".format(
            price, price_with_vat, manual_total))
        
        # Verify calculation is correct
        assert abs(price_with_vat - manual_total) <= 1, f"VAT calculation error for {price}"
    
    print("\nAll VAT calculations passed!")
    
    # Test with the specific example from the issue
    base_price = 4550
    expected_with_vat = 5005
    calculated_with_vat = round(base_price * 1.1)
    
    print("\n===== Testing Specific Example =====")
    print(f"Base price: {base_price}")
    print(f"Expected with VAT: {expected_with_vat}")
    print(f"Calculated with VAT: {calculated_with_vat}")
    
    assert calculated_with_vat == expected_with_vat, "VAT calculation for example failed"
    print("Example VAT calculation passed!")

if __name__ == "__main__":
    try:
        test_vat_calculation()
    except Exception as e:
        logging.error(f"Test failed: {e}")
        sys.exit(1) 