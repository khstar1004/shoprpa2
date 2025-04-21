import sys
import os
from PyQt6.QtWidgets import QApplication
from shoprpa_gui import MainWindow

def main():
    # Create application
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle("Fusion")
    
    # Create and show main window
    window = MainWindow()
    window.show()
    
    # Run application
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 