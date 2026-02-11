"""Constants and configuration for the bank parser system."""

import os

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'vip2025')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
LOG_DIR = os.path.join(BASE_DIR, 'logs')
CHECK_FILE = os.path.join(BASE_DIR, 'check.xlsx')

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Direction constants
DIRECTION_INCOME = 'Приход'
DIRECTION_EXPENSE = 'Расход'

# How many rows to scan for header detection
MAX_HEADER_SCAN_ROWS = 30
