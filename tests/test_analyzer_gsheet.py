import os
import sys
import logging

# 2. Prepare the command to execute process_message.py
# Correct path: Go up one level from tests/, then into src/, then analyzer.py
script_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'analyzer.py')
command = [sys.executable, script_path, input_json_str]

logging.info(f"Running processor for row {row_index} (Link: {input_json_obj.get('message_link', 'N/A')})")

try:
    # ... existing code ...
except FileNotFoundError:
    # Update error message to reflect new expected path
    logging.error(f"Error: analyzer.py not found at {script_path}")
    return "Error: analyzer.py not found"
except Exception as e:
    # Include row_index in error logging
    # ... existing code ... 