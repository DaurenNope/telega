import json
import subprocess
import sys
import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timezone # For fallback timestamp

# --- Supabase Configuration ---
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env')) # Load .env from parent dir
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") # Use SERVICE key for backend operations
TABLE_NAME = "telegram_project_updates" # Table where raw messages are stored
# Columns to fetch (adjust if your column names are different)
MESSAGE_TEXT_COL = "full_message_text"
CHANNEL_COL = "source_channel"
TIMESTAMP_COL = "message_timestamp" # Assuming this stores the original timestamp
MESSAGE_LINK_COL = "source_message_link"
# --- End Supabase Configuration ---

# Configure basic logging for the runner script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - SupabaseTestRunner - %(message)s')

def get_supabase_data(limit=None):
    """Fetches the latest data rows from the Supabase table. Returns list of dicts."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Supabase URL or Key not configured in .env file.")
        return None

    supabase: Client | None = None
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Supabase client configured.")
    except Exception as e:
        logging.error(f"Failed to configure Supabase client: {e}", exc_info=True)
        return None

    try:
        logging.info(f"Fetching data from Supabase table '{TABLE_NAME}'...")
        query = supabase.table(TABLE_NAME).select(
            f"{MESSAGE_TEXT_COL},{CHANNEL_COL},{TIMESTAMP_COL},{MESSAGE_LINK_COL}"
        ).order(TIMESTAMP_COL, desc=True) # Order by timestamp descending to get latest

        if limit:
            query = query.limit(limit)

        response = query.execute()

        if response.data:
            logging.info(f"Successfully fetched {len(response.data)} data rows from Supabase.")
            # Format data into the expected dictionary structure
            formatted_data = []
            for row in response.data:
                formatted_data.append({
                    "Message Text": row.get(MESSAGE_TEXT_COL, ""),
                    "Channel": row.get(CHANNEL_COL, "Unknown"),
                    "Timestamp": row.get(TIMESTAMP_COL, datetime.now(timezone.utc).isoformat()), # Fallback timestamp
                    "Message Link": row.get(MESSAGE_LINK_COL, "")
                })
            return formatted_data
        else:
            logging.warning("No data returned from Supabase query.")
            # Log potential API error if available
            error_details = getattr(response, 'error', 'No specific error details in response')
            if error_details:
                 logging.error(f"Supabase API error: {error_details}")
            return []

    except Exception as e:
        logging.error(f"An unexpected error occurred fetching Supabase data: {e}", exc_info=True)
        return None

def run_processor_for_row(row_data, row_link):
    """Formats row data and runs analyzer.py as a subprocess."""
    try:
        # 1. Format the input JSON object from the fetched Supabase data
        input_json_obj = {
            "message_text": row_data.get('Message Text', ''),
            "channel": row_data.get('Channel', 'Unknown'),
            "timestamp": row_data.get('Timestamp', ''), # Should exist from query
            "message_link": row_data.get('Message Link', '') # Should exist
        }

        if not input_json_obj["message_text"]:
            logging.warning(f"Row {row_link}: Message text is empty.")
            # return None # Optional: Skip if message_text is mandatory

        # Convert the dict to a JSON string
        input_json_str = json.dumps(input_json_obj, ensure_ascii=False)

        # 2. Prepare the command to execute analyzer.py (adjust path)
        script_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'analyzer.py')
        command = [sys.executable, script_path, input_json_str]

        logging.info(f"Running processor for row (Link: {row_link})")

        # 3. Execute the command
        process = subprocess.run(
            command,
            capture_output=True,
            check=False
        )

        stdout_str = process.stdout.decode('utf-8', errors='replace')
        stderr_str = process.stderr.decode('utf-8', errors='ignore')

        # 4. Process the result
        if process.returncode != 0:
            logging.error(f"Processor script failed for row (Link: {row_link}) with return code {process.returncode}.")
            logging.error(f"Stderr: {stderr_str}")
            return stderr_str if not stdout_str else stdout_str

        if stderr_str:
            logging.warning(f"Processor script stderr for row (Link: {row_link}): {stderr_str}")

        return stdout_str

    except json.JSONDecodeError as e:
        logging.error(f"Failed to create JSON for row (Link: {row_link}). Error: {e}")
        return f"Error: JSON creation failed - {e}"
    except FileNotFoundError:
        logging.error(f"Error: analyzer.py not found at {script_path}")
        return "Error: analyzer.py not found"
    except Exception as e:
        logging.error(f"An unexpected error occurred running subprocess for row (Link: {row_link}): {e}", exc_info=True)
        return f"Error: Unexpected subprocess failure - {e}"

if __name__ == "__main__":
    num_rows_to_process = 10 # Default number of latest rows to process
    if len(sys.argv) > 1:
        try:
            num_rows_to_process = int(sys.argv[1])
            if num_rows_to_process <= 0:
                raise ValueError("Number of rows must be positive.")
            logging.info(f"Received command-line argument to process the latest {num_rows_to_process} data rows from Supabase.")
        except ValueError as e:
            print(f"Invalid number of rows specified: {sys.argv[1]}. Processing default ({num_rows_to_process}). Error: {e}")

    logging.info(f"Attempting to fetch latest {num_rows_to_process} rows from Supabase...")
    supabase_data = get_supabase_data(limit=num_rows_to_process)

    if supabase_data is None:
        logging.error("Failed to fetch data from Supabase. Exiting.")
        sys.exit(1)

    if not supabase_data:
        logging.info("No data returned from Supabase. Exiting.")
        sys.exit(0)

    results = []
    processed_count = 0
    skipped_count = 0

    try:
        for i, row in enumerate(supabase_data):
            row_link = row.get('Message Link', f'Row {i+1} NoLink')
            logging.info(f"--- Evaluating row (Link: {row_link}) ---")

            if not row.get('Message Text') and not row.get('Message Link'):
                 logging.warning(f"Skipping row (Link: {row_link}) due to missing Message Text and Message Link.")
                 skipped_count += 1
                 continue

            result = run_processor_for_row(row, row_link)
            if result is not None:
                print(f"--- Result (Link: {row_link}) ---")
                print(result.strip())
                print("--------------")
                results.append(result)
                processed_count += 1
            else:
                 logging.info(f"Skipped processing row (Link: {row_link}) (processor returned None).")
                 skipped_count += 1

    except KeyboardInterrupt:
        logging.warning("\n--- Processing interrupted by user (Ctrl+C) ---")
        print(f"\nInterrupted after attempting {i+1} rows out of {len(supabase_data)} fetched.")
        print(f"Processed: {processed_count}, Skipped: {skipped_count} before interruption.")
        sys.exit(0)

    logging.info(f"Finished test run. Attempted: {len(supabase_data)}, Processed: {processed_count}, Skipped: {skipped_count} messages.") 