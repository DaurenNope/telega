import os
import sys
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from notion_client import Client as NotionClient
from notion_client.helpers import get_id # Helper for URLs/IDs
import schedule # Optional, for running within script

# --- Configuration ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_PROJECTS_DB_ID = os.getenv("NOTION_PROJECTS_DB_ID")
NOTION_REVIEW_DB_ID = os.getenv("NOTION_REVIEW_DB_ID")

# Max rows to process per run to avoid overwhelming APIs
PROCESS_LIMIT = 50

# --- Initialize Clients ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client initialized.")
except Exception as e:
    logging.error(f"Failed to initialize Supabase client: {e}", exc_info=True)
    sys.exit(1)

try:
    notion = NotionClient(auth=NOTION_API_KEY)
    logging.info("Notion client initialized.")
except Exception as e:
    logging.error(f"Failed to initialize Notion client: {e}", exc_info=True)
    sys.exit(1)

# --- Helper Functions ---

def find_project_page_id(project_name: str) -> str | None:
    """Searches the Notion 'Projects Master' DB for a matching project name."""
    if not project_name:
        return None
    try:
        # Adjust 'Project Name' to the EXACT name of your title property in Projects Master
        response = notion.databases.query(
            database_id=NOTION_PROJECTS_DB_ID,
            filter={
                "property": "Project Name", # Or whatever your Title property is called
                "title": {
                    "equals": project_name
                }
            },
            page_size=1 # We only need one match
        )
        if response and response.get("results"):
            page_id = response["results"][0]["id"]
            logging.info(f"Found Notion Project Page ID: {page_id} for '{project_name}'")
            return page_id
        else:
            logging.warning(f"Could not find project '{project_name}' in Notion Projects Master DB.")
            return None
    except Exception as e:
        logging.error(f"Error searching Notion Projects Master DB for '{project_name}': {e}", exc_info=True)
        return None

def format_links(links: list | None) -> str:
    """Formats a list of links into a newline-separated string for Notion text field."""
    if not links or not isinstance(links, list):
        return ""
    return "\n".join(link for link in links if link) # Basic formatting

# --- Main Sync Logic ---

def sync_supabase_to_notion():
    logging.info("Starting Supabase to Notion sync run...")
    try:
        # Fetch rows from Supabase that haven't been sent and are not duplicates
        logging.info("Attempting to fetch rows from Supabase...") # Add log before query
        response = supabase.table("telegram_project_updates")\
            .select("*")\
            .eq("sent_to_notion", False)\
            .eq("is_duplicate", False) # Only sync non-duplicates
            .limit(PROCESS_LIMIT)\
            .execute()
        logging.info("Supabase fetch executed.") # Add log after query attempt

        # --- Add more specific error handling for the data access ---
        # The supabase-py client might raise specific exceptions or return errors
        # Check response structure based on library documentation if necessary
        # For now, let's assume execute() raises on error or response.data is None/empty on logical failure

        if not response or not getattr(response, 'data', None):
            # Handle cases where execute might return something unexpected or empty data
            # Check if there's an error attribute (structure might vary)
            error_info = getattr(response, 'error', None)
            if error_info:
                logging.error(f"Supabase query failed with error: {error_info}")
            elif not getattr(response, 'data', None):
                logging.info("No new rows found in Supabase to sync (response.data is empty).")
            else:
                logging.warning(f"Supabase query returned unexpected response structure: {response}")
            return # Exit function if no data or error
        # --- End specific error handling section ---

        logging.info(f"Found {len(response.data)} new rows to process.")

        for row in response.data:
            supabase_row_id = row.get("id")
            project_name = row.get("project_name")
            logging.info(f"Processing row ID: {supabase_row_id}, Project: {project_name}")

            try:
                # 1. Find related Project Page ID in Notion
                related_project_page_id = find_project_page_id(project_name)

                # 2. Prepare Notion Page Properties
                properties_to_create = {
                    # --- MAP YOUR EXACT NOTION PROPERTY NAMES BELOW ---
                    "Update Summary": {"title": [{"text": {"content": row.get("summary", "No Summary Provided")[:2000]}}]}, # Notion title limit 2000 chars
                    "Activity Type": {"select": {"name": row.get("activity_type", "Unknown")}}, # Assumes 'Select' type
                    "Source Link": {"url": row.get("source_message_link")},
                    "Timestamp": {"date": {"start": row.get("message_timestamp")}}, # Assumes ISO format from Supabase
                    "Review Status": {"select": {"name": "New"}}, # Default status
                    "Key Links": {"rich_text": [{"text": {"content": format_links(row.get("key_links"))[:2000]}}]}, # Adjust limit if needed
                    "Referral Links": {"rich_text": [{"text": {"content": format_links(row.get("referral_links"))[:2000]}}]},
                    "Needs Review": {"checkbox": row.get("needs_review", False)},
                    "Supabase ID": {"rich_text": [{"text": {"content": str(supabase_row_id)}}]},
                    # --- Relation ---
                    # Only add relation if project page was found
                    "Project": { "relation": [{"id": related_project_page_id}] } if related_project_page_id else None
                }
                # Remove None values for properties not set (like relation if not found)
                properties_to_create = {k: v for k, v in properties_to_create.items() if v is not None}

                # Handle Full Message Text (often added as page content, not property)
                full_message = row.get("full_message_text", "")
                # Notion API expects content as blocks
                page_content_blocks = []
                if full_message:
                     # Split into paragraphs to avoid block size limits (max 2000 chars per block)
                     for paragraph in full_message.split('\n\n'):
                         if paragraph.strip(): # Avoid empty paragraphs
                             for i in range(0, len(paragraph), 2000):
                                 chunk = paragraph[i:i+2000]
                                 page_content_blocks.append({
                                     "object": "block",
                                     "type": "paragraph",
                                     "paragraph": {
                                         "rich_text": [{"type": "text", "text": {"content": chunk}}]
                                     }
                                 })


                # 3. Create Notion Page
                notion.pages.create(
                    parent={"database_id": NOTION_REVIEW_DB_ID},
                    properties=properties_to_create,
                    children=page_content_blocks # Add full text as content
                )
                logging.info(f"Successfully created Notion page for Supabase ID: {supabase_row_id}")

                # 4. Update Supabase row - Mark as sent
                try:
                    supabase.table("telegram_project_updates")\
                        .update({"sent_to_notion": True})\
                        .eq("id", supabase_row_id)\
                        .execute()
                    logging.info(f"Marked Supabase row {supabase_row_id} as sent_to_notion=true.")
                except Exception as e_update:
                     logging.error(f"Failed to mark Supabase row {supabase_row_id} as sent: {e_update}", exc_info=True)
                     # Decide how to handle this - maybe retry later?

            except Exception as e_page:
                logging.error(f"Failed to process Supabase row {supabase_row_id} or create Notion page: {e_page}", exc_info=True)
                # Optional: Implement retry logic or flag row for manual check
                # DO NOT mark as sent if Notion creation failed

    except Exception as e_main:
        # --- Enhanced logging for the main exception block ---
        logging.error(f"An error occurred during the main sync loop: {e_main}", exc_info=True)
        # Attempt to print Supabase client specific details if available in exception
        # This depends on the exception type raised by supabase-py
        if hasattr(e_main, 'json'): # Example check for potential detail
            logging.error(f"Supabase error details (if available): {e_main.json()}")
        elif hasattr(e_main, 'message'):
             logging.error(f"Supabase error message (if available): {e_main.message}")
        # --- End enhanced logging ---

    logging.info("Sync run finished.")


# --- Execution ---
if __name__ == "__main__":
    # Option 1: Run once when script is called (e.g., by cron)
    sync_supabase_to_notion()

    # Option 2: Run continuously using 'schedule' library
    # schedule.every(1).minute.do(sync_supabase_to_notion)
    # logging.info("Scheduler started. Running sync every minute. Press Ctrl+C to exit.")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1) 