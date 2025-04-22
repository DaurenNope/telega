import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
import time

# --- Static Configuration ---
SUPABASE_TABLE_NAME = "telegram_project_updates"
EMBEDDING_MODEL = "models/text-embedding-004"
# How many records to process in each batch
BATCH_SIZE = 50
# Wait time in seconds after hitting API rate limits
RETRY_WAIT_SECONDS = 60

# --- Load Environment Variables ---
# Assumes .env is in the same directory or parent directory
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

# --- Initialization ---
def initialize_clients():
    """Initializes and returns Supabase and Gemini clients."""
    supabase = None
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            log.info("Supabase client configured.")
        except Exception as e:
            log.error(f"Failed to configure Supabase client: {e}", exc_info=True)
            return None, None
    else:
        log.error("Supabase URL or Key not found in environment variables.")
        return None, None

    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY not found in environment variables.")
        return supabase, None
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        log.info(f"Gemini client configured for embedding model: {EMBEDDING_MODEL}")
    except Exception as e:
        log.error(f"Failed to configure Gemini client: {e}", exc_info=True)
        return supabase, None

    return supabase, genai # Return client and configured module

# --- Get Records to Process ---
def get_records_without_embedding(supabase: Client, project_filter: str = None, limit: int = BATCH_SIZE):
    """Fetches records from Supabase that are missing embeddings."""
    try:
        query = supabase.table(SUPABASE_TABLE_NAME)\
            .select("id, project_name, activity_type, summary")\
            .is_("embedding", "null") # Filter for rows where embedding is NULL

        if project_filter:
            log.info(f"Filtering for project: {project_filter}")
            query = query.eq("project_name", project_filter)

        response = query.limit(limit).execute()

        if response.data:
            log.info(f"Fetched {len(response.data)} records to process.")
            return response.data
        else:
            log.info("No records found without embeddings (matching filter)." + (f" for project {project_filter}" if project_filter else ""))
            return []
    except Exception as e:
        log.error(f"Error fetching records from Supabase: {e}", exc_info=True)
        return []

# --- Generate Embedding ---
def generate_embedding(text: str, max_retries=3):
    """Generates embedding for the given text using Gemini API with retries."""
    if not text or not isinstance(text, str):
        log.warning("Skipping embedding generation for empty or invalid text.")
        return None

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            result = genai.embed_content(
                model=EMBEDDING_MODEL,
                content=text,
                task_type="retrieval_document"
            )
            return result['embedding']
        except ResourceExhausted as e:
            log.warning(f"Embedding API Rate Limit Hit (Attempt {attempt}/{max_retries}). Waiting {RETRY_WAIT_SECONDS}s... Error: {e}")
            if attempt >= max_retries:
                log.error(f"Embedding API rate limit exceeded after {max_retries} attempts.")
                return None
            time.sleep(RETRY_WAIT_SECONDS)
        except Exception as e:
            log.error(f"Error generating embedding (Attempt {attempt}/{max_retries}): {e}", exc_info=True)
            # Don't retry on other errors for now, might be persistent issue
            return None
    return None # Should not be reached if retries are exhausted, but included for safety

# --- Update Record ---
def update_record_embedding(supabase: Client, record_id: str, embedding_vector: list):
    """Updates a single record in Supabase with the generated embedding."""
    try:
        response = supabase.table(SUPABASE_TABLE_NAME)\
            .update({"embedding": embedding_vector})\
            .eq("id", record_id)\
            .execute()

        # Basic check if update was acknowledged (more robust checks could be added)
        if response.data or (hasattr(response, 'status_code') and 200 <= response.status_code < 300):
             log.debug(f"Successfully updated embedding for record ID: {record_id}")
             return True
        else:
             log.warning(f"Supabase update for record ID {record_id} might have failed or returned unexpected data: {response}")
             return False

    except Exception as e:
        log.error(f"Error updating record ID {record_id} in Supabase: {e}", exc_info=True)
        return False

# --- Main Execution ---
if __name__ == "__main__":
    log.info("--- Starting Embedding Backfill Script ---")
    supabase_client, gemini_client = initialize_clients()

    if not supabase_client or not gemini_client:
        log.critical("Failed to initialize clients. Exiting.")
        sys.exit(1)

    # --- Configuration: Set project_to_process to a specific name or None --- #
    # project_to_process = "Linera"  # Process only 'Linera'
    project_to_process = None       # Process all projects (up to BATCH_SIZE)
    # ----------------------------------------------------------------------- #

    total_processed_overall = 0
    total_updated_overall = 0
    batch_num = 0

    while True: # Loop until no more records are found
        batch_num += 1
        log.info(f"--- Starting Batch {batch_num} ---")
        records = get_records_without_embedding(supabase_client, project_filter=project_to_process, limit=BATCH_SIZE)

        if not records:
            log.info("No more records found without embeddings (matching filter). Exiting loop.")
            break # Exit the while loop

        processed_count_batch = 0
        updated_count_batch = 0

        log.info(f"Processing {len(records)} records for Batch {batch_num}...")
        for record in records:
            processed_count_batch += 1
            record_id = record.get('id')
            project_name = record.get('project_name', 'N/A')
            activity_type = record.get('activity_type', 'N/A')
            summary = record.get('summary', '') # Use empty string if summary is None
            key_links = record.get('key_links', []) # Get key_links if available

            if not record_id:
                log.warning(f"Skipping record due to missing ID: {record}")
                continue

            log.info(f"Processing record {processed_count_batch}/{len(records)} (Batch {batch_num}): ID={record_id}, Project={project_name}")

            # Construct text including key_links
            links_string = " Links: " + " ".join(key_links) if key_links else ""
            embedding_text = f"Project: {project_name}, Activity: {activity_type}, Summary: {summary}{links_string}"
            log.debug(f"Embedding Text: {embedding_text[:100]}...") # Log truncated text

            # Generate embedding
            embedding_vector = generate_embedding(embedding_text)

            # Update if embedding successful
            if embedding_vector:
                if update_record_embedding(supabase_client, record_id, embedding_vector):
                    updated_count_batch += 1
                else:
                    log.warning(f"Failed to update embedding for record ID: {record_id}")
            else:
                 log.warning(f"Failed to generate embedding for record ID: {record_id}. Skipping update.")

            # Optional: Add a small delay to avoid hitting API limits too quickly if processing many
            # time.sleep(0.1)

        total_processed_overall += processed_count_batch
        total_updated_overall += updated_count_batch
        log.info(f"--- Finished Batch {batch_num} --- Records checked: {processed_count_batch}, Successfully updated: {updated_count_batch} ---")

        if len(records) < BATCH_SIZE:
            log.info(f"Last batch processed less than BATCH_SIZE ({len(records)}). Assuming all records are processed. Exiting loop.")
            break # Exit the while loop if last fetch was not full

        # Optional: Add a delay between full batches
        # log.info(f"Waiting 5 seconds before next batch...")
        # time.sleep(5)


    log.info(f"--- Embedding Backfill Script Finished ---")
    log.info(f"Total records checked across all batches: {total_processed_overall}")
    log.info(f"Successfully updated across all batches: {total_updated_overall}") 