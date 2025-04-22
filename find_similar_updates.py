import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import math

# --- Configuration ---
SUPABASE_TABLE_NAME = "telegram_project_updates"
# Similarity threshold (0.0 to 1.0). Higher means more similar.
# Start maybe around 0.85-0.9 and adjust based on results.
SIMILARITY_THRESHOLD = 0.88
# How many similar matches to request per item (max)
MATCH_COUNT = 5
# How many records to fetch from DB in each batch
FETCH_BATCH_SIZE = 100
# Optional: Only check records newer than X days (set to None to check all)
LOOKBACK_DAYS = 7
# ---------------------

# --- Load Environment Variables ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

# --- Initialization ---
def initialize_supabase():
    """Initializes and returns Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.critical("Supabase URL or Key not found in environment variables. Exiting.")
        sys.exit(1)
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        log.info("Supabase client configured.")
        return supabase
    except Exception as e:
        log.critical(f"Failed to configure Supabase client: {e}", exc_info=True)
        sys.exit(1)

# --- Fetch Records ---
def get_records_to_analyze(supabase: Client, batch_size: int, offset: int, lookback_days: int = None):
    """Fetches a batch of records with embeddings to analyze."""
    try:
        query = supabase.table(SUPABASE_TABLE_NAME)\
            .select("id, project_name, summary, source_message_link, message_timestamp")\
            .not_.is_("embedding", "null") # Only get records WITH embeddings

        if lookback_days is not None:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            log.info(f"Filtering records newer than {cutoff_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            query = query.gte("message_timestamp", cutoff_date.isoformat())

        response = query.order("id").range(offset, offset + batch_size - 1).execute()

        if response.data:
            log.debug(f"Fetched {len(response.data)} records starting from offset {offset}.")
            return response.data
        else:
            log.debug(f"No more records found at offset {offset}.")
            return []
    except Exception as e:
        log.error(f"Error fetching records from Supabase (offset {offset}): {e}", exc_info=True)
        return []

# --- Find Matches using DB Function ---
def find_matches_for_record(supabase: Client, record_id: str, threshold: float, count: int):
    """Calls the Supabase RPC function to find similar records."""
    try:
        # We need the embedding for the RPC call, but it's not selected by default
        # In a real app, you might pass the embedding if you already have it
        # Here, we do an extra fetch, or assume the function handles fetching it (which ours does via subquery)

        response = supabase.rpc('match_similar_updates', {
            'query_embedding': None, # The function expects this but uses the id to find it via subquery
            'match_threshold': threshold,
            'match_count': count,
            'exclude_id': record_id
            # Note: The RPC call needs modification if the function cannot get the embedding itself.
            # Let's assume the function as written previously works:
            # It takes exclude_id (uuid) and uses a subquery like:
            # (SELECT embedding FROM telegram_project_updates WHERE id = exclude_id)
            # **Correction**: The function *does* need the embedding passed to it.
            # We need to fetch the embedding first.
        }).execute()

        # Fetch the embedding first
        embedding_response = supabase.table(SUPABASE_TABLE_NAME)\
                                     .select("embedding")\
                                     .eq("id", record_id)\
                                     .maybe_single()\
                                     .execute()

        if not embedding_response.data or not embedding_response.data.get('embedding'):
            log.warning(f"Could not fetch embedding for record ID {record_id}. Skipping match search.")
            return []

        query_embedding = embedding_response.data['embedding']

        # Now call the RPC function correctly
        response = supabase.rpc('match_similar_updates', {
            'query_embedding': query_embedding,
            'match_threshold': threshold,
            'match_count': count,
            'exclude_id': record_id
        }).execute()


        if response.data:
            return response.data
        elif hasattr(response, 'error') and response.error:
             log.error(f"Error calling RPC for record ID {record_id}: {response.error}")
             return []
        else:
            # No error, but no data - means no matches found above threshold
            return []
    except Exception as e:
        log.error(f"Exception calling RPC for record ID {record_id}: {e}", exc_info=True)
        return []

# --- Main Execution ---
if __name__ == "__main__":
    log.info("--- Starting Similarity Analysis Script ---")
    log.info(f"Using Similarity Threshold: {SIMILARITY_THRESHOLD}")
    if LOOKBACK_DAYS:
        log.info(f"Checking records from the last {LOOKBACK_DAYS} days.")
    else:
        log.info("Checking all records with embeddings.")

    supabase_client = initialize_supabase()

    processed_ids = set() # Keep track of IDs we've already reported matches for
    potential_duplicates = []
    current_offset = 0
    total_processed = 0

    while True:
        log.info(f"Fetching batch of records starting at offset {current_offset}...")
        records_batch = get_records_to_analyze(supabase_client, FETCH_BATCH_SIZE, current_offset, LOOKBACK_DAYS)

        if not records_batch:
            log.info("No more records found to analyze.")
            break

        log.info(f"Analyzing batch of {len(records_batch)} records...")
        for record in records_batch:
            record_id = record.get('id')
            if not record_id or record_id in processed_ids:
                continue # Skip if no ID or already processed

            total_processed += 1
            log.debug(f"Analyzing record ID: {record_id}")
            matches = find_matches_for_record(supabase_client, record_id, SIMILARITY_THRESHOLD, MATCH_COUNT)

            if matches:
                log.info(f"---> Potential matches found for ID: {record_id}")
                log.info(f"    Source: Project='{record.get('project_name')}', Summary='{record.get('summary', '')[:80]}...' ({record.get('source_message_link')})" )
                potential_duplicates.append({
                    'source_id': record_id,
                    'source_summary': record.get('summary'),
                    'matches': matches
                })
                processed_ids.add(record_id) # Add source ID to processed set
                for match in matches:
                    match_id = match.get('id')
                    log.info(f"      Match: ID={match_id}, Similarity={match.get('similarity'):.4f}, Project='{match.get('project_name')}', Summary='{match.get('summary', '')[:80]}...' ({match.get('source_message_link')})")
                    processed_ids.add(match_id) # Add matched ID to processed set to avoid re-checking later
                log.info("---> End of matches <---")

        current_offset += len(records_batch)
        # Optional: Add a small delay between batches if needed
        # time.sleep(1)

    log.info(f"--- Similarity Analysis Finished ---")
    log.info(f"Total records analyzed: {total_processed}")
    log.info(f"Found {len(potential_duplicates)} records with potential matches above threshold {SIMILARITY_THRESHOLD}.")

    # Optional: Further processing/reporting of potential_duplicates list here 