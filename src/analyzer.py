import os
import sys
import json
import google.generativeai as genai
from dotenv import load_dotenv
import logging
import re
from supabase import create_client, Client
from datetime import datetime, timezone, timedelta
import time
from google.api_core.exceptions import ResourceExhausted # Import specific exception
from typing import Union, Dict, Any, Optional, Tuple

# --- Static Configuration ---
MODEL_NAME = "gemini-1.5-flash-latest"
SUPABASE_TABLE_NAME = "telegram_project_updates"
# Wait time in seconds after hitting API rate limits
GENERATION_RETRY_WAIT_SECONDS = 60

# --- Load Environment Variables ---
# Assumes .env is in the parent directory relative to src
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# --- Global Clients (Initialized by init_analyzer) ---
model: Union[genai.GenerativeModel, None] = None
supabase: Union[Client, None] = None
is_initialized = False

# --- Initialization Function ---
def init_analyzer():
    """Initializes Gemini and Supabase clients."""
    global model, supabase, is_initialized
    if is_initialized:
        logging.info("Analyzer already initialized.")
        return True

    logging.info("Initializing Analyzer...")
    gemini_ok = False

    # Configure Gemini (Generation)
    if not GEMINI_API_KEY:
        logging.error("ANALYZER_ERROR: GEMINI_API_KEY not found in environment variables.")
        return False
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(MODEL_NAME)
        logging.info(f"Gemini generation client configured with model: {MODEL_NAME}")
        gemini_ok = True
    except Exception as e:
        logging.error(f"ANALYZER_ERROR: Failed to configure Gemini generation client: {e}", exc_info=True)
        model = None

    # Configure Supabase
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logging.info("Supabase client configured.")
            # Optionally test connection here if needed, though first operation will test it
        except Exception as e:
            logging.error(f"ANALYZER_ERROR: Failed to configure Supabase client: {e}", exc_info=True)
            supabase = None # Ensure supabase is None on failure
            # Allow proceeding without Supabase if desired, but log error
    else:
        logging.warning("Supabase URL or Key not found in environment variables. Supabase integration disabled.")
        supabase = None

    # Initialization complete only if critical components are ready
    is_initialized = gemini_ok and (supabase is not None)
    if is_initialized:
        logging.info("Analyzer initialization complete.")
    else:
        logging.error("Analyzer initialization failed due to errors in client setup (Gemini Gen or Supabase).")

    return is_initialized

# --- Timestamp Conversion Function ---
def convert_timestamp_to_iso(timestamp_input):
    """Converts various timestamp inputs to an ISO 8601 string (UTC)."""
    if not timestamp_input:
        return None

    # Handle datetime objects (common from telethon)
    if isinstance(timestamp_input, datetime):
        # Ensure timezone info exists, default to UTC if naive
        if timestamp_input.tzinfo is None:
            dt_obj = timestamp_input.replace(tzinfo=timezone.utc)
        else:
            dt_obj = timestamp_input.astimezone(timezone.utc) # Convert to UTC
        return dt_obj.isoformat()

    # Handle numeric/string inputs (like Sheets serial or ISO strings)
    timestamp_str = str(timestamp_input)
    try:
        # Try parsing as ISO format first (handles cases where it might already be correct)
         dt_obj = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
         if dt_obj.tzinfo is None:
             dt_obj = dt_obj.replace(tzinfo=timezone.utc)
         else:
             dt_obj = dt_obj.astimezone(timezone.utc)
         return dt_obj.isoformat()
    except ValueError:
         # If ISO parsing fails, try Sheets serial number conversion
         try:
            serial_number = float(timestamp_str)
            base_date = datetime(1899, 12, 30, tzinfo=timezone.utc)
            actual_datetime = base_date + timedelta(days=serial_number)
            return actual_datetime.isoformat()
         except (ValueError, TypeError) as e:
            logging.warning(f"Could not convert timestamp '{timestamp_str}' (tried ISO and serial): {e}. Returning original.")
            return timestamp_str # Return original as last resort

# --- Supabase Save Function ---
def save_to_supabase(data_to_save: Dict[str, Any]):
    """Saves the provided data dictionary to the Supabase table."""
    if not supabase:
        logging.warning("Supabase client not initialized. Skipping save operation.")
        return False

    # Revert log identifier to just use message link
    log_identifier = f"message link: {data_to_save.get('source_message_link')}"

    try:
        logging.info(f"Attempting to insert data into Supabase table '{SUPABASE_TABLE_NAME}' for {log_identifier}")
        response = supabase.table(SUPABASE_TABLE_NAME).insert(data_to_save).execute()
        logging.debug(f"Supabase insert response raw: {response}")

        # Revert error checking to look for simple duplicate link key
        if hasattr(response, 'error') and response.error:
             error_details = response.error
             # Check for duplicate key error on the source_message_link constraint
             # Ensure your primary/unique constraint is ONLY on source_message_link now
             error_str = str(error_details).lower()
             # Use a simpler pattern check relevant to single column constraint
             if '23505' in error_str and ('telegram_project_updates_pkey' in error_str or 'telegram_project_updates_source_message_link_key' in error_str):
                  logging.warning(f"Message already processed (duplicate source_message_link): {log_identifier}")
                  return False # Indicate expected failure (already exists)
             else:
                  logging.error(f"Supabase insert failed for {log_identifier}. Error: {error_details} Response: {response}")
                  return False
        elif response.data is not None and len(response.data) > 0: # Original check was looking for non-empty data
             logging.info(f"Successfully saved data to Supabase for {log_identifier}")
             return True
        else:
            # Original logic might have treated empty data on success as failure/warning
             logging.error(f"Supabase insert failed or returned unexpected data for {log_identifier}. Response: {response}")
             # Or: logging.warning(...) if empty data can mean success
             return False # Assuming original logic treated this as failure

    except Exception as e:
        # Revert exception handling for duplicate key
        error_str = str(e).lower()
        if '23505' in error_str and ('telegram_project_updates_pkey' in error_str or 'telegram_project_updates_source_message_link_key' in error_str):
             logging.warning(f"Message already processed (duplicate source_message_link on exception): {log_identifier}")
             return False # Indicate expected failure
        else:
             logging.error(f"Failed to save data to Supabase for {log_identifier}: {e}", exc_info=True)
             return False

# --- Gemini Analysis Function (Multi-Project + Guide Logic) ---
def extract_message_data(message_text: str, channel: str, timestamp: Any, message_link: str) -> Tuple[int, bool, Optional[str]]:
    """
    Extracts structured data for ALL distinct project updates mentioned in a message,
    plus identifies if the message primarily serves as a guide.
    Saves each identified update and guide as separate rows in Supabase.
    Maps AI uncertainty to the 'needs_review' flag.
    Returns (updates_saved_count, guide_saved_flag, error_message).
    """
    if not is_initialized or not model:
        logging.error("ANALYZER_ERROR: Analyzer not initialized or Gemini model unavailable.")
        return 0, False, "Analyzer not initialized."

    # --- ADDED: Pre-check for existing message link ---
    if supabase:
        try:
            count_response = supabase.table(SUPABASE_TABLE_NAME)\
                .select('id', count='exact')\
                .eq('source_message_link', message_link)\
                .limit(1)\
                .execute()

            if count_response.count > 0:
                logging.info(f"Skipping analysis: Message link already exists in DB. Link: {message_link}")
                return 0, False, "Skipped: Message link already processed"
        except Exception as e:
            logging.warning(f"Could not perform pre-check for message link {message_link}: {e}", exc_info=True)
            # Decide if you want to proceed anyway or return an error. Proceeding for now.
    # --- END ADDED ---

    # 1. Basic Input Validation
    if not message_text or message_text.strip() == "" or "[Media message]" in message_text:
        logging.warning(f"Skipping message analysis due to empty or media content. Link: {message_link}")
        return 0, False, "Skipped: Empty or media message"

    # Clean message text
    try:
        MAX_MSG_LENGTH = 5000
        cleaned_message_text = message_text.encode('utf-8', errors='ignore').decode('utf-8')
        if len(cleaned_message_text) > MAX_MSG_LENGTH:
            logging.warning(f"Message text truncated to {MAX_MSG_LENGTH} chars for analysis. Link: {message_link}")
            cleaned_message_text = cleaned_message_text[:MAX_MSG_LENGTH] + "..."
    except Exception as e:
        logging.error(f"Failed to clean message text for link {message_link}: {e}", exc_info=True)
        cleaned_message_text = message_text[:MAX_MSG_LENGTH]

    # 2. Construct Prompt
    # Updated prompt AGAIN for stricter uniqueness and noise handling
    prompt_template = f"""
**ULTRA-CRITICAL INSTRUCTION: Ensure every object in the final 'identified_projects' list is UNIQUE for this message. Do NOT repeat the exact same combination of 'project_name' and 'activity_type'. If the same update is mentioned multiple times, represent it only ONCE.**

Analyze the following Telegram message content. Identify ALL distinct crypto projects mentioned **that are directly associated with a specific update, task, or actionable event relevant to the project itself (e.g., testnet, airdrop, protocol change, partnership, guide for specific project tasks)**. Extract the specified information for EACH relevant project/activity pair into a JSON object containing a list called "identified_projects".

**CRITICAL INSTRUCTIONS (Continued):**
1.  **Focus:** Only include projects with concrete updates relevant to their ecosystem (testnets, airdrops, mainnet launches, important governance votes, specific user tasks/quests, major partnerships, exchange listings for THAT project's token, project-specific guides).
2.  **Exclusions:**
    *   Exclude general market commentary, price speculation unrelated to a specific project event, predictions, generic advice, channel self-promotion, giveaways not tied to a specific project's activity, and cross-promotions unless it's a significant partnership announcement.
    *   Exclude simple mentions of a project name without a clear, actionable update or event tied to it.
    *   Exclude lists of many projects (e.g., "Top 5 projects to watch") unless each has a *distinct, specific* update described in the message.
    *   Exclude projects mentioned only as examples or comparisons.
3.  **Project Name:** Use the primary, recognizable name (e.g., "EigenLayer", "LayerZero", "ZkSync"). If multiple names are used, pick the most common or official one. Handle variations (e.g., "ZkSync Era" -> "ZkSync").
4.  **Activity Type:** Use ONE category from this specific list:
    *   `Testnet`: For testnet phases, incentivized testnets, instructions related to testnets.
    *   `Airdrop Claim`: For instructions or announcements about claiming an airdrop.
    *   `Airdrop Check`: For tools or announcements about checking eligibility for an airdrop.
    *   `Potential Airdrop`: Hints or activities suggested to qualify for a *future* airdrop (use this sparingly).
    *   `Token Sale/IDO`: For public/private sales, IDOs, launchpad events.
    *   `Mainnet Launch`: For the launch of a project's main network.
    *   `Protocol Update`: Significant changes, new features, upgrades to the core protocol.
    *   `Governance`: Important proposals, voting periods, results.
    *   `Partnership`: Significant collaboration announced between projects.
    *   `Exchange Listing`: Announcement of a project's token listing on a specific exchange.
    *   `Waitlist/Form`: Requirement to sign up via waitlist or form for access/rewards.
    *   `Quest/Task`: Specific actions users need to take (Galxe, Zealy, etc.) for rewards/participation *directly related to a project*.
    *   `NFT Mint`: Related to minting NFTs for a specific project.
    *   `Community/Social`: Contests, events, AMAs, Discord/Twitter specific activities related to the project.
    *   `Guide/Tutorial`: If the message provides instructions or a guide *for a specific project's activities* (e.g., "How to farm ZkSync airdrop").
    *   `Security Alert`: Warnings about scams or vulnerabilities related to the project.
    *   `General Update`: If the update is significant but doesn't fit neatly above (use sparingly).
    *   `Funding/Investment`: Announcement of funding rounds.
    *   `Noise/Advertisement`: For messages that are primarily market commentary, speculation, channel promotion, or other non-actionable content *even if they mention project names*. If a message mentions multiple projects but is mostly noise, you might extract a single "Noise/Advertisement" entry with `project_name: "Market Analysis"` or similar.
5.  **Summary:** Provide a concise (1-2 sentences) summary of the specific update or activity for *that* project. Focus on the core action or news.
6.  **Uncertainty (`needs_review`):** Set to `true` if you are uncertain about the project name, activity type, or if the update is truly significant/actionable. Otherwise, set to `false`. Be conservative; flag if unsure.
7.  **Deadlines:**
    a. `deadline_original_text`: Extract the *exact text* indicating a deadline (e.g., "until March 15th", "by 20:00 UTC", "next Friday", "in 2 weeks"). If no specific deadline is mentioned, output `null`. Do NOT include vague terms like "soon" or "later".
    b. `deadline_parsed`: If `deadline_original_text` is not null, parse it into a standard `YYYY-MM-DD HH:MM:SS+ZZ:ZZ` timestamp format. Assume the current year if not specified. Assume a default time of `19:00:00` if only a date is given. **Assume the time is relative to UTC+5 unless another timezone is explicitly mentioned (convert that timezone to UTC+5 equivalent).** If the deadline text is ambiguous or cannot be parsed confidently, output `null`. **If only a time is mentioned without a date (e.g., '12:00 UTC'), try to infer the date from context if possible (e.g., 'today', 'tomorrow'). If no date context exists, output null for `deadline_parsed`.**
8.  **Output Format:** Return ONLY a valid JSON object with the key "identified_projects" whose value is a list of JSON objects, each representing a unique project update found. If no relevant project updates are found, return `{{"identified_projects": []}}`.

**Example 1 (Multiple Projects, Deadline):**
*Message:* "Big news! EigenLayer Phase 2 restaking is live until June 30th. Also, ZkSync just launched their mainnet v2.4 upgrade. Don't forget the LayerZero airdrop checker is up!"
*Output:*
```json
{{
  "identified_projects": [
    {{
      "project_name": "EigenLayer",
      "activity_type": "Protocol Update",
      "summary": "Phase 2 restaking is now live.",
      "needs_review": false,
      "deadline_original_text": "until June 30th",
      "deadline_parsed": "2024-06-30 19:00:00+05:00"
    }},
    {{
      "project_name": "ZkSync",
      "activity_type": "Protocol Update",
      "summary": "Mainnet v2.4 upgrade has been launched.",
      "needs_review": false,
      "deadline_original_text": null,
      "deadline_parsed": null
    }},
    {{
      "project_name": "LayerZero",
      "activity_type": "Airdrop Check",
      "summary": "Airdrop eligibility checker is available.",
      "needs_review": false,
      "deadline_original_text": null,
      "deadline_parsed": null
    }}
  ]
}}
```

**Example 2 (Noise/Advertisement):**
*Message:* "BTC looking weak, might drop to 50k. Altcoins bleeding. Check out my premium channel for alpha calls! DYOR. Maybe keep an eye on SOL."
*Output:*
```json
{{
  "identified_projects": [
    {{
      "project_name": "Market Analysis",
      "activity_type": "Noise/Advertisement",
      "summary": "General market commentary and channel promotion.",
      "needs_review": false,
      "deadline_original_text": null,
      "deadline_parsed": null
    }}
  ]
}}
```

**Example 3 (Quest with Deadline):**
*Message:* "Complete the new Galxe quest for Project Neptune by tomorrow 8 PM CET to be eligible for rewards. Link in bio."
*Output:*
```json
{{
  "identified_projects": [
    {{
      "project_name": "Project Neptune",
      "activity_type": "Quest/Task",
      "summary": "New Galxe quest available for rewards.",
      "needs_review": false,
      "deadline_original_text": "by tomorrow 8 PM CET",
      "deadline_parsed": "2024-10-27 20:00:00+01:00" // Example assuming 'today' is Oct 26, 2024, CET is UTC+1
    }}
  ]
}}
```
**Example 4 (Vague Deadline - Null Parsed):**
*Message:* "WenMoon token presale ending very soon! Get in now!"
*Output:*
```json
{{
  "identified_projects": [
    {{
      "project_name": "WenMoon",
      "activity_type": "Token Sale/IDO",
      "summary": "Token presale is ending soon.",
      "needs_review": false,
      "deadline_original_text": null, // "very soon" is too vague for original text
      "deadline_parsed": null
    }}
  ]
}}
```

**Message to Analyze:**
```text
{cleaned_message_text}
```

**Your JSON Output:**
"""
    prompt = prompt_template

    # 3. Call Gemini API (Generation) - WITH RETRY
    max_retries = 3
    attempt = 0
    raw_response_text = None
    parsed_response = None # Full response object

    while attempt < max_retries:
        attempt += 1
        logging.info(f"Calling Gemini API (Attempt {attempt}/{max_retries}). Link: {message_link}")
        try:
            # Generate content using the model
            response = model.generate_content(prompt)
            # --- REMOVED DEBUGGING SECTION ---
            llm_response_text = response.text

            # --- PARSING (Expecting main object with list and guide flags) ---
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', llm_response_text, re.DOTALL | re.IGNORECASE)
            if json_match:
                json_str = json_match.group(1).strip()
            else:
                json_str = llm_response_text.strip()
                if not json_str.startswith('{') or not json_str.endswith('}'):
                    raise ValueError("Response does not appear to be a JSON object.")

            parsed_response = json.loads(json_str)

            # Validate structure
            if not isinstance(parsed_response, dict) or 'identified_projects' not in parsed_response:
                 # Allow for guide-only responses where identified_projects might be empty but is_guide is present
                 if not parsed_response.get('is_guide'):
                     raise ValueError("Parsed JSON is not a dictionary or missing 'identified_projects' key (and not a guide).")
                 else: # It's likely a guide-only response
                     if 'identified_projects' not in parsed_response:
                          parsed_response['identified_projects'] = [] # Ensure the list exists even if empty

            if 'identified_projects' in parsed_response and not isinstance(parsed_response['identified_projects'], list):
                 raise ValueError("'identified_projects' key does not contain a list.")

            logging.info(f"Successfully parsed Gemini response for {message_link}. Updates found: {len(parsed_response.get('identified_projects', []))}. Is Guide: {parsed_response.get('is_guide')}")
            break # Exit retry loop on successful parse

        # --- EXCEPT BLOCKS --- (Handle errors during API call or parsing)
        except json.JSONDecodeError as json_err:
            logging.error(f"ANALYZER_ERROR: Failed to parse JSON response from Gemini. Error: {json_err}. Link: {message_link}\nRaw Response:\n{llm_response_text}", exc_info=True)
            return 0, False, f"JSON Parsing Error: {json_err}"
        except ValueError as val_err:
             logging.error(f"ANALYZER_ERROR: Invalid JSON structure or missing fields. Error: {val_err}. Link: {message_link}\nRaw Response:\n{llm_response_text}", exc_info=True)
             return 0, False, f"Invalid Data Structure: {val_err}"
        except ResourceExhausted as rate_limit_err:
            logging.warning(f"ANALYZER_WARNING: Gemini API rate limit hit (Attempt {attempt}/{max_retries}). Waiting {GENERATION_RETRY_WAIT_SECONDS}s... Link: {message_link}")
            if attempt >= max_retries:
                logging.error(f"ANALYZER_ERROR: Gemini API rate limit exceeded after {max_retries} attempts. Link: {message_link}")
                return 0, False, "API Rate Limit Exceeded"
            time.sleep(GENERATION_RETRY_WAIT_SECONDS)
        except Exception as e:
            logging.error(f"ANALYZER_ERROR: An unexpected error occurred during Gemini API call or processing. Error: {e}. Link: {message_link}", exc_info=True)
            return 0, False, f"Unexpected API/Processing Error: {e}"

    # If loop finished due to retries without success or if parsing failed
    if parsed_response is None:
         return 0, False, "Failed to get or parse valid data from AI after retries."

    # 4. Process and Save Each Identified Update
    updates_list = parsed_response.get('identified_projects', [])
    saved_updates_count = 0
    save_errors = []
    iso_timestamp = convert_timestamp_to_iso(timestamp)

    if not updates_list:
         logging.info(f"No relevant project updates identified in message list. Link: {message_link}")
    else:
        for update_data in updates_list:
            if not isinstance(update_data, dict) or not update_data.get('project_name'):
                logging.warning(f"Skipping invalid or incomplete update item from AI for message {message_link}: {update_data}")
                continue

            # Prepare payload for this specific update
            # Extract deadline fields first and remove them from the AI output dict
            deadline_parsed_from_ai = update_data.pop('deadline_parsed', None)
            deadline_original_from_ai = update_data.pop('deadline_original_text', None)

            # Create a copy to avoid modifying the original dict in the list
            final_payload = update_data.copy() 

            # Add common fields
            final_payload['source_channel'] = channel
            final_payload['source_message_link'] = message_link
            final_payload['message_timestamp'] = iso_timestamp # Assign the converted timestamp
            final_payload['full_message_text'] = cleaned_message_text

            # Add the correctly named deadline fields for Supabase
            final_payload['deadline'] = deadline_parsed_from_ai # Mapped to timestamptz column
            final_payload['deadline_original_text'] = deadline_original_from_ai # Mapped to text column

            # Map AI uncertainty to 'needs_review' flag
            is_uncertain = final_payload.pop('is_uncertain', False) # Get flag and remove from payload if it exists
            final_payload['needs_review'] = is_uncertain # Set DB flag based on AI output

            # --- Generate Embedding ---
            # Construct text including key_links
            key_links = final_payload.get('key_links', [])
            links_string = " Links: " + " ".join(key_links) if key_links else ""
            embedding_text = f"Project: {final_payload.get('project_name', '')}, Activity: {final_payload.get('activity_type', '')}, Summary: {final_payload.get('summary', '')}{links_string}"
            
            embedding_vector = None # Initialize embedding vector
            try:
                # Using a standard text embedding model from Gemini
                embedding_response = genai.embed_content(
                    model="models/text-embedding-004",
                    content=embedding_text,
                    task_type="retrieval_document" # Appropriate for DB storage/similarity search
                )
                embedding_vector = embedding_response['embedding']
                final_payload['embedding'] = embedding_vector
                logging.debug(f"Generated embedding for project '{final_payload.get('project_name')}'.")
            except Exception as emb_err:
                logging.warning(f"Failed to generate embedding for project '{final_payload.get('project_name')}'. Error: {emb_err}")
                final_payload['embedding'] = None # Ensure embedding is null on failure
            # --- End Embedding Generation ---

            # --- Real-time Duplicate Check --- 
            final_payload['is_duplicate'] = False # Default to False
            if embedding_vector and supabase: # Only check if embedding was successful and supabase is available
                try:
                    # Use a higher threshold for automated flagging
                    duplicate_threshold = 0.92 
                    match_count = 1 # We only need to know if at least one high-similarity match exists
                    
                    logging.debug(f"Checking for duplicates for project '{final_payload.get('project_name')}' (Threshold: {duplicate_threshold})")
                    response = supabase.rpc('match_similar_updates', {
                        'query_embedding': embedding_vector,
                        'match_threshold': duplicate_threshold,
                        'match_count': match_count,
                        'exclude_id': None # Check against all existing records
                    }).execute()

                    if response.data: # If the RPC returned any matches above the threshold
                        logging.info(f"Potential duplicate found for project '{final_payload.get('project_name')}'. Flagging record. Match details: {response.data[0]}")
                        final_payload['is_duplicate'] = True
                    elif hasattr(response, 'error') and response.error:
                        logging.warning(f"Error during real-time duplicate check RPC call: {response.error}")
                    else:
                        logging.debug("No high-similarity duplicates found.")

                except Exception as rpc_err:
                    logging.warning(f"Exception during real-time duplicate check for project '{final_payload.get('project_name')}': {rpc_err}")
            # --- End Real-time Duplicate Check ---

            # Save this individual update payload
            if save_to_supabase(final_payload):
                logging.info(f"Successfully saved update for project '{update_data.get('project_name')}'. Needs Review: {is_uncertain}. Link: {message_link}")
                saved_updates_count += 1
            else:
                # Log specific save error (duplicate or DB issue)
                logging.warning(f"Failed to save update for project '{update_data.get('project_name')}' (DB error). Link: {message_link}")
                save_errors.append(update_data.get('project_name', 'Unknown'))

    # 5. Process and Save Guide Information (if applicable)
    is_guide = parsed_response.get('is_guide', False)
    guide_summary = parsed_response.get('guide_summary')
    primary_subject_project = parsed_response.get('primary_subject_project') # Get the subject project
    guide_saved_flag = False

    if is_guide and guide_summary:
         logging.info(f"Message identified as a guide. Saving guide entry. Link: {message_link}")
         guide_payload = {
             'project_name': primary_subject_project, # Use identified subject, defaults to None if not found
             'activity_type': 'Guide/Tutorial', # Specific type for guides
             'summary': guide_summary,
             'is_node_opportunity': None, # Or False? Set to None for clarity
             'key_links': [], # Maybe parse links from guide summary later?
             'referral_links': [],
             'deadline_original_text': None,
             'deadline_parsed': None,
             'required_actions_summary': None,
             'source_channel': channel,
             'source_message_link': message_link, # Shares link with updates
             'message_timestamp': iso_timestamp,
             'full_message_text': cleaned_message_text,
             'needs_review': False # Assume guide identification is reliable
             # Add other relevant fields if needed, ensure they default/are nullable
         }
         # Ensure the deadline fields for the guide are correctly named
         guide_payload['deadline'] = None # Parsed deadline is likely null for guides
         guide_payload['deadline_original_text'] = None # Original text is likely null too

         if save_to_supabase(guide_payload):
             logging.info(f"Successfully saved guide entry for project: {primary_subject_project or 'Generic'}. Link: {message_link}")
             guide_saved_flag = True
         else:
             logging.warning(f"Failed to save guide entry (DB error). Link: {message_link}")
             # Add guide summary to save errors? Maybe not critical.

    # 6. Return overall result
    final_error_message = None
    if saved_updates_count == 0 and not guide_saved_flag and not updates_list and not is_guide:
         final_error_message = "No relevant updates or guide identified."
    elif save_errors:
         final_error_message = f"Partially saved. Failed updates for: {', '.join(save_errors)}".strip()
    elif not guide_saved_flag and is_guide: # Updates saved fine (or none existed), but guide failed
         final_error_message = "Updates processed, but failed to save guide entry."

    return saved_updates_count, guide_saved_flag, final_error_message

# --- Helper to join list items safely (if needed elsewhere, currently removed) ---
# def join_items(items):
#     if isinstance(items, list):
#         return ', '.join(map(str, items))
#     return str(items) # Return as string if not a list

# --- (Optional) Add any other helper functions needed ---