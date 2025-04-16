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

    # 2. Construct Prompt (Multi-update + Guide detection + Uncertainty flag)
    prompt_template = f"""
Analyze the following Telegram message content. Perform two tasks:

**Task 1: Identify ALL Distinct Project Updates**
Extract details for EACH specific crypto project update into a list called "identified_updates".

**Task 2: Identify if Message is a Guide**
Determine if the overall message primarily functions as a step-by-step guide or tutorial. Provide this as top-level boolean `is_guide` and text `guide_summary` fields.

**CRITICAL INSTRUCTIONS for Task 1 (Project Updates):**
*   **Focus:** Identify specific updates, events, or tasks related to distinct crypto projects/protocols/tokens.
*   **Project Definition:** Only identify specific blockchain projects, protocols, dApps, or tokens as 'project_name'.
*   **Noise/Irrelevant:** Ignore general market commentary, pure promotions, simple mentions without context. If no specific updates found, return an empty list `[]` for "identified_updates".
*   **Granularity:** Use the most specific `activity_type` possible from the list below.

**Message Content:**
---
{cleaned_message_text}
---

**Source Channel:** {channel}

**Detailed Extraction Instructions for EACH object in "identified_updates" list:**
1.  `project_name`: The specific project/protocol/token this update is about.
2.  `activity_type`: Classify the specific activity. Choose ONE, be specific: Testnet, Airdrop Check, Airdrop Claim, Galxe Quest, Zealy Quest, Layer3 Quest, Other Quest/Task, Waitlist/Form, Partnership, Protocol Upgrade, Network Upgrade, New Feature Launch, Token Launch, Token Sale/IDO, Token Unlock, Token Burn, Exchange Listing, Staking Update, Yield Opportunity, DeFi Strategy, Vote/Governance, New Project Announcement, Community Call/AMA, Giveaway/Contest, Funding Round, Node Opportunity, Security Alert, General News/Update.
3.  `summary`: A brief 1-2 sentence summary of THIS specific update.
4.  `is_node_opportunity`: true/false if THIS update involves running nodes/validators.
5.  `key_links`: List relevant non-referral URLs related to THIS update.
6.  `referral_links`: List ONLY referral/invite URLs related to THIS update.
7.  `deadline`: Note any specific deadline mentioned for THIS update (text format, otherwise null).
8.  `required_actions_summary`: Briefly summarize required actions for THIS update (text format, otherwise null).
9.  `is_uncertain`: Boolean `true` if you are not confident about the classification or details of THIS specific update, `false` otherwise.

**Extraction Instructions for Task 2 (Guide Identification):**
*   `is_guide`: Top-level boolean field. Set to `true` if the *primary purpose* of the overall message is a step-by-step guide/tutorial, `false` otherwise.
*   `guide_summary`: Top-level text field. If `is_guide` is `true`, provide a brief summary describing the guide\'s topic (e.g., "Guide on farming ZkSync airdrop"). Omit or set to null if `is_guide` is `false`.
*   `primary_subject_project`: Top-level text field. If `is_guide` is `true`, identify the **single, main** project/protocol/token the guide is about. Even if other projects are mentioned as *part* of the guide (e.g., bridging via another protocol), choose the one that represents the **core topic or goal**. Use `null` ONLY if the guide is truly generic (e.g., 'How to set up a wallet') or if multiple distinct projects are discussed with clearly **equal importance** and no single main focus can be reasonably determined.

**Output Format:**
Output ONLY a single JSON object containing the top-level `is_guide`, `guide_summary`, `primary_subject_project` (if applicable), and the `identified_updates` list. Ensure valid JSON.

**Example Output (Message contains updates AND is also a guide):**
```json
{{
  "is_guide": true,
  "guide_summary": "Guide explaining how to participate in ZetaChain quests and check Hyperlane airdrop.",
  "primary_subject_project": "ZetaChain",
  "identified_updates": [
    {{
      "project_name": "ZetaChain",
      "activity_type": "Galxe Quest",
      "summary": "ZetaChain released new weekly quests on Galxe for XP farming.",
      "is_node_opportunity": false,
      "key_links": ["https://galxe.com/zetachain/campaign/GCxyz"],
      "referral_links": [],
      "deadline": null,
      "required_actions_summary": "Complete tasks on Galxe platform.",
      "is_uncertain": false
    }},
    {{
      "project_name": "Hyperlane",
      "activity_type": "Airdrop Check",
      "summary": "Hyperlane airdrop claim registration extended.",
      "is_node_opportunity": false,
      "key_links": ["https://x.com/hyperlane/status/1911788309119918171"],
      "referral_links": [],
      "deadline": "15.04.25 22:00 MSK",
      "required_actions_summary": "Register for claim if eligible.",
      "is_uncertain": false
    }}
  ]
}}
```

**Example Output (Message is ONLY a guide, no specific project updates):**
```json
{{
  "is_guide": true,
  "guide_summary": "General guide on setting up a Metamask wallet.",
  "primary_subject_project": null,
  "identified_updates": []
}}
```

**Example Output (Message contains ONLY updates, is NOT a guide):**
```json
{{
  "is_guide": false,
  "guide_summary": null,
  "primary_subject_project": null,
  "identified_updates": [
    {{
      "project_name": "Initia",
      "activity_type": "Exchange Listing",
      "summary": "INIT token listed on Bybit pre-market, price reached $0.70.",
      "is_node_opportunity": false,
      "key_links": ["https://www.bybit.com/trade/usdt/INITUSDT"],
      "referral_links": [],
      "deadline": null,
      "required_actions_summary": null,
      "is_uncertain": false
    }}
  ]
}}
```
"""

    # 3. Call Gemini API with retry logic
    max_retries = 3
    attempt = 0
    raw_response_text = None
    parsed_response = None # Full response object

    while attempt < max_retries:
        attempt += 1
        logging.info(f"Calling Gemini API (Attempt {attempt}/{max_retries}). Link: {message_link}")
        try:
            # Generate content using the model
            response = model.generate_content(prompt_template)
            raw_response_text = response.text
            logging.debug(f"Raw Gemini response for {message_link}:\n{raw_response_text}")

            # --- PARSING (Expecting main object with list and guide flags) ---
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', raw_response_text, re.DOTALL | re.IGNORECASE)
            if json_match:
                json_str = json_match.group(1).strip()
            else:
                json_str = raw_response_text.strip()
                if not json_str.startswith('{') or not json_str.endswith('}'):
                    raise ValueError("Response does not appear to be a JSON object.")

            parsed_response = json.loads(json_str)

            # Validate structure
            if not isinstance(parsed_response, dict) or 'identified_updates' not in parsed_response:
                 # Allow for guide-only responses where identified_updates might be empty but is_guide is present
                 if not parsed_response.get('is_guide'):
                     raise ValueError("Parsed JSON is not a dictionary or missing 'identified_updates' key (and not a guide).")
                 else: # It's likely a guide-only response
                     if 'identified_updates' not in parsed_response:
                          parsed_response['identified_updates'] = [] # Ensure the list exists even if empty

            if 'identified_updates' in parsed_response and not isinstance(parsed_response['identified_updates'], list):
                 raise ValueError("'identified_updates' key does not contain a list.")

            logging.info(f"Successfully parsed Gemini response for {message_link}. Updates found: {len(parsed_response.get('identified_updates', []))}. Is Guide: {parsed_response.get('is_guide')}")
            break # Exit retry loop on successful parse

        # --- EXCEPT BLOCKS --- (Handle errors during API call or parsing)
        except json.JSONDecodeError as json_err:
            logging.error(f"ANALYZER_ERROR: Failed to parse JSON response from Gemini. Error: {json_err}. Link: {message_link}\nRaw Response:\n{raw_response_text}", exc_info=True)
            return 0, False, f"JSON Parsing Error: {json_err}"
        except ValueError as val_err:
             logging.error(f"ANALYZER_ERROR: Invalid JSON structure or missing fields. Error: {val_err}. Link: {message_link}\nRaw Response:\n{raw_response_text}", exc_info=True)
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
    updates_list = parsed_response.get('identified_updates', [])
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
            final_payload = update_data # Start with AI output for this update
            final_payload['source_channel'] = channel
            final_payload['source_message_link'] = message_link
            final_payload['message_timestamp'] = iso_timestamp
            final_payload['full_message_text'] = cleaned_message_text

            # Map AI uncertainty to 'needs_review' flag
            is_uncertain = final_payload.pop('is_uncertain', False) # Get flag and remove from payload
            final_payload['needs_review'] = is_uncertain # Set DB flag based on AI output

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
             'deadline': None,
             'required_actions_summary': None,
             'source_channel': channel,
             'source_message_link': message_link, # Shares link with updates
             'message_timestamp': iso_timestamp,
             'full_message_text': cleaned_message_text,
             'needs_review': False # Assume guide identification is reliable
             # Add other relevant fields if needed, ensure they default/are nullable
         }
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