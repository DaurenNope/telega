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

# --- Gemini Analysis Function ---
def extract_message_data(message_text: str, channel: str, timestamp: Any, message_link: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Extracts structured data for ALL projects mentioned in a message,
    aggregates results into a single record, and returns it.
    Returns (aggregated_payload, error_message).
    """
    if not is_initialized or not model:
        logging.error("ANALYZER_ERROR: Analyzer not initialized or Gemini model unavailable.")
        # Return format needs to match type hint: (dict | None, str | None)
        return None, "Analyzer not initialized."

    # 1. Basic Input Validation
    if not message_text or message_text.strip() == "" or "[Media message]" in message_text:
        logging.warning(f"Skipping message analysis due to empty or media content. Link: {message_link}")
        return None, "Skipped: Empty or media message"

    # Clean message text
    try:
        cleaned_message_text = message_text.encode('utf-8', errors='ignore').decode('utf-8')
    except Exception as e:
        logging.error(f"Failed to clean message text for link {message_link}: {e}", exc_info=True)
        cleaned_message_text = message_text # Use original as fallback

    # 2. Construct Prompt
    # Updated prompt to be stricter and identify noise
    prompt_template = f"""
Analyze the following Telegram message content. Identify ALL distinct crypto projects mentioned **that are directly associated with a specific update, task, or actionable event relevant to the project itself (e.g., testnet, airdrop, protocol change, partnership)**. Extract the specified information for EACH relevant project into a JSON object containing a list called "identified_projects".

**CRITICAL INSTRUCTIONS:**
*   **Focus:** Prioritize messages announcing concrete project activities (Testnets, Airdrops, Quests, Governance Votes, Partnerships, Protocol Updates, Token Sales). General market commentary, price speculation, simple mentions without context, or advertisements for channels/groups should be classified as Noise/Advertisement.
*   **Project Definition:** Only identify specific blockchain projects, protocols, dApps, or tokens as 'project_name'. Do NOT identify general terms (like 'crypto', 'altcoin'), channel names (unless the channel *is* the project), or people's names/aliases as projects.
*   **Be Selective:** If a message is purely promotional, contains only referral links without substance, is vague market commentary, or discusses trading results without actionable project news, classify the primary entity (or the message context if no entity) as 'Noise/Advertisement' and minimize other extracted fields or return an empty list.

Message Content:
---
{cleaned_message_text}
---

Source Channel: {channel}

Detailed Extraction Instructions:
1.  Find every distinct relevant 'project_name' based on the criteria above. If no relevant project update is found, return an empty list [].
2.  For EACH project found, determine the primary 'activity_type'. Choose ONE from: Testnet, Airdrop Check, Airdrop Claim, Quest/Task, Waitlist/Form, Partnership, Protocol Update, Token Sale/IDO, Vote, New Project Announcement, Guide/Tutorial, Community/Social, Funding, Tokenomics/Sale, General Update, **Noise/Advertisement**. Use 'Noise/Advertisement' for non-actionable mentions, general market talk, or promotional content.
3.  For EACH project, determine if the message specifically discusses running a node, validator setup, or node operator incentives ('is_node_opportunity': true/false).
4.  For EACH project, write a brief 1-sentence 'summary' of the core update specific to that project. For 'Noise/Advertisement', the summary can be brief (e.g., "General market commentary", "Channel promotion").
5.  For EACH project, list relevant non-referral URLs in 'key_links'.
6.  For EACH project, list ONLY referral/invite URLs in 'referral_links'.
7.  For EACH project, note any specific deadline mentioned in 'deadline' (text format, otherwise null).
8.  For EACH project, summarize specific required actions in 'required_actions_summary'. If none or 'Noise/Advertisement', use null.

Output ONLY the JSON object containing the "identified_projects" list. Do not include any other text before or after the JSON. If no relevant projects are identified, the list should be empty.

Example (Relevant Project):
{{
  "identified_projects": [
    {{
      "project_name": "Project Alpha",
      "activity_type": "Testnet",
      "is_node_opportunity": true,
      "summary": "Project Alpha launched its incentivized testnet phase 2, node operators needed.",
      "key_links": ["https://alpha.example/testnet"],
      "referral_links": [],
      "deadline": "August 1st",
      "required_actions_summary": "Set up node according to docs and participate."
    }}
  ]
}}

Example (Noise/Advertisement Message - identify the main subject if possible, or just classify the message):
{{
  "identified_projects": [
    {{
      "project_name": "Ethereum", // Or potentially null if just general market talk
      "activity_type": "Noise/Advertisement",
      "is_node_opportunity": false,
      "summary": "General market commentary predicting price movement.",
      "key_links": [],
      "referral_links": [],
      "deadline": null,
      "required_actions_summary": null
    }},
    {{
      "project_name": "Oracle Channel", // Example if a channel is the main subject
      "activity_type": "Noise/Advertisement",
      "is_node_opportunity": false,
      "summary": "Promotion for a Telegram channel.",
      "key_links": ["https://t.me/channel_link"], // Link might be the channel itself
      "referral_links": [],
      "deadline": null,
      "required_actions_summary": null
    }}
  ]
}}

Example (No Relevant Projects Identified):
{{
  "identified_projects": []
}}
"""
    prompt = prompt_template

    # 3. Call Gemini API (Generation) - WITH RETRY
    max_retries = 1 # Retry only once on rate limit error
    attempt = 0
    raw_llm_response = None
    error_message = None
    while attempt <= max_retries:
        attempt += 1
        try:
            logging.info(f"Sending request to Gemini (Attempt {attempt}). Link: {message_link}")
            response = model.generate_content(prompt)
            raw_llm_response = response.text
            logging.debug(f"Raw Gemini response sample: {raw_llm_response[:200]}...")

            # Check for blocking immediately after getting a response
            if not response.parts:
                 if response.prompt_feedback and response.prompt_feedback.block_reason:
                    block_reason = response.prompt_feedback.block_reason
                    logging.warning(f"Prompt blocked by Gemini for link {message_link}. Reason: {block_reason}")
                    return None, f"Prompt blocked by Gemini. Reason: {block_reason}"
                 else:
                    logging.warning(f"Gemini returned an empty response with no block reason for link {message_link}.")
                    return None, "Gemini returned an empty response."

            # If successful, break the loop
            error_message = None
            break
        except ResourceExhausted as e:
            logging.warning(f"Gemini Generation API rate limit hit (Attempt {attempt}/{max_retries+1}). Waiting {GENERATION_RETRY_WAIT_SECONDS} seconds. Link: {message_link}. Error: {e}")
            error_message = f"Gemini Generation API rate limit hit: {e}"
            # Only wait if we are going to retry
            if attempt <= max_retries:
                 time.sleep(GENERATION_RETRY_WAIT_SECONDS)
            # If it's the last attempt, the loop will terminate, and error_message will be returned below
        except Exception as e:
            logging.error(f"Gemini Generation API error on attempt {attempt} for link {message_link}: {e}", exc_info=True)
            error_message = f"Gemini Generation API error: {e}"
            # Decide if you want to retry on other errors, currently breaks on first non-429 error
            break # Break on other errors

    # If loop finished due to error or max retries, error_message will be set
    if error_message:
        return None, error_message

    if not raw_llm_response:
        # Error handled within the retry loop or blocking check
        error_message = "Failed to get valid response from Gemini Generation API."
        logging.error(f"{error_message} Link: {message_link}")
        return None, error_message # Return immediately if generation failed

    # 4. Parse Gemini Response
    parsed_response_object = None
    parsing_error = None
    try:
        # Use the more robust parsing logic from before, trying regex then direct find
        json_string = None
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', raw_llm_response, re.DOTALL | re.IGNORECASE)
        if json_match:
            json_string = json_match.group(1)
        else:
            # Fallback: try finding the first '{' and last '}'
            start_index = raw_llm_response.find('{')
            end_index = raw_llm_response.rfind('}')
            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_string = raw_llm_response[start_index:end_index+1]
            else:
                 raise ValueError("Could not find valid JSON block in response.")

        parsed_response_object = json.loads(json_string)

        if not isinstance(parsed_response_object, dict) or "identified_projects" not in parsed_response_object:
             raise ValueError("Parsed JSON does not contain the expected 'identified_projects' key.")
        if not isinstance(parsed_response_object["identified_projects"], list):
             raise ValueError("'identified_projects' key does not contain a list.")

        logging.info(f"Successfully parsed JSON data from Gemini for link: {message_link}. Found {len(parsed_response_object.get('identified_projects', []))} projects.")

    except (json.JSONDecodeError, ValueError) as e:
        parsing_error = f"LLM response parsing error: {str(e)}"
        logging.error(f"{parsing_error} for link {message_link}. Response: {raw_llm_response[:500]}...", exc_info=False) # Maybe less verbose logging
        # logging.debug(f"Problematic JSON string attempt: {json_string}") # Debug if needed
        return None, parsing_error
    except Exception as e:
        parsing_error = f"Unexpected parsing error: {str(e)}"
        logging.error(f"{parsing_error} for link {message_link}: {e}", exc_info=True)
        return None, parsing_error

    # 5. Aggregate Results & Prepare Single Payload
    aggregated_data_payload = None
    final_error_message = None # Use this for save errors

    if parsed_response_object:
        projects_list = parsed_response_object.get("identified_projects", [])
        num_projects = len(projects_list)

        if num_projects == 0:
            logging.info(f"No projects identified by LLM for link {message_link}. Nothing to save.")
            # Return success, but indicate no projects found
            # Create a minimal dict to satisfy type hint, or adjust hint
            # Let's return None for payload if nothing to save, but no error.
            return None, None
        else:
            logging.info(f"{num_projects} projects identified by LLM for link {message_link}. Aggregating...")

            # --- Reintroduce Aggregation Logic --- (Copied from previous version)
            all_project_names = []
            all_activity_types = set()
            all_summaries = []
            all_key_links = set()
            all_referral_links = set()
            all_deadlines = set()
            all_required_actions = set()
            any_node_opportunity = False # Track this again

            for i, project_data in enumerate(projects_list):
                 if not isinstance(project_data, dict): continue

                 # Check for node opportunity
                 if project_data.get("is_node_opportunity") is True:
                      any_node_opportunity = True

                 name = project_data.get("project_name")
                 if name: all_project_names.append(name)
                 activity = project_data.get("activity_type")
                 if activity: all_activity_types.add(activity)
                 summary = project_data.get("summary")
                 # Aggregate summaries with project context
                 if summary: all_summaries.append(f"[{name or 'Unknown Project'}]: {summary}")
                 key_links = project_data.get("key_links", [])
                 if key_links and isinstance(key_links, list): all_key_links.update(key_links)
                 ref_links = project_data.get("referral_links", [])
                 if ref_links and isinstance(ref_links, list): all_referral_links.update(ref_links)
                 deadline = project_data.get("deadline")
                 if deadline: all_deadlines.add(str(deadline))
                 actions = project_data.get("required_actions_summary")
                 if actions: all_required_actions.add(actions)
            # --- End Aggregation Logic ---

            # --- Prepare the single payload --- (Copied from previous version)
            iso_timestamp = convert_timestamp_to_iso(timestamp)
            def join_items(items):
                 filtered_items = [str(item) for item in items if item]
                 return ", ".join(sorted(filtered_items)) if filtered_items else None

            aggregated_data_payload = {
                # Aggregated fields
                "project_name": join_items(all_project_names),
                "activity_type": join_items(all_activity_types),
                "identified_project_names": join_items(all_project_names), # Explicitly keep this if needed downstream
                "summary": "\n".join(all_summaries) if all_summaries else None,
                "key_links": sorted(list(all_key_links)), # Keep as array type
                "referral_links": sorted(list(all_referral_links)), # Keep as array type
                "deadline": join_items(all_deadlines),
                "required_actions_summary": join_items(all_required_actions),
                "mentions_node_opportunity": any_node_opportunity,
                # Raw LLM output might still be useful
                "raw_llm_output": parsed_response_object,
                # Original message fields
                "source_channel": channel,
                "source_message_link": message_link,
                "message_timestamp": iso_timestamp,
                "full_message_text": cleaned_message_text, # Use cleaned text
                # Add other fields expected by DB (adjust defaults as needed)
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "needs_review": False, # Default, adjust if needed
                "is_duplicate": False, # Default
                # "embedding", "summary_embedding" are removed
            }

            # 6. Attempt to Save Single Payload (Moved from main.py back here)
            if supabase:
                if not save_to_supabase(aggregated_data_payload):
                     # Set error message, payload remains for potential logging but indicates failure
                     final_error_message = f"Failed to save aggregated data (or message already processed). Link: {message_link}"
                     # Keep payload but return error
                     return aggregated_data_payload, final_error_message
                else:
                    # Save was successful, return payload and no error
                    return aggregated_data_payload, None
            else:
                logging.info("Supabase client not available, skipping save.")
                # Return payload but no error, as analysis succeeded but save was skipped
                return aggregated_data_payload, None
    else:
         # This case occurs if parsing failed earlier
         return None, parsing_error

# --- Utility Functions (e.g., join_items) ---
# Already defined within extract_message_data

# --- Main execution block for testing (optional) ---
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#     if init_analyzer():
#         logging.info("Analyzer initialized successfully for standalone test.")
#         # Add test calls here if needed
#         # test_message = "Check out the new testnet for Project Alpha! Node setup details available. https://alpha.example"
#         # test_channel = "Test Channel"
#         # test_timestamp = datetime.now(timezone.utc)
#         # test_link = "https://t.me/testchannel/123"
#         # status, summary = extract_message_data(test_message, test_channel, test_timestamp, test_link)
#         # logging.info(f"Test Run Status: {status}, Summary: {summary}")
#     else:
#         logging.error("Failed to initialize analyzer for standalone test.") 