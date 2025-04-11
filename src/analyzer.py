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

# --- Static Configuration ---
MODEL_NAME = "gemini-1.5-flash-latest"
SUPABASE_TABLE_NAME = "telegram_project_updates" # Ensure this matches your table

# --- Load Environment Variables ---
# Assumes .env is in the parent directory relative to src
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# --- Global Clients (Initialized by init_analyzer) ---
model: genai.GenerativeModel | None = None
supabase: Client | None = None
is_initialized = False

# --- Initialization Function ---
def init_analyzer():
    """Initializes Gemini and Supabase clients."""
    global model, supabase, is_initialized
    if is_initialized:
        logging.info("Analyzer already initialized.")
        return True

    logging.info("Initializing Analyzer...")

    # Configure Gemini
    if not GEMINI_API_KEY:
        logging.error("ANALYZER_ERROR: GEMINI_API_KEY not found in environment variables.")
        return False
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(MODEL_NAME)
        logging.info(f"Gemini client configured with model: {MODEL_NAME}")
    except Exception as e:
        logging.error(f"ANALYZER_ERROR: Failed to configure Gemini client: {e}", exc_info=True)
        model = None # Ensure model is None on failure
        return False

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

    is_initialized = True
    logging.info("Analyzer initialization complete.")
    return True # Indicate success

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
def save_to_supabase(data_to_save):
    """Saves the provided data dictionary to the Supabase table."""
    if not supabase:
        logging.warning("Supabase client not initialized. Skipping save operation.")
        return False

    # Use message link as primary identifier for logging now
    log_identifier = f"message link: {data_to_save.get('source_message_link')}"

    try:
        logging.info(f"Attempting to insert data into Supabase table '{SUPABASE_TABLE_NAME}' for {log_identifier}")
        response = supabase.table(SUPABASE_TABLE_NAME).insert(data_to_save).execute()
        logging.debug(f"Supabase insert response raw: {response}")

        if response.data and len(response.data) > 0:
             logging.info(f"Successfully saved data to Supabase for {log_identifier}")
             return True
        else:
             error_details = getattr(response, 'error', 'No specific error details in response')
             # Check for duplicate key error explicitly - this indicates already processed
             if error_details and '23505' in str(error_details) and 'telegram_project_updates_source_message_link_key' in str(error_details):
                  logging.warning(f"Message already processed (duplicate source_message_link): {log_identifier}")
                  return False # Indicate failure, but it's an expected "failure"
             else:
                  logging.error(f"Supabase insert failed or returned unexpected data for {log_identifier}. Error: {error_details} Response: {response}")
                  return False

    except Exception as e:
        # Catch potential db errors during insert more broadly
        if 'duplicate key value violates unique constraint "telegram_project_updates_source_message_link_key"' in str(e):
             logging.warning(f"Message already processed (duplicate source_message_link on exception): {log_identifier}")
             return False # Indicate expected failure
        else:
             logging.error(f"Failed to save data to Supabase for {log_identifier}: {e}", exc_info=True)
             return False

# --- Gemini Analysis Function ---
def extract_message_data(message_text, channel, timestamp, message_link):
    """
    Extracts structured data for ALL projects, including node opportunities,
    aggregates results into a single record per message, and attempts to save to Supabase.
    """
    if not is_initialized or not model:
        logging.error("ANALYZER_ERROR: Analyzer not initialized or Gemini model unavailable.")
        return None, "Analyzer not initialized."

    raw_llm_response = None
    error_message = None
    parsed_response_object = None

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

    # 2. Construct Prompt - MODIFIED FOR NODE OPPORTUNITIES
    prompt_template = f"""
Analyze the following Telegram message content. Identify ALL distinct crypto projects mentioned and extract the specified information for EACH project into a JSON object containing a list called "identified_projects".

Message Content:
---
{cleaned_message_text}
---

Source Channel: {channel}

Instructions:
1. Find every distinct 'project_name' being discussed (e.g., "Babylon", "Mind Network", "Seal"). If no specific project is clearly mentioned, return an empty list [].
2. For EACH project found, determine the primary 'activity_type' relevant to that project's mention in the message. Choose ONE from: Testnet, Airdrop Check, Airdrop Claim, Quest/Task, Waitlist/Form, Partnership, Protocol Update, Token Sale/IDO, Vote, New Project Announcement, Guide/Tutorial, Community/Social, Funding, Tokenomics/Sale, General Update, Market Commentary, Noise/Other.
3. **NEW:** For EACH project, determine if the message content specifically discusses running a node, setting up a validator, node requirements, or participating in an incentivized testnet *as a node operator* for that project. Set 'is_node_opportunity' to true if yes, otherwise false.
4. For EACH project, write a brief 1-sentence 'summary' of the core update specific to that project.
5. For EACH project, list all non-referral URLs relevant to that project in the 'key_links' array.
6. For EACH project, list ONLY referral/invite URLs relevant to that project in the 'referral_links' array.
7. For EACH project, if a specific deadline is mentioned relevant to it, note it in 'deadline' (text format). Otherwise, use null.
8. For EACH project, briefly summarize any specific actions the user needs to take related to it in 'required_actions_summary'. If none, use null.

Output ONLY the JSON object containing the "identified_projects" list. Do not include any other text before or after the JSON. If no projects are identified, the list should be empty.

Example JSON format for a message mentioning two projects (one with node info):
{{
  "identified_projects": [
    {{
      "project_name": "Project Alpha",
      "activity_type": "Testnet",
      "is_node_opportunity": true, // << NEW FIELD
      "summary": "Project Alpha launched its incentivized testnet phase 2, node operators needed.",
      "key_links": ["https://alpha.example/testnet", "https://alpha.example/node-docs"],
      "referral_links": [],
      "deadline": "August 1st",
      "required_actions_summary": "Set up node according to docs and participate."
    }},
    {{
      "project_name": "Project Beta",
      "activity_type": "Airdrop Claim",
      "is_node_opportunity": false, // << NEW FIELD
      "summary": "Project Beta airdrop claim is now live for early users.",
      "key_links": ["https://beta.example/claim"],
      "referral_links": [],
      "deadline": null,
      "required_actions_summary": "Check eligibility and claim tokens."
    }}
  ]
}}

Example JSON format if no project identified:
{{
  "identified_projects": []
}}
"""
    prompt = prompt_template

    # 3. Call Gemini API - WITH RETRY for 429
    max_retries = 1 # Retry only once on rate limit error
    base_wait_time = 5 # Seconds to wait if API doesn't suggest a delay
    attempt = 0
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
            logging.warning(f"Gemini API rate limit hit (429) on attempt {attempt}. Link: {message_link}. Error: {e}")
            error_message = f"Gemini API error: {str(e)}" # Store the error message

            if attempt > max_retries:
                logging.error(f"Max retries ({max_retries}) exceeded for Gemini API call. Link: {message_link}")
                break # Exit loop after max retries

            # Extract suggested retry delay from the error metadata if possible
            wait_time = base_wait_time
            try:
                # The structure might vary, need to inspect the actual error object 'e'
                # This is an example based on the log format, adjust if needed
                if hasattr(e, 'metadata') and isinstance(e.metadata, tuple):
                     for item in e.metadata:
                         if hasattr(item, 'key') and item.key == 'retry_delay' and hasattr(item, 'value'):
                              delay_seconds = int(item.value.split('.')[0]) # Get seconds part
                              wait_time = max(delay_seconds, 1) # Use API delay, ensure at least 1s
                              logging.info(f"API suggested retry delay: {wait_time} seconds.")
                              break
            except Exception as parse_err:
                 logging.warning(f"Could not parse suggested retry delay from error metadata: {parse_err}")

            logging.info(f"Waiting for {wait_time} seconds before retrying Gemini call. Link: {message_link}")
            time.sleep(wait_time) # Use time.sleep as this runs in a thread

        except Exception as e:
            # Handle other unexpected API errors
            logging.error(f"Gemini API call failed (Attempt {attempt}). Link: {message_link}: {e}", exc_info=True)
            error_message = f"Gemini API error: {str(e)}"
            break # Exit loop on non-retryable error

    # If loop finished due to error or max retries, error_message will be set
    if error_message:
        return None, error_message

    # 4. Robust Response Parsing
    json_string = None
    try:
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', raw_llm_response, re.DOTALL | re.IGNORECASE)
        if json_match:
            json_string = json_match.group(1)
        else:
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
        logging.error(f"{parsing_error} for link {message_link}. Response: {raw_llm_response}", exc_info=True)
        logging.debug(f"Problematic JSON string attempt: {json_string}")
        return None, parsing_error # Return error early
    except Exception as e:
        parsing_error = f"Unexpected parsing error: {str(e)}"
        logging.error(f"{parsing_error} for link {message_link}: {e}", exc_info=True)
        return None, parsing_error # Return error early

    # 5. Aggregate Results & Prepare Single Payload (MODIFIED AGGREGATION)
    aggregated_data_payload = None
    final_error_message = None

    if parsed_response_object:
        projects_list = parsed_response_object.get("identified_projects", [])
        num_projects = len(projects_list)

        if num_projects == 0:
            logging.info(f"No projects identified by LLM for link {message_link}. Nothing to save.")
            return {"identified_project_names": ""}, None # Indicate success, but no projects
        else:
            logging.info(f"{num_projects} projects identified by LLM for link {message_link}. Aggregating...")

            # --- MODIFIED Aggregation Logic ---
            all_project_names = []
            all_activity_types = set()
            all_summaries = []
            all_key_links = set()
            all_referral_links = set()
            all_deadlines = set()
            all_required_actions = set()
            any_node_opportunity = False # <<< Flag to track if any project mentions nodes

            for i, project_data in enumerate(projects_list):
                 if not isinstance(project_data, dict): continue

                 # Check for node opportunity
                 if project_data.get("is_node_opportunity") is True: # <<< Check the new flag
                      any_node_opportunity = True

                 name = project_data.get("project_name")
                 if name: all_project_names.append(name)
                 activity = project_data.get("activity_type")
                 if activity: all_activity_types.add(activity)
                 summary = project_data.get("summary")
                 if summary: all_summaries.append(f"[{name or 'Unknown Project'}]: {summary}")
                 key_links = project_data.get("key_links", [])
                 if key_links: all_key_links.update(key_links)
                 ref_links = project_data.get("referral_links", [])
                 if ref_links: all_referral_links.update(ref_links)
                 deadline = project_data.get("deadline")
                 if deadline: all_deadlines.add(str(deadline))
                 actions = project_data.get("required_actions_summary")
                 if actions: all_required_actions.add(actions)
            # --- End Modified Aggregation ---

            # --- Prepare the single payload - ADD mentions_node_opportunity ---
            iso_timestamp = convert_timestamp_to_iso(timestamp)
            def join_items(items): # Keep helper function
                 filtered_items = [str(item) for item in items if item]
                 return ", ".join(sorted(filtered_items)) if filtered_items else None

            aggregated_data_payload = {
                # Use comma-separated lists for multi-item fields
                "project_name": join_items(all_project_names), # Mirror identified_project_names
                "activity_type": join_items(all_activity_types),
                "identified_project_names": join_items(all_project_names), # Keep this specific field too
                "summary": "\n".join(all_summaries) if all_summaries else None, # Join summaries with newlines
                "key_links": sorted(list(all_key_links)), # Keep as array type
                "referral_links": sorted(list(all_referral_links)), # Keep as array type
                "deadline": join_items(all_deadlines),
                "required_actions_summary": join_items(all_required_actions),
                # Full LLM output is still valuable
                "raw_llm_output": parsed_response_object,
                # Original message fields
                "source_channel": channel,
                "source_message_link": message_link,
                "message_timestamp": iso_timestamp,
                "full_message_text": message_text,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "needs_review": True, # Flag for review might be useful now
                "is_duplicate": False,
                "mentions_node_opportunity": any_node_opportunity # <<< Set the new field
            }

            # 6. Attempt to Save Single Payload
            if supabase:
                if not save_to_supabase(aggregated_data_payload):
                     final_error_message = f"Failed to save aggregated data (or message already processed). Link: {message_link}"
                     aggregated_data_payload = None
            else:
                logging.info("Supabase client not available, skipping save.")
    else:
         final_error_message = "LLM Response parsing failed."
         aggregated_data_payload = None

    return aggregated_data_payload, final_error_message 