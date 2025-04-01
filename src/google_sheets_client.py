import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta
from rich.console import Console
import time
import random
from typing import Optional
import re # Import regex module


class GSheetClient:
    def __init__(self, creds_path, sheet_url, max_retries=5, batch_size=25):
        self.console = Console()
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_url(sheet_url).sheet1
        self.max_retries = max_retries
        self.batch_size = batch_size

        # Force header creation and formatting for 5 columns
        current_headers = self.sheet.row_values(1)
        expected_headers = ["Timestamp", "Channel", "Message Text", "Message Link", "Tags"]
        self.console.print(f"Current headers: {current_headers}")
        self.console.print(f"Expected headers: {expected_headers}")
        # Check if headers match up to the expected length
        if len(current_headers) < len(expected_headers) or current_headers[:len(expected_headers)] != expected_headers:
            self.console.print("Updating headers...")
            self.sheet.update(
                "A1:E1", [expected_headers], value_input_option="USER_ENTERED" # Update range to E1
            )
        self.format_sheet()  # Always apply formatting

    def append_message(self, message):
        """Add message to top of sheet with deduplication check and retry logic"""
        try:
            existing = self.sheet.col_values(4)  # Link column (still D)
            if message["link"] not in existing:
                # Updated timestamp format to ISO 8601 with UTC+5 offset
                timestamp = message["timestamp"].astimezone(timezone(timedelta(hours=5)))
                formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[
                    :-3
                ] + timestamp.strftime("%z")
                formatted_timestamp = (
                    formatted_timestamp[:-2] + ":" + formatted_timestamp[-2:]
                )

                # Clean and truncate text to avoid potential issues
                message_text = message["text"].replace("\n", " ")[:495] if message["text"] else "[Media message]"
                tags = message.get("tags", "") # Get tags, default to empty string
                
                # Apply retry logic - Insert 5 columns
                self._execute_with_retry(
                    lambda: self.sheet.insert_row(
                        [
                            formatted_timestamp,
                            message["channel"][:50],
                            message_text,
                            message["link"],
                            tags # Add tags column
                        ],
                        index=2,
                        value_input_option="USER_ENTERED" # Ensure strings are treated as strings
                    )
                )
                self.console.print(f"[green]Successfully added message from {message['channel']}[/]")
                
        except Exception as e:
            self.console.print(f"[red]Error in append_message: {str(e)}[/]")
            # Log more details about the problematic message for diagnosis
            self.console.print(f"[yellow]Problem message details:[/]")
            self.console.print(f"Channel: {message.get('channel', 'N/A')}")
            self.console.print(f"Text length: {len(message.get('text', ''))}")
            self.console.print(f"Tags: {message.get('tags', 'N/A')}")
            self.console.print(f"Has special chars: {any(ord(c) > 127 for c in message.get('text', ''))}")

    def batch_append(self, messages):
        """Enhanced batch append with better deduplication, smaller batches, and retry logic"""
        if not messages:
            return 0
            
        self.console.print(f"[dim]Processing {len(messages)} new messages[/]")

        if messages:
            # Sort new messages by timestamp descending
            sorted_new_messages = sorted(
                messages, key=lambda x: x["timestamp"], reverse=True
            )

            # Prepare new rows with formatted timestamp and tags
            new_rows_to_write = []
            problem_messages = []
            for msg in sorted_new_messages:
                try:
                    # Convert to UTC+5 and format as ISO 8601
                    timestamp = msg["timestamp"].astimezone(
                        timezone(timedelta(hours=5))
                    )
                    formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[
                        :-3
                    ] + timestamp.strftime("%z")
                    formatted_timestamp = (
                        formatted_timestamp[:-2] + ":" + formatted_timestamp[-2:]
                    )
                    # Clean and sanitize text
                    clean_text = msg["text"].replace("\n", " ")[:495] if msg["text"] else "[Media message]"
                    tags = msg.get("tags", "") # Get tags
                    new_rows_to_write.append(
                        [
                            formatted_timestamp,
                            msg["channel"][:50],  # Truncate long channel names
                            clean_text,
                            msg["link"],
                            tags # Add tags
                        ]
                    )
                except Exception as e:
                    self.console.print(f"[red]Error formatting message: {str(e)}[/]")
                    problem_messages.append(msg) # Keep track but don't stop processing others
                    continue # Skip this message

            # -------- SAFER WRITE STRATEGY V3 (Combined Deduplication) --------
            try:
                self.console.print("[dim]Fetching all existing sheet data (excluding header)...[/]")
                all_existing_values = self._execute_with_retry(
                    # Fetch up to column E
                    lambda: self.sheet.get('A2:E' + str(self.sheet.row_count))
                )
                # Handle case where sheet might be empty except header
                if not all_existing_values:
                     all_existing_values = [] # Ensure it's an empty list, not None
                     
                self.console.print(f"[dim]Fetched {len(all_existing_values)} existing rows.[/]")

                # Combine new formatted rows with existing rows
                combined_rows_raw = new_rows_to_write + all_existing_values
                self.console.print(f"[dim]Combined {len(new_rows_to_write)} new and {len(all_existing_values)} existing rows. Total before dedupe: {len(combined_rows_raw)}[/]")

                # --- Comprehensive Deduplication (Prioritize ID-based links) ---
                self.console.print("[dim]Performing comprehensive deduplication on combined data (prioritizing ID links)...[/]")
                seen_links = set()
                deduplicated_rows = []
                duplicate_count = 0
                skipped_format_count = 0 # Count rows skipped due to non-ID link format
                processed_count = 0 
                # Define the expected link pattern
                id_link_pattern = re.compile(r"^https://t\.me/c/\d+/\d+$")

                for row_index, row in enumerate(combined_rows_raw):
                    processed_count += 1
                    link = None
                    try:
                        if len(row) > 3:
                            link_str = str(row[3]).strip() # Ensure string and strip whitespace
                            
                            # Check if the link matches the canonical /c/ID/ID format
                            if id_link_pattern.match(link_str):
                                link = link_str # Use the validated link
                                if link not in seen_links:
                                    seen_links.add(link)
                                    # Pad row if necessary
                                    if len(row) < 5:
                                        row.extend([''] * (5 - len(row)))
                                    deduplicated_rows.append(row[:5])
                                    # Optional Debug log (unique)
                                    # if row_index < 10 or row_index > len(combined_rows_raw) - 10: 
                                    #      self.console.print(f"[dim]Dedupe: Added unique link #{len(seen_links)}: '{link}'[/]")
                                else:
                                    duplicate_count += 1
                                    # Optional Debug log (duplicate)
                                    # if duplicate_count < 20: 
                                    #      self.console.print(f"[yellow]Dedupe: Skipped duplicate link: '{link}'[/]")
                            else:
                                # Link is not in the expected /c/ID/ID format, skip it
                                skipped_format_count += 1
                                # Optional Debug log (skipped format)
                                # if skipped_format_count < 10:
                                #      self.console.print(f"[cyan]Dedupe: Skipped non-ID format link: '{link_str}'[/]")
                                continue # Skip to next row
                        else:
                             self.console.print(f"[yellow]Dedupe Warning: Skipping row {row_index} - insufficient columns: {row}[/]")
                    except Exception as dedupe_err:
                         self.console.print(f"[red]Dedupe Error processing row {row_index} (Link: {link}): {dedupe_err} - Row data: {row}[/]")
                         continue
                 
                self.console.print(f"[dim]Dedupe: Processed {processed_count} raw rows.[/]")
                if skipped_format_count > 0:
                     self.console.print(f"[dim]Skipped {skipped_format_count} rows due to non-ID link format.[/]")
                if duplicate_count > 0:
                    self.console.print(f"[dim]Removed {duplicate_count} duplicate ID-link messages.[/]")
                self.console.print(f"[dim]Total rows after deduplication: {len(deduplicated_rows)}[/]")
                # --- End Comprehensive Deduplication ---
                
                # Sort the *fully deduplicated* rows by timestamp (first column) descending
                def get_sort_key(row):
                    try:
                        ts_str = row[0]
                        dt_obj = datetime.fromisoformat(ts_str)
                        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                            aware_dt = dt_obj.replace(tzinfo=timezone(timedelta(hours=5)))
                        else:
                            aware_dt = dt_obj
                        return aware_dt.astimezone(timezone.utc)
                    except (ValueError, IndexError, TypeError):
                        return datetime.min.replace(tzinfo=timezone.utc)

                self.console.print("[dim]Sorting deduplicated data...[/]")
                # Sort the final list
                deduplicated_rows.sort(key=get_sort_key, reverse=True)

                # Limit rows (apply to the final list)
                max_rows = 10000 
                if len(deduplicated_rows) > max_rows:
                    self.console.print(f"[yellow]Warning: Truncating final data to {max_rows} rows.[/]")
                    deduplicated_rows = deduplicated_rows[:max_rows]

                # Prepare data for the update (Header + deduplicated rows)
                self.console.print("[dim]Preparing final data for full sheet update...[/]")
                # Fetch expected 5 headers
                header_row = ["Timestamp", "Channel", "Message Text", "Message Link", "Tags"] 
                data_to_write = [header_row] + deduplicated_rows # Use the final deduplicated list

                # Define the target range
                num_rows_to_write = len(data_to_write)
                target_range = f"A1:E{num_rows_to_write}" if num_rows_to_write > 0 else "A1:E1" # Handle empty case
                
                # Clear any *extra* rows
                current_total_rows = self._execute_with_retry(lambda: self.sheet.row_count)
                # Calculate the target number of rows in the sheet (header + data rows)
                target_sheet_rows = num_rows_to_write
                if current_total_rows > target_sheet_rows:
                     # If the sheet has more rows than we are about to write, clear the excess
                     self.console.print(f"[dim]Clearing {current_total_rows - target_sheet_rows} extra rows from sheet bottom...[/]")
                     # Ensure we don't try to delete row 1 (header) if target_sheet_rows becomes 0 or 1
                     start_delete_row = max(2, target_sheet_rows + 1)
                     if start_delete_row <= current_total_rows:
                          self._execute_with_retry(
                               lambda: self.sheet.delete_rows(start_delete_row, current_total_rows)
                          )
                     else:
                          # This case might happen if the sheet only had a header (current_total_rows=1) and we have no data (target_sheet_rows=1)
                          self.console.print("[dim]No extra rows need clearing.[/]")
                
                self.console.print(f"[dim]Executing sheet update for range {target_range}...[/]")
                # Use a single update call
                # Handle the case where data_to_write might only contain the header
                if num_rows_to_write > 0:
                     self._execute_with_retry(
                          lambda: self.sheet.update(target_range, data_to_write, value_input_option="USER_ENTERED")
                     )
                else: 
                     # If there are no messages (new or old), just ensure the header exists
                     self.console.print("[yellow]No data to write (sheet might be empty except header).[/]")
                     # Optional: could clear everything except header here if desired, but update should handle A1:E1
                     self._execute_with_retry(
                          lambda: self.sheet.update("A1:E1", [header_row], value_input_option="USER_ENTERED")
                     )

                final_row_count = len(deduplicated_rows) # Actual data rows written
                self.console.print(f"[green]Sheet update complete. Total data rows: {final_row_count}[/]")
                
                # Calculate how many *new* unique messages were added in this specific run
                # This requires comparing the initial `new_rows_to_write` links 
                # with the `seen_links` set *after* processing existing values.
                initial_new_links = {row[3] for row in new_rows_to_write if len(row) > 3 and id_link_pattern.match(str(row[3]))} # Count only valid new links
                existing_links = {row[3] for row in all_existing_values if len(row) > 3 and id_link_pattern.match(str(row[3]))} # Count only valid existing links
                truly_new_links_added = initial_new_links - existing_links
                total_added_this_run = len(truly_new_links_added)
                self.console.print(f"[green]Total unique new messages added in this run: {total_added_this_run}[/]")
                return total_added_this_run

            except gspread.exceptions.APIError as e:
                self.console.print(f"[red]Error during sheet update operation: {str(e)}[/]")
                if problem_messages:
                     self.console.print(f"[yellow]Found {len(problem_messages)} problematic messages during formatting[/]")
                return 0 # Indicate failure

            # -------- END SAFER WRITE STRATEGY V3 --------

            # --- OLD BATCH INSERT LOGIC (commented out/removed) ---
            # # Process in smaller batches
            # total_added = 0
            # batches = [sorted_messages[i:i + self.batch_size] for i in range(0, len(sorted_messages), self.batch_size)]
            # self.console.print(f"[dim]Split into {len(batches)} batches of max {self.batch_size} messages[/]")
            #
            # for batch_num, batch in enumerate(batches, 1):
            #     # ... (rest of the old batch processing logic) ...
            # --- END OLD BATCH INSERT LOGIC ---

        return 0 # Return 0 if there were no new messages to process

    def deduplicate_and_rewrite_sheet(self):
        """Fetches all data, deduplicates based on link, sorts, and rewrites the sheet."""
        self.console.print("\n[bold cyan]Starting Sheet Deduplication and Rewrite Process[/]")

        try:
            # 1. Fetch all existing data (including header) - Fetch up to column E
            self.console.print("[dim]Fetching all existing sheet data...[/]")
            # Use get() to fetch up to expected column E, safer than get_all_values if columns vary
            try:
                all_sheet_values = self._execute_with_retry(
                     # Fetch slightly more rows initially in case sheet grows during operation
                     lambda: self.sheet.get(f'A1:E{self.sheet.row_count + 50}') 
                )
            except gspread.exceptions.APIError as api_error:
                 # If range is invalid (e.g., sheet completely empty), handle gracefully
                 if 'exceeds grid limits' in str(api_error) or 'Unable to parse range' in str(api_error):
                      self.console.print("[yellow]Sheet appears empty or has fewer columns than expected. Assuming empty.[/]")
                      all_sheet_values = []
                 else:
                      raise api_error # Re-raise other API errors
                      
            if not all_sheet_values:
                self.console.print("[yellow]Sheet appears to be empty. Nothing to deduplicate.[/]")
                # Ensure header exists if sheet was totally empty
                expected_headers = ["Timestamp", "Channel", "Message Text", "Message Link", "Tags"]
                self._execute_with_retry(lambda: self.sheet.update("A1:E1", [expected_headers], value_input_option="USER_ENTERED"))
                return
                
            # Use expected headers, don't rely on fetched headers if sheet was malformed
            header_row = ["Timestamp", "Channel", "Message Text", "Message Link", "Tags"]
            data_rows = all_sheet_values[1:] # Skip header row assumed to be fetched
            self.console.print(f"[dim]Fetched {len(data_rows)} data rows (plus header).[/]")

            # 2. Comprehensive Deduplication (Prioritize ID-based links)
            self.console.print("[dim]Performing comprehensive deduplication on fetched data (prioritizing ID links)...[/]")
            seen_links = set()
            deduplicated_rows = []
            duplicate_count = 0
            skipped_format_count = 0 # Count rows skipped due to non-ID link format
            # Define the expected link pattern
            id_link_pattern = re.compile(r"^https://t\.me/c/\d+/\d+$")
            
            for row in data_rows:
                try:
                    if len(row) > 3:
                        link_str = str(row[3]).strip()
                        # Check if the link matches the canonical /c/ID/ID format
                        if id_link_pattern.match(link_str):
                            link = link_str # Use the validated link
                            if link not in seen_links:
                                seen_links.add(link)
                                # Pad row if necessary
                                if len(row) < 5:
                                    row.extend([''] * (5 - len(row)))
                                deduplicated_rows.append(row[:5])
                            else:
                                duplicate_count += 1
                        else:
                            # Link is not in the expected /c/ID/ID format, skip it
                            skipped_format_count += 1
                            continue # Skip to next row
                    else:
                        self.console.print(f"[yellow]Warning: Skipping row with unexpected format during dedupe: {row}[/]")
                        continue
                except Exception as dedupe_err:
                     self.console.print(f"[red]Dedupe Error processing row: {dedupe_err} - Row data: {row}[/]")
                     continue
            
            if skipped_format_count > 0:
                 self.console.print(f"[dim]Skipped {skipped_format_count} rows due to non-ID link format.[/]")
            if duplicate_count > 0:
                self.console.print(f"[dim]Removed {duplicate_count} duplicate ID-link messages.[/]")
            self.console.print(f"[dim]Total rows after deduplication: {len(deduplicated_rows)}[/]")

            # 3. Sort the deduplicated rows
            # Reusing the same sort key logic from batch_append
            def get_sort_key(row):
                try:
                    ts_str = row[0]
                    dt_obj = datetime.fromisoformat(ts_str)
                    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                        aware_dt = dt_obj.replace(tzinfo=timezone(timedelta(hours=5)))
                    else:
                        aware_dt = dt_obj
                    return aware_dt.astimezone(timezone.utc)
                except (ValueError, IndexError, TypeError):
                    return datetime.min.replace(tzinfo=timezone.utc)

            self.console.print("[dim]Sorting deduplicated data...[/]")
            deduplicated_rows.sort(key=get_sort_key, reverse=True)

            # 4. Limit rows (optional but good practice)
            max_rows = 10000 
            if len(deduplicated_rows) > max_rows:
                self.console.print(f"[yellow]Warning: Truncating final data to {max_rows} rows.[/]")
                deduplicated_rows = deduplicated_rows[:max_rows]

            # 5. Prepare final data payload
            self.console.print("[dim]Preparing final data for full sheet update...[/]")
            data_to_write = [header_row] + deduplicated_rows

            # 6. Define target range (A1:E...) and clear extra rows
            num_rows_to_write = len(data_to_write)
            target_range = f"A1:E{num_rows_to_write}" if num_rows_to_write > 0 else "A1:E1"
            current_total_rows = self._execute_with_retry(lambda: self.sheet.row_count)
            target_sheet_rows = num_rows_to_write
            if current_total_rows > target_sheet_rows:
                 self.console.print(f"[dim]Clearing {current_total_rows - target_sheet_rows} extra rows from sheet bottom...[/]")
                 start_delete_row = max(2, target_sheet_rows + 1)
                 if start_delete_row <= current_total_rows:
                      self._execute_with_retry(
                           lambda: self.sheet.delete_rows(start_delete_row, current_total_rows)
                      )
            
            # 7. Execute single update for A1:E...
            self.console.print(f"[dim]Executing sheet update for range {target_range}...[/]")
            if num_rows_to_write > 0:
                 self._execute_with_retry(
                      lambda: self.sheet.update(target_range, data_to_write, value_input_option="USER_ENTERED")
                 )
            else: # Should only happen if sheet was empty initially
                 self.console.print("[yellow]Sheet is now empty except for header.[/]")
                 self._execute_with_retry(
                      lambda: self.sheet.update("A1:E1", [header_row], value_input_option="USER_ENTERED")
                 )

            final_row_count = len(deduplicated_rows)
            self.console.print(f"[green]Sheet cleanup complete. Total data rows: {final_row_count}[/]")

        except gspread.exceptions.APIError as e:
            self.console.print(f"[red]Error during sheet deduplication and rewrite: {str(e)}[/]")
        except Exception as e:
            self.console.print(f"[red]An unexpected error occurred during sheet cleanup: {str(e)}[/]")

    def get_last_timestamp_for_channel(self, channel_name: str) -> Optional[datetime]:
        """Fetches data and returns the latest timestamp for a specific channel."""
        self.console.print(f"[dim]Fetching last timestamp for channel: {channel_name}...[/]")
        try:
            # Fetch Timestamp (Col A) and Channel (Col B)
            # Fetch more rows than strictly necessary in case of ongoing appends
            data = self._execute_with_retry(
                lambda: self.sheet.get(f'A2:B{self.sheet.row_count + 50}')
            )
            if not data:
                self.console.print(f"[dim]No existing data found for {channel_name}.[/]")
                return None

            latest_timestamp = None
            target_channel_lower = channel_name.lower()

            for row in data:
                # Check if row has at least 2 columns and channel name matches (case-insensitive)
                if len(row) >= 2 and row[1].lower() == target_channel_lower:
                    try:
                        # Parse timestamp (assuming ISO format string from column A)
                        dt_obj = datetime.fromisoformat(row[0])
                        
                        # Make aware if naive (assuming UTC+5)
                        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                             dt_obj = dt_obj.replace(tzinfo=timezone(timedelta(hours=5)))
                        else:
                             # Ensure it's in the correct target timezone for comparison
                             dt_obj = dt_obj.astimezone(timezone(timedelta(hours=5)))
                             
                        # Update latest_timestamp if this one is newer
                        if latest_timestamp is None or dt_obj > latest_timestamp:
                            latest_timestamp = dt_obj
                            
                    except (ValueError, TypeError) as parse_error:
                        # Log problematic timestamp format but continue
                        self.console.print(f"[yellow]Warning: Could not parse timestamp '{row[0]}' for channel {channel_name}: {parse_error}[/]")
                        continue
                    except IndexError:
                         # Should be caught by len(row) check, but just in case
                         self.console.print(f"[yellow]Warning: Row format issue for {channel_name}: {row}[/]")
                         continue
                         
            if latest_timestamp:
                 self.console.print(f"[dim]Found last timestamp for {channel_name}: {latest_timestamp}[/]")
                 # Add a small buffer (e.g., 1 second) to avoid re-fetching the last message itself
                 return latest_timestamp + timedelta(seconds=1)
            else:
                 self.console.print(f"[dim]No valid previous timestamp found for {channel_name}.[/]")
                 return None

        except gspread.exceptions.APIError as e:
            self.console.print(f"[red]API Error fetching last timestamp for {channel_name}: {e}[/]")
            return None # Indicate failure to find timestamp
        except Exception as e:
            self.console.print(f"[red]Unexpected Error fetching last timestamp for {channel_name}: {e}[/]")
            return None
            
    def _execute_with_retry(self, operation, initial_delay=1, backoff_factor=2, jitter=0.1):
        """Execute an operation with exponential backoff retry logic"""
        delay = initial_delay
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return operation()
            except gspread.exceptions.APIError as e:
                last_exception = e
                # Only retry on server errors (5xx) or rate limit errors (429)
                if "500" not in str(e) and "429" not in str(e) and "503" not in str(e):
                    raise  # Don't retry client errors or other issues
                
                # Calculate jittered delay
                jitter_amount = random.uniform(-jitter * delay, jitter * delay)
                sleep_time = delay + jitter_amount
                
                self.console.print(f"[yellow]API error on attempt {attempt+1}/{self.max_retries}: {str(e)}[/]")
                self.console.print(f"[yellow]Retrying in {sleep_time:.2f} seconds...[/]")
                
                time.sleep(sleep_time)
                delay *= backoff_factor  # Exponential backoff
        
        # If we get here, all retries failed
        raise last_exception

    def test_connection(self):
        """Test connection to Google Sheets with a harmless operation"""
        try:
            # Try to fetch cell A1 (header) as a simple test
            self.console.print("[dim]Testing Google Sheets connection...[/]")
            test_cell = self.sheet.acell('A1').value
            self.console.print(f"[green]Connection successful! Found header: '{test_cell}'[/]")
            
            # Try a simple write operation to cell A1 and revert it
            original_value = test_cell
            
            # Fix: Use the correct format for the update method
            self._execute_with_retry(
                lambda: self.sheet.update('A1:A1', [[f"Test - {original_value}"]])
            )
            self.console.print(f"[green]Write test successful![/]")
            
            # Revert back to original value
            self._execute_with_retry(
                lambda: self.sheet.update('A1:A1', [[original_value]])
            )
            self.console.print(f"[green]✓ Google Sheets connection is working properly[/]")
            return True
        except Exception as e:
            self.console.print(f"[red]Failed to connect to Google Sheets: {str(e)}[/]")
            return False

    def format_sheet(self):
        """Enhanced sheet formatting with professional styling for 5 columns"""
        # Header formatting (up to E1)
        header_format = {
            "textFormat": {
                "bold": True,
                "fontSize": 12,
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
            },
            "backgroundColor": {"red": 0.15, "green": 0.35, "blue": 0.65},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE",
            "padding": {"top": 10, "bottom": 10},
        }
        self.sheet.format("A1:E1", header_format) # Apply to A1:E1

        # Set optimal column widths (add width for Tags column E)
        # Column width requests (A=220, B=180, C=500, D=350, E=200)
        column_requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": self.sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1,
                    },
                    "properties": {"pixelSize": size},
                    "fields": "pixelSize",
                }
            }
            for i, size in enumerate([220, 180, 500, 350, 200]) # Added width for E
        ]
        self.sheet.spreadsheet.batch_update({"requests": column_requests})

        # Format timestamp column A
        self.sheet.format(
            "A2:A",
            {
                "numberFormat": {"type": "TEXT"},
                "horizontalAlignment": "LEFT",
                "textFormat": {"fontFamily": "Roboto Mono"},
            },
        )
        # Format Tags column E
        self.sheet.format(
            "E2:E",
            {
                 "numberFormat": {"type": "TEXT"},
                 "horizontalAlignment": "LEFT",
                 "textFormat": {"fontFamily": "Roboto"}, # Standard font
                 "wrapStrategy": "WRAP" # Wrap long tag lists
            }
        )
        
        # Add frozen header and filter view (applies to all columns automatically)
        self.sheet.freeze(rows=1)
        self.sheet.set_basic_filter()

        # Set default row heights (adjust if needed)
        row_height_request = {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": self.sheet.id,
                    "dimension": "ROWS",
                    "startIndex": 1, # Start from row 2
                },
                "properties": {"pixelSize": 30},
                "fields": "pixelSize",
            }
        }
        self.sheet.spreadsheet.batch_update({"requests": [row_height_request]})
