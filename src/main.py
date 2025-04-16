import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict
from rich.console import Console
from rich.progress import Progress
from dotenv import load_dotenv
from telegram_client import TelegramScraper
from telethon import events
from telethon.tl.types import Message
import requests
import time # Import time module

# --- NEW: Import analyzer functions ---
from analyzer import init_analyzer, extract_message_data # Relative import

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)

console = Console()

CHANNELS_FILE = "channels.txt"

# --- NEW: Define keys for environment variables ---
TELEGRAM_API_ID_KEY = "TELEGRAM_API_ID"
TELEGRAM_API_HASH_KEY = "TELEGRAM_API_HASH"
TELEGRAM_SESSION_NAME_KEY = "TELEGRAM_SESSION_NAME" # e.g., "telegram_scraper"

# --- Notification Helper ---
def send_telegram_notification(message: str):
    """Attempts to send a notification message via the Telegram Bot API."""
    bot_token = os.getenv("NOTIFICATION_BOT_TOKEN")
    chat_id = os.getenv("NOTIFICATION_CHAT_ID")
    
    if not bot_token or not chat_id:
        console.print("[yellow]NOTIFICATION_BOT_TOKEN or NOTIFICATION_CHAT_ID not set in .env, skipping notification.[/]")
        return

    # Wrap the incoming message in a Markdown code block for safety
    safe_message = f"```\n{message}\n```"

    payload = {
        'chat_id': chat_id,
        'text': f"⚠️ Scraper Alert ⚠️\n\n{safe_message}",
        'parse_mode': 'Markdown'
    }
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    console.print(f"[dim]Notification URL: {url}[/]")
    console.print(f"[dim]Notification Payload: {payload}[/]")
    
    response = None
    try:
        console.print(f"[dim]Attempting to send notification via Bot API to chat ID {chat_id}...[/]")
        # Make the request synchronously
        response = requests.post(url, data=payload, timeout=15)
        
        console.print(f"[dim]Notification Response Status Code: {response.status_code}[/]")
        console.print(f"[dim]Notification Response Raw Text: {response.text}[/]")
        
        response.raise_for_status()
        
        response_json = response.json()
        if response_json.get("ok"):
            console.print("[green]Notification sent successfully via Bot API.[/]")
        else:
            # Ensure error description is also sent safely if possible
            error_desc = response_json.get('description', 'Unknown API Error')
            console.print(f"[red]Telegram Bot API returned an error: ```{error_desc}```[/]")
            
    except requests.exceptions.RequestException as req_err:
        console.print(f"[red]Failed to send Telegram notification (Request Error): {str(req_err)}[/]")
        if response is not None:
             console.print(f"[dim]Failed Response Status: {response.status_code}[/]")
             # Send raw response text safely
             safe_body = f"```\n{response.text}\n```"
             console.print(f"[dim]Failed Response Body: {safe_body}[/]")
    except Exception as notify_err:
        console.print(f"[red]Failed to send Telegram notification (Other Error): {str(notify_err)}[/]")
        if response is not None:
             console.print(f"[dim]Error Response Status: {response.status_code}[/]")
             # Send raw response text safely
             safe_body = f"```\n{response.text}\n```"
             console.print(f"[dim]Error Response Body: {safe_body}[/]")
        
# --- End Notification Helper ---


def load_channels():
    """Load and deduplicate channels from storage file"""
    try:
        with open(CHANNELS_FILE, "r") as f:
            raw_channels = [line.strip() for line in f if line.strip()]

            # Deduplicate while preserving order
            seen = set()
            channels = []
            for chan in raw_channels:
                # Skip comments and empty lines
                if chan.startswith("#") or not chan:
                    continue
                # Normalize channel names
                clean_chan = chan.lower().replace("@", "").strip()
                if clean_chan not in seen:
                    seen.add(clean_chan)
                    channels.append(chan.strip())
            return channels

    except FileNotFoundError:
        open(CHANNELS_FILE, "w").close()  # Create empty file
        return []


def save_channel(channel):
    """Add new channel with deduplication check"""
    # Normalize input
    clean_channel = channel.lower().replace("@", "").strip()

    existing = load_channels()
    existing_clean = [c.lower().replace("@", "") for c in existing]

    if clean_channel not in existing_clean:
        with open(CHANNELS_FILE, "a") as f:
            f.write(f"{channel}\n")
        console.print(f"[green]✓[/] Added channel: {channel}")
    else:
        console.print(f"[yellow]⚠[/] Channel already exists: {channel}")


def clean_channels_file():
    """Remove duplicate channels from file"""
    channels = load_channels()  # Uses new deduplicated list
    with open(CHANNELS_FILE, "w") as f:
        f.write("\n".join(channels))
    console.print(f"[green]Cleaned {len(channels)} unique channels[/]")


async def interactive_mode(tg_client: TelegramScraper):
    """Interactive command-line interface (GSheet options removed)"""
    while True:
        console.print("\n[bold cyan]Telegram Scraper Menu[/]", justify="center")
        console.print("1. Scrape channel history")
        console.print("2. Start real-time listener")
        console.print("4. Clean channel list")
        console.print("6. Exit")

        choice = console.input("\n[bold]Enter your choice (1-2, 4, 6): [/]")

        if choice == "1":
            await handle_scrape_mode(tg_client)
        elif choice == "2":
            await handle_listen_mode(tg_client)
        elif choice == "4":
            clean_channels_file()
        elif choice == "6":
            console.print("[yellow]Exiting...[/]")
            break
        else:
            console.print("[red]Invalid choice! Please try again.[/]")


async def handle_scrape_mode(tg_client: TelegramScraper):
    """Updated scraping menu (GSheet options removed)"""
    console.print("\n[bold]Scraping Options:[/]")
    console.print("1. Scrape single channel")
    console.print("2. Scrape all saved channels")
    console.print("3. Inspect channel for problematic content")
    choice = console.input("Enter option (1-3): ")

    if choice == "3":
        await inspect_channel_content(tg_client)
        return

    default_start_date = get_default_start_date_from_user()
    all_scraped_messages_data: List[Dict] = [] # Store processed dicts now

    if choice == "1":
        channel = console.input("[bold]Enter channel username: [/]").strip()
        start_date_for_channel = default_start_date
        console.print(f"[dim]Using start date for {channel}: {start_date_for_channel}[/]")
        # --- MODIFIED: Iterate async generator ---
        try:
             async for message_data in tg_client.scrape_history(channel, start_date_for_channel):
                  if message_data: # scrape_history yields processed dicts or None
                       all_scraped_messages_data.append(message_data)
        except Exception as e:
             console.print(f"[red]Error during scraping history for {channel}: {e}[/]")
             logging.exception(f"Scrape history error for {channel}")
            
    elif choice == "2":
        channels = load_channels()
        if not channels:
            console.print("[yellow]No saved channels found![/]")
            return
            
        console.print("[info]Processing all channels in batch mode for efficiency with per-channel start dates.[/]")
        process_one_by_one = False
        
        total_scraped_count = 0
        for channel in channels:
            start_date_for_channel = default_start_date
            console.print(f"[dim]Using start date for {channel}: {start_date_for_channel}[/]")
            
            # --- MODIFIED: Iterate async generator ---
            try:
                channel_message_count = 0
                async for message_data in tg_client.scrape_history(channel, start_date_for_channel):
                     if message_data:
                          all_scraped_messages_data.append(message_data)
                          channel_message_count += 1
                console.print(f"[dim]   Fetched {channel_message_count} messages from {channel}[/]")
            except Exception as e:
                 console.print(f"[red]Error during scraping history for {channel}: {e}[/]")
                 logging.exception(f"Scrape history error for {channel}")
            
            total_scraped_count += channel_message_count
            
        if all_scraped_messages_data:
             console.print(f"\n[bold]Finished scraping all channels. Total messages fetched: {total_scraped_count}[/]")
        else:
             console.print(f"\n[yellow]No new messages found across any channels.[/]")
             
    else:
        console.print("[red]Invalid choice![/]")
        return

    # --- NEW: Process scraped message *data* ---
    total_processed = 0
    total_failed = 0
    total_projects_identified_overall = 0 # Track identified projects

    if not all_scraped_messages_data:
        console.print(f"\n[yellow]No new message data found during scraping.[/]")
    else:
        console.print(f"\n[bold]Finished scraping. Total message data items fetched: {len(all_scraped_messages_data)}. Analyzing and saving to Supabase...[/]")
        # Sort by timestamp if available in the dict
        all_scraped_messages_data.sort(key=lambda data: data.get('timestamp', datetime.min.replace(tzinfo=timezone.utc)))

        # --- Add configuration for delay ---
        # Delay in seconds between processing each message (adjust as needed)
        DELAY_BETWEEN_MESSAGES_SEC = 2

        with Progress() as progress:
            task = progress.add_task("[green]Analyzing message data...", total=len(all_scraped_messages_data))
            for i, msg_data in enumerate(all_scraped_messages_data):
                 progress.advance(task) # Advance progress first
                 # Extract data needed by analyzer (handle potential missing keys)
                 message_text = msg_data.get("text", "")
                 channel_name = msg_data.get("channel", "Unknown")
                 timestamp = msg_data.get("timestamp") # Should be datetime obj or None
                 message_link = msg_data.get("link", "Unknown Link") # Get link if available

                 # Validate required data
                 if not message_text or not timestamp:
                     console.print(f"[yellow]Skipping message data item (missing text or timestamp): {msg_data.get('link')}[/]")
                     total_failed += 1 # Count as failed if crucial data is missing
                     continue

                 console.print(f"\n[bold]Processing message {i+1}/{len(all_scraped_messages_data)}[/] Link: {message_link}")

                 # --- Call the Analyzer (Reverted Logic) ---
                 try:
                     # Reverted: returns (aggregated_payload, error_message)
                     aggregated_payload, error = extract_message_data(
                         message_text=message_text,
                         channel=channel_name,
                         timestamp=timestamp,
                         message_link=message_link
                     )

                     # --- Handle Analyzer Results --- 
                     if error:
                         console.print(f"[yellow]Analysis skipped/failed for msg {message_link}: {error}[/]")
                         total_failed +=1
                     elif aggregated_payload:
                         # Check if projects were actually identified in the successful analysis
                         # Use 'identified_project_names' which should be a comma-sep string or None
                         if aggregated_payload.get("identified_project_names"):
                             # Count identified projects by splitting the string
                             num_projects = len(aggregated_payload["identified_project_names"].split(','))
                             total_projects_identified_overall += num_projects
                             total_processed += 1
                             console.print(f"[dim]   Analyzed & Saved {message_link}: {num_projects} projects identified.[/]")
                         else:
                             # Analysis succeeded, but Gemini identified no projects
                             total_processed += 1 # Still counts as processed
                             console.print(f"[dim]   Analyzed & Saved {message_link}: No projects identified.[/]")
                     else:
                          # This case means analysis was successful but returned None (no projects found)
                          total_processed += 1 # Still count as processed
                          console.print(f"[dim]   Analyzed {message_link}: No projects identified, nothing saved.[/]")

                 except Exception as analysis_err:
                     console.print(f"[bold red]CRITICAL ERROR[/] during message analysis: {analysis_err}. Link: {message_link}")
                     logging.exception(f"Message analysis critical error. Link: {message_link}")
                     total_failed += 1

                 # --- Apply Delay ---
                 console.print(f"[dim]Waiting {DELAY_BETWEEN_MESSAGES_SEC} seconds...")
                 time.sleep(DELAY_BETWEEN_MESSAGES_SEC)

        console.print(f"\n[bold green]Finished processing message data.[/]")
        console.print(f"Successfully Processed (Messages Analyzed): {total_processed}")
        console.print(f"Total Projects Identified (Across All Processed Messages): {total_projects_identified_overall}")
        console.print(f"Failed/Skipped (Scraping or Analysis Error): {total_failed}")


def get_default_start_date_from_user():
    """Get validated default start date from user"""
    while True:
        date_str = console.input(
            f"[bold]Enter default start date (YYYY-MM-DD) [if no history found for a channel]: [/]"
        ).strip()
        if not date_str:
             # Default to beginning of today if user presses Enter
             today_start = datetime.now(timezone(timedelta(hours=5))).replace(hour=0, minute=0, second=0, microsecond=0)
             console.print(f"[dim]No date entered, defaulting to start of today: {today_start.date()}[/]")
             return today_start
        try:
            # Return date localized to UTC+5
            return datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=timezone(timedelta(hours=5))
            )
        except ValueError:
            console.print("[red]Invalid date format. Please use YYYY-MM-DD.[/]")


async def inspect_channel_content(tg_client: TelegramScraper):
    """Analyze channel content for potential issues without inserting to sheets"""
    channel = console.input("[bold]Enter channel to inspect: [/]").strip()
    limit = int(console.input("[bold]Number of messages to inspect (default 50): [/]") or "50")
    
    try:
        console.print(f"\n[bold]Inspecting {channel}[/]")
        entity = await tg_client.client.get_entity(channel)
        
        console.print(f"[dim]Channel info:[/]")
        console.print(f"ID: {entity.id}")
        console.print(f"Title: {getattr(entity, 'title', 'Unknown')}")
        console.print(f"Username: {getattr(entity, 'username', 'Unknown')}")
        console.print(f"Type: {'Supergroup' if getattr(entity, 'megagroup', False) else 'Channel'}")
        
        with Progress() as progress:
            task = progress.add_task(f"Analyzing messages", total=limit)
            
            long_messages = []
            special_char_messages = []
            media_messages = []
            
            async for message in tg_client.client.iter_messages(entity, limit=limit):
                processed = tg_client._process_message(message)
                if processed:
                    # Check message length
                    if processed["text"] and len(processed["text"]) > 300:
                        long_messages.append(processed)
                    
                    # Check for special characters
                    if processed["text"] and any(ord(c) > 127 for c in processed["text"]):
                        special_char_messages.append(processed)
                    
                    # Check for media
                    if processed["text"] == "[Media message]":
                        media_messages.append(processed)
                
                progress.update(task, advance=1)
            
            console.print("\n[bold]Analysis Results:[/]")
            console.print(f"Long messages (>300 chars): {len(long_messages)}")
            console.print(f"Messages with special characters: {len(special_char_messages)}")
            console.print(f"Media messages: {len(media_messages)}")
            
            if long_messages:
                console.print("\n[yellow]Sample of long messages:[/]")
                for i, msg in enumerate(long_messages[:3]):
                    console.print(f"[dim]Message {i+1} ({len(msg['text'])} chars):[/]")
                    console.print(f"{msg['text'][:100]}...")
            
            if special_char_messages:
                console.print("\n[yellow]Sample of messages with special characters:[/]")
                for i, msg in enumerate(special_char_messages[:3]):
                    console.print(f"[dim]Message {i+1}:[/]")
                    console.print(f"{msg['text'][:100]}...")
                    special_chars = [c for c in msg['text'] if ord(c) > 127][:10]
                    console.print(f"Special chars: {' '.join(special_chars)}")
            
    except Exception as e:
        console.print(f"[red]Error inspecting {channel}: {str(e)}[/]")


# --- NEW FUNCTION ---
async def handle_listen_mode(tg_client: TelegramScraper):
    """Starts the real-time listener for new messages in specified channels."""
    channels = load_channels()
    if not channels:
        console.print("[yellow]No channels found in channels.txt. Add channels first.[/]")
        return

    # Convert channel names/URLs to Telethon entities if needed, or use usernames directly
    # For simplicity, assuming `channels` list contains usernames/IDs Telethon understands
    console.print(f"[info]Attempting to listen to {len(channels)} channels: {', '.join(channels)}[/]")

    # Define the event handler function
    @tg_client.client.on(events.NewMessage(chats=channels))
    async def message_handler(event: events.NewMessage.Event):
        # Initialize link variable to prevent UnboundLocalError in except block
        message_link = "Unknown Link"
        # --- ADD DELAY FOR REAL-TIME LISTENER TOO (OPTIONAL BUT RECOMMENDED) ---
        # Delay in seconds between processing each incoming message in real-time
        REALTIME_DELAY_SEC = 2
        # --- Process Message (Existing Logic) ---
        try:
            message_object: Message = event.message

            # Get entity info using the underlying client
            entity = await tg_client.client.get_entity(message_object.peer_id)
            # Prefer username, fallback to title, then ID
            channel_name = getattr(entity, 'username', None)
            if not channel_name:
                channel_name = getattr(entity, 'title', None)
            if not channel_name:
                channel_name = str(getattr(entity, 'id', 'UnknownID'))

            message_text = message_object.text or "[Media message]"
            # Construct the link directly using entity.id (chat_id) and message_object.id
            message_link = f"https://t.me/c/{entity.id}/{message_object.id}"
            timestamp = message_object.date # Already timezone-aware

            console.print(f"\n[bold blue]---> Real-time message received:[/]")
            console.print(f"Channel: @{channel_name}")
            console.print(f"Link: {message_link}")
            console.print(f"Timestamp: {timestamp}")
            console.print(f"Content: {message_text[:100]}...")

            # --- Call Analyzer (Reverted Logic) ---
            console.print("[dim]Analyzing message...[/]")
            # Reverted: returns (aggregated_payload, error_message)
            aggregated_payload, error = extract_message_data(
                message_text=message_text,
                channel=channel_name,
                timestamp=timestamp,
                message_link=message_link
            )

            # --- Handle Analyzer Results --- 
            if error:
                console.print(f"[yellow]Analysis skipped/failed for msg {message_link}: {error}[/]")
            elif aggregated_payload:
                if aggregated_payload.get("identified_project_names"):
                    num_projects = len(aggregated_payload["identified_project_names"].split(','))
                    console.print(f"[green]   Analyzed & Saved {message_link}: {num_projects} projects identified.[/]")
                else:
                    console.print(f"[dim]   Analyzed & Saved {message_link}: No projects identified.[/]")
            else:
                console.print(f"[dim]   Analyzed {message_link}: No projects identified, nothing saved.[/]")

        except Exception as e:
            console.print(f"[bold red]ERROR[/] processing real-time message: {e}")
            # message_link should now be defined, even if it's "Unknown Link"
            logging.exception(f"Error in real-time message handler for link {message_link}")

        # --- Apply Delay ---
        finally:
            console.print(f"[dim]Waiting {REALTIME_DELAY_SEC} seconds before processing next event...")
            await asyncio.sleep(REALTIME_DELAY_SEC) # Use asyncio.sleep in async context

    # Start listening
    console.print("[bold green]Listener started! Waiting for new messages... (Press Ctrl+C to stop)[/]")
    send_telegram_notification("Listener mode started.") # Notify on listener start
    # Keep the script running to listen for events
    try:
        await tg_client.client.run_until_disconnected()
    finally:
        console.print("[bold yellow]Listener stopped.[/]")
        send_telegram_notification("Listener mode stopped.") # Notify on listener stop


# --- Main Execution ---
async def main():
    # Use the correct path relative to main.py for dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
    console.print("[bold]Starting Telegram Scraper...[/]", justify="center")

    # --- Initialize Analyzer ---
    if not init_analyzer():
         console.print("[bold red]Failed to initialize Analyzer (Gemini/Supabase). Check logs and .env file. Exiting.[/]")
         return

    # --- Load Telegram Credentials ---
    api_id_str = os.getenv(TELEGRAM_API_ID_KEY)
    api_hash = os.getenv(TELEGRAM_API_HASH_KEY)
    session_name = os.getenv(TELEGRAM_SESSION_NAME_KEY, "telegram_scraper") # Default session name if not set

    if not api_id_str or not api_hash:
        console.print(f"[bold red]Missing {TELEGRAM_API_ID_KEY} or {TELEGRAM_API_HASH_KEY} in .env file. Exiting.[/]")
        return

    try:
        api_id = int(api_id_str) # API ID should be an integer
    except ValueError:
         console.print(f"[bold red]{TELEGRAM_API_ID_KEY} in .env file is not a valid integer. Exiting.[/]")
         return

    # --- Initialize Telegram Client ---
    tg_client = TelegramScraper(session_name=session_name, api_id=api_id, api_hash=api_hash)

    # --- NEW: Explicitly start/connect the client ---
    console.print("[dim]Connecting to Telegram...[/]")
    try:
        # Use client.start() which handles authorization if needed
        # If phone code is needed, it will prompt here in the console
        await tg_client.client.start()
        # Check if authorized
        if not await tg_client.client.is_user_authorized():
             console.print("[bold red]Telegram authorization required. Please follow the prompts.[/]")
             # The start() method should handle the auth flow. If it fails, exit?
             # Or rely on is_connected check below.
        # Double check connection
        if not tg_client.client.is_connected():
             console.print("[bold red]Failed to establish connection to Telegram after start().[/]")
             return

        console.print("[green]Telegram client connected successfully.[/]")
        
    except Exception as e:
        console.print(f"[bold red]Failed to connect/authorize Telegram: {e}[/]")
        logging.exception("Telegram connection/authorization failed.")
        return

    console.print("[bold green]Initialization Complete![/]")
    send_telegram_notification("Scraper initialized successfully.") # Notify on successful start

    await interactive_mode(tg_client) # Pass the initialized client

    # Disconnect Telegram client gracefully
    console.print("[dim]Disconnecting Telegram client...[/]")
    await tg_client.client.disconnect() # Disconnect the underlying client
    console.print("[bold]Telegram client disconnected.[/]")
    send_telegram_notification("Scraper shut down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Ctrl+C detected. Shutting down...[/]")
    except Exception as e:
         console.print(f"[bold red]\n--- UNEXPECTED CRITICAL ERROR ---[/]")
         logging.exception("Critical error in main execution loop.")
         console.print(f"[red]{e}[/]")
         try:
             send_telegram_notification(f"Scraper CRASHED: {str(e)[:500]}")
         except Exception as notify_err:
             console.print(f"[red]Failed to send crash notification: {notify_err}[/]")
