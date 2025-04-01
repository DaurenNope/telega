import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from rich.console import Console
from rich.progress import Progress
from dotenv import load_dotenv
from telegram_client import TelegramScraper
from google_sheets_client import GSheetClient
from telethon import events
import requests

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)

console = Console()

CHANNELS_FILE = "channels.txt"

# --- Notification Helper ---
def send_telegram_notification(message: str):
    """Attempts to send a notification message via the Telegram Bot API."""
    bot_token = os.getenv("NOTIFICATION_BOT_TOKEN")
    chat_id = os.getenv("NOTIFICATION_CHAT_ID")
    
    if not bot_token or not chat_id:
        console.print("[yellow]NOTIFICATION_BOT_TOKEN or NOTIFICATION_CHAT_ID not set in .env, skipping notification.[/]")
        return

    payload = {
        'chat_id': chat_id,
        'text': f"⚠️ Scraper Alert ⚠️\n\n{message}",
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
            console.print(f"[red]Telegram Bot API returned an error: {response_json.get('description')}[/]")
            
    except requests.exceptions.RequestException as req_err:
        console.print(f"[red]Failed to send Telegram notification (Request Error): {str(req_err)}[/]")
        if response is not None:
             console.print(f"[dim]Failed Response Status: {response.status_code}[/]")
             console.print(f"[dim]Failed Response Body: {response.text}[/]")
    except Exception as notify_err:
        console.print(f"[red]Failed to send Telegram notification (Other Error): {str(notify_err)}[/]")
        if response is not None:
             console.print(f"[dim]Error Response Status: {response.status_code}[/]")
             console.print(f"[dim]Error Response Body: {response.text}[/]")
        
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


async def interactive_mode(tg_client: TelegramScraper, gs_client: GSheetClient):
    """Interactive command-line interface"""
    while True:
        console.print("\n[bold cyan]Telegram Scraper Menu[/]", justify="center")
        console.print("1. Scrape channel history")
        console.print("2. Start real-time listener")
        console.print("3. Test Google Sheets connection")
        console.print("4. Clean channel list")
        console.print("5. Clean and Deduplicate Sheet")
        console.print("6. Exit")

        choice = console.input("\n[bold]Enter your choice (1-6): [/]")

        if choice == "1":
            await handle_scrape_mode(tg_client, gs_client)
        elif choice == "2":
            # Test connection before starting listener
            if gs_client.test_connection():
                await handle_listen_mode(tg_client, gs_client)
            else:
                console.print("[red]Google Sheets connection test failed. Please check your settings.[/]")
        elif choice == "3":
            test_gsheets_connection(gs_client)
        elif choice == "4":
            clean_channels_file()
        elif choice == "5":
            # Add confirmation step for safety
            confirm = console.input("[bold yellow]This will fetch all data, deduplicate, sort, and rewrite the sheet. Are you sure? (y/n): [/]").lower()
            if confirm == 'y':
                gs_client.deduplicate_and_rewrite_sheet()
            else:
                console.print("[yellow]Sheet cleanup cancelled.[/]")
        elif choice == "6":
            console.print("[yellow]Exiting...[/]")
            break
        else:
            console.print("[red]Invalid choice! Please try again.[/]")


async def handle_scrape_mode(tg_client: TelegramScraper, gs_client: GSheetClient):
    """Updated scraping menu with global sorting and per-channel start dates."""
    console.print("\n[bold]Scraping Options:[/]")
    console.print("1. Scrape single channel")
    console.print("2. Scrape all saved channels")
    console.print("3. Inspect channel for problematic content")
    choice = console.input("Enter option (1-3): ")

    # Test Google Sheets connection first
    if not gs_client.test_connection():
        console.print("[red]Google Sheets connection test failed. Check credentials/connection.[/]")
        return

    if choice == "3":
        await inspect_channel_content(tg_client)
        return

    # Get a *default* start date from the user (used if no channel-specific date found)
    default_start_date = get_default_start_date_from_user()
    all_messages = []

    if choice == "1":
        channel = console.input("[bold]Enter channel username: [/]").strip()
        # For single channel, find its specific start date
        channel_start_date = gs_client.get_last_timestamp_for_channel(channel) or default_start_date
        console.print(f"[dim]Using start date for {channel}: {channel_start_date}[/]")
        messages = await scrape_single_channel(tg_client, channel, channel_start_date)
        # If scraping one channel, just process it immediately
        if messages:
            added = gs_client.batch_append(messages)
            console.print(f"[green]Total added: {added} messages from {channel}[/]")
        else:
            console.print(f"[yellow]No new messages found for {channel}.[/]")
            
    elif choice == "2":
        channels = load_channels()
        if not channels:
            console.print("[yellow]No saved channels found![/]")
            return
            
        # Ask for batch processing preference - BATCHING IS RECOMMENDED for efficiency
        # Force batching for per-channel start date logic
        # process_one_by_one = console.input("[bold]Process channels one by one? (y/n, default=n): [/]").strip().lower() == "y"
        console.print("[info]Processing all channels in batch mode for efficiency with per-channel start dates.[/]")
        process_one_by_one = False # Force batch processing
        
        total_scraped_count = 0
        for channel in channels:
            # Determine start date for *this* channel
            channel_start_date = gs_client.get_last_timestamp_for_channel(channel) or default_start_date
            console.print(f"[dim]Using start date for {channel}: {channel_start_date}[/]")
            
            messages = await scrape_single_channel(tg_client, channel, channel_start_date)
            all_messages.extend(messages)
            total_scraped_count += len(messages)
            # Optional: Add small sleep between channels if hitting rate limits
            # await asyncio.sleep(1) 
            
        # Process the accumulated messages from all channels at the end
        if all_messages:
             console.print(f"\n[bold]Finished scraping all channels. Total messages fetched: {total_scraped_count}[/]")
             added = gs_client.batch_append(all_messages)
             console.print(f"[green]Batch append complete. Total unique new messages added: {added}[/]")
        else:
             console.print(f"\n[yellow]No new messages found across any channels.[/]")
             
    else:
        console.print("[red]Invalid choice![/]")
        return

    # Batch append logic moved inside the choice == '2' block
    # if not process_one_by_one and all_messages: # This check is redundant now
    #    pass 


# Renamed function slightly for clarity
def get_default_start_date_from_user():
    """Get validated default start date from user"""
    # Simplified: No longer suggests date from sheet, as it's per-channel now
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


async def scrape_single_channel(tg_client, channel, start_date):
    """Scrape messages and return them without inserting to sheet"""
    try:
        console.print(f"\n[bold]Scraping {channel}[/]")
        entity = await tg_client.client.get_entity(channel)

        with Progress() as progress:
            task = progress.add_task(f"Scraping from {start_date.date()}", total=None)
            messages = []
            error_count = 0
            max_errors = 5

            async for message in tg_client.scrape_history(entity, start_date):
                try:
                    if message:  # Skip None messages
                        messages.append(message)
                        progress.update(task, advance=1)
                        
                        # Add periodic progress info for large channels
                        if len(messages) % 100 == 0:
                            console.print(f"[dim]Progress: {len(messages)} messages scraped[/]")
                except Exception as msg_error:
                    error_count += 1
                    if error_count <= max_errors:
                        console.print(f"[yellow]Error processing message: {str(msg_error)}[/]")
                    if error_count > max_errors:
                        console.print(f"[red]Too many errors ({error_count}), stopping message processing[/]")
                        break

            console.print(f"[green]Total scraped: {len(messages)} messages from {channel}[/]")
            return messages

    except Exception as e:
        console.print(f"[red]Error scraping {channel}: {str(e)}[/]")
        return []


async def handle_listen_mode(tg_client: TelegramScraper, gs_client: GSheetClient):
    """Handle real-time listening with debug"""
    console.print(f"[dim]Loaded channels: {load_channels()}[/]")  # Show actual channels

    channels = load_channels()
    if not channels:
        console.print("[yellow]No channels found in watchlist! Add channels first.[/]")
        return

    was_connected = False # Track if client actually connected in this scope
    try:
        # Get valid channel entities
        valid_channels = []
        for channel in channels:
            try:
                entity = await tg_client.client.get_entity(channel)
                console.print(
                    f"[dim]Channel info: {entity.id} | {getattr(entity, 'title', '')} | Private: {entity.megagroup}[/]"
                )
                valid_channels.append(entity)
                console.print(
                    f"[green]✓[/] Listening to: {getattr(entity, 'title', channel)}"
                )
            except Exception as e:
                console.print(f"[red]×[/] Failed to access {channel}: {str(e)}")

        if not valid_channels:
            console.print("[red]No valid channels to listen to[/]")
            return

        # Ensure client is connected before attaching handler and running
        if not tg_client.client.is_connected():
             console.print("[dim]Connecting client for listener mode...[/]")
             await tg_client.client.connect()
        
        if not tg_client.client.is_connected():
            console.print("[red]Failed to connect Telegram client for listener.[/]")
            # Try sending notification even on connection failure - NOW SYNCHRONOUS
            send_telegram_notification("Failed to connect Telegram client for listener.")
            return # Exit if connection failed
            
        # If we reach here, connection succeeded or was already active
        was_connected = True
        console.print("\n[yellow]Listener active. Press Ctrl+C to stop listening[/]")

        @tg_client.client.on(events.NewMessage(chats=valid_channels))
        async def handler(event):
            if os.getenv("DEBUG_LOGS", "false").lower() == "true":
                console.print(f"[dim]RAW: {event.message.id}[/]")
    
            try:
                processed = tg_client._process_message(event.message)
                if processed:
                    gs_client.append_message(processed)
                    console.print(
                        f"\n[bold cyan]New message @ {processed['timestamp'].astimezone(timezone(timedelta(hours=5))).strftime('%H:%M:%S')}[/]"
                    )
                    console.print(
                        f"[bold]{processed['channel']}:[/] {processed['text'][:100]}..."
                    )
                    if 'tags' in processed and processed['tags']:
                        console.print(f"[dim blue]Tags: {processed['tags']}[/]") 
                    console.print(f"[dim blue]{processed['link']}[/]")
    
            except Exception as e:
                console.print(f"[red]Error processing message:[/] {str(e)}")

        # Run until disconnected; this blocks until stop or error
        await tg_client.client.run_until_disconnected()
        
        # If run_until_disconnected finishes without an exception
        console.print("[yellow]Listener stopped unexpectedly (run_until_disconnected finished).[/]")
        # SYNCHRONOUS CALL
        send_telegram_notification("Listener stopped unexpectedly (run_until_disconnected finished).")

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped listening due to user interruption (Ctrl+C) - Caught inside listener function.[/]")
        # Although we try to notify here, the top-level handler is more likely to execute fully.
        # SYNCHRONOUS CALL
        send_telegram_notification("Listener stopping normally by user (Ctrl+C).") 

    except Exception as e:
        error_msg = f"Listener stopped due to error: {str(e)}"
        console.print(f"[red]Listener error:[/] {str(e)}")
        # Send notification immediately upon catching other errors - NOW SYNCHRONOUS
        send_telegram_notification(error_msg)
    finally:
        # Cleanup: Attempt to disconnect only if the client was successfully connected in this function
        if was_connected and tg_client and tg_client.client and tg_client.client.is_connected():
            console.print("[dim]Attempting to disconnect Telegram client...[/]")
            try:
                await tg_client.client.disconnect()
                console.print("[dim]Disconnected Telegram client after listener stop.[/]")
            except Exception as disconn_err:
                console.print(f"[yellow]Error during disconnect after listener stop: {disconn_err}[/]")
        else:
            console.print("[dim]Client was not connected in listener scope, skipping disconnect attempt.[/]")


def test_gsheets_connection(gs_client: GSheetClient):
    """Test Google Sheets connection and API quota status"""
    console.print("\n[bold]Testing Google Sheets Connection[/]")
    
    if gs_client.test_connection():
        console.print("[green]✓ Connection test passed![/]")
        
        # Display quota information
        console.print("\n[bold]Google Sheets API Quota Information:[/]")
        console.print("[dim]To check your quota usage:[/]")
        console.print("1. Go to https://console.cloud.google.com/apis/dashboard")
        console.print("2. Select your project")
        console.print("3. Check 'Google Sheets API' usage and quota information")
        
        # Advanced test - try inserting and deleting a test row
        try:
            console.print("\n[bold]Testing write operations...[/]")
            test_row = ["TEST", "TEST", "This is a test message", "https://t.me/test"]
            
            # Use retry logic from GSheetClient to handle potential errors
            # Fix: Make sure we're using the correct format for batch operations
            try:
                gs_client._execute_with_retry(
                    lambda: gs_client.sheet.append_row(test_row, table_range="A1:D1")
                )
                console.print("[green]✓ Test row appended successfully[/]")
                
                # Now try to find and delete the test row
                values = gs_client.sheet.get_all_values()
                for i, row in enumerate(values):
                    if row[2] == "This is a test message":
                        # Found the test row, delete it
                        gs_client._execute_with_retry(
                            lambda: gs_client.sheet.delete_rows(i+1, i+2)  # +1 because sheet is 1-indexed
                        )
                        console.print("[green]✓ Test row removed successfully[/]")
                        break
            except Exception as row_error:
                console.print(f"[yellow]Row operations failed: {str(row_error)}[/]")
                console.print("[yellow]Trying alternative method...[/]")
                
                # Alternative approach: Insert at specific position then delete
                gs_client._execute_with_retry(
                    lambda: gs_client.sheet.insert_row(test_row, index=2)
                )
                console.print("[green]✓ Test row inserted successfully[/]")
                
                # Delete the row we just added
                gs_client._execute_with_retry(
                    lambda: gs_client.sheet.delete_row(2)
                )
                console.print("[green]✓ Test row removed successfully[/]")
            
            console.print("\n[green bold]All Google Sheets tests passed successfully![/]")
        except Exception as e:
            console.print(f"[red]Advanced test failed: {str(e)}[/]")
            console.print("[yellow]Note: Basic connection is working, but some advanced operations may fail[/]")
    else:
        console.print("[red]Failed to connect to Google Sheets[/]")
        console.print("[yellow]Check your credentials file and internet connection[/]")


async def inspect_channel_content(tg_client):
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


async def main():
    load_dotenv()

    session_name = os.getenv("SESSION_NAME")
    session_file = f"{session_name}.session"

    # Initialize clients
    tg_client = TelegramScraper(
        session_name=session_name,
        api_id=int(os.getenv("API_ID")),
        api_hash=os.getenv("API_HASH"),
    )

    gs_client = GSheetClient(
        creds_path=os.getenv("GSHEET_CREDENTIALS"), sheet_url=os.getenv("SHEET_URL")
    )

    try:
        await tg_client.client.start()
        console.print("[green]Connected to Telegram API[/]")

        await interactive_mode(tg_client, gs_client)
        
    except Exception as e:
        if "authorization key" in str(e) and "different IP addresses" in str(e):
            console.print(
                "\n[red]Session Error:[/] The Telegram session is invalid due to multiple IP usage."
            )

            # First disconnect the client
            try:
                if tg_client.client.is_connected(): await tg_client.client.disconnect()
            except: pass # Ignore disconnection errors

            # Then try to delete the session file
            if os.path.exists(session_file):
                try:
                    os.remove(session_file)
                    console.print(
                        f"[yellow]Deleted invalid session file: {session_file}[/]"
                    )
                except Exception as del_err:
                    console.print(
                        f"[red]Failed to delete session file: {str(del_err)}[/]"
                    )
                    console.print(
                        "[yellow]Please manually delete the session file before restarting.[/]"
                    )

            console.print("\n[bold]To fix this:[/]")
            console.print("1. The session file has been deleted")
            console.print("2. Please restart the application")
            console.print("3. You'll need to re-authenticate with Telegram")
            # Don't try to send notification here, client is likely dead
            return # Exit cleanly 
        else:
            error_message = f"Scraper stopped with critical error: {str(e)}"
            console.print(f"[red]Critical error:[/] {str(e)}")
            # Attempt to send notification for other critical errors - NOW SYNCHRONOUS
            send_telegram_notification(error_message)
    finally:
        try:
            if tg_client and tg_client.client and tg_client.client.is_connected():
                await tg_client.client.disconnect()
        except: pass # Ignore any disconnection errors


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # This top-level handler is the most reliable place for Ctrl+C notification
        console.print("\n[yellow]Operation cancelled by user (Detected at top level)[/]")
        # Try final notification here using synchronous call
        send_telegram_notification("Scraper process terminated by user (Ctrl+C).")
    except Exception as e:
        # Log critical errors that weren't caught elsewhere
        console.print(f"[red]Unhandled critical error at top level:[/] {str(e)}")
        # Try a final notification attempt
        send_telegram_notification(f"Scraper process terminated with unhandled error: {str(e)}")
