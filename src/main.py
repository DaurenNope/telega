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

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

console = Console()

CHANNELS_FILE = 'channels.txt'

def load_channels():
    """Load and deduplicate channels from storage file"""
    try:
        with open(CHANNELS_FILE, 'r') as f:
            raw_channels = [line.strip() for line in f if line.strip()]
            
            # Deduplicate while preserving order
            seen = set()
            channels = []
            for chan in raw_channels:
                # Skip comments and empty lines
                if chan.startswith('#') or not chan:
                    continue
                # Normalize channel names
                clean_chan = chan.lower().replace('@', '').strip()
                if clean_chan not in seen:
                    seen.add(clean_chan)
                    channels.append(chan.strip())
            return channels
            
    except FileNotFoundError:
        open(CHANNELS_FILE, 'w').close()  # Create empty file
        return []

def save_channel(channel):
    """Add new channel with deduplication check"""
    # Normalize input
    clean_channel = channel.lower().replace('@', '').strip()
    
    existing = load_channels()
    existing_clean = [c.lower().replace('@', '') for c in existing]
    
    if clean_channel not in existing_clean:
        with open(CHANNELS_FILE, 'a') as f:
            f.write(f"{channel}\n")
        console.print(f"[green]✓[/] Added channel: {channel}")
    else:
        console.print(f"[yellow]⚠[/] Channel already exists: {channel}")

def clean_channels_file():
    """Remove duplicate channels from file"""
    channels = load_channels()  # Uses new deduplicated list
    with open(CHANNELS_FILE, 'w') as f:
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
        console.print("5. Exit")
        
        choice = console.input("\n[bold]Enter your choice (1-5): [/]")
        
        if choice == '1':
            await handle_scrape_mode(tg_client, gs_client)
        elif choice == '2':
            await handle_listen_mode(tg_client, gs_client)
        elif choice == '3':
            test_gsheets_connection(gs_client)
        elif choice == '4':
            clean_channels_file()
        elif choice == '5':
            console.print("[yellow]Exiting...[/]")
            break
        else:
            console.print("[red]Invalid choice! Please try again.[/]")

async def handle_scrape_mode(tg_client: TelegramScraper, gs_client: GSheetClient):
    """Updated scraping menu with global sorting"""
    console.print("\n[bold]Scraping Options:[/]")
    console.print("1. Scrape single channel")
    console.print("2. Scrape all saved channels")
    choice = console.input("Enter option (1-2): ")
    
    start_date = get_start_date_from_user(gs_client)
    all_messages = []

    if choice == '1':
        channel = console.input("[bold]Enter channel username: [/]").strip()
        messages = await scrape_single_channel(tg_client, channel, start_date)
        all_messages.extend(messages)
    elif choice == '2':
        channels = load_channels()
        if not channels:
            console.print("[yellow]No saved channels found![/]")
            return
        for channel in channels:
            messages = await scrape_single_channel(tg_client, channel, start_date)
            all_messages.extend(messages)
    else:
        console.print("[red]Invalid choice![/]")
        return

    # Add all messages in chronological order
    added = gs_client.batch_append(all_messages)
    console.print(f"[green]Total added: {added} messages[/]")

def get_start_date_from_user(gs_client: GSheetClient):
    """Get validated start date from user"""
    # Suggest last scraped date if available
    last_date = gs_client.sheet.cell(2, 1).value if gs_client.sheet.row_count > 1 else None
    suggestion = f" [or press Enter for {last_date}]" if last_date else ""
    
    while True:
        date_str = console.input(f"[bold]Enter start date (YYYY-MM-DD){suggestion}: [/]").strip()
        if not date_str and last_date:
            return datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
        try:
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
            
            async for message in tg_client.scrape_history(entity, start_date):
                messages.append(message)
                progress.update(task, advance=1)
                
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

    try:
        # Get valid channel entities
        valid_channels = []
        for channel in channels:
            try:
                entity = await tg_client.client.get_entity(channel)
                console.print(f"[dim]Channel info: {entity.id} | {getattr(entity, 'title', '')} | Private: {entity.megagroup}[/]")
                valid_channels.append(entity)
                console.print(f"[green]✓[/] Listening to: {getattr(entity, 'title', channel)}")
            except Exception as e:
                console.print(f"[red]×[/] Failed to access {channel}: {str(e)}")

        if not valid_channels:
            console.print("[red]No valid channels to listen to[/]")
            return

        console.print("\n[yellow]Press Ctrl+C to stop listening[/]")

        @tg_client.client.on(events.NewMessage(chats=valid_channels))
        async def handler(event):
            if os.getenv('DEBUG_LOGS', 'false').lower() == 'true':
                console.print(f"[dim]RAW: {event.message.id}[/]")
            
            try:
                processed = tg_client._process_message(event.message)
                gs_client.append_message(processed)
                
                # Immediate feedback
                console.print(f"\n[bold cyan]New message @ {processed['timestamp'].astimezone(timezone(timedelta(hours=5))).strftime('%H:%M:%S')}[/]")
                console.print(f"[bold]{processed['channel']}:[/] {processed['text'][:100]}...")
                console.print(f"[dim blue]{processed['link']}[/]")
                
                # Force sheet refresh
                gs_client.sheet.spreadsheet.batch_update({
                    "requests": [{
                        "repeatCell": {
                            "range": {"sheetId": gs_client.sheet.id},
                            "fields": "userEnteredFormat",
                        }
                    }]
                })
                
            except Exception as e:
                console.print(f"[red]Error processing message:[/] {str(e)}")

        await tg_client.client.start()
        await tg_client.client.run_until_disconnected()

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped listening[/]")
    except Exception as e:
        console.print(f"[red]Listener error:[/] {str(e)}")
    finally:
        await tg_client.client.disconnect()

def test_gsheets_connection(gs_client: GSheetClient):
    """Test and clean Google Sheets connection"""
    try:
        # Clear existing data except header
        if input("Clear all sheet data? (y/n): ").lower() == 'y':
            gs_client.sheet.clear()
            console.print("[green]Sheet cleared successfully![/]")
        
        # Rest of test code...
        last_update = gs_client.sheet.cell(1, 1).value
        console.print(f"[green]Connection successful![/] Last update: {last_update}")
    except Exception as e:
        console.print(f"[red]Connection failed:[/] {str(e)}")

async def main():
    load_dotenv()
    
    # Initialize clients
    tg_client = TelegramScraper(
        session_name=os.getenv('SESSION_NAME'),
        api_id=int(os.getenv('API_ID')),
        api_hash=os.getenv('API_HASH')
    )
    
    gs_client = GSheetClient(
        creds_path=os.getenv('GSHEET_CREDENTIALS'),
        sheet_url=os.getenv('SHEET_URL')
    )
    
    try:
        await tg_client.client.start()
        console.print("[green]Connected to Telegram API[/]")
        
        await interactive_mode(tg_client, gs_client)
    finally:
        await tg_client.client.disconnect()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/]")
    except Exception as e:
        console.print(f"[red]Critical error:[/] {str(e)}") 