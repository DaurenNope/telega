from telethon import TelegramClient, events
import hashlib


class TelegramScraper:
    def __init__(self, session_name, api_id, api_hash):
        self.client = TelegramClient(session_name, api_id, api_hash)

    async def scrape_history(self, channel, start_date):
        """Batch mode: Scrape messages from start_date"""
        async for message in self.client.iter_messages(
            channel,
            offset_date=start_date,
            reverse=True,  # Get messages in chronological order
        ):
            yield self._process_message(message)

    async def start_listening(self, channel, callback):
        """Real-time mode: Listen for new messages"""

        @self.client.on(events.NewMessage(chats=channel))
        async def handler(event):
            processed = self._process_message(event.message)
            callback(processed)

        await self.client.start()
        await self.client.run_until_disconnected()

    def _process_message(self, message):
        """Handle messages with empty text"""
        if not message.id or not message.chat:
            return None

        # Standardize channel name
        channel_name = str(message.chat.title or message.chat.id)
        channel_name = channel_name.replace("\n", " ").strip()

        # Generate canonical link
        message_link = f"https://t.me/c/{message.chat.id}/{message.id}"

        # Handle empty text (media messages)
        message_text = message.text or "[Media message]"

        return {
            "timestamp": message.date,
            "channel": channel_name,
            "text": message_text,
            "link": message_link,
        }
