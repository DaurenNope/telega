from telethon import TelegramClient, events
import hashlib
import logging
import html
import re
from typing import Optional, Dict, Any


class TelegramScraper:
    def __init__(self, session_name, api_id, api_hash):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.logger = logging.getLogger("TelegramScraper")

    async def scrape_history(self, channel, start_date):
        """Batch mode: Scrape messages from start_date with improved error handling"""
        try:
            async for message in self.client.iter_messages(
                channel,
                offset_date=start_date,
                reverse=True,  # Get messages in chronological order
            ):
                try:
                    processed = self._process_message(message)
                    if processed:  # Skip None messages
                        yield processed
                except Exception as e:
                    self.logger.error(f"Error processing message {getattr(message, 'id', 'unknown')}: {str(e)}")
                    # Generate a placeholder message for troubleshooting
                    try:
                        error_message = {
                            "timestamp": getattr(message, "date", None),
                            "channel": str(getattr(message, "chat.title", "Unknown") or getattr(message, "chat.id", "Unknown")),
                            "text": f"[Error processing message: {str(e)}]",
                            "link": f"https://t.me/c/{getattr(message, 'chat.id', 'unknown')}/{getattr(message, 'id', 'unknown')}",
                            "has_error": True
                        }
                        yield error_message
                    except:
                        # If we can't even create an error message, just continue
                        continue
        except Exception as e:
            self.logger.error(f"Error in scrape_history: {str(e)}")
            raise

    async def start_listening(self, channel, callback):
        """Real-time mode: Listen for new messages"""

        @self.client.on(events.NewMessage(chats=channel))
        async def handler(event):
            try:
                processed = self._process_message(event.message)
                if processed:  # Skip None messages
                    callback(processed)
            except Exception as e:
                self.logger.error(f"Error in message handler: {str(e)}")

        await self.client.start()
        await self.client.run_until_disconnected()

    def _process_message(self, message) -> Optional[Dict[str, Any]]:
        """Extract relevant data including hashtags and use consistent ID-based link."""
        if not message or not message.date or not hasattr(message, 'chat') or not hasattr(message, 'id'):
            # Added checks for chat and id existence
            self.logger.warning(f"Skipping message due to missing essential attributes: {message}")
            return None 
            
        # Extract hashtags using improved regex and cleanup
        text_content = message.text or ""
        # Regex to capture # followed by one or more word characters (incl. underscore) or hyphens
        # Allows for tags like #Node_Forto or #web-3
        raw_hashtags = re.findall(r"#([\w_-]+)", text_content)
        
        # Clean up extracted tags: remove trailing punctuation/underscores and ensure not purely numeric
        cleaned_hashtags = set()
        for tag in raw_hashtags:
            # Strip common trailing punctuation
            cleaned_tag = tag.rstrip('_.!?,:')
            # Optional: Ignore tags that are purely numeric after cleaning
            if cleaned_tag and not cleaned_tag.isdigit():
                cleaned_hashtags.add(cleaned_tag)
        
        # Create comma-separated string
        tags_str = ", ".join(sorted(list(cleaned_hashtags)))

        # --- Always use the canonical ID-based link for consistency --- 
        chat_id = message.chat.id
        message_id = message.id
        # Handle potential negative chat IDs (for user accounts, though less common here)
        # The 'c' prefix generally handles this, but ensure we have the base ID.
        # No, the c/<peer_id> format handles groups/channels correctly.
        link = f"https://t.me/c/{chat_id}/{message_id}" 
        # --- End Link Logic Change ---

        return {
            "timestamp": message.date,
            "channel": getattr(message.chat, 'title', f'ChatID:{chat_id}'), # Fallback to ID if title missing
            "text": text_content if text_content else "[Media message]",
            "link": link, # Use the consistent link
            "tags": tags_str
        }

    def _sanitize_text(self, text: str) -> str:
        """Sanitize text to prevent Google Sheets API issues"""
        if not text:
            return "[Empty]"
            
        # Replace newlines with spaces
        text = text.replace("\n", " ")
        
        # Replace tabs with spaces
        text = text.replace("\t", " ")
        
        # Replace null characters
        text = text.replace("\0", "")
        
        # HTML escape special characters that might cause issues
        text = html.escape(text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1F\x7F]', '', text)
        
        # Trim extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Truncate very long strings
        if len(text) > 50000:  # Google Sheets cell content limit
            text = text[:49997] + "..."
            
        return text
