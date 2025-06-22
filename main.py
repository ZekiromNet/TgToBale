import asyncio
import json
import os
import requests
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention,
    MessageEntityHashtag, MessageEntityCashtag, MessageEntityBotCommand,
    MessageEntityEmail, MessageEntityPhone, MessageEntityBold,
    MessageEntityItalic, MessageEntityCode, MessageEntityPre,
    MessageEntityStrike, MessageEntityUnderline, MessageEntitySpoiler,
    MessageEntityMentionName, MessageEntityCustomEmoji
)
from telethon.tl.functions.messages import GetHistoryRequest
import mimetypes
from typing import Dict, List, Optional, Any

class TelethonForwarder:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = TelegramClient(
            StringSession(config['session_string']),
            config['api_id'],
            config['api_hash']
        )
        self.bot_token = config['bot_token']
        self.base_url = config['base_url']
        self.channels = config['channels']  # List of channel configs
        self.db_file = config.get('db_file', 'forwarded_messages.json')
        self.forwarded_messages = self.load_database()

    def load_database(self) -> Dict[str, Dict[str, bool]]:
        """Load the database of forwarded messages"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    def save_database(self):
        """Save the database of forwarded messages"""
        with open(self.db_file, 'w', encoding='utf-8') as f:
            json.dump(self.forwarded_messages, f, ensure_ascii=False, indent=2)

    def is_message_forwarded(self, source_channel: str, message_id: int) -> bool:
        """Check if message is already forwarded"""
        return source_channel in self.forwarded_messages and str(message_id) in self.forwarded_messages[source_channel]

    def mark_message_forwarded(self, source_channel: str, message_id: int):
        """Mark message as forwarded"""
        if source_channel not in self.forwarded_messages:
            self.forwarded_messages[source_channel] = {}
        self.forwarded_messages[source_channel][str(message_id)] = True
        self.save_database()

    def convert_entities_to_plain_text(self, entities: List, text: str) -> str:
        """Convert only URL entities to plain text, append URLs at the end"""
        if not entities or not text:
            return text

        # Collect URLs to append at the end
        urls_to_append = []
        
        print(f"Processing {len(entities)} entities...")
        
        for i, entity in enumerate(entities):
            print(f"Entity {i}: {type(entity).__name__}")
            
            if isinstance(entity, MessageEntityTextUrl):
                print(f"  TextUrl found: {entity.url}")
                # For text URLs, we want to append the actual URL
                if entity.url not in urls_to_append:
                    urls_to_append.append(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                # For direct URLs, extract the URL from the text
                start = entity.offset
                end = entity.offset + entity.length
                if 0 <= start < len(text) and start < end <= len(text):
                    url = text[start:end]
                    print(f"  DirectUrl found: {url}")
                    if url not in urls_to_append:
                        urls_to_append.append(url)

        print(f"Found {len(urls_to_append)} unique URLs to append")
        
        # Return original text with URLs appended at the end
        result_text = text
        if urls_to_append:
            result_text += "\n\n" + "\n".join(urls_to_append)

        return result_text

    async def download_media(self, message) -> Optional[str]:
        """Download media from message and return file path"""
        if not message.media:
            return None

        # Create downloads directory if it doesn't exist
        os.makedirs('downloads', exist_ok=True)

        try:
            # Get original filename if available
            filename = None
            if hasattr(message.media, 'document') and message.media.document:
                for attr in message.media.document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        filename = attr.file_name
                        break

            # If no filename, create one based on message ID and media type
            if not filename:
                media_type = self.get_media_type(message)
                if media_type == 'photo':
                    filename = f"photo_{message.id}.jpg"
                elif media_type == 'video':
                    filename = f"video_{message.id}.mp4"
                elif media_type == 'audio':
                    filename = f"audio_{message.id}.mp3"
                else:
                    filename = f"document_{message.id}"

            # Ensure safe filename
            filename = "".join(c for c in filename if c.isalnum() or c in (' ', '.', '_', '-')).rstrip()
            file_path = os.path.join('downloads', filename)

            # Download the media
            downloaded_path = await self.client.download_media(message, file_path)
            return downloaded_path
        except Exception as e:
            print(f"Error downloading media: {e}")
            return None

    def get_media_type(self, message) -> Optional[str]:
        """Determine media type from message"""
        if not message.media:
            return None

        if isinstance(message.media, MessageMediaPhoto):
            return 'photo'
        elif isinstance(message.media, MessageMediaDocument):
            if message.media.document.mime_type:
                if message.media.document.mime_type.startswith('video/'):
                    return 'video'
                elif message.media.document.mime_type.startswith('audio/'):
                    return 'audio'
                elif message.media.document.mime_type.startswith('image/'):
                    return 'photo'
            return 'document'

        return 'document'

    def send_to_bot_api(self, method: str, data: Dict, files: Optional[Dict] = None) -> requests.Response:
        """Send request to Bot API"""
        url = f"{self.base_url}/bot{self.bot_token}/{method}"

        if files:
            response = requests.post(url, data=data, files=files)
        else:
            response = requests.post(url, json=data)

        return response

    async def forward_message(self, message, source_channel: str, target_channel: str):
        """Forward a single message"""
        if self.is_message_forwarded(source_channel, message.id):
            print(f"Message {message.id} from {source_channel} already forwarded, skipping...")
            return

        try:
            # Debug: Print message details
            print(f"Processing message {message.id} from {source_channel}")
            print(f"Original text: {repr(message.text)}")
            print(f"Message attribute: {repr(getattr(message, 'message', None))}")
            print(f"Message type: {type(message.media) if message.media else 'text only'}")
            print(f"Has entities: {bool(message.entities)}")
            
            # Use message.message if message.text is None, otherwise use message.text
            text = message.text if message.text is not None else (getattr(message, 'message', '') or '')
            
            # Convert URL entities to plain text, ignore all other formatting
            plain_text = self.convert_entities_to_plain_text(message.entities, text)
            
            # Debug: Print processed text
            print(f"Processed text: {repr(plain_text)}")
            
            # Handle media messages
            if message.media:
                media_type = self.get_media_type(message)
                print(f"Media type: {media_type}")
                
                file_path = await self.download_media(message)

                if not file_path:
                    print(f"Failed to download media for message {message.id} from {source_channel}")
                    return

                # Prepare common data for media
                data = {
                    'chat_id': target_channel
                }

                # Add caption if text exists
                if plain_text.strip():  # Check if text is not empty after stripping whitespace
                    data['caption'] = plain_text.strip()

                try:
                    with open(file_path, 'rb') as f:
                        files = {}

                        if media_type == 'photo':
                            files['photo'] = f
                            response = self.send_to_bot_api('sendPhoto', data, files)
                        elif media_type == 'video':
                            files['video'] = f
                            response = self.send_to_bot_api('sendVideo', data, files)
                        elif media_type == 'audio':
                            files['audio'] = f
                            response = self.send_to_bot_api('sendAudio', data, files)
                        else:  # document
                            files['document'] = f
                            response = self.send_to_bot_api('sendDocument', data, files)

                        if response.status_code == 200:
                            print(f"Successfully forwarded media message {message.id} from {source_channel} to {target_channel}")
                            self.mark_message_forwarded(source_channel, message.id)
                        else:
                            print(f"Failed to forward media message {message.id} from {source_channel}: {response.text}")

                finally:
                    # Clean up downloaded file
                    if os.path.exists(file_path):
                        os.remove(file_path)

            # Handle text-only messages
            elif plain_text.strip():  # Check if text is not empty after stripping whitespace
                data = {
                    'chat_id': target_channel,
                    'text': plain_text.strip()
                }

                response = self.send_to_bot_api('sendMessage', data)

                if response.status_code == 200:
                    print(f"Successfully forwarded text message {message.id} from {source_channel} to {target_channel}")
                    self.mark_message_forwarded(source_channel, message.id)
                else:
                    print(f"Failed to forward text message {message.id} from {source_channel}: {response.text}")

            else:
                print(f"Message {message.id} from {source_channel} has no content to forward")
                print(f"Final text check - text: {repr(text)}, plain_text: {repr(plain_text)}")
                self.mark_message_forwarded(source_channel, message.id)  # Mark as processed to avoid reprocessing

        except Exception as e:
            print(f"Error forwarding message {message.id} from {source_channel}: {e}")
            import traceback
            traceback.print_exc()

    async def get_last_messages(self, source_channel: str, limit: int = 20):
        """Get last N messages from source channel"""
        try:
            # Get the entity for the source channel
            entity = await self.client.get_entity(source_channel)

            # Get message history
            history = await self.client(GetHistoryRequest(
                peer=entity,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))

            return history.messages

        except Exception as e:
            print(f"Error getting messages from {source_channel}: {e}")
            return []

    async def process_channel(self, channel_config: Dict[str, Any]):
        """Process a single channel configuration"""
        source_channel = channel_config['source']
        target_channel = channel_config['target']
        limit = channel_config.get('limit', 20)

        print(f"Processing channel: {source_channel} -> {target_channel}")

        # Get last messages
        print(f"Fetching last {limit} messages from {source_channel}...")
        messages = await self.get_last_messages(source_channel, limit)

        if not messages:
            print(f"No messages found in {source_channel}")
            return

        messages.reverse()

        print(f"Found {len(messages)} messages in {source_channel}")

        # Process messages in chronological order (newest first, as they come from Telegram)
        for message in messages:
            await self.forward_message(message, source_channel, target_channel)
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.5)

        print(f"Finished processing {source_channel}")

    async def run(self):
        """Main run method"""
        print("Starting Telethon forwarder...")

        # Connect to Telethon
        await self.client.start()
        print("Connected to Telethon")

        # Process each channel
        for channel_config in self.channels:
            await self.process_channel(channel_config)

        print("Finished forwarding messages from all channels")

async def main():
    # Configuration
    config = {
        'api_id': os.environ.get("API_ID"),  # Your API ID
        'api_hash': os.environ.get("API_HASH"),  # Your API hash
        'session_string': os.environ.get("STRING_SESSION"),  # Your string session (leave empty for first run)
        'bot_token': os.environ.get("TOKEN"),  # Your bot token
        'base_url': 'https://tapi.bale.ai',  # Base URL for API
        'channels': [
            {
                'source': '@mitivpn',  # Source channel username or ID
                'target': '5385300781',
                'limit': 20
            }
        ],
        'db_file': 'forwarded_messages.json'
    }

    # If no session string provided, generate one
    if not config['session_string']:
        print("No session string provided. Generating a new session...")
        client = TelegramClient(StringSession(), config['api_id'], config['api_hash'])
        await client.start()
        session_string = client.session.save()
        print(f"Your session string: {session_string}")
        print("Please save this session string and add it to the config!")
        await client.disconnect()
        return

    forwarder = TelethonForwarder(config)
    await forwarder.run()

if __name__ == '__main__':
    asyncio.run(main())
