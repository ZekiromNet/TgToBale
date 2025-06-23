# This script was lovingly procrastinated into existence by Claude AI
# because the original author had better things to do (like literally nothing)

import asyncio
import json
import os
import aiohttp
import aiofiles
from config import config
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageEntityTextUrl, MessageEntityUrl
)
from telethon.tl.functions.messages import GetHistoryRequest

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class TelethonForwarder:
    __slots__ = ('config', 'client', 'bot_token', 'base_url', 'channels', 
                 'db_file', 'forwarded_messages', 'session', 'semaphore')

    def __init__(self, config):
        self.config = config
        self.client = TelegramClient(
            StringSession(config['session_string']),
            config['api_id'],
            config['api_hash']
        )
        self.bot_token = config['bot_token']
        self.base_url = config['base_url']
        self.channels = config['channels']
        self.db_file = config.get('db_file', 'forwarded_messages.json')
        self.forwarded_messages = self.load_database()
        self.session = None
        # Limit concurrent operations (except uploads which are sequential)
        self.semaphore = asyncio.Semaphore(20)

    def load_database(self):
        """Load the database of forwarded messages"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    async def save_database(self):
        """Save the database of forwarded messages asynchronously"""
        async with aiofiles.open(self.db_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(self.forwarded_messages, ensure_ascii=False, indent=2))

    def is_message_forwarded(self, source_channel, message_id):
        """Check if message is already forwarded"""
        return source_channel in self.forwarded_messages and str(message_id) in self.forwarded_messages[source_channel]

    async def mark_message_forwarded(self, source_channel, message_id):
        """Mark message as forwarded"""
        if source_channel not in self.forwarded_messages:
            self.forwarded_messages[source_channel] = {}
        self.forwarded_messages[source_channel][str(message_id)] = True
        await self.save_database()

    def convert_entities_to_plain_text(self, entities, text):
        """Convert only URL entities to plain text, append URLs at the end"""
        if not entities or not text:
            return text

        urls_to_append = []
        
        for entity in entities:
            if isinstance(entity, MessageEntityTextUrl):
                if entity.url not in urls_to_append:
                    urls_to_append.append(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                start = entity.offset
                end = entity.offset + entity.length
                if 0 <= start < len(text) and start < end <= len(text):
                    url = text[start:end]
                    if url not in urls_to_append:
                        urls_to_append.append(url)

        result_text = text
        if urls_to_append:
            result_text += "\n\n" + "\n".join(urls_to_append)

        return result_text

    async def download_media(self, message):
        """Download media from message and return file path"""
        if not message.media:
            return None

        os.makedirs('downloads', exist_ok=True)

        try:
            filename = None
            if hasattr(message.media, 'document') and message.media.document:
                for attr in message.media.document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        filename = attr.file_name
                        break

            if not filename:
                media_type = self.get_media_type(message)
                ext_map = {'photo': '.jpg', 'video': '.mp4', 'audio': '.mp3', 'document': ''}
                filename = f"{media_type}_{message.id}{ext_map.get(media_type, '')}"

            filename = "".join(c for c in filename if c.isalnum() or c in (' ', '.', '_', '-')).rstrip()
            file_path = os.path.join('downloads', filename)

            downloaded_path = await self.client.download_media(message, file_path)
            return downloaded_path
        except Exception as e:
            print(f"Error downloading media: {e}")
            return None

    def get_media_type(self, message):
        """Determine media type from message"""
        if not message.media:
            return None

        if isinstance(message.media, MessageMediaPhoto):
            return 'photo'
        elif isinstance(message.media, MessageMediaDocument):
            mime_type = message.media.document.mime_type
            if mime_type:
                if mime_type.startswith('video/'):
                    return 'video'
                elif mime_type.startswith('audio/'):
                    return 'audio'
                elif mime_type.startswith('image/'):
                    return 'photo'
            return 'document'

        return 'document'

    async def send_to_bot_api(self, method, data, file_path=None):
        """Send request to Bot API with persistent session"""
        url = f"{self.base_url}/bot{self.bot_token}/{method}"

        try:
            if file_path:
                async with aiofiles.open(file_path, 'rb') as f:
                    file_content = await f.read()
                
                form_data = aiohttp.FormData(quote_fields=False)
                for key, value in data.items():
                    form_data.add_field(key, value)
                
                # Determine field name based on method
                field_name = 'photo' if 'Photo' in method else \
                           'video' if 'Video' in method else \
                           'audio' if 'Audio' in method else 'document'
                
                filename = os.path.basename(file_path)
                form_data.add_field(
                    field_name, 
                    file_content, 
                    filename=filename,
                    content_type='application/octet-stream'
                )
                
                async with self.session.post(url, data=form_data) as response:
                    return response.status, await response.text()
            else:
                async with self.session.post(url, json=data) as response:
                    return response.status, await response.text()
        except Exception as e:
            print(f"API request error: {e}")
            return 500, str(e)

    async def forward_message(self, message, source_channel, target_channel):
        """Forward a single message"""
        if self.is_message_forwarded(source_channel, message.id):
            return

        async with self.semaphore:
            try:
                text = message.text if message.text is not None else (getattr(message, 'message', '') or '')
                plain_text = self.convert_entities_to_plain_text(message.entities, text)
                
                if message.media:
                    media_type = self.get_media_type(message)
                    file_path = await self.download_media(message)

                    if not file_path:
                        print(f"Failed to download media for message {message.id}")
                        return

                    data = {'chat_id': target_channel}
                    if plain_text.strip():
                        data['caption'] = plain_text.strip()

                    try:
                        method_map = {
                            'photo': 'sendPhoto',
                            'video': 'sendVideo', 
                            'audio': 'sendAudio',
                            'document': 'sendDocument'
                        }
                        method = method_map.get(media_type, 'sendDocument')
                        
                        status, response_text = await self.send_to_bot_api(method, data, file_path)

                        if status == 200:
                            print(f"✓ Media message {message.id} forwarded")
                            await self.mark_message_forwarded(source_channel, message.id)
                        else:
                            print(f"✗ Failed to forward media message {message.id}: {response_text}")

                    finally:
                        if os.path.exists(file_path):
                            os.remove(file_path)

                elif plain_text.strip():
                    data = {
                        'chat_id': target_channel,
                        'text': plain_text.strip()
                    }

                    status, response_text = await self.send_to_bot_api('sendMessage', data)

                    if status == 200:
                        print(f"✓ Text message {message.id} forwarded")
                        await self.mark_message_forwarded(source_channel, message.id)
                    else:
                        print(f"✗ Failed to forward text message {message.id}: {response_text}")
                else:
                    await self.mark_message_forwarded(source_channel, message.id)

            except Exception as e:
                print(f"Error forwarding message {message.id}: {e}")

    async def get_last_messages(self, source_channel, limit=20):
        """Get last N messages from source channel"""
        try:
            entity = await self.client.get_entity(source_channel)
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

    async def process_channel(self, channel_config):
        """Process a single channel configuration"""
        source_channel = channel_config['source']
        target_channel = channel_config['target']
        limit = channel_config.get('limit', 20)

        print(f"Processing: {source_channel} -> {target_channel}")

        messages = await self.get_last_messages(source_channel, limit)
        if not messages:
            print(f"No messages found in {source_channel}")
            return

        # Reverse to maintain chronological order
        messages.reverse()
        print(f"Found {len(messages)} messages")

        # Process messages sequentially to maintain order
        for message in messages:
            await self.forward_message(message, source_channel, target_channel)
            await asyncio.sleep(0.1)  # Reduced delay

        print(f"Finished processing {source_channel}")

    async def run(self):
        """Main run method"""
        print("Starting ultra-optimized Telethon forwarder...")

        # Create optimized HTTP session
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )

        try:
            await self.client.start()
            print("Connected to Telethon")

            # Process channels concurrently
            tasks = [self.process_channel(config) for config in self.channels]
            await asyncio.gather(*tasks)

            print("Finished forwarding messages from all channels")

        finally:
            await self.session.close()


async def main():
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
