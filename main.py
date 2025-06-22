import uvloop
uvloop.install()

import asyncio
import json
import os
import aiohttp
import aiofiles
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    MessageEntityTextUrl, MessageEntityUrl
)
from telethon.tl.functions.messages import GetHistoryRequest
from typing import Dict, List, Optional, Any, Set
from pathlib import Path
import time

class OptimizedTelethonForwarder:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = TelegramClient(
            StringSession(config['session_string']),
            config['api_id'],
            config['api_hash']
        )
        
        self.bot_token = config['bot_token']
        self.base_url = config['base_url']
        self.channels = config['channels']
        self.db_file = Path(config.get('db_file', 'forwarded_messages.json'))
        self.downloads_dir = Path('downloads')
        
        # Create downloads directory
        self.downloads_dir.mkdir(exist_ok=True)
        
        # Pre-load database
        self.forwarded_messages = self._load_database_sync()
        
        # HTTP session for API calls
        self.session = None
        
        # Batch processing settings
        self.batch_size = 10
        self.max_concurrent_downloads = 5
        self.max_concurrent_uploads = 3
        
        # Rate limiting
        self.last_api_call = 0
        self.api_delay = 0.03  # 30ms between API calls

    def _load_database_sync(self) -> Dict[str, Set[str]]:
        """Load database synchronously with set for faster lookups"""
        if self.db_file.exists():
            try:
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Convert to sets for O(1) lookup
                return {k: set(v.keys()) if isinstance(v, dict) else set(v) 
                       for k, v in data.items()}
            except (json.JSONDecodeError, KeyError):
                return {}
        return {}

    async def _save_database(self):
        """Save database asynchronously"""
        data = {k: {msg_id: True for msg_id in v} for k, v in self.forwarded_messages.items()}
        async with aiofiles.open(self.db_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=2))

    def _is_forwarded(self, source: str, msg_id: int) -> bool:
        """O(1) lookup for forwarded messages"""
        return source in self.forwarded_messages and str(msg_id) in self.forwarded_messages[source]

    def _mark_forwarded(self, source: str, msg_id: int):
        """Mark message as forwarded without immediate save"""
        if source not in self.forwarded_messages:
            self.forwarded_messages[source] = set()
        self.forwarded_messages[source].add(str(msg_id))

    def _extract_urls(self, entities: List, text: str) -> List[str]:
        """Fast URL extraction with minimal processing"""
        if not entities or not text:
            return []
        
        urls = []
        text_len = len(text)
        
        for entity in entities:
            if isinstance(entity, MessageEntityTextUrl):
                urls.append(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                start, end = entity.offset, entity.offset + entity.length
                if 0 <= start < text_len and start < end <= text_len:
                    urls.append(text[start:end])
        
        return list(dict.fromkeys(urls))  # Remove duplicates preserving order

    def _prepare_text(self, message) -> str:
        """Prepare message text with URLs appended"""
        text = message.text or getattr(message, 'message', '') or ''
        if not text.strip():
            return ''
        
        urls = self._extract_urls(message.entities, text)
        return text + ('\n\n' + '\n'.join(urls) if urls else '')

    async def _ensure_session(self):
        """Ensure HTTP session exists"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def _rate_limit(self):
        """Simple rate limiting"""
        now = time.time()
        elapsed = now - self.last_api_call
        if elapsed < self.api_delay:
            await asyncio.sleep(self.api_delay - elapsed)
        self.last_api_call = time.time()

    async def _api_call(self, method: str, data: Dict, files: Optional[Dict] = None) -> bool:
        """Optimized API call with connection reuse"""
        await self._ensure_session()
        await self._rate_limit()
        
        url = f"{self.base_url}/bot{self.bot_token}/{method}"
        
        try:
            if files:
                form_data = aiohttp.FormData()
                for key, value in data.items():
                    form_data.add_field(key, str(value))
                for key, file_data in files.items():
                    form_data.add_field(key, file_data[1], filename=file_data[0])
                
                async with self.session.post(url, data=form_data) as response:
                    return response.status == 200
            else:
                async with self.session.post(url, json=data) as response:
                    return response.status == 200
        except Exception as e:
            print(f"API call failed: {e}")
            return False

    async def _download_media(self, message) -> Optional[Path]:
        """Optimized media download"""
        if not message.media:
            return None

        # Generate filename
        filename = f"{message.id}_{int(time.time())}"
        if isinstance(message.media, MessageMediaPhoto):
            filename += ".jpg"
        elif isinstance(message.media, MessageMediaDocument):
            mime = getattr(message.media.document, 'mime_type', '')
            if mime.startswith('video/'):
                filename += ".mp4"
            elif mime.startswith('audio/'):
                filename += ".mp3"
            elif mime.startswith('image/'):
                filename += ".jpg"
            else:
                filename += ".bin"
        
        file_path = self.downloads_dir / filename
        
        try:
            await self.client.download_media(message, file_path)
            return file_path
        except Exception as e:
            print(f"Download failed for message {message.id}: {e}")
            return None

    def _get_media_method(self, message) -> str:
        """Determine upload method based on media type"""
        if isinstance(message.media, MessageMediaPhoto):
            return 'sendPhoto', 'photo'
        elif isinstance(message.media, MessageMediaDocument):
            mime = getattr(message.media.document, 'mime_type', '')
            if mime.startswith('video/'):
                return 'sendVideo', 'video'
            elif mime.startswith('audio/'):
                return 'sendAudio', 'audio'
        return 'sendDocument', 'document'

    async def _send_media(self, message, text: str, target: str, file_path: Path) -> bool:
        """Send media message"""
        method, field = self._get_media_method(message)
        
        data = {'chat_id': target}
        if text.strip():
            data['caption'] = text.strip()

        try:
            async with aiofiles.open(file_path, 'rb') as f:
                file_data = await f.read()
                files = {field: (file_path.name, file_data)}
                return await self._api_call(method, data, files)
        finally:
            # Cleanup
            file_path.unlink(missing_ok=True)

    async def _send_text(self, text: str, target: str) -> bool:
        """Send text message"""
        data = {'chat_id': target, 'text': text.strip()}
        return await self._api_call('sendMessage', data)

    async def _process_message(self, message, source: str, target: str) -> bool:
        """Process single message"""
        if self._is_forwarded(source, message.id):
            return True

        text = self._prepare_text(message)
        
        try:
            success = False
            
            if message.media:
                file_path = await self._download_media(message)
                if file_path:
                    success = await self._send_media(message, text, target, file_path)
            elif text.strip():
                success = await self._send_text(text, target)
            else:
                success = True  # Empty message, mark as processed
            
            if success:
                self._mark_forwarded(source, message.id)
                return True
                
        except Exception as e:
            print(f"Error processing message {message.id}: {e}")
        
        return False

    async def _process_messages_batch(self, messages: List, source: str, target: str):
        """Process messages in batches while maintaining chronological order"""
        # Sort messages by ID to ensure chronological order (older messages have lower IDs)
        messages.sort(key=lambda m: m.id)
        
        # Process messages sequentially to maintain order, but with controlled concurrency for downloads
        download_semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        async def download_with_semaphore(message):
            if message.media:
                async with download_semaphore:
                    return await self._download_media(message)
            return None
        
        # Pre-download all media files concurrently while maintaining message order
        download_tasks = [download_with_semaphore(msg) for msg in messages]
        downloaded_files = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Now process messages sequentially to maintain order
        success_count = 0
        for i, message in enumerate(messages):
            if self._is_forwarded(source, message.id):
                success_count += 1
                continue
                
            text = self._prepare_text(message)
            success = False
            
            try:
                if message.media:
                    file_path = downloaded_files[i]
                    if isinstance(file_path, Exception):
                        print(f"Download failed for message {message.id}: {file_path}")
                        continue
                    if file_path:
                        success = await self._send_media(message, text, target, file_path)
                elif text.strip():
                    success = await self._send_text(text, target)
                else:
                    success = True  # Empty message, mark as processed
                
                if success:
                    self._mark_forwarded(source, message.id)
                    success_count += 1
                    # Small delay between messages to maintain order and avoid rate limits
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                print(f"Error processing message {message.id}: {e}")
        
        print(f"Processed {success_count}/{len(messages)} messages in order")

    async def _get_messages(self, source: str, limit: int) -> List:
        """Get messages from source channel in chronological order (oldest first)"""
        try:
            entity = await self.client.get_entity(source)
            history = await self.client(GetHistoryRequest(
                peer=entity, limit=limit, offset_date=None, offset_id=0,
                max_id=0, min_id=0, add_offset=0, hash=0
            ))
            # Telegram returns newest first, so reverse to get oldest first
            return list(reversed(history.messages))
        except Exception as e:
            print(f"Error getting messages from {source}: {e}")
            return []

    async def _process_channel(self, channel_config: Dict[str, Any]):
        """Process single channel maintaining message order"""
        source = channel_config['source']
        target = channel_config['target']
        limit = channel_config.get('limit', 20)

        print(f"Processing {source} -> {target}")
        
        messages = await self._get_messages(source, limit)
        if not messages:
            print(f"No messages in {source}")
            return

        # Filter out already processed messages while maintaining order
        new_messages = [m for m in messages if not self._is_forwarded(source, m.id)]
        
        if not new_messages:
            print(f"All messages from {source} already processed")
            return

        print(f"Processing {len(new_messages)}/{len(messages)} new messages from {source} in chronological order")
        
        await self._process_messages_batch(new_messages, source, target)
        
        # Save database after each channel
        await self._save_database()
        print(f"Completed {source}")

    async def run(self):
        """Main execution method"""
        print("Starting optimized forwarder...")
        
        try:
            # Start the client
            await self.client.start()
            print("Connected to Telegram")
            
            # Process all channels concurrently (each channel maintains its own message order)
            tasks = [self._process_channel(config) for config in self.channels]
            await asyncio.gather(*tasks)
            
            print("All channels processed successfully")
            
        finally:
            # Cleanup
            if self.session:
                await self.session.close()
            await self.client.disconnect()
            await self._save_database()

async def main():
    config = {
        'api_id': os.environ.get("API_ID"),
        'api_hash': os.environ.get("API_HASH"),
        'session_string': os.environ.get("STRING_SESSION"),
        'bot_token': os.environ.get("TOKEN"),
        'base_url': 'https://tapi.bale.ai',
        'channels': [
            {
                'source': '@mitivpn',
                'target': '5385300781',
                'limit': 20
            }
        ],
        'db_file': 'forwarded_messages.json'
    }

    if not config['session_string']:
        print("Generating session string...")
        client = TelegramClient(StringSession(), config['api_id'], config['api_hash'])
        await client.start()
        print(f"Session string: {client.session.save()}")
        await client.disconnect()
        return

    forwarder = OptimizedTelethonForwarder(config)
    await forwarder.run()

if __name__ == '__main__':
    asyncio.run(main())
