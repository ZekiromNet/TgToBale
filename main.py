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
from typing import Dict, List, Optional, Any, Set, Tuple
from pathlib import Path
import time

class OptimizedTelethonForwarder:
    __slots__ = (
        'config', 'client', 'bot_token', 'base_url', 'channels', 'db_file',
        'forwarded_messages', 'session', 'last_api_call', 'api_delay'
    )
    
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
        
        # Pre-load database
        self.forwarded_messages = self._load_database_sync()
        
        # HTTP session for API calls
        self.session = None
        
        # Rate limiting
        self.last_api_call = 0
        self.api_delay = 0.02  # Reduced from 30ms to 20ms

    def _load_database_sync(self) -> Dict[str, Set[str]]:
        """Load database synchronously with set for faster lookups"""
        try:
            if self.db_file.exists():
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Convert to sets for O(1) lookup
                return {k: set(v.keys()) if isinstance(v, dict) else set(v) 
                       for k, v in data.items()}
        except:
            pass
        return {}

    async def _save_database(self):
        """Save database asynchronously - non-blocking"""
        data = {k: {msg_id: True for msg_id in v} for k, v in self.forwarded_messages.items()}
        try:
            async with aiofiles.open(self.db_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, separators=(',', ':')))
        except:
            pass

    def _is_forwarded(self, source: str, msg_id: str) -> bool:
        """O(1) lookup for forwarded messages"""
        return source in self.forwarded_messages and msg_id in self.forwarded_messages[source]

    def _mark_forwarded(self, source: str, msg_id: str):
        """Mark message as forwarded without immediate save"""
        if source not in self.forwarded_messages:
            self.forwarded_messages[source] = set()
        self.forwarded_messages[source].add(msg_id)

    def _extract_urls_fast(self, entities, text: str) -> str:
        """Ultra-fast URL extraction and text preparation in one pass"""
        if not entities or not text:
            return text
        
        urls = []
        for entity in entities:
            if isinstance(entity, MessageEntityTextUrl):
                urls.append(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                start, end = entity.offset, entity.offset + entity.length
                if 0 <= start < len(text) and start < end <= len(text):
                    urls.append(text[start:end])
        
        return text + ('\n\n' + '\n'.join(dict.fromkeys(urls))) if urls else text

    def _get_original_filename(self, message) -> str:
        """Get original filename from message media"""
        if not message.media:
            return f"{message.id}"
            
        if isinstance(message.media, MessageMediaPhoto):
            return f"{message.id}.jpg"
        elif isinstance(message.media, MessageMediaDocument):
            doc = message.media.document
            # Try to get original filename from attributes
            for attr in getattr(doc, 'attributes', []):
                if hasattr(attr, 'file_name') and attr.file_name:
                    # Clean the filename but keep extension
                    name = attr.file_name.replace('/', '_').replace('\\', '_')
                    return f"{message.id}_{name}"
            
            # Fallback to mime type
            mime = getattr(doc, 'mime_type', '')
            if mime.startswith('video/'):
                return f"{message.id}.mp4"
            elif mime.startswith('audio/'):
                return f"{message.id}.mp3"
            elif mime.startswith('image/'):
                return f"{message.id}.jpg"
            else:
                return f"{message.id}.bin"
        
        return f"{message.id}"

    async def _ensure_session(self):
        """Ensure HTTP session exists with optimized settings"""
        if not self.session:
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                keepalive_timeout=60,
                enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(total=20, connect=5)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'Connection': 'keep-alive'}
            )

    async def _api_call(self, method: str, data: Dict, files: Optional[Dict] = None) -> bool:
        """Ultra-optimized API call"""
        await self._ensure_session()
        
        # Micro rate limiting
        now = time.time()
        if now - self.last_api_call < self.api_delay:
            await asyncio.sleep(self.api_delay - (now - self.last_api_call))
        self.last_api_call = time.time()
        
        url = f"{self.base_url}/bot{self.bot_token}/{method}"
        
        try:
            if files:
                form_data = aiohttp.FormData()
                for k, v in data.items():
                    form_data.add_field(k, str(v))
                for k, (filename, file_data) in files.items():
                    form_data.add_field(k, file_data, filename=filename)
                
                async with self.session.post(url, data=form_data) as resp:
                    return resp.status == 200
            else:
                async with self.session.post(url, json=data) as resp:
                    return resp.status == 200
        except:
            return False

    def _get_media_params(self, message) -> Tuple[str, str]:
        """Get media method and field in one call"""
        if isinstance(message.media, MessageMediaPhoto):
            return 'sendPhoto', 'photo'
        elif isinstance(message.media, MessageMediaDocument):
            mime = getattr(message.media.document, 'mime_type', '')
            if mime.startswith('video/'):
                return 'sendVideo', 'video'
            elif mime.startswith('audio/'):
                return 'sendAudio', 'audio'
        return 'sendDocument', 'document'

    async def _download_and_send_media(self, message, text: str, target: str) -> bool:
        """Download and send media in one optimized flow"""
        try:
            # Get original filename
            filename = self._get_original_filename(message)
            
            # Download to memory instead of disk for speed
            file_bytes = await self.client.download_media(message, file=bytes)
            if not file_bytes:
                return False
            
            # Get upload parameters
            method, field = self._get_media_params(message)
            
            # Prepare data
            data = {'chat_id': target}
            if text.strip():
                data['caption'] = text.strip()
            
            # Send directly from memory
            files = {field: (filename, file_bytes)}
            return await self._api_call(method, data, files)
            
        except Exception as e:
            print(f"Media error {message.id}: {e}")
            return False

    async def _send_text(self, text: str, target: str) -> bool:
        """Send text message"""
        return await self._api_call('sendMessage', {'chat_id': target, 'text': text.strip()})

    async def _process_single_message(self, message, source: str, target: str) -> bool:
        """Process single message with all optimizations"""
        msg_id = str(message.id)
        
        if self._is_forwarded(source, msg_id):
            return True

        # Prepare text with URLs in one pass
        text = self._extract_urls_fast(message.entities, message.text or '')
        
        success = False
        
        if message.media:
            success = await self._download_and_send_media(message, text, target)
        elif text.strip():
            success = await self._send_text(text, target)
        else:
            success = True  # Empty message
        
        if success:
            self._mark_forwarded(source, msg_id)
        
        return success

    async def _get_messages_fast(self, source: str, limit: int) -> List:
        """Get messages optimized"""
        try:
            entity = await self.client.get_entity(source)
            result = await self.client(GetHistoryRequest(
                peer=entity, limit=limit, offset_date=None, offset_id=0,
                max_id=0, min_id=0, add_offset=0, hash=0
            ))
            # Return in chronological order (oldest first)
            return list(reversed(result.messages))
        except:
            return []

    async def _process_channel_sequential(self, channel_config: Dict[str, Any]):
        """Process channel with messages in strict chronological order"""
        source = channel_config['source']
        target = channel_config['target']
        limit = channel_config.get('limit', 20)

        print(f"Processing {source}")
        
        messages = await self._get_messages_fast(source, limit)
        if not messages:
            return

        # Filter new messages while maintaining order
        new_messages = [m for m in messages if not self._is_forwarded(source, str(m.id))]
        
        if not new_messages:
            print(f"All messages processed for {source}")
            return

        print(f"Sending {len(new_messages)} messages in order")
        
        # Process messages sequentially to maintain exact order
        success_count = 0
        for message in new_messages:
            if await self._process_single_message(message, source, target):
                success_count += 1
                # Minimal delay to maintain order and avoid rate limits
                if success_count < len(new_messages):  # No delay after last message
                    await asyncio.sleep(0.05)  # Reduced from 0.1s
        
        print(f"Sent {success_count}/{len(new_messages)} messages")

    async def run(self):
        """Main execution - ultra optimized"""
        try:
            # Parallel client start and session creation
            await asyncio.gather(
                self.client.start(),
                self._ensure_session()
            )
            
            # Process channels sequentially to maintain global message order
            for config in self.channels:
                await self._process_channel_sequential(config)
            
        finally:
            # Parallel cleanup
            cleanup_tasks = [self._save_database()]
            if self.session:
                cleanup_tasks.append(self.session.close())
            cleanup_tasks.append(self.client.disconnect())
            
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

async def main():
    config = {
        'api_id': int(os.environ["API_ID"]),
        'api_hash': os.environ["API_HASH"],
        'session_string': os.environ["STRING_SESSION"],
        'bot_token': os.environ["TOKEN"],
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
        client = TelegramClient(StringSession(), config['api_id'], config['api_hash'])
        await client.start()
        print(f"Session string: {client.session.save()}")
        await client.disconnect()
        return

    forwarder = OptimizedTelethonForwarder(config)
    await forwarder.run()

if __name__ == '__main__':
    asyncio.run(main())
