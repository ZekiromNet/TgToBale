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
from typing import Dict, List, Optional, Any
import time
from pathlib import Path
import hashlib
from collections import OrderedDict
import logging
from dataclasses import dataclass
from urllib.parse import unquote

# Use uvloop for maximum performance
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop for enhanced performance")
except ImportError:
    print("uvloop not available, using default event loop")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('forwarder.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class MediaInfo:
    file_path: str
    media_type: str
    filename: str
    mime_type: Optional[str] = None

class OptimizedTelethonForwarder:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = TelegramClient(
            StringSession(config['session_string']),
            config['api_id'],
            config['api_hash'],
            timeout=30,
            retry_delay=1,
            auto_reconnect=True
        )
        self.bot_token = config['bot_token']
        self.base_url = config['base_url']
        self.channels = config['channels']
        self.db_file = Path(config.get('db_file', 'forwarded_messages.json'))
        self.downloads_dir = Path('downloads')
        self.downloads_dir.mkdir(exist_ok=True)

        # Performance optimizations
        self.max_concurrent_downloads = config.get('max_concurrent_downloads', 5)
        self.max_concurrent_uploads = config.get('max_concurrent_uploads', 3)
        self.session_timeout = aiohttp.ClientTimeout(total=120, connect=30)

        # Caching and batching
        self.forwarded_messages = OrderedDict()
        self.last_save_time = time.time()
        self.save_interval = 10  # Save every 10 seconds

        # Session management
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._db_loaded = False

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    async def initialize(self):
        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        self.http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.session_timeout,
            headers={'User-Agent': 'TelethonForwarder/2.0'}
        )
        await self.load_database()
        await self.client.start()
        logger.info("Initialized all components successfully")

    async def cleanup(self):
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        await self.save_database()
        if self.client and self.client.is_connected():
            await self.client.disconnect()
        logger.info("Cleanup completed")

    async def load_database(self):
        if self.db_file.exists():
            try:
                async with aiofiles.open(self.db_file, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    data = json.loads(content)
                    self.forwarded_messages = OrderedDict(
                        (channel, OrderedDict(messages)) for channel, messages in data.items()
                    )
                    logger.info(f"Loaded {len(self.forwarded_messages)} channel records from database")
            except Exception as e:
                logger.warning(f"Failed to load database: {e}")
                self.forwarded_messages = OrderedDict()
        else:
            self.forwarded_messages = OrderedDict()
        self._db_loaded = True

    async def save_database(self):
        try:
            temp_file = self.db_file.with_suffix('.tmp')
            # Convert nested OrderedDicts to regular dicts
            serializable_data = {channel: dict(messages) for channel, messages in self.forwarded_messages.items()}
            async with aiofiles.open(temp_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(serializable_data, ensure_ascii=False, indent=2))
            temp_file.replace(self.db_file)
            self.last_save_time = time.time()
            logger.debug("Database saved successfully")
        except Exception as e:
            logger.error(f"Failed to save database: {e}")

    async def periodic_save(self):
        if time.time() - self.last_save_time > self.save_interval:
            await self.save_database()

    def is_message_forwarded(self, source_channel: str, message_id: int) -> bool:
        return (source_channel in self.forwarded_messages and str(message_id) in self.forwarded_messages[source_channel])

    def mark_message_forwarded(self, source_channel: str, message_id: int):
        if source_channel not in self.forwarded_messages:
            self.forwarded_messages[source_channel] = OrderedDict()
        self.forwarded_messages[source_channel][str(message_id)] = True

    def extract_urls_optimized(self, entities: List, text: str) -> List[str]:
        if not entities or not text:
            return []
        urls = []
        length = len(text)
        for ent in entities:
            if isinstance(ent, MessageEntityTextUrl) and ent.url:
                if ent.url not in urls:
                    urls.append(ent.url)
            elif isinstance(ent, MessageEntityUrl):
                s, e = ent.offset, ent.offset + ent.length
                if 0 <= s < length and e <= length:
                    url = text[s:e]
                    if url not in urls:
                        urls.append(url)
        return urls

    def get_original_filename(self, message) -> Optional[str]:
        if not message.media or not hasattr(message.media, 'document'):
            return None
        doc = message.media.document
        if not doc or not hasattr(doc, 'attributes'):
            return None
        for attr in doc.attributes:
            if hasattr(attr, 'file_name') and attr.file_name:
                # Decode percent-encoding and sanitize
                filename = unquote(attr.file_name.strip())
                filename = filename.replace('/', '_').replace('\\', '_')
                return filename
        return None

    def generate_safe_filename(self, message, media_type: str, mime_type: str = None) -> str:
        orig = self.get_original_filename(message)
        if orig:
            return orig
        ext_map = {'photo': '.jpg', 'video': '.mp4', 'audio': '.mp3', 'voice': '.ogg', 'document': ''}
        ext = ''
        if mime_type:
            import mimetypes
            guessed = mimetypes.guess_extension(mime_type) or ''
            ext = guessed
        base = f"{media_type}_{message.id}_{int(time.time())}"
        return base + (ext_map.get(media_type, '') or ext)

    async def download_media_optimized(self, message) -> Optional[MediaInfo]:
        if not message.media:
            return None
        try:
            mtype = self.get_media_type(message)
            if not mtype:
                return None
            mime = getattr(message.media.document, 'mime_type', None) if hasattr(message.media, 'document') else None
            filename = self.generate_safe_filename(message, mtype, mime)
            final = self.downloads_dir / filename
            temp = self.downloads_dir / f"tmp_{hashlib.md5(str(message.id).encode()).hexdigest()[:8]}_{filename}"
            path = await self.client.download_media(message, file=str(temp), progress_callback=lambda c,t: None)
            if path:
                Path(path).rename(final)
                return MediaInfo(str(final), mtype, filename, mime)
        except Exception as e:
            logger.error(f"Error downloading media {message.id}: {e}")
        return None

    def get_media_type(self, message) -> Optional[str]:
        if not message.media:
            return None
        if isinstance(message.media, MessageMediaPhoto):
            return 'photo'
        if isinstance(message.media, MessageMediaDocument):
            doc = message.media.document
            mt = getattr(doc, 'mime_type', '')
            if mt.startswith('video/'):
                return 'video'
            if mt.startswith('audio/'):
                return 'audio'
            if mt.startswith('image/'):
                return 'photo'
            if any(hasattr(a, 'voice') and a.voice for a in doc.attributes):
                return 'voice'
            return 'document'
        return 'document'

    async def send_media_to_bot(self, media_info: MediaInfo, target_channel: str, caption: str = None) -> bool:
        try:
            url = f"{self.base_url}/bot{self.bot_token}/send{media_info.media_type.capitalize()}"
            data = aiohttp.FormData()
            data.add_field('chat_id', target_channel)
            if caption:
                data.add_field('caption', caption)
            async with aiofiles.open(media_info.file_path, 'rb') as f:
                content = await f.read()
                data.add_field(media_info.media_type, content, filename=media_info.filename, content_type=media_info.mime_type or 'application/octet-stream')
            async with self.http_session.post(url, data=data) as r:
                if r.status == 200:
                    logger.debug(f"Sent {media_info.filename}")
                    return True
                logger.error(f"Send failed {r.status}:", await r.text())
        except Exception as e:
            logger.error(f"Error sending media {media_info.filename}: {e}")
        finally:
            try: Path(media_info.file_path).unlink(missing_ok=True)
            except: pass
        return False

    async def send_text_to_bot(self, text: str, target_channel: str) -> bool:
        try:
            url = f"{self.base_url}/bot{self.bot_token}/sendMessage"
            payload = {'chat_id': target_channel, 'text': text}
            async with self.http_session.post(url, json=payload) as r:
                return r.status == 200
        except Exception as e:
            logger.error(f"Error sending text: {e}")
            return False

    async def process_message(self, msg, source: str, target: str) -> bool:
        if self.is_message_forwarded(source, msg.id): return True
        try:
            text = msg.text or getattr(msg, 'message', '') or ''
            urls = self.extract_urls_optimized(msg.entities, text)
            if urls:
                text += '\n'.join(urls)
            if msg.media:
                info = await self.download_media_optimized(msg)
                success = await self.send_media_to_bot(info, target, text) if info else False
            elif text.strip():
                success = await self.send_text_to_bot(text, target)
            else:
                success = True
            if success:
                self.mark_message_forwarded(source, msg.id)
                await self.periodic_save()
            return success
        except Exception as e:
            logger.error(f"Error processing msg {msg.id}: {e}")
            return False

    async def get_messages_batch(self, source_channel: str, limit: int = 50) -> List[Any]:
        try:
            ent = await self.client.get_entity(source_channel)
            history = await self.client(GetHistoryRequest(peer=ent, limit=limit, offset_id=0, offset_date=None, add_offset=0, max_id=0, min_id=0, hash=0))
            return history.messages
        except Exception as e:
            logger.error(f"Error fetching history for {source_channel}: {e}")
            return []

    async def process_channel(self, cfg: Dict[str, Any]):
        src, tgt = cfg['source'], cfg['target']
        msgs = await self.get_messages_batch(src, cfg.get('limit',45))
        msgs.reverse()
        unprocessed = [m for m in msgs if not self.is_message_forwarded(src,m.id)]
        sem_d = asyncio.Semaphore(self.max_concurrent_downloads)
        sem_u = asyncio.Semaphore(self.max_concurrent_uploads)
        async def worker(m):
            async with sem_d, sem_u:
                return await self.process_message(m, src, tgt)
        for i in range(0,len(unprocessed),5):
            batch = unprocessed[i:i+5]
            results = await asyncio.gather(*(worker(m) for m in batch), return_exceptions=True)
            logger.info(f"Batch {i//5+1} results: {results}")
            if i+5 < len(unprocessed):
                await asyncio.sleep(0.5)

    async def run(self):
        for ch in self.channels:
            await self.process_channel(ch)
        await self.save_database()

async def main():
    config = {
        'api_id': os.environ.get('API_ID'),
        'api_hash': os.environ.get('API_HASH'),
        'session_string': os.environ.get('STRING_SESSION'),
        'bot_token': os.environ.get('TOKEN'),
        'base_url': 'https://tapi.bale.ai',
        'channels': [{'source':'@mitivpn','target':'5385300781','limit':20}],
        'db_file':'forwarded_messages.json',
        'max_concurrent_downloads':5,
        'max_concurrent_uploads':1
    }
    if not config['session_string']:
        async with TelegramClient(StringSession(), config['api_id'], config['api_hash']) as tmp:
            await tmp.start()
            print("Save this STRING_SESSION=", tmp.session.save())
        return
    async with OptimizedTelethonForwarder(config) as fwd:
        await fwd.run()

if __name__ == '__main__':
    asyncio.run(main())
