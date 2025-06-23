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
from typing import Dict, List, Optional, Any, Tuple
import time
from pathlib import Path
import hashlib
from collections import OrderedDict
import logging
from dataclasses import dataclass

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
        self.chunk_size = config.get('chunk_size', 8192)
        self.session_timeout = aiohttp.ClientTimeout(total=120, connect=30)
        
        # Caching and batching
        self.forwarded_messages = OrderedDict()
        self.pending_saves = []
        self.last_save_time = time.time()
        self.save_interval = 10  # Save every 10 seconds
        
        # Session management
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Load database asynchronously
        self._db_loaded = False

    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()

    async def initialize(self):
        """Initialize all async components"""
        # Initialize HTTP session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=50,  # Total connection pool size
            limit_per_host=10,  # Per-host connection limit
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        self.http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.session_timeout,
            headers={'User-Agent': 'TelethonForwarder/2.0'}
        )
        
        # Load database
        await self.load_database()
        
        # Connect to Telethon
        await self.client.start()
        logger.info("Initialized all components successfully")

    async def cleanup(self):
        """Cleanup resources"""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        
        await self.save_database()
        
        if self.client and self.client.is_connected():
            await self.client.disconnect()
        
        logger.info("Cleanup completed")

    async def load_database(self):
        """Asynchronously load the database of forwarded messages"""
        if self.db_file.exists():
            try:
                async with aiofiles.open(self.db_file, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    data = json.loads(content)
                    # Use OrderedDict for better performance
                    self.forwarded_messages = OrderedDict(data)
                    logger.info(f"Loaded {len(self.forwarded_messages)} channel records from database")
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"Failed to load database: {e}")
                self.forwarded_messages = OrderedDict()
        else:
            self.forwarded_messages = OrderedDict()
        
        self._db_loaded = True

    async def save_database(self):
        """Asynchronously save the database with atomic writes"""
        try:
            temp_file = self.db_file.with_suffix('.tmp')
            
            async with aiofiles.open(temp_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(dict(self.forwarded_messages), ensure_ascii=False, indent=2))
            
            # Atomic rename
            temp_file.replace(self.db_file)
            self.last_save_time = time.time()
            logger.debug("Database saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save database: {e}")

    async def periodic_save(self):
        """Periodically save database to avoid data loss"""
        if time.time() - self.last_save_time > self.save_interval:
            await self.save_database()

    def is_message_forwarded(self, source_channel: str, message_id: int) -> bool:
        """Check if message is already forwarded with O(1) lookup"""
        return (source_channel in self.forwarded_messages and 
                str(message_id) in self.forwarded_messages[source_channel])

    def mark_message_forwarded(self, source_channel: str, message_id: int):
        """Mark message as forwarded in memory"""
        if source_channel not in self.forwarded_messages:
            self.forwarded_messages[source_channel] = {}
        self.forwarded_messages[source_channel][str(message_id)] = True

    def extract_urls_optimized(self, entities: List, text: str) -> List[str]:
        """Optimized URL extraction with minimal processing"""
        if not entities or not text:
            return []

        urls = []
        text_len = len(text)
        
        for entity in entities:
            if isinstance(entity, MessageEntityTextUrl):
                if entity.url and entity.url not in urls:
                    urls.append(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                start, end = entity.offset, entity.offset + entity.length
                if 0 <= start < text_len and start < end <= text_len:
                    url = text[start:end]
                    if url and url not in urls:
                        urls.append(url)

        return urls

    def get_original_filename(self, message) -> Optional[str]:
        """Extract original filename with proper handling"""
        if not message.media or not hasattr(message.media, 'document'):
            return None
            
        document = message.media.document
        if not document or not hasattr(document, 'attributes'):
            return None
            
        # Look for filename in document attributes
        for attr in document.attributes:
            if hasattr(attr, 'file_name') and attr.file_name:
                # Clean filename but preserve extension
                filename = attr.file_name.strip()
                # Remove path separators for security
                filename = filename.replace('/', '_').replace('\\', '_')
                return filename
                
        return None

    def generate_safe_filename(self, message, media_type: str, mime_type: str = None) -> str:
        """Generate safe filename based on message and media type"""
        original = self.get_original_filename(message)
        if original:
            return original
            
        # Generate based on media type and mime type
        extensions = {
            'photo': '.jpg',
            'video': '.mp4',
            'audio': '.mp3',
            'voice': '.ogg',
            'document': ''
        }
        
        # Try to get extension from mime type
        if mime_type:
            import mimetypes
            ext = mimetypes.guess_extension(mime_type)
            if ext:
                extensions[media_type] = ext
        
        base_name = f"{media_type}_{message.id}_{int(time.time())}"
        return base_name + extensions.get(media_type, '')

    async def download_media_optimized(self, message) -> Optional[MediaInfo]:
        """Optimized media download with proper filename handling"""
        if not message.media:
            return None

        try:
            media_type = self.get_media_type(message)
            if not media_type:
                return None
                
            # Get mime type
            mime_type = None
            if hasattr(message.media, 'document') and message.media.document:
                mime_type = getattr(message.media.document, 'mime_type', None)
            
            # Generate filename (preserving original when possible)
            filename = self.generate_safe_filename(message, media_type, mime_type)
            file_path = self.downloads_dir / filename
            
            # Use message hash for unique temporary filename during download
            temp_name = f"temp_{hashlib.md5(str(message.id).encode()).hexdigest()[:8]}_{filename}"
            temp_path = self.downloads_dir / temp_name
            
            # Download with progress tracking for large files
            downloaded_path = await self.client.download_media(
                message, 
                file=str(temp_path),
                progress_callback=lambda current, total: None  # Disable progress logging for performance
            )
            
            if downloaded_path:
                # Rename to final filename
                Path(downloaded_path).rename(file_path)
                
                return MediaInfo(
                    file_path=str(file_path),
                    media_type=media_type,
                    filename=filename,
                    mime_type=mime_type
                )
                
        except Exception as e:
            logger.error(f"Error downloading media for message {message.id}: {e}")
            
        return None

    def get_media_type(self, message) -> Optional[str]:
        """Optimized media type detection"""
        if not message.media:
            return None

        if isinstance(message.media, MessageMediaPhoto):
            return 'photo'
        elif isinstance(message.media, MessageMediaDocument):
            doc = message.media.document
            if not doc:
                return 'document'
                
            mime_type = getattr(doc, 'mime_type', '')
            
            # Fast mime type checking
            if mime_type.startswith('video/'):
                return 'video'
            elif mime_type.startswith('audio/'):
                return 'audio'
            elif mime_type.startswith('image/'):
                return 'photo'
            
            # Check for voice messages
            if hasattr(doc, 'attributes'):
                for attr in doc.attributes:
                    if hasattr(attr, 'voice') and attr.voice:
                        return 'voice'
                        
            return 'document'

        return 'document'

    async def send_media_to_bot(self, media_info: MediaInfo, target_channel: str, caption: str = None) -> bool:
        """Optimized media sending with streaming upload"""
        try:
            url = f"{self.base_url}/bot{self.bot_token}/send{media_info.media_type.capitalize()}"
            
            # Prepare form data
            data = aiohttp.FormData()
            data.add_field('chat_id', target_channel)
            
            if caption and caption.strip():
                data.add_field('caption', caption.strip())
            
            # Stream file upload
            file_path = Path(media_info.file_path)
            async with aiofiles.open(file_path, 'rb') as f:
                file_data = await f.read()
                data.add_field(
                    media_info.media_type,
                    file_data,
                    filename=media_info.filename,
                    content_type=media_info.mime_type or 'application/octet-stream'
                )

            async with self.http_session.post(url, data=data) as response:
                if response.status == 200:
                    logger.debug(f"Successfully sent {media_info.media_type}: {media_info.filename}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to send {media_info.media_type}: HTTP {response.status} - {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error sending media {media_info.filename}: {e}")
            return False
        finally:
            # Cleanup file
            try:
                Path(media_info.file_path).unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {media_info.file_path}: {e}")

    async def send_text_to_bot(self, text: str, target_channel: str) -> bool:
        """Optimized text message sending"""
        try:
            url = f"{self.base_url}/bot{self.bot_token}/sendMessage"
            
            payload = {
                'chat_id': target_channel,
                'text': text.strip()
            }

            async with self.http_session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.debug("Successfully sent text message")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to send text message: HTTP {response.status} - {error_text}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error sending text message: {e}")
            return False

    async def process_message(self, message, source_channel: str, target_channel: str) -> bool:
        """Process a single message with optimized handling"""
        if self.is_message_forwarded(source_channel, message.id):
            return True  # Already processed

        try:
            # Get text content
            text = message.text or getattr(message, 'message', '') or ''
            
            # Extract URLs efficiently
            urls = self.extract_urls_optimized(message.entities, text)
            if urls:
                text += "\n\n" + "\n".join(urls)

            success = False
            
            # Handle media messages
            if message.media:
                media_info = await self.download_media_optimized(message)
                if media_info:
                    success = await self.send_media_to_bot(media_info, target_channel, text)
                else:
                    logger.warning(f"Failed to download media for message {message.id}")
                    
            # Handle text-only messages
            elif text.strip():
                success = await self.send_text_to_bot(text, target_channel)
            else:
                # Empty message, mark as processed
                success = True
                logger.debug(f"Empty message {message.id}, marking as processed")

            if success:
                self.mark_message_forwarded(source_channel, message.id)
                logger.info(f"Successfully processed message {message.id} from {source_channel}")
                
                # Periodic save
                await self.periodic_save()
                
            return success
            
        except Exception as e:
            logger.error(f"Error processing message {message.id} from {source_channel}: {e}")
            return False

    async def get_messages_batch(self, source_channel: str, limit: int = 50) -> List:
        """Get messages in batches with error handling"""
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
            logger.error(f"Error getting messages from {source_channel}: {e}")
            return []

    async def process_channel_optimized(self, channel_config: Dict[str, Any]):
        """Process channel with concurrent downloads but sequential uploads for order preservation"""
        source_channel = channel_config['source']
        target_channel = channel_config['target']
        limit = channel_config.get('limit', 50)

        logger.info(f"Processing channel: {source_channel} -> {target_channel}")

        # Get messages
        messages = await self.get_messages_batch(source_channel, limit)
        if not messages:
            logger.warning(f"No messages found in {source_channel}")
            return

        # Reverse for chronological order (oldest first)
        messages.reverse()
        
        # Filter unprocessed messages
        unprocessed_messages = [
            msg for msg in messages 
            if not self.is_message_forwarded(source_channel, msg.id)
        ]
        
        if not unprocessed_messages:
            logger.info(f"All messages in {source_channel} already processed")
            return

        logger.info(f"Processing {len(unprocessed_messages)} new messages from {source_channel}")

        # Process messages sequentially to maintain order
        # But use semaphores to limit concurrent operations
        download_semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        upload_semaphore = asyncio.Semaphore(self.max_concurrent_uploads)

        async def process_with_semaphores(message):
            async with download_semaphore:
                async with upload_semaphore:
                    return await self.process_message(message, source_channel, target_channel)

        # Process messages in small batches to maintain order while allowing some concurrency
        batch_size = 5
        for i in range(0, len(unprocessed_messages), batch_size):
            batch = unprocessed_messages[i:i + batch_size]
            
            # Process batch concurrently
            tasks = [process_with_semaphores(msg) for msg in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Log results
            successful = sum(1 for r in results if r is True)
            logger.info(f"Batch {i//batch_size + 1}: {successful}/{len(batch)} messages processed successfully")
            
            # Small delay between batches to avoid rate limiting
            if i + batch_size < len(unprocessed_messages):
                await asyncio.sleep(0.5)

        logger.info(f"Finished processing {source_channel}")

    async def run(self):
        """Main execution method with comprehensive error handling"""
        logger.info("Starting ultra-optimized Telethon forwarder...")
        
        start_time = time.time()
        
        try:
            # Process all channels
            for channel_config in self.channels:
                try:
                    await self.process_channel_optimized(channel_config)
                except Exception as e:
                    logger.error(f"Error processing channel {channel_config.get('source', 'unknown')}: {e}")
                    continue

            # Final save
            await self.save_database()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Completed forwarding in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Critical error in main run: {e}")
            raise

async def main():
    """Main function with proper resource management"""
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
                'limit': 20  # Increased batch size
            }
        ],
        'db_file': 'forwarded_messages.json',
        # Performance tuning
        'max_concurrent_downloads': 5,
        'max_concurrent_uploads': 1,
        'chunk_size': 8192
    }

    # Generate session string if needed
    if not config['session_string']:
        logger.info("No session string provided. Generating a new session...")
        client = TelegramClient(StringSession(), config['api_id'], config['api_hash'])
        await client.start()
        session_string = client.session.save()
        logger.info(f"Your session string: {session_string}")
        logger.info("Please save this session string and add it to the config!")
        await client.disconnect()
        return

    # Run forwarder with proper resource management
    async with OptimizedTelethonForwarder(config) as forwarder:
        await forwarder.run()

if __name__ == '__main__':
    asyncio.run(main())
