import asyncio
import os
import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageEntityTextUrl, MessageEntityUrl
)

class RealtimeForwarder:
    def __init__(self, config):
        self.config = config
        self.client = TelegramClient(
            StringSession(config['session_string']),
            config['api_id'],
            config['api_hash']
        )
        self.bot_token = config['bot_token']
        self.base_url = config['base_url']
        self.channels = config['channels']  # List of dicts with source and target

    def convert_entities_to_plain_text(self, entities, text):
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

    def get_media_type(self, message):
        if not message.media:
            return None

        media = message.media
        if hasattr(media, 'document') and media.document:
            mime = media.document.mime_type or ''
            if mime.startswith('video/'):
                return 'video'
            if mime.startswith('audio/'):
                return 'audio'
            if mime.startswith('image/'):
                return 'photo'
            return 'document'

        if 'photo' in str(type(media)).lower():
            return 'photo'

        return 'document'

    def send_to_bot_api(self, method, data, files=None):
        url = f"{self.base_url}/bot{self.bot_token}/{method}"
        if files:
            return requests.post(url, data=data, files=files)
        return requests.post(url, json=data)

    async def handle_message(self, event, target_chat):
        try:
            message = event.message
            text = message.text or getattr(message, 'message', '') or ''
            plain_text = self.convert_entities_to_plain_text(message.entities, text)

            if message.media:
                file_path = await self.client.download_media(message, file=bytes)
                media_type = self.get_media_type(message)

                data = { 'chat_id': target_chat }
                if plain_text.strip():
                    data['caption'] = plain_text.strip()

                files = {}
                if media_type == 'photo':
                    files['photo'] = ('photo.jpg', file_path)
                    method = 'sendPhoto'
                elif media_type == 'video':
                    files['video'] = ('video.mp4', file_path)
                    method = 'sendVideo'
                elif media_type == 'audio':
                    files['audio'] = ('audio.mp3', file_path)
                    method = 'sendAudio'
                else:
                    files['document'] = ('document', file_path)
                    method = 'sendDocument'

                response = self.send_to_bot_api(method, data, files)
            else:
                data = {'chat_id': target_chat, 'text': plain_text.strip()}
                response = self.send_to_bot_api('sendMessage', data)

            print(f"Forwarded message {message.id}: {response.status_code}")

        except Exception as e:
            print(f"Error handling message: {e}")

    async def run(self):
        await self.client.start()
        print("Listening for new messages...")

        for channel in self.channels:
            source = channel['source']
            target = channel['target']

            @self.client.on(events.NewMessage(chats=source))
            async def handler(event, target_chat=target):
                await self.handle_message(event, target_chat)

        await self.client.run_until_disconnected()


async def main():
    config = {
        'api_id': int(os.environ.get('API_ID')),
        'api_hash': os.environ.get('API_HASH'),
        'session_string': os.environ.get('STRING_SESSION'),
        'bot_token': os.environ.get('TOKEN'),
        'base_url': 'https://tapi.bale.ai',
        'channels': [
            {
                'source': '@kanale_rozane',
                'target': '5385300781'
            }
        ]
    }

    if not config['session_string']:
        client = TelegramClient(StringSession(), config['api_id'], config['api_hash'])
        await client.start()
        print("Session string:", client.session.save())
        await client.disconnect()
        return

    forwarder = RealtimeForwarder(config)
    await forwarder.run()

if __name__ == '__main__':
    asyncio.run(main())

