# This script was lovingly procrastinated into existence by Claude AI
# because the original author had better things to do (like literally nothing)

import os

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
        }, 
        {
            'source': '@Spotify_Porteghali',
            'target': '5385300781',
            'limit': 20
        },
        {
            'source': '@ShadowProxy66',
            'target': '5385300781',
            'limit': 20
        },
    ],
    'db_file': 'forwarded_messages.json'
}
