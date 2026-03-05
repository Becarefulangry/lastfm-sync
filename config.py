import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Last.fm configuration
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_USER = os.getenv('LASTFM_USER')

# Supabase configuration
SUPABASE_CONFIG = {
    'host': os.getenv('SUPABASE_HOST'),
    'port': os.getenv('SUPABASE_PORT'),
    'database': os.getenv('SUPABASE_DATABASE'),
    'user': os.getenv('SUPABASE_USER'),
    'password': os.getenv('SUPABASE_PASSWORD')
}

# Проверка, что все переменные загружены
if not all([LASTFM_API_KEY, LASTFM_USER]):
    print("⚠️  Внимание: не все Last.fm переменные заполнены в .env файле")
    
if not all(SUPABASE_CONFIG.values()):
    print("⚠️  Внимание: не все Supabase переменные заполнены в .env файле")