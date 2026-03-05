import requests
import psycopg2
from datetime import datetime
from config import SUPABASE_CONFIG, LASTFM_API_KEY, LASTFM_USER

def get_lastfm_recent():
    """Получает последние треки из Last.fm"""
    params = {
        'method': 'user.getRecentTracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 10
    }
    
    url = "http://ws.audioscrobbler.com/2.0/"
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        tracks = data.get('recenttracks', {}).get('track', [])
        
        print("\n🎵 ПОСЛЕДНИЕ ТРЕКИ ИЗ LAST.FM:")
        print("-" * 60)
        for track in tracks[:5]:  # первые 5
            if '@attr' in track and track['@attr'].get('nowplaying') == 'true':
                print(f"🔴 СЕЙЧАС: {track['artist']['#text']} - {track['name']}")
            else:
                date = track.get('date', {}).get('#text', '')
                print(f"📀 {date}: {track['artist']['#text']} - {track['name']}")
        return tracks
    else:
        print("❌ Ошибка получения данных из Last.fm")
        return []

def get_db_recent():
    """Получает последние треки из базы"""
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        cur = conn.cursor()
        
        cur.execute("""
            SELECT artist, track, play_date 
            FROM public.scrobbles 
            ORDER BY play_date DESC 
            LIMIT 5
        """)
        
        rows = cur.fetchall()
        
        print("\n💿 ПОСЛЕДНИЕ ТРЕКИ ИЗ БАЗЫ:")
        print("-" * 60)
        for artist, track, play_date in rows:
            print(f"📀 {play_date}: {artist} - {track}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")

def check_count():
    """Проверяет общее количество записей"""
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM public.scrobbles")
        count = cur.fetchone()[0]
        
        print(f"\n📊 Всего записей в базе: {count}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("ПРОВЕРКА СИНХРОНИЗАЦИИ")
    print("=" * 60)
    
    get_lastfm_recent()
    get_db_recent()
    check_count()