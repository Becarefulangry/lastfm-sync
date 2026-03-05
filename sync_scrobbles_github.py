import requests
import psycopg2
import os
from datetime import datetime, timedelta

# ===== НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ =====
SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_PORT = os.getenv('SUPABASE_PORT')
SUPABASE_DATABASE = os.getenv('SUPABASE_DATABASE')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')

LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_USER = os.getenv('LASTFM_USER')
# ==============================================

def get_last_date():
    """Получает самую свежую дату из scrobbles"""
    try:
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            port=SUPABASE_PORT,
            database=SUPABASE_DATABASE,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD
        )
        cur = conn.cursor()
        
        cur.execute("SELECT MAX(play_date) FROM public.scrobbles")
        last_date = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if last_date:
            print(f"📅 Последняя запись: {last_date}")
            return last_date
        else:
            print("📅 База пуста")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

def fetch_tracks(from_timestamp=None, page=1):
    """Получает треки из Last.fm"""
    
    params = {
        'method': 'user.getRecentTracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 200,
        'page': page
    }
    
    if from_timestamp:
        if isinstance(from_timestamp, datetime):
            params['from'] = int(from_timestamp.timestamp())
    
    try:
        url = "http://ws.audioscrobbler.com/2.0/"
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            tracks = data.get('recenttracks', {}).get('track', [])
            total_pages = int(data.get('recenttracks', {}).get('@attr', {}).get('totalPages', 1))
            
            print(f"📥 Получено {len(tracks)} треков, стр {page}/{total_pages}")
            return tracks, total_pages
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            return [], 0
            
    except Exception as e:
        print(f"❌ Ошибка запроса: {e}")
        return [], 0

def parse_track(track):
    """Парсит трек"""
    
    if '@attr' in track and track['@attr'].get('nowplaying') == 'true':
        return None
    
    try:
        date_str = track.get('date', {}).get('#text', '')
        if not date_str:
            return None
            
        play_date = datetime.strptime(date_str, "%d %b %Y, %H:%M")
        
        return {
            'artist': track.get('artist', {}).get('#text', ''),
            'album': track.get('album', {}).get('#text', ''),
            'track': track.get('name', ''),
            'play_date': play_date
        }
        
    except Exception as e:
        return None

def insert_tracks(tracks):
    """Вставляет только новые треки в scrobbles"""
    
    if not tracks:
        return 0
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            port=SUPABASE_PORT,
            database=SUPABASE_DATABASE,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD
        )
        cur = conn.cursor()
        
        inserted = 0
        for track in tracks:
            try:
                cur.execute("""
                    INSERT INTO public.scrobbles (artist, album, track, play_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (artist, track, play_date) DO NOTHING;
                """, (track['artist'], track['album'], track['track'], track['play_date']))
                
                if cur.rowcount > 0:
                    inserted += 1
                
            except Exception as e:
                print(f"⚠️ Ошибка: {track['artist']} - {track['track']}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Добавлено {inserted} новых записей")
        return inserted
        
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")
        if conn:
            conn.close()
        return 0

def main():
    print("=" * 60)
    print("СИНХРОНИЗАЦИЯ LAST.FM (GITHUB ACTIONS)")
    print("=" * 60)
    
    if not all([LASTFM_API_KEY, LASTFM_USER, SUPABASE_HOST, SUPABASE_PASSWORD]):
        print("❌ Не все переменные окружения заданы")
        return
    
    last_date = get_last_date()
    
    if last_date:
        from_time = last_date + timedelta(seconds=1)
        print(f"🔍 Ищем после: {from_time}")
    else:
        from_time = None
        print("🔍 Загружаем всё")
    
    # Собираем новые треки
    page = 1
    total_pages = 1
    all_new = []
    
    while page <= total_pages:
        print(f"\n📄 Страница {page}...")
        
        tracks, total_pages = fetch_tracks(from_time, page)
        
        new = []
        for t in tracks:
            parsed = parse_track(t)
            if parsed:
                new.append(parsed)
        
        if new:
            all_new.extend(new)
            print(f"   +{len(new)} новых")
        else:
            print("   Новых нет")
            break
        
        page += 1
    
    # Вставляем
    if all_new:
        print(f"\n📦 Всего новых: {len(all_new)}")
        all_new.sort(key=lambda x: x['play_date'])
        inserted = insert_tracks(all_new)
        print(f"\n✅ Готово! Добавлено {inserted}")
    else:
        print("\n✅ Новых записей нет")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()