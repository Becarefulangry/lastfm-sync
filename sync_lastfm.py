import requests
import psycopg2
import time
from datetime import datetime, timedelta
from config import SUPABASE_CONFIG, LASTFM_API_KEY, LASTFM_USER

# ===== НАСТРОЙКИ =====
BATCH_SIZE = 200  # Last.fm API отдает по 200 записей за раз
# =====================

def get_last_date_from_db():
    """Получает самую свежую дату из таблицы scrobbles"""
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        cur = conn.cursor()
        
        cur.execute("SELECT MAX(play_date) FROM public.scrobbles")
        last_date = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if last_date:
            print(f"📅 Последняя запись в БД: {last_date}")
            return last_date
        else:
            print("📅 База данных пуста, будем загружать всё")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка получения последней даты: {e}")
        return None

def fetch_from_lastfm(from_timestamp=None, page=1):
    """Получает прослушивания из Last.fm API"""
    
    params = {
        'method': 'user.getRecentTracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': BATCH_SIZE,
        'page': page,
        'extended': 1  # получаем расширенную информацию (альбомы)
    }
    
    if from_timestamp:
        # Преобразуем datetime в timestamp для API
        if isinstance(from_timestamp, datetime):
            params['from'] = int(from_timestamp.timestamp())
    
    try:
        url = "http://ws.audioscrobbler.com/2.0/"
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            tracks = data.get('recenttracks', {}).get('track', [])
            total_pages = int(data.get('recenttracks', {}).get('@attr', {}).get('totalPages', 1))
            
            print(f"📥 Получено {len(tracks)} треков со страницы {page}/{total_pages}")
            return tracks, total_pages
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            return [], 0
            
    except Exception as e:
        print(f"❌ Ошибка запроса к Last.fm: {e}")
        return [], 0

def parse_track(track):
    """Парсит трек из API в формат для БД"""
    
    # Проверяем, играет ли трек сейчас (у него нет даты)
    if '@attr' in track and track['@attr'].get('nowplaying') == 'true':
        return None  # Пропускаем "сейчас играет"
    
    try:
        # Парсим дату
        date_str = track.get('date', {}).get('#text', '')
        if not date_str:
            return None
            
        # Конвертируем дату из формата "dd MMM yyyy, HH:mm" в datetime
        # Пример: "27 Feb 2026, 17:32"
        play_date = datetime.strptime(date_str, "%d %b %Y, %H:%M")
        
        # Извлекаем данные
        artist = track.get('artist', {}).get('#text', '') or track.get('artist', {}).get('name', '')
        album = track.get('album', {}).get('#text', '')
        track_name = track.get('name', '')
        
        return {
            'artist': artist,
            'album': album,
            'track': track_name,
            'play_date': play_date
        }
        
    except Exception as e:
        print(f"⚠️ Ошибка парсинга трека: {e}")
        return None

def insert_into_db(tracks):
    """Вставляет новые треки в базу данных"""
    
    if not tracks:
        return 0
    
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        cur = conn.cursor()
        
        inserted = 0
        for track in tracks:
            try:
                cur.execute("""
                    INSERT INTO public.scrobbles (artist, album, track, play_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (artist, track, play_date) DO NOTHING
                    RETURNING id;
                """, (track['artist'], track['album'], track['track'], track['play_date']))
                
                if cur.fetchone():
                    inserted += 1
                    
            except Exception as e:
                print(f"⚠️ Ошибка вставки трека {track['artist']} - {track['track']}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Добавлено {inserted} новых записей")
        return inserted
        
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return 0

def update_scrobbles_unic():
    """Обновляет таблицу уникальных треков"""
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        cur = conn.cursor()
        
        # Обновляем уникальные треки
        cur.execute("""
            INSERT INTO public.scrobbles_unic (artist, track, first_seen, last_seen, play_count)
            SELECT 
                artist, 
                track, 
                MIN(play_date) as first_seen,
                MAX(play_date) as last_seen,
                COUNT(*) as play_count
            FROM public.scrobbles
            WHERE (artist, track) NOT IN (SELECT artist, track FROM public.scrobbles_unic)
            GROUP BY artist, track
            ON CONFLICT (artist, track) 
            DO UPDATE SET 
                last_seen = EXCLUDED.last_seen,
                play_count = public.scrobbles_unic.play_count + EXCLUDED.play_count;
        """)
        
        conn.commit()
        print("✅ Таблица уникальных треков обновлена")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка обновления уникальных треков: {e}")

def main():
    print("=" * 60)
    print("СИНХРОНИЗАЦИЯ С LAST.FM API")
    print("=" * 60)
    
    # Проверяем наличие ключей
    if not LASTFM_API_KEY or not LASTFM_USER:
        print("❌ Ошибка: Не заполнены Last.fm данные в .env")
        print("   LASTFM_API_KEY и LASTFM_USER должны быть указаны")
        return
    
    # Получаем последнюю дату из БД
    last_date = get_last_date_from_db()
    
    # Определяем стартовую дату
    if last_date:
        # Добавляем 1 секунду, чтобы не дублировать последнюю запись
        from_timestamp = last_date + timedelta(seconds=1)
        print(f"🔍 Ищем записи после: {from_timestamp}")
    else:
        from_timestamp = None
        print("🔍 База пуста, загружаем все доступные записи")
    
    # Загружаем данные пачками
    page = 1
    total_pages = 1
    all_new_tracks = []
    
    while page <= total_pages:
        print(f"\n📄 Страница {page}...")
        
        tracks, total_pages = fetch_from_lastfm(from_timestamp, page)
        
        new_tracks = []
        for track in tracks:
            parsed = parse_track(track)
            if parsed:
                new_tracks.append(parsed)
        
        if new_tracks:
            all_new_tracks.extend(new_tracks)
            print(f"   Найдено {len(new_tracks)} новых треков на странице")
        else:
            print("   Новых треков на странице нет")
            break
        
        page += 1
        time.sleep(1)  # Вежливая пауза между запросами
    
    # Вставляем новые треки
    if all_new_tracks:
        print(f"\n📦 Всего найдено новых треков: {len(all_new_tracks)}")
        
        # Сортируем по дате (старые сначала, чтобы избежать конфликтов)
        all_new_tracks.sort(key=lambda x: x['play_date'])
        
        inserted = insert_into_db(all_new_tracks)
        
        # Обновляем таблицу уникальных треков
        if inserted > 0:
            update_scrobbles_unic()
            
        print(f"\n✅ Синхронизация завершена. Добавлено {inserted} записей.")
    else:
        print("\n✅ Новых записей не найдено. База данных актуальна.")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()