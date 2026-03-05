import psycopg2
from config import SUPABASE_CONFIG
from datetime import datetime

def verify_upload():
    """Проверяет, что данные загрузились правильно"""
    try:
        # Подключаемся к Supabase
        print("🔄 Подключаюсь к Supabase...")
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        cur = conn.cursor()
        
        print("\n" + "=" * 60)
        print("ПРОВЕРКА ДАННЫХ В SUPABASE")
        print("=" * 60)
        
        # 1. Проверяем общее количество записей в scrobbles
        cur.execute("SELECT COUNT(*) FROM public.scrobbles")
        scrobbles_count = cur.fetchone()[0]
        print(f"\n📊 scrobbles: {scrobbles_count} записей")
        
        # 2. Последние 5 прослушиваний
        cur.execute("""
            SELECT artist, track, play_date 
            FROM public.scrobbles 
            ORDER BY play_date DESC 
            LIMIT 5
        """)
        recent = cur.fetchall()
        
        print("\n📝 Последние 5 прослушиваний:")
        for artist, track, play_date in recent:
            # Форматируем дату для читаемости
            if isinstance(play_date, datetime):
                play_date = play_date.strftime('%Y-%m-%d %H:%M')
            print(f"   🎵 {artist} - {track} ({play_date})")
        
        # 3. Первые 5 записей (самые старые)
        cur.execute("""
            SELECT artist, track, play_date 
            FROM public.scrobbles 
            ORDER BY play_date ASC 
            LIMIT 5
        """)
        oldest = cur.fetchall()
        
        print("\n📀 Самые старые записи:")
        for artist, track, play_date in oldest:
            if isinstance(play_date, datetime):
                play_date = play_date.strftime('%Y-%m-%d %H:%M')
            print(f"   💿 {artist} - {track} ({play_date})")
        
        # 4. Проверяем scrobbles_unic
        cur.execute("SELECT COUNT(*) FROM public.scrobbles_unic")
        unic_count = cur.fetchone()[0]
        print(f"\n📊 scrobbles_unic: {unic_count} записей")
        
        # 5. Топ-5 треков по прослушиваниям
        cur.execute("""
            SELECT artist, track, play_count 
            FROM public.scrobbles_unic 
            ORDER BY play_count DESC 
            LIMIT 5
        """)
        top = cur.fetchall()
        
        print("\n🏆 Топ-5 треков по прослушиваниям:")
        for i, (artist, track, count) in enumerate(top, 1):
            print(f"   {i}. {artist} - {track}: {count} раз")
        
        # 6. Топ-5 исполнителей (агрегация по artist)
        cur.execute("""
            SELECT artist, SUM(play_count) as total_plays
            FROM public.scrobbles_unic 
            GROUP BY artist
            ORDER BY total_plays DESC 
            LIMIT 5
        """)
        top_artists = cur.fetchall()
        
        print("\n🎤 Топ-5 исполнителей по прослушиваниям:")
        for i, (artist, total) in enumerate(top_artists, 1):
            print(f"   {i}. {artist}: {total} прослушиваний")
        
        # 7. Статистика по датам
        cur.execute("""
            SELECT 
                MIN(play_date) as first_date,
                MAX(play_date) as last_date,
                COUNT(DISTINCT DATE(play_date)) as days_with_music
            FROM public.scrobbles
        """)
        first, last, days = cur.fetchone()
        
        print(f"\n📅 Статистика:")
        print(f"   Первое прослушивание: {first}")
        print(f"   Последнее прослушивание: {last}")
        print(f"   Дней с музыкой: {days}")
        
        # 8. Проверка целостности (сравнение с ожидаемыми значениями)
        print("\n" + "=" * 60)
        print("ПРОВЕРКА ЦЕЛОСТНОСТИ:")
        print("=" * 60)
        
        expected_scrobbles = 466465
        expected_unic = 287673
        
        scrobbles_diff = abs(scrobbles_count - expected_scrobbles)
        unic_diff = abs(unic_count - expected_unic)
        
        if scrobbles_count == expected_scrobbles:
            print(f"✅ scrobbles: {scrobbles_count} записей (совпадает с ожидаемым)")
        else:
            print(f"⚠️ scrobbles: {scrobbles_count} записей (отклонение от {expected_scrobbles}: {scrobbles_diff})")
        
        if unic_count == expected_unic:
            print(f"✅ scrobbles_unic: {unic_count} записей (совпадает с ожидаемым)")
        else:
            print(f"⚠️ scrobbles_unic: {unic_count} записей (отклонение от {expected_unic}: {unic_diff})")
        
        # 9. Проверка на наличие NULL значений в важных полях
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE artist IS NULL) as null_artist,
                COUNT(*) FILTER (WHERE track IS NULL) as null_track,
                COUNT(*) FILTER (WHERE play_date IS NULL) as null_date
            FROM public.scrobbles
        """)
        nulls = cur.fetchone()
        
        if sum(nulls) == 0:
            print("✅ Нет NULL значений в обязательных полях scrobbles")
        else:
            print(f"⚠️ Найдены NULL значения: artist={nulls[0]}, track={nulls[1]}, date={nulls[2]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")
        print("\nПроверьте:")
        print("1. Правильно ли указаны параметры подключения в .env?")
        print("2. Существуют ли таблицы в Supabase?")
        print("3. Есть ли в них данные?")

if __name__ == "__main__":
    verify_upload()