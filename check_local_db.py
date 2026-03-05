import psycopg2

# Параметры твоей локальной БД (без пароля)
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'lastfm',
    'user': 'artemrostokin',
    'password': ''  # пустой пароль
}

def check_local_db():
    """Проверяем локальную базу данных"""
    try:
        print("🔄 Подключаюсь к локальной БД...")
        conn = psycopg2.connect(**LOCAL_DB_CONFIG)
        cur = conn.cursor()
        
        # Проверяем scrobbles
        cur.execute("SELECT COUNT(*) FROM public.scrobbles")
        scrobbles_count = cur.fetchone()[0]
        print(f"📊 Локальная таблица scrobbles: {scrobbles_count} записей")
        
        # Покажем несколько примеров
        if scrobbles_count > 0:
            cur.execute("""
                SELECT artist, track, play_date 
                FROM public.scrobbles 
                ORDER BY play_date DESC 
                LIMIT 3
            """)
            print("\n📝 Последние 3 записи в scrobbles:")
            for row in cur.fetchall():
                print(f"   {row[0]} - {row[1]} ({row[2]})")
        
        # Проверяем scrobbles_unic
        cur.execute("SELECT COUNT(*) FROM public.scrobbles_unic")
        unic_count = cur.fetchone()[0]
        print(f"\n📊 Локальная таблица scrobbles_unic: {unic_count} записей")
        
        if unic_count > 0:
            cur.execute("""
                SELECT artist, track, play_count 
                FROM public.scrobbles_unic 
                ORDER BY play_count DESC 
                LIMIT 3
            """)
            print("📝 Топ-3 трека по прослушиваниям:")
            for row in cur.fetchall():
                print(f"   {row[0]} - {row[1]} ({row[2]} прослушиваний)")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка подключения к локальной БД: {e}")
        print("Убедись, что локальный PostgreSQL запущен")

if __name__ == "__main__":
    check_local_db()