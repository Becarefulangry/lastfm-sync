import psycopg2
from config import SUPABASE_CONFIG

def check_tables():
    """Проверяем, существуют ли нужные таблицы в Supabase"""
    try:
        conn = psycopg2.connect(
            host=SUPABASE_CONFIG['host'],
            port=SUPABASE_CONFIG['port'],
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password']
        )
        
        cur = conn.cursor()
        
        # Проверяем наличие таблиц
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('scrobbles', 'scrobbles_unic');
        """)
        
        tables = cur.fetchall()
        existing_tables = [t[0] for t in tables]
        
        if 'scrobbles' in existing_tables:
            # Получаем количество записей
            cur.execute("SELECT COUNT(*) FROM public.scrobbles")
            count = cur.fetchone()[0]
            print(f"✅ Таблица scrobbles существует, записей: {count}")
        else:
            print("❌ Таблица scrobbles не найдена")
            
        if 'scrobbles_unic' in existing_tables:
            cur.execute("SELECT COUNT(*) FROM public.scrobbles_unic")
            count = cur.fetchone()[0]
            print(f"✅ Таблица scrobbles_unic существует, записей: {count}")
        else:
            print("❌ Таблица scrobbles_unic не найдена")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_tables()