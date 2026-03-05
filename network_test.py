import socket
import psycopg2
from config import SUPABASE_CONFIG

def test_network():
    """Тестируем доступность хоста"""
    host = SUPABASE_CONFIG['host']
    port = int(SUPABASE_CONFIG['port'])
    
    print(f"🔄 Проверяю доступность {host}:{port}...")
    
    # Проверка DNS
    try:
        ip = socket.gethostbyname(host)
        print(f"✅ DNS резолвинг работает: {host} -> {ip}")
    except Exception as e:
        print(f"❌ DNS ошибка: {e}")
        return
    
    # Проверка соединения (таймаут 5 секунд)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        if result == 0:
            print(f"✅ Порт {port} открыт")
        else:
            print(f"❌ Порт {port} закрыт или фильтруется (код: {result})")
        sock.close()
    except Exception as e:
        print(f"❌ Ошибка при проверке порта: {e}")
    
    # Попытка psycopg2 подключения с таймаутом
    try:
        print("\n🔄 Пробую подключиться через psycopg2...")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=SUPABASE_CONFIG['database'],
            user=SUPABASE_CONFIG['user'],
            password=SUPABASE_CONFIG['password'],
            connect_timeout=10
        )
        print("✅ Подключение через psycopg2 успешно!")
        conn.close()
    except Exception as e:
        print(f"❌ psycopg2 ошибка: {type(e).__name__}: {e}")

if __name__ == "__main__":
    test_network()