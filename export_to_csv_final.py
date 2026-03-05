import psycopg2
import csv
import os
from datetime import datetime

# Параметры локальной БД
LOCAL_DB = {
    'host': 'localhost',
    'port': 5432,
    'database': 'lastfm',
    'user': 'artemrostokin',
    'password': ''
}

def export_table_to_csv(table_name, output_file):
    """Экспортирует таблицу в CSV файл"""
    try:
        print(f"🔄 Экспортирую {table_name}...")
        
        # Подключаемся к локальной БД
        conn = psycopg2.connect(**LOCAL_DB)
        cur = conn.cursor()
        
        # Получаем все данные
        cur.execute(f"SELECT * FROM public.{table_name} ORDER BY id")
        rows = cur.fetchall()
        
        # Получаем названия колонок
        colnames = [desc[0] for desc in cur.description]
        
        # Записываем в CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)  # заголовки
            writer.writerows(rows)     # данные
        
        print(f"✅ {table_name}: {len(rows)} записей сохранено в {output_file}")
        print(f"   Размер файла: {os.path.getsize(output_file) / 1024 / 1024:.2f} MB")
        
        cur.close()
        conn.close()
        return len(rows)
        
    except Exception as e:
        print(f"❌ Ошибка при экспорте {table_name}: {e}")
        return 0

def main():
    print("=" * 60)
    print("ЭКСПОРТ ДАННЫХ ИЗ ЛОКАЛЬНОЙ БД В CSV")
    print("=" * 60)
    
    # Экспортируем обе таблицы
    count1 = export_table_to_csv('scrobbles', 'scrobbles.csv')
    count2 = export_table_to_csv('scrobbles_unic', 'scrobbles_unic.csv')
    
    print("\n" + "=" * 60)
    print("ИТОГИ ЭКСПОРТА:")
    print(f"  scrobbles.csv: {count1} записей")
    print(f"  scrobbles_unic.csv: {count2} записей")
    print("=" * 60)
    
    # Проверка соответствия
    if count1 == 466465:
        print("✅ scrobbles: OK")
    else:
        print(f"⚠️ scrobbles: ожидалось 466465, получено {count1}")
    
    if count2 == 287673:
        print("✅ scrobbles_unic: OK")
    else:
        print(f"⚠️ scrobbles_unic: ожидалось 287673, получено {count2}")

if __name__ == "__main__":
    main()