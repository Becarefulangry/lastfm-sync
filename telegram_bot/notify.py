import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('ALLOWED_USERS')

def send_sync_notification(success=True, added_count=0, error_msg=None):
    """Отправляет уведомление о результате синхронизации"""
    
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("❌ Нет TELEGRAM_TOKEN или CHAT_ID в .env")
        return
    
    if success:
        message = (
            f"✅ **Синхронизация с Last.fm завершена**\n"
            f"🕒 Время: {datetime.now().strftime('%H:%M, %d.%m.%Y')}\n"
            f"📊 Добавлено записей: {added_count}\n"
            f"📦 Таблица: scrobbles"
        )
    else:
        message = (
            f"❌ **Ошибка синхронизации с Last.fm**\n"
            f"🕒 Время: {datetime.now().strftime('%H:%M, %d.%m.%Y')}\n"
            f"⚠️ Ошибка: {error_msg}"
        )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("✅ Уведомление отправлено")
        else:
            print(f"❌ Ошибка отправки: {response.text}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def send_test_notification():
    """Отправляет тестовое уведомление"""
    send_sync_notification(success=True, added_count=42)

if __name__ == "__main__":
    send_test_notification()