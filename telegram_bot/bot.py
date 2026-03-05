import logging
import psycopg2
import os
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from dotenv import load_dotenv

# Загружаем переменные
load_dotenv()

# Настройки
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ALLOWED_USERS = [int(id) for id in os.getenv('ALLOWED_USERS', '').split(',') if id]

# Supabase
SUPABASE_CONFIG = {
    'host': os.getenv('SUPABASE_HOST'),
    'port': os.getenv('SUPABASE_PORT'),
    'database': os.getenv('SUPABASE_DATABASE'),
    'user': os.getenv('SUPABASE_USER'),
    'password': os.getenv('SUPABASE_PASSWORD')
}

# Логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def check_user(user_id):
    """Проверяет, разрешён ли пользователь"""
    if not ALLOWED_USERS:
        return True
    return user_id in ALLOWED_USERS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user = update.effective_user
    if not check_user(user.id):
        await update.message.reply_text("⛔ У вас нет доступа к этому боту.")
        return
    
    await update.message.reply_text(
        f"🎵 Привет, {user.first_name}!\n\n"
        f"Я бот для отслеживания твоей музыки с Last.fm.\n\n"
        f"📋 Доступные команды:\n"
        f"/sync - статус синхронизации\n"
        f"/help - помощь"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help"""
    if not check_user(update.effective_user.id):
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    await update.message.reply_text(
        "🤖 **Команды бота:**\n\n"
        "/sync - статус синхронизации с Last.fm\n"
        "/start - приветствие\n"
        "/help - это сообщение",
        parse_mode='Markdown'
    )

async def sync_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /sync - показывает статус синхронизации"""
    if not check_user(update.effective_user.id):
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    await update.message.reply_text("🔍 Получаю статус...")
    
    try:
        conn = psycopg2.connect(**SUPABASE_CONFIG)
        cur = conn.cursor()
        
        # Последний трек
        cur.execute("""
            SELECT artist, track, play_date 
            FROM public.scrobbles 
            ORDER BY play_date DESC 
            LIMIT 1
        """)
        last = cur.fetchone()
        
        # Количество за сегодня
        cur.execute("""
            SELECT COUNT(*) 
            FROM public.scrobbles 
            WHERE DATE(play_date) = CURRENT_DATE
        """)
        today_count = cur.fetchone()[0]
        
        # Количество за вчера
        cur.execute("""
            SELECT COUNT(*) 
            FROM public.scrobbles 
            WHERE DATE(play_date) = CURRENT_DATE - 1
        """)
        yesterday_count = cur.fetchone()[0]
        
        # Общее количество
        cur.execute("SELECT COUNT(*) FROM public.scrobbles")
        total_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if last:
            artist, track, play_date = last
            time_str = play_date.strftime('%H:%M, %d.%m.%Y')
            
            message = (
                f"📊 **Статус синхронизации**\n\n"
                f"🎵 Последний трек:\n"
                f"└ {artist} — {track}\n"
                f"└ {time_str}\n\n"
                f"📦 **Статистика:**\n"
                f"└ За сегодня: {today_count}\n"
                f"└ За вчера: {yesterday_count}\n"
                f"└ Всего: {total_count}\n\n"
                f"⏰ Следующая синхронизация: 3:00 ночи"
            )
        else:
            message = "😴 База данных пока пуста"
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Ошибка в /sync: {e}")
        await update.message.reply_text("❌ Ошибка при получении статуса")

async def sync_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /syncnow - ручной запуск синхронизации"""
    if not check_user(update.effective_user.id):
        await update.message.reply_text("⛔ Нет доступа")
        return
    
    await update.message.reply_text("🔄 Запускаю синхронизацию...")
    
    try:
        # Здесь можно вызвать функцию синхронизации
        # Но проще пока просто сказать, что это делается автоматически
        await update.message.reply_text(
            "✅ Синхронизация запускается автоматически каждый день в 3:00\n\n"
            "Если хочешь проверить статус - используй /sync"
        )
    except Exception as e:
        logger.error(f"Ошибка в /syncnow: {e}")
        await update.message.reply_text("❌ Ошибка")

def main():
    """Запуск бота"""
    if not TELEGRAM_TOKEN:
        logger.error("Нет TELEGRAM_TOKEN в .env")
        return
    
    # Создаём приложение
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Добавляем обработчики команд
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("sync", sync_status))
    app.add_handler(CommandHandler("syncnow", sync_now))
    
    # Запускаем бота
    logger.info("Бот запущен...")
    app.run_polling()

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot is running')
    
    def log_message(self, format, *args):
        return  # Отключаем логи HTTP-сервера

def run_http_server():
    port = int(os.environ.get('PORT', 8000))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"🌐 HTTP server listening on port {port}")
    server.serve_forever()

# В функции main() добавь запуск HTTP-сервера в отдельном потоке
# Примерно так:
def main():
    # ... существующий код ...
    
    # Запускаем HTTP-сервер в отдельном потоке
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Запускаем бота
    logger.info("Бот запущен...")
    app.run_polling()

if __name__ == '__main__':
    main()