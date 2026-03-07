import os
import logging
import sqlite3
from datetime import datetime, timedelta
import datetime as dt
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, ConversationHandler, MessageHandler, filters
import threading
import time
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

# Состояния для ConversationHandler
(NAME, PHONE, TG_ID, PARENT_NAME, PARENT_PHONE, PARENT_TG, LESSONS, DAYS, 
 EXTEND_DAYS, GROUP_NAME, REQUEST_NAME, REQUEST_PHONE) = range(12)

# Дополнительные состояния
SELECT_STUDENT_FOR_MEMBERSHIP = 100
SELECT_STUDENT_FOR_EXTEND = 101
DELETE_ATTENDANCE_DATE = 102

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Парсинг ID администраторов
admin_raw = os.getenv("ADMIN_CHAT_ID", "")
admin_clean = ''.join(c for c in admin_raw if c.isdigit() or c == ',')
ADMIN_IDS = [int(x) for x in admin_clean.split(',') if x.strip()]
BOT_TOKEN = os.getenv("BOT_TOKEN")

logger.info(f"👑 Загружены админы: {ADMIN_IDS}")

# ===== АНТИ-ЛАГ (для хостинга) =====
class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')
    def log_message(self, format, *args): pass

def run_http_server():
    try:
        server = HTTPServer(('0.0.0.0', 8080), PingHandler)
        logger.info("🌐 HTTP сервер запущен")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server error: {e}")

def ping_self():
    time.sleep(60)
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', 8080))
            sock.send(b'GET /ping HTTP/1.0\r\n\r\n')
            sock.close()
        except:
            pass
        time.sleep(180)

# Запуск анти-лаг системы в отдельных потоках
threading.Thread(target=run_http_server, daemon=True).start()
threading.Thread(target=ping_self, daemon=True).start()

# ===== БАЗА ДАННЫХ =====
db_path = "/data/school.db" if os.path.exists("/data") else "school.db"
conn = sqlite3.connect(db_path, check_same_thread=False)
cursor = conn.cursor()
logger.info(f"📦 База данных: {db_path}")

# Создание таблиц
cursor.execute('''CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE,
    name TEXT,
    phone TEXT
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS student_group (
    student_id INTEGER,
    group_id INTEGER,
    FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE,
    FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE,
    PRIMARY KEY(student_id, group_id)
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER,
    lessons_left INTEGER DEFAULT 0,
    valid_until TEXT,
    status TEXT DEFAULT 'active',
    frozen_days INTEGER DEFAULT 0,
    FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER,
    date TEXT,
    present INTEGER DEFAULT 1,
    FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS parents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE,
    name TEXT,
    phone TEXT
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS parent_child (
    parent_id INTEGER,
    student_id INTEGER,
    FOREIGN KEY(parent_id) REFERENCES parents(id) ON DELETE CASCADE,
    FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE,
    PRIMARY KEY(parent_id, student_id)
)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS last_mark (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER,
    student_id INTEGER,
    date TEXT,
    mark_type INTEGER
)''')

# ===== ТАБЛИЦА ДЛЯ ЗАЯВОК =====
cursor.execute('''CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    username TEXT,
    name TEXT,
    phone TEXT,
    role TEXT,
    status TEXT DEFAULT 'pending',
    created_at TEXT
)''')
conn.commit()

# Добавление колонки notifications, если её нет
try:
    cursor.execute("ALTER TABLE students ADD COLUMN notifications INTEGER DEFAULT 1")
    logger.info("✅ Добавлена колонка notifications в students")
except:
    pass

try:
    cursor.execute("ALTER TABLE parents ADD COLUMN notifications INTEGER DEFAULT 1")
    logger.info("✅ Добавлена колонка notifications в parents")
except:
    pass

# Колонка frozen_days уже создана в таблице memberships, но на всякий случай проверим
try:
    cursor.execute("ALTER TABLE memberships ADD COLUMN frozen_days INTEGER DEFAULT 0")
    logger.info("✅ Добавлена колонка frozen_days в memberships")
except:
    pass
conn.commit()

# ===== УВЕДОМЛЕНИЯ =====
async def notify_student_and_parents(student_id, new_balance, context):
    """Отправляет уведомление ученику и его родителям"""
    student = cursor.execute("SELECT telegram_id, name, notifications FROM students WHERE id = ?", (student_id,)).fetchone()
    if not student: 
        return
    
    student_name = student[1]
    
    if new_balance == 1:
        message = f"⚠️ У тебя осталось **последнее занятие**! Не забудь продлить абонемент."
    elif new_balance == 0:
        message = f"❌ Твои занятия закончились!\n\nПросьба оплатить абонемент перед следующим занятием."
    elif new_balance < 0:
        message = f"⛔ У тебя задолженность: **{abs(new_balance)} занятий**.\n\nПросьба оплатить абонемент."
    else:
        return
    
    # Отправка ученику
    if student[2] == 1:
        try:
            await context.bot.send_message(student[0], message, parse_mode="Markdown")
            logger.info(f"📨 Уведомление отправлено ученику {student_name}")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки ученику {student_name}: {e}")
    
    # Отправка родителям
    parents = cursor.execute("""
        SELECT p.telegram_id, p.notifications FROM parents p
        JOIN parent_child pc ON p.id = pc.parent_id
        WHERE pc.student_id = ?
    """, (student_id,)).fetchall()
    
    for parent in parents:
        if parent[1] == 1:
            try:
                await context.bot.send_message(
                    parent[0], 
                    f"👪 **{student_name}**: {message}", 
                    parse_mode="Markdown"
                )
                logger.info(f"📨 Уведомление отправлено родителю")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки родителю: {e}")

async def notify_admin(student_id, new_balance, context):
    """Отправляет уведомление администратору и при необходимости ученику"""
    student = cursor.execute("SELECT name FROM students WHERE id = ?", (student_id,)).fetchone()
    if not student: 
        return
    student_name = student[0]
    
    # Уведомление админам
    for admin_id in ADMIN_IDS:
        try:
            if new_balance < 0:
                await context.bot.send_message(admin_id, f"⛔ {student_name}: долг {abs(new_balance)} занятий")
            elif new_balance == 0:
                await context.bot.send_message(admin_id, f"❌ {student_name}: занятия закончились!")
            else:
                await context.bot.send_message(admin_id, f"📊 {student_name}: осталось {new_balance} занятий")
        except:
            pass
    
    # Ученику только при 0 или минусе
    if new_balance <= 0:
        await notify_student_and_parents(student_id, new_balance, context)

# ===== УВЕДОМЛЕНИЕ ОБ ИСТЕЧЕНИИ АБОНЕМЕНТА =====
async def check_expiring_memberships(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет абонементы, которые истекают через 5 дней, и отправляет уведомления"""
    today = datetime.now().date()
    warning_date = (today + timedelta(days=5)).strftime("%Y-%m-%d")
    
    expiring = cursor.execute("""
        SELECT m.id, m.student_id, s.name, s.telegram_id, s.notifications, m.valid_until 
        FROM memberships m
        JOIN students s ON m.student_id = s.id
        WHERE m.status = 'active' AND m.valid_until = ?
    """, (warning_date,)).fetchall()
    
    for mem in expiring:
        mem_id, student_id, student_name, tg_id, notif, valid_until = mem
        
        student_msg = (
            f"⚠️ Твой абонемент закончится через 5 дней (до {valid_until}).\n"
            f"Обратись к администратору для продления."
        )
        
        if notif == 1 and tg_id:
            try:
                await context.bot.send_message(tg_id, student_msg)
                logger.info(f"📨 Уведомление об истечении отправлено ученику {student_name}")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки ученику {student_name}: {e}")
        
        admin_msg = f"⚠️ Ученик {student_name}: абонемент истекает через 5 дней (до {valid_until})"
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(admin_id, admin_msg)
            except:
                pass

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ENTRY POINTS =====
async def add_student_entry(update, context): return NAME
async def add_parent_entry(update, context): return PARENT_NAME
async def add_group_entry(update, context): return GROUP_NAME
async def membership_lessons_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("🎯 membership_lessons_entry ВЫЗВАНА!")
    return LESSONS

# ===== СТАРТ (БЕЗ РОДИТЕЛЕЙ) =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню бота"""
    uid = update.effective_user.id
    logger.info(f"🚀 /start от пользователя {uid}")
    
    # Для администраторов
    if uid in ADMIN_IDS:
        kb = [
            [InlineKeyboardButton("👥 Ученики", callback_data="admin_students")],
            [InlineKeyboardButton("📚 Группы", callback_data="admin_groups")],
            [InlineKeyboardButton("➕ Ученик", callback_data="add_student")],
            [InlineKeyboardButton("🎟 Абонемент", callback_data="add_membership")],
            [InlineKeyboardButton("➕ Группа", callback_data="add_group")],
            [InlineKeyboardButton("📚 В группу", callback_data="add_to_group")],
            [InlineKeyboardButton("📋 Отметка", callback_data="mark_group")],
            [InlineKeyboardButton("⏱ Продлить", callback_data="extend_menu")],
            [InlineKeyboardButton("🗑 Удаление", callback_data="delete_menu")],
            [InlineKeyboardButton("❄️ Заморозка", callback_data="freeze_menu")],
            [InlineKeyboardButton("📋 Заявки", callback_data="admin_requests")],
        ]
        await update.message.reply_text("🔐 Админ-панель", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END
    
    # Для учеников
    student = cursor.execute("SELECT id, name, notifications FROM students WHERE telegram_id = ?", (uid,)).fetchone()
    if student:
        kb = [
            [InlineKeyboardButton("📊 Баланс", callback_data=f"balance_{student[0]}")],
            [InlineKeyboardButton("📅 Посещения", callback_data=f"attendance_{student[0]}")],
        ]
        notif_text = "🔔 Уведомления вкл" if student[2] == 1 else "🔕 Уведомления выкл"
        kb.append([InlineKeyboardButton(notif_text, callback_data="toggle_student_notifications")])
        await update.message.reply_text(f"👋 {student[1]}", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END
    
    # Для новых пользователей - сразу запрашиваем ФИО
    context.user_data['request_role'] = 'student'
    await update.message.reply_text(
        "👋 Добро пожаловать!\n\n"
        "Для регистрации введите ваши имя и фамилию:"
    )
    return REQUEST_NAME

# ===== ПОКАЗ ЗАЯВОК =====
async def show_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать все необработанные заявки (только для админов)"""
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Нет доступа")
        return
    
    # Определяем, откуда вызвано (кнопка или команда)
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        message = q.message
        edit = True
    else:
        message = update.message
        edit = False
    
    requests = cursor.execute("""
        SELECT id, username, name, phone, role, created_at 
        FROM requests 
        WHERE status = 'pending' 
        ORDER BY created_at DESC
        LIMIT 10
    """).fetchall()
    
    if not requests:
        text = "📭 Нет ожидающих заявок"
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="start")]]
        if edit:
            await message.edit_text(text, reply_markup=InlineKeyboardMarkup(kb))
        else:
            await message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))
        return
    
    for req in requests:
        text = f"📩 Заявка #{req[0]}\n"
        text += f"👤 {req[2]} (@{req[1]})\n"
        text += f"📞 {req[3]}\n"
        text += f"🎭 Роль: {req[4]}\n"
        text += f"⏱ {req[5]}"
        
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Принять", callback_data=f"approve_req_{req[0]}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_req_{req[0]}")
        ]])
        
        await message.reply_text(text, reply_markup=kb)
    
    # Кнопка "Назад" в конце
    if edit:
        await message.reply_text("⬆️ Заявки выше", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="start")]]))

# ===== КНОПКИ =====
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик всех callback кнопок"""
    # Если это не callback query - выходим
    if not update.callback_query:
        logger.warning("⚠️ Получено сообщение не callback в button_handler")
        return
    
    q = update.callback_query
    await q.answer()
    d = q.data
    uid = update.effective_user.id

    logger.info(f"📩 Получен callback: {d} от пользователя {uid}")

    # --- Для не-админов (ученики) ---
    if uid not in ADMIN_IDS:
        # Просмотр баланса
        if d.startswith("balance_"):
            sid = int(d.split("_")[1])
            mem = cursor.execute("""
                SELECT lessons_left, valid_until FROM memberships 
                WHERE student_id = ? AND status = 'active' AND valid_until > date('now')
            """, (sid,)).fetchone()
            
            if mem:
                text = f"📊 Осталось: {mem[0]}\n📅 Действует до: {mem[1]}"
            else:
                text = "📊 Осталось: 0\n📅 Нет активного абонемента"
            
            kb = [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_student_{sid}")]]
            await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
            return

        # Просмотр посещений (выбор месяца)
        elif d.startswith("attendance_"):
            sid = int(d.split("_")[1])
            student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
            
            months = cursor.execute("""
                SELECT DISTINCT strftime('%Y-%m', date) as month 
                FROM attendance 
                WHERE student_id = ? 
                ORDER BY month DESC
            """, (sid,)).fetchall()
            
            if months:
                kb = []
                for month in months:
                    year, month_num = month[0].split('-')
                    month_names = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                                  "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
                    month_name = month_names[int(month_num)-1]
                    btn_text = f"📅 {month_name} {year}"
                    kb.append([InlineKeyboardButton(btn_text, callback_data=f"attendance_month_{sid}_{month[0]}")])
                
                kb.append([InlineKeyboardButton("📋 Все посещения", callback_data=f"attendance_all_{sid}")])
                kb.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_student_{sid}")])
                
                await q.edit_message_text(f"👤 {student[0]}\nВыберите месяц:", reply_markup=InlineKeyboardMarkup(kb))
            else:
                await q.edit_message_text("📭 Нет посещений", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_student_{sid}")]]))
            return

        # Просмотр посещений за конкретный месяц
        elif d.startswith("attendance_month_"):
            parts = d.split("_")
            sid = int(parts[2])
            month = parts[3]
            
            year, month_num = month.split('-')
            month_names = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                          "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
            month_name = month_names[int(month_num)-1]
            
            rows = cursor.execute("""
                SELECT date FROM attendance 
                WHERE student_id = ? AND strftime('%Y-%m', date) = ?
                ORDER BY date DESC
            """, (sid, month)).fetchall()
            
            if rows:
                text = f"📅 {month_name} {year}\n\n"
                for r in rows:
                    date_obj = datetime.strptime(r[0], "%Y-%m-%d")
                    date_display = date_obj.strftime("%d.%m.%Y")
                    text += f"• {date_display}\n"
                
                kb = [[InlineKeyboardButton("🔙 Назад", callback_data=f"attendance_{sid}")]]
                await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
            else:
                await q.edit_message_text(f"📭 В {month_name} {year} посещений нет", 
                                         reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"attendance_{sid}")]]))
            return

        # Просмотр всех посещений
        elif d.startswith("attendance_all_"):
            sid = int(d.split("_")[2])
            student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
            
            rows = cursor.execute("""
                SELECT date FROM attendance 
                WHERE student_id = ? 
                ORDER BY date DESC
                LIMIT 50
            """, (sid,)).fetchall()
            
            if rows:
                text = f"📋 Все посещения {student[0]}\n\n"
                current_month = ""
                for r in rows:
                    date_obj = datetime.strptime(r[0], "%Y-%m-%d")
                    month_key = date_obj.strftime("%Y-%m")
                    date_display = date_obj.strftime("%d.%m.%Y")
                    
                    if month_key != current_month:
                        current_month = month_key
                        year, month_num = month_key.split('-')
                        month_name = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                                     "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"][int(month_num)-1]
                        text += f"\n📅 {month_name} {year}\n"
                    
                    text += f"  • {date_display}\n"
                
                kb = [[InlineKeyboardButton("🔙 Назад", callback_data=f"attendance_{sid}")]]
                await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
            else:
                await q.edit_message_text("📭 Нет посещений", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_student_{sid}")]]))
            return

        # Возврат к ученику
        elif d.startswith("back_to_student_"):
            sid = int(d.split("_")[3])
            student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
            kb = [
                [InlineKeyboardButton("📊 Баланс", callback_data=f"balance_{sid}")],
                [InlineKeyboardButton("📅 Посещения", callback_data=f"attendance_{sid}")],
            ]
            await q.edit_message_text(f"👋 {student[0]}", reply_markup=InlineKeyboardMarkup(kb))
            return

        # Переключение уведомлений для ученика
        elif d == "toggle_student_notifications":
            current = cursor.execute("SELECT notifications FROM students WHERE telegram_id = ?", (uid,)).fetchone()
            if current:
                new_val = 0 if current[0] == 1 else 1
                cursor.execute("UPDATE students SET notifications = ? WHERE telegram_id = ?", (new_val, uid))
                conn.commit()
                student = cursor.execute("SELECT id, name FROM students WHERE telegram_id = ?", (uid,)).fetchone()
                kb = [
                    [InlineKeyboardButton("📊 Баланс", callback_data=f"balance_{student[0]}")],
                    [InlineKeyboardButton("📅 Посещения", callback_data=f"attendance_{student[0]}")],
                ]
                notif_text = "🔔 Уведомления вкл" if new_val == 1 else "🔕 Уведомления выкл"
                kb.append([InlineKeyboardButton(notif_text, callback_data="toggle_student_notifications")])
                await q.edit_message_text(f"👋 {student[1]}", reply_markup=InlineKeyboardMarkup(kb))
            return
        
        return  # Выход для не-админов

    # ===== АДМИН: ВСЕ ФУНКЦИИ НИЖЕ =====

    # Возврат в админ-панель
    if d == "start":
        kb = [
            [InlineKeyboardButton("👥 Ученики", callback_data="admin_students")],
            [InlineKeyboardButton("📚 Группы", callback_data="admin_groups")],
            [InlineKeyboardButton("➕ Ученик", callback_data="add_student")],
            [InlineKeyboardButton("🎟 Абонемент", callback_data="add_membership")],
            [InlineKeyboardButton("➕ Группа", callback_data="add_group")],
            [InlineKeyboardButton("📚 В группу", callback_data="add_to_group")],
            [InlineKeyboardButton("📋 Отметка", callback_data="mark_group")],
            [InlineKeyboardButton("⏱ Продлить", callback_data="extend_menu")],
            [InlineKeyboardButton("🗑 Удаление", callback_data="delete_menu")],
            [InlineKeyboardButton("❄️ Заморозка", callback_data="freeze_menu")],
            [InlineKeyboardButton("📋 Заявки", callback_data="admin_requests")],
        ]
        await q.edit_message_text("🔐 Админ-панель", reply_markup=InlineKeyboardMarkup(kb))
        return

    # Просмотр списка учеников
    if d == "admin_students":
        rows = cursor.execute("""
            SELECT s.name, s.phone, s.telegram_id, g.name 
            FROM students s 
            LEFT JOIN student_group sg ON s.id = sg.student_id 
            LEFT JOIN groups g ON sg.group_id = g.id 
            ORDER BY s.name
        """).fetchall()
        
        txt = "👥 Список учеников:\n" + "\n".join([f"• {r[0]} {r[1]} 🆔 {r[2]}" + (f" [{r[3]}]" if r[3] else "") for r in rows]) if rows else "👥 Нет учеников"
        kb = [[InlineKeyboardButton("➕ Ученик", callback_data="add_student")], [InlineKeyboardButton("🔙 Назад", callback_data="start")]]
        await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb))

    # Просмотр списка групп
    elif d == "admin_groups":
        rows = cursor.execute("SELECT id, name FROM groups ORDER BY name").fetchall()
        if rows:
            kb = [[InlineKeyboardButton(f"📚 {r[1]}", callback_data=f"group_{r[0]}")] for r in rows]
            kb.append([InlineKeyboardButton("➕ Группа", callback_data="add_group")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="start")])
            await q.edit_message_text("📚 Группы:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("📚 Нет групп", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Группа", callback_data="add_group")], [InlineKeyboardButton("🔙 Назад", callback_data="start")]]))

    # Просмотр конкретной группы
    elif d.startswith("group_"):
        gid = int(d.split("_")[1])
        group = cursor.execute("SELECT name FROM groups WHERE id = ?", (gid,)).fetchone()
        rows = cursor.execute("""
            SELECT s.id, s.name FROM students s 
            JOIN student_group sg ON s.id = sg.student_id 
            WHERE sg.group_id = ? ORDER BY s.name
        """, (gid,)).fetchall()
        
        if rows:
            txt = f"📚 *{group[0]}*\n\n"
            for r in rows:
                mem = cursor.execute("""
                    SELECT lessons_left FROM memberships 
                    WHERE student_id = ? AND status = 'active' AND valid_until > date('now') 
                    LIMIT 1
                """, (r[0],)).fetchone()
                txt += f"• {r[1]} — {mem[0] if mem else '❌ нет абонемента'}\n"
        else:
            txt = f"📚 {group[0]}: нет учеников"
        
        await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_groups")]]))

    # Просмотр заявок (через кнопку)
    elif d == "admin_requests":
        await show_requests(update, context)
        return

    # --- ОДОБРЕНИЕ ЗАЯВОК ---
    elif d.startswith("approve_req_"):
        request_id = int(d.split("_")[2])
        
        request = cursor.execute("""
            SELECT user_id, name, phone, role FROM requests 
            WHERE id = ? AND status = 'pending'
        """, (request_id,)).fetchone()
        
        if not request:
            await q.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
            await q.edit_message_text("❌ Заявка уже обработана")
            return
            
        user_id, name, phone, role = request
        
        try:
            if role == "student":
                cursor.execute("INSERT INTO students (telegram_id, name, phone) VALUES (?, ?, ?)", 
                             (user_id, name, phone))
            
            cursor.execute("UPDATE requests SET status = 'approved' WHERE id = ?", (request_id,))
            conn.commit()
            
            await q.edit_message_text(f"✅ Заявка #{request_id} одобрена, {name} добавлен как {role}")
            
            try:
                await context.bot.send_message(
                    user_id, 
                    f"✅ Администратор добавил тебя как **{role}**!\nНапиши /start"
                )
            except:
                logger.warning(f"Не удалось уведомить пользователя {user_id}")
                
        except Exception as e:
            logger.error(f"Ошибка при одобрении заявки: {e}")
            await q.edit_message_text(f"❌ Ошибка: {e}")

    elif d.startswith("reject_req_"):
        request_id = int(d.split("_")[2])
        
        request = cursor.execute("SELECT user_id FROM requests WHERE id = ?", (request_id,)).fetchone()
        
        if request:
            user_id = request[0]
            cursor.execute("UPDATE requests SET status = 'rejected' WHERE id = ?", (request_id,))
            conn.commit()
            
            await q.edit_message_text(f"❌ Заявка #{request_id} отклонена")
            
            try:
                await context.bot.send_message(
                    user_id, 
                    "❌ К сожалению, твоя заявка отклонена администратором."
                )
            except:
                pass
        else:
            await q.edit_message_text("❌ Заявка не найдена")

    # --- ДОБАВЛЕНИЕ УЧЕНИКА ---
    elif d == "add_student":
        logger.info("🔹 Нажата кнопка add_student")
        await q.edit_message_text("✏️ Введите имя ученика:")
        return NAME

    # --- ДОБАВЛЕНИЕ ГРУППЫ ---
    elif d == "add_group":
        logger.info("🔹 Нажата кнопка add_group")
        await q.edit_message_text("✏️ Введите название группы:")
        return GROUP_NAME

    # --- ДОБАВЛЕНИЕ АБОНЕМЕНТА ---
    elif d == "add_membership":
        students = cursor.execute("SELECT id, name FROM students ORDER BY name").fetchall()
        if students:
            kb = [[InlineKeyboardButton(f"👤 {s[1]}", callback_data=f"select_student_membership_{s[0]}")] for s in students]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="start")])
            await q.edit_message_text("👤 Выберите ученика для абонемента:", reply_markup=InlineKeyboardMarkup(kb))
            return
        else:
            await q.edit_message_text("👥 Сначала добавьте учеников", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="start")]]))
            return

    elif d.startswith("select_student_membership_"):
        sid = int(d.split("_")[3])
        context.user_data['membership_student'] = sid
        await q.edit_message_text("🔢 Введите количество занятий:")
        return LESSONS

    # --- ДОБАВЛЕНИЕ УЧЕНИКА В ГРУППУ ---
    elif d == "add_to_group":
        students = cursor.execute("SELECT id, name FROM students ORDER BY name").fetchall()
        if students:
            kb = [[InlineKeyboardButton(f"👤 {s[1]}", callback_data=f"select_student_{s[0]}")] for s in students]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="start")])
            await q.edit_message_text("👤 Выберите ученика:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("👥 Нет учеников")

    elif d.startswith("select_student_"):
        sid = int(d.split("_")[2])
        context.user_data['selected_student'] = sid
        groups = cursor.execute("SELECT id, name FROM groups ORDER BY name").fetchall()
        if groups:
            kb = []
            for g in groups:
                exists = cursor.execute("SELECT 1 FROM student_group WHERE student_id = ? AND group_id = ?", (sid, g[0])).fetchone()
                if not exists:
                    kb.append([InlineKeyboardButton(f"📚 {g[1]}", callback_data=f"select_group_{g[0]}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="add_to_group")])
            await q.edit_message_text("📚 Выберите группу:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("📚 Нет групп")

    elif d.startswith("select_group_"):
        gid = int(d.split("_")[2])
        sid = context.user_data.get('selected_student')
        cursor.execute("INSERT OR IGNORE INTO student_group (student_id, group_id) VALUES (?, ?)", (sid, gid))
        conn.commit()
        await q.edit_message_text("✅ Добавлено", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="add_to_group")]]))

    # --- ЗАМОРОЗКА ---
    elif d == "freeze_menu":
        students = cursor.execute("SELECT id, name FROM students ORDER BY name").fetchall()
        if students:
            kb = [[InlineKeyboardButton(f"👤 {s[1]}", callback_data=f"freeze_student_{s[0]}")] for s in students]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="start")])
            await q.edit_message_text("👤 Выберите ученика:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("👥 Нет учеников", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="start")]]))

    elif d.startswith("freeze_student_"):
        sid = int(d.split("_")[2])
        memberships = cursor.execute("""
            SELECT m.id, m.lessons_left, m.valid_until, m.status, m.frozen_days 
            FROM memberships m
            WHERE m.student_id = ? AND m.status != 'inactive' AND m.lessons_left > 0
            ORDER BY m.valid_until ASC
        """, (sid,)).fetchall()

        if memberships:
            kb = []
            for m in memberships:
                status_emoji = "✅" if m[3] == "active" else "❄️"
                if m[3] == "frozen" and m[4] > 0:
                    btn_text = f"{status_emoji} {m[1]} занятий (заморожен, оставалось {m[4]} дн.)"
                elif m[3] == "frozen":
                    btn_text = f"{status_emoji} {m[1]} занятий (заморожен, срок истёк)"
                else:
                    btn_text = f"{status_emoji} {m[1]} занятий до {m[2]}"
                
                cb_data = f"toggle_freeze_{m[0]}_{m[3]}"
                kb.append([InlineKeyboardButton(btn_text, callback_data=cb_data)])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="freeze_menu")])
            await q.edit_message_text("Выберите абонемент для заморозки/разморозки:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("Нет активных абонементов с занятиями", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="freeze_menu")]]))

    elif d.startswith("toggle_freeze_"):
        parts = d.split("_")
        mid = int(parts[2])
        current_status = parts[3]
        today = datetime.now().date()
        
        if current_status == "active":
            membership = cursor.execute("""
                SELECT valid_until, frozen_days FROM memberships WHERE id = ?
            """, (mid,)).fetchone()
            
            if membership:
                valid_until = datetime.strptime(membership[0], "%Y-%m-%d").date()
                days_left = (valid_until - today).days
                
                if days_left < 0:
                    days_left = 0
                
                cursor.execute("""
                    UPDATE memberships 
                    SET status = 'frozen', frozen_days = ? 
                    WHERE id = ?
                """, (days_left, mid))
                
                status_text = f"❄️ заморожен (оставалось {days_left} дн.)"
                logger.info(f"❄️ Абонемент {mid} заморожен, оставалось дней: {days_left}")
        else:
            membership = cursor.execute("""
                SELECT valid_until, frozen_days FROM memberships WHERE id = ?
            """, (mid,)).fetchone()
            
            if membership:
                frozen_days = membership[1]
                
                if frozen_days > 0:
                    new_valid_until = (today + timedelta(days=frozen_days)).strftime("%Y-%m-%d")
                    
                    cursor.execute("""
                        UPDATE memberships 
                        SET status = 'active', valid_until = ?, frozen_days = 0 
                        WHERE id = ?
                    """, (new_valid_until, mid))
                    
                    status_text = f"✅ разморожен, новый срок до {new_valid_until}"
                    logger.info(f"✅ Абонемент {mid} разморожен, новый срок: {new_valid_until}")
                else:
                    cursor.execute("""
                        UPDATE memberships 
                        SET status = 'active', frozen_days = 0 
                        WHERE id = ?
                    """, (mid,))
                    status_text = "✅ разморожен (срок истёк)"
        
        conn.commit()
        await q.answer(f"Абонемент {status_text}")
        
        mem_info = cursor.execute("SELECT student_id FROM memberships WHERE id = ?", (mid,)).fetchone()
        if mem_info:
            sid = mem_info[0]
            memberships = cursor.execute("""
                SELECT m.id, m.lessons_left, m.valid_until, m.status, m.frozen_days 
                FROM memberships m
                WHERE m.student_id = ? AND m.status != 'inactive' AND m.lessons_left > 0
                ORDER BY m.valid_until ASC
            """, (sid,)).fetchall()

            if memberships:
                kb = []
                for m in memberships:
                    status_emoji = "✅" if m[3] == "active" else "❄️"
                    if m[3] == "frozen" and m[4] > 0:
                        btn_text = f"{status_emoji} {m[1]} занятий (заморожен, оставалось {m[4]} дн.)"
                    elif m[3] == "frozen":
                        btn_text = f"{status_emoji} {m[1]} занятий (заморожен, срок истёк)"
                    else:
                        btn_text = f"{status_emoji} {m[1]} занятий до {m[2]}"
                    
                    cb_data = f"toggle_freeze_{m[0]}_{m[3]}"
                    kb.append([InlineKeyboardButton(btn_text, callback_data=cb_data)])
                kb.append([InlineKeyboardButton("🔙 Назад", callback_data="freeze_menu")])
                await q.edit_message_text("Выберите абонемент для заморозки/разморозки:", reply_markup=InlineKeyboardMarkup(kb))
            else:
                await q.edit_message_text("Нет активных абонементов с занятиями", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="freeze_menu")]]))

    # --- ОТМЕТКА ПОСЕЩЕНИЙ ---
    elif d == "mark_group":
        groups = cursor.execute("SELECT id, name FROM groups ORDER BY name").fetchall()
        if groups:
            kb = [[InlineKeyboardButton(f"📚 {g[1]}", callback_data=f"mark_group_{g[0]}")] for g in groups]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="start")])
            await q.edit_message_text("📚 Выберите группу:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("📚 Нет групп", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="start")]]))

    elif d.startswith("mark_group_"):
        gid = int(d.split("_")[2])
        await show_mark_group(q, context, gid)

    elif d.startswith("mark_student_"):
        logger.info(f"✅ Обработка mark_student: {d}")
        parts = d.split("_")
        
        if len(parts) < 5:
            logger.error(f"❌ Неправильный формат mark_student: {d}")
            await q.answer("❌ Ошибка формата данных", show_alert=True)
            return
            
        try:
            sid = int(parts[2])
            present = int(parts[3])
            gid = int(parts[4])
            logger.info(f"📊 Разобрано: sid={sid}, present={present}, gid={gid}")
        except ValueError as e:
            logger.error(f"❌ Ошибка преобразования чисел: {e}")
            await q.answer("❌ Ошибка данных", show_alert=True)
            return

        student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
        if not student:
            logger.error(f"❌ Ученик с id {sid} не найден")
            await q.answer("❌ Ученик не найден", show_alert=True)
            return
            
        today = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"📅 Сегодня: {today}")
        
        already_marked = cursor.execute("SELECT id, present FROM attendance WHERE student_id = ? AND date = ?", (sid, today)).fetchone()
        logger.info(f"🔍 Уже отмечен сегодня: {already_marked}")

        if already_marked:
            if present == 1:
                await q.answer(f"⚠️ {student[0]} уже отмечен сегодня!", show_alert=True)
            else:
                await q.answer(f"❌ {student[0]} уже отмечен как пропуск", show_alert=True)
            await show_mark_group(q, context, gid)
            return

        if present == 1:
            logger.info(f"🔍 Это отметка присутствия для ученика {sid}")
            mem = cursor.execute("""
                SELECT id, lessons_left FROM memberships 
                WHERE student_id = ? AND status = 'active' AND valid_until > date('now')
                ORDER BY valid_until ASC LIMIT 1
            """, (sid,)).fetchone()
            
            logger.info(f"🔍 Результат запроса абонемента: {mem}")
            
            if not mem:
                await q.answer(f"❌ Нет активного абонемента!", show_alert=True)
                await show_mark_group(q, context, gid)
                return
            
            new_left = mem[1] - 1
            logger.info(f"🔍 Было занятий: {mem[1]}, стало: {new_left}")
            
            try:
                cursor.execute("UPDATE memberships SET lessons_left = ? WHERE id = ?", (new_left, mem[0]))
                cursor.execute("INSERT INTO attendance (student_id, date) VALUES (?, ?)", (sid, today))
                conn.commit()
                logger.info("✅ Данные сохранены в БД")
            except Exception as e:
                logger.error(f"❌ Ошибка при сохранении в БД: {e}")
                await q.answer("❌ Ошибка базы данных", show_alert=True)
                return

            try:
                cursor.execute("DELETE FROM last_mark WHERE admin_id = ?", (uid,))
                cursor.execute("INSERT INTO last_mark (admin_id, student_id, date, mark_type) VALUES (?, ?, ?, ?)", (uid, sid, today, 1))
                conn.commit()
                logger.info("✅ last_mark обновлён")
            except Exception as e:
                logger.error(f"❌ Ошибка при обновлении last_mark: {e}")

            await notify_admin(sid, new_left, context)

            kb_undo = InlineKeyboardMarkup([[
                InlineKeyboardButton("↩️ Отменить посещение", callback_data="undo_last_mark")
            ]])
            await context.bot.send_message(
                uid,
                f"✅ {student[0]} отмечен на занятии — осталось {new_left}",
                reply_markup=kb_undo
            )
        else:
            logger.info(f"🔍 Это отметка пропуска для ученика {sid}")
            cursor.execute("INSERT INTO attendance (student_id, date, present) VALUES (?, ?, 0)", (sid, today))
            conn.commit()
            cursor.execute("DELETE FROM last_mark WHERE admin_id = ?", (uid,))
            cursor.execute("INSERT INTO last_mark (admin_id, student_id, date, mark_type) VALUES (?, ?, ?, ?)", (uid, sid, today, 0))
            conn.commit()
            kb_undo = InlineKeyboardMarkup([[
                InlineKeyboardButton("↩️ Отменить посещение", callback_data="undo_last_mark")
            ]])
            await context.bot.send_message(uid, f"❌ {student[0]} отмечен как пропуск", reply_markup=kb_undo)

        await show_mark_group(q, context, gid)

    elif d == "undo_last_mark":
        last = cursor.execute("SELECT student_id, date, mark_type FROM last_mark WHERE admin_id = ?", (uid,)).fetchone()
        if not last:
            await q.answer("❌ Нет отметки для отмены", show_alert=True)
            return
        sid, date, mark_type = last
        cursor.execute("DELETE FROM attendance WHERE student_id = ? AND date = ?", (sid, date))
        if mark_type == 1:
            mem = cursor.execute("SELECT id, lessons_left FROM memberships WHERE student_id = ? AND status = 'active' AND valid_until > date('now') ORDER BY valid_until ASC LIMIT 1", (sid,)).fetchone()
            if mem:
                new_left = mem[1] + 1
                cursor.execute("UPDATE memberships SET lessons_left = ? WHERE id = ?", (new_left, mem[0]))
        cursor.execute("DELETE FROM last_mark WHERE admin_id = ?", (uid,))
        conn.commit()
        student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
        await q.edit_message_text(f"↩️ Отметка для {student[0]} отменена")
        await q.answer("✅ Отметка отменена")

    elif d.startswith("mark_all_"):
        parts = d.split("_")
        present = int(parts[2])
        gid = int(parts[3])
        students = cursor.execute("SELECT s.id, s.name FROM students s JOIN student_group sg ON s.id = sg.student_id WHERE sg.group_id = ?", (gid,)).fetchall()
        today = datetime.now().strftime("%Y-%m-%d")
        success, failed, already = 0, 0, 0
        marked_list = []
        for s in students:
            sid = s[0]
            already_marked = cursor.execute("SELECT id, present FROM attendance WHERE student_id = ? AND date = ?", (sid, today)).fetchone()
            if already_marked:
                already += 1
                continue
            if present == 1:
                mem = cursor.execute("SELECT id, lessons_left FROM memberships WHERE student_id = ? AND status = 'active' AND valid_until > date('now') ORDER BY valid_until ASC LIMIT 1", (sid,)).fetchone()
                if mem:
                    new_left = mem[1] - 1
                    cursor.execute("UPDATE memberships SET lessons_left = ? WHERE id = ?", (new_left, mem[0]))
                    cursor.execute("INSERT INTO attendance (student_id, date) VALUES (?, ?)", (sid, today))
                    success += 1
                    marked_list.append(f"✅ {s[1]}")
                    
                    await notify_admin(sid, new_left, context)
                else:
                    marked_list.append(f"❌ {s[1]} (нет абонемента)")
                    failed += 1
            else:
                cursor.execute("INSERT INTO attendance (student_id, date, present) VALUES (?, ?, 0)", (sid, today))
                success += 1
                marked_list.append(f"❌ {s[1]}")
        conn.commit()
        msg = f"✅ Отмечено: {success}"
        if failed > 0: msg += f"\n❌ Пропущено (нет абонемента): {failed}"
        if already > 0: msg += f"\n⚠️ Уже отмечены: {already}"
        await q.answer(msg)
        if marked_list:
            await context.bot.send_message(uid, "Отмечены:\n" + "\n".join(marked_list))
        await show_mark_group(q, context, gid)

    # --- ПРОДЛЕНИЕ АБОНЕМЕНТА ---
    elif d == "extend_menu":
        students = cursor.execute("SELECT id, name FROM students ORDER BY name").fetchall()
        if students:
            kb = [[InlineKeyboardButton(f"👤 {s[1]}", callback_data=f"extend_student_{s[0]}")] for s in students]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="start")])
            await q.edit_message_text("👤 Выберите ученика для продления:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("👥 Нет учеников")

    elif d.startswith("extend_student_"):
        sid = int(d.split("_")[2])
        context.user_data['extend_student'] = sid
        await q.edit_message_text("📅 Введите количество дней для продления:")
        return EXTEND_DAYS

    # --- УДАЛЕНИЕ ---
    elif d == "delete_menu":
        kb = [
            [InlineKeyboardButton("👤 Ученика", callback_data="delete_student_menu")],
            [InlineKeyboardButton("🎟 Абонемент", callback_data="delete_membership_menu")],
            [InlineKeyboardButton("📚 Группу", callback_data="delete_group_menu")],
            [InlineKeyboardButton("📅 Посещение", callback_data="delete_attendance_menu")],
            [InlineKeyboardButton("🔙 Назад", callback_data="start")],
        ]
        await q.edit_message_text("🗑 Что удаляем?", reply_markup=InlineKeyboardMarkup(kb))

    elif d == "delete_student_menu":
        students = cursor.execute("SELECT id, name FROM students ORDER BY name").fetchall()
        if students:
            kb = [[InlineKeyboardButton(f"👤 {s[1]}", callback_data=f"delete_student_{s[0]}")] for s in students]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")])
            await q.edit_message_text("Выбери ученика:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("👥 Нет учеников", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    elif d.startswith("delete_student_"):
        sid = int(d.split("_")[2])
        student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_student_{sid}"),
            InlineKeyboardButton("❌ Нет", callback_data="delete_student_menu")
        ]])
        await q.edit_message_text(f"Точно удалить ученика {student[0]}?", reply_markup=kb)

    elif d.startswith("confirm_delete_student_"):
        sid = int(d.split("_")[3])
        cursor.execute("DELETE FROM students WHERE id = ?", (sid,))
        conn.commit()
        await q.edit_message_text("✅ Ученик удалён", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    elif d == "delete_membership_menu":
        memberships = cursor.execute("""
            SELECT m.id, s.name, m.lessons_left, m.valid_until, m.status 
            FROM memberships m
            JOIN students s ON m.student_id = s.id
            WHERE m.status != 'inactive'
            ORDER BY s.name
        """).fetchall()
        if memberships:
            kb = []
            for m in memberships:
                status_emoji = "✅" if m[4] == "active" else "❄️"
                btn_text = f"{status_emoji} {m[1]} — {m[2]} занятий, до {m[3]}"
                kb.append([InlineKeyboardButton(btn_text, callback_data=f"delete_membership_{m[0]}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")])
            await q.edit_message_text("Выбери абонемент для удаления:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("🎟 Нет абонементов", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    elif d.startswith("delete_membership_"):
        mid = int(d.split("_")[2])
        cursor.execute("DELETE FROM memberships WHERE id = ?", (mid,))
        conn.commit()
        await q.edit_message_text("✅ Абонемент удалён", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    elif d == "delete_group_menu":
        groups = cursor.execute("SELECT id, name FROM groups ORDER BY name").fetchall()
        if groups:
            kb = [[InlineKeyboardButton(f"📚 {g[1]}", callback_data=f"delete_group_{g[0]}")] for g in groups]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")])
            await q.edit_message_text("Выбери группу:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("📚 Нет групп", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    elif d.startswith("delete_group_"):
        gid = int(d.split("_")[2])
        cursor.execute("DELETE FROM groups WHERE id = ?", (gid,))
        conn.commit()
        await q.edit_message_text("✅ Группа удалена", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    # --- УДАЛЕНИЕ ПОСЕЩЕНИЙ ---
    elif d == "delete_attendance_menu":
        students = cursor.execute("SELECT id, name FROM students ORDER BY name").fetchall()
        if students:
            kb = [[InlineKeyboardButton(f"👤 {s[1]}", callback_data=f"delete_attendance_student_{s[0]}")] for s in students]
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")])
            await q.edit_message_text("👤 Выберите ученика:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("👥 Нет учеников", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]]))

    elif d.startswith("delete_attendance_student_"):
        sid = int(d.split("_")[3])
        attendances = cursor.execute("""
            SELECT date FROM attendance 
            WHERE student_id = ? 
            ORDER BY date DESC
        """, (sid,)).fetchall()
        
        if attendances:
            kb = []
            for att in attendances:
                date_obj = datetime.strptime(att[0], "%Y-%m-%d")
                date_display = date_obj.strftime("%d.%m.%Y")
                kb.append([InlineKeyboardButton(f"📅 {date_display}", callback_data=f"delete_attendance_date_{sid}_{att[0]}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="delete_attendance_menu")])
            await q.edit_message_text("📅 Выберите дату для удаления:", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await q.edit_message_text("📭 У этого ученика нет посещений", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_attendance_menu")]]))

    elif d.startswith("delete_attendance_date_"):
        parts = d.split("_")
        sid = int(parts[3])
        date = parts[4]
        
        student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
        date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_attendance_{sid}_{date}"),
            InlineKeyboardButton("❌ Нет", callback_data=f"delete_attendance_student_{sid}")
        ]])
        await q.edit_message_text(f"🗑 Удалить посещение {student[0]} за {date_display}?", reply_markup=kb)

    elif d.startswith("confirm_delete_attendance_"):
        parts = d.split("_")
        sid = int(parts[3])
        date = parts[4]
        
        was_present = cursor.execute(
            "SELECT present FROM attendance WHERE student_id = ? AND date = ?", 
            (sid, date)
        ).fetchone()
        
        cursor.execute("DELETE FROM attendance WHERE student_id = ? AND date = ?", (sid, date))
        
        if was_present and was_present[0] == 1:
            mem = cursor.execute("""
                SELECT id, lessons_left FROM memberships 
                WHERE student_id = ? AND status = 'active' AND valid_until > date('now')
                ORDER BY valid_until ASC LIMIT 1
            """, (sid,)).fetchone()
            
            if mem:
                new_left = mem[1] + 1
                cursor.execute("UPDATE memberships SET lessons_left = ? WHERE id = ?", (new_left, mem[0]))
                logger.info(f"✅ Занятие возвращено ученику {sid}, новый баланс: {new_left}")
        
        conn.commit()
        
        student = cursor.execute("SELECT name FROM students WHERE id = ?", (sid,)).fetchone()
        date_display = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        
        await q.edit_message_text(
            f"✅ Посещение {student[0]} за {date_display} удалено, занятие возвращено", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="delete_menu")]])
        )

# ===== ПОКАЗ ГРУППЫ ДЛЯ ОТМЕТОК =====
async def show_mark_group(q, context, gid):
    """Показывает группу для отметки посещений"""
    group = cursor.execute("SELECT name FROM groups WHERE id = ?", (gid,)).fetchone()
    today = datetime.now().strftime("%Y-%m-%d")
    today_display = datetime.now().strftime("%d.%m.%Y")
    students = cursor.execute("SELECT s.id, s.name FROM students s JOIN student_group sg ON s.id = sg.student_id WHERE sg.group_id = ? ORDER BY s.name", (gid,)).fetchall()
    kb = []
    for s in students:
        marked_today = cursor.execute("SELECT present FROM attendance WHERE student_id = ? AND date = ?", (s[0], today)).fetchone()
        if marked_today:
            btn_text = f"✅ {s[1]}" if marked_today[0] == 1 else f"❌ {s[1]}"
        else:
            btn_text = s[1]
        kb.append([
            InlineKeyboardButton(f"{btn_text} ✅", callback_data=f"mark_student_{s[0]}_1_{gid}"),
            InlineKeyboardButton("❌", callback_data=f"mark_student_{s[0]}_0_{gid}")
        ])
    kb.append([InlineKeyboardButton("✅ Все", callback_data=f"mark_all_1_{gid}"), InlineKeyboardButton("❌ Все", callback_data=f"mark_all_0_{gid}")])
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="mark_group")])
    try:
        await q.edit_message_text(f"📋 {group[0]} на {today_display}", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        if "Message is not modified" not in str(e):
            logger.error(f"Ошибка при обновлении сообщения: {e}")

# ===== ДИАЛОГИ (ConversationHandler) =====

# 1. Диалог заявки
async def request_name(update, context):
    context.user_data['req_name'] = update.message.text
    await update.message.reply_text("📞 Теперь напиши свой телефон (например, +375291234567):")
    return REQUEST_PHONE

async def request_phone(update, context):
    uid = update.effective_user.id
    name = context.user_data.get('req_name')
    phone = update.message.text
    role = context.user_data.get('request_role', 'student')
    username = update.effective_user.username or "нет"
    role_text = "ученик" if role == "student" else "родитель"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(f"📩 Заявка от {username} ({uid}): {name}, {phone}, роль: {role_text}")
    
    # Сохраняем заявку в БД
    cursor.execute("""
        INSERT INTO requests (user_id, username, name, phone, role, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (uid, username, name, phone, role, now))
    conn.commit()
    request_id = cursor.lastrowid
    
    # Отправляем админам с ID заявки
    sent_count = 0
    for admin_id in ADMIN_IDS:
        try:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Принять", callback_data=f"approve_req_{request_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_req_{request_id}")
            ]])
            
            await context.bot.send_message(
                admin_id, 
                f"📩 Заявка #{request_id} от @{username}\n"
                f"Имя: {name}\n"
                f"Телефон: {phone}\n"
                f"Роль: {role_text}\n"
                f"ID: {uid}",
                reply_markup=kb
            )
            sent_count += 1
        except Exception as e:
            logger.error(f"Ошибка отправки админу {admin_id}: {e}")
    
    if sent_count == 0:
        await update.message.reply_text("❌ Техническая ошибка. Попробуйте позже или свяжитесь с администратором.")
    else:
        await update.message.reply_text(f"✅ Заявка #{request_id} отправлена администратору. Ожидайте подтверждения.")
    
    context.user_data.clear()
    return ConversationHandler.END

# 2. Диалог добавления ученика
async def add_student_name(update, context):
    context.user_data['name'] = update.message.text
    await update.message.reply_text("📞 Введите телефон (например, +375291234567):")
    return PHONE

async def add_student_phone(update, context):
    context.user_data['phone'] = update.message.text
    await update.message.reply_text("🆔 Введите Telegram ID (число):")
    return TG_ID

async def add_student_id(update, context):
    try:
        tid = int(update.message.text)
        cursor.execute("INSERT INTO students (telegram_id, name, phone) VALUES (?, ?, ?)", 
                      (tid, context.user_data['name'], context.user_data['phone']))
        conn.commit()
        await update.message.reply_text("✅ Ученик добавлен")
    except Exception as e:
        logger.error(f"Ошибка добавления ученика: {e}")
        await update.message.reply_text("❌ Ошибка. Проверьте, что ID - число и уникально")
    context.user_data.clear()
    return ConversationHandler.END

# 3. Диалог добавления родителя
async def add_parent_name(update, context):
    context.user_data['name'] = update.message.text
    await update.message.reply_text("📞 Введите телефон (например, +375291234567):")
    return PARENT_PHONE

async def add_parent_phone(update, context):
    context.user_data['phone'] = update.message.text
    await update.message.reply_text("🆔 Введите Telegram ID (число):")
    return PARENT_TG

async def add_parent_id(update, context):
    try:
        tid = int(update.message.text)
        cursor.execute("INSERT INTO parents (telegram_id, name, phone) VALUES (?, ?, ?)", 
                      (tid, context.user_data['name'], context.user_data['phone']))
        conn.commit()
        await update.message.reply_text("✅ Родитель добавлен")
    except Exception as e:
        logger.error(f"Ошибка добавления родителя: {e}")
        await update.message.reply_text("❌ Ошибка")
    context.user_data.clear()
    return ConversationHandler.END

# 4. Диалог добавления абонемента
async def add_membership_lessons(update, context):
    try:
        lessons = int(update.message.text)
        if lessons <= 0:
            await update.message.reply_text("❌ Введите положительное число")
            return LESSONS
        context.user_data['mem_lessons'] = lessons
        await update.message.reply_text("📅 Введите количество дней действия:")
        return DAYS
    except ValueError:
        await update.message.reply_text("❌ Введите число")
        return LESSONS

async def add_membership_days(update, context):
    try:
        days = int(update.message.text)
        if days <= 0:
            await update.message.reply_text("❌ Введите положительное число")
            return DAYS
        context.user_data['mem_days'] = days
        await add_membership_final(update, context)
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Введите число")
        return DAYS

async def add_membership_final(update, context):
    try:
        student_id = context.user_data.get('membership_student')
        if not student_id:
            await update.message.reply_text("❌ Ошибка: ученик не выбран")
            return ConversationHandler.END
            
        student = cursor.execute("SELECT name FROM students WHERE id = ?", (student_id,)).fetchone()
        if not student:
            await update.message.reply_text("❌ Ученик не найден")
            return ConversationHandler.END
        
        new_lessons = context.user_data.get('mem_lessons')
        days = context.user_data.get('mem_days')
        new_valid_until = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
        # Проверяем существующие абонементы с долгом
        total_balance = cursor.execute("""
            SELECT SUM(lessons_left) FROM memberships
            WHERE student_id = ? AND status = 'active'
        """, (student_id,)).fetchone()[0] or 0
        
        if total_balance < 0:
            debt = abs(total_balance)
            
            if new_lessons <= debt:
                # Частично погашаем долг
                cursor.execute("""
                    UPDATE memberships SET lessons_left = lessons_left + ?
                    WHERE student_id = ? AND status = 'active'
                """, (new_lessons, student_id))
                await update.message.reply_text(
                    f"✅ Долг частично погашен. Текущий баланс: {total_balance + new_lessons}"
                )
            else:
                # Погашаем долг и создаём новый абонемент на остаток
                remaining = new_lessons - debt
                
                cursor.execute("""
                    UPDATE memberships SET lessons_left = 0
                    WHERE student_id = ? AND status = 'active' AND lessons_left < 0
                """, (student_id,))
                
                cursor.execute("""
                    INSERT INTO memberships (student_id, lessons_left, valid_until, status, frozen_days)
                    VALUES (?, ?, ?, 'active', 0)
                """, (student_id, remaining, new_valid_until))
                
                await update.message.reply_text(
                    f"✅ Долг погашен. Остаток {remaining} занятий зачислен на новый абонемент (до {new_valid_until})"
                )
        else:
            # Просто добавляем новый абонемент
            cursor.execute("""
                INSERT INTO memberships (student_id, lessons_left, valid_until, status, frozen_days)
                VALUES (?, ?, ?, 'active', 0)
            """, (student_id, new_lessons, new_valid_until))
            
            await update.message.reply_text(
                f"✅ Добавлен новый абонемент на {new_lessons} занятий (до {new_valid_until})"
            )
        
        conn.commit()
        await notify_admin(student_id, new_lessons, context)
        
    except Exception as e:
        logger.error(f"Ошибка в add_membership_final: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
    
    context.user_data.clear()

# 5. Диалог добавления группы
async def add_group_name(update, context):
    name = update.message.text
    try:
        cursor.execute("INSERT INTO groups (name) VALUES (?)", (name,))
        conn.commit()
        await update.message.reply_text(f"✅ Группа '{name}' создана")
    except Exception as e:
        logger.error(f"Ошибка создания группы: {e}")
        await update.message.reply_text("❌ Ошибка (возможно, группа уже существует)")
    context.user_data.clear()
    return ConversationHandler.END

# 6. Диалог продления абонемента
async def extend_days_input(update, context):
    try:
        days = int(update.message.text)
        if days <= 0:
            await update.message.reply_text("❌ Введите положительное число")
            return EXTEND_DAYS
            
        sid = context.user_data.get('extend_student')
        if not sid:
            await update.message.reply_text("❌ Ошибка: ученик не выбран")
            return ConversationHandler.END
            
        mem = cursor.execute("""
            SELECT id, valid_until FROM memberships 
            WHERE student_id = ? AND status = 'active' 
            ORDER BY valid_until ASC LIMIT 1
        """, (sid,)).fetchone()
        
        if mem:
            new = (datetime.strptime(mem[1], "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")
            cursor.execute("UPDATE memberships SET valid_until = ? WHERE id = ?", (new, mem[0]))
            conn.commit()
            await update.message.reply_text(f"✅ Продлён до {new}")
        else:
            await update.message.reply_text("❌ Нет активных абонементов")
    except Exception as e:
        logger.error(f"Ошибка продления: {e}")
        await update.message.reply_text("❌ Ошибка")
    context.user_data.clear()
    return ConversationHandler.END

# 7. Отмена диалога
async def cancel(update, context):
    await update.message.reply_text("❌ Отменено")
    context.user_data.clear()
    return ConversationHandler.END

# ===== ЗАПУСК БОТА =====
def main():
    """Главная функция запуска бота"""
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("requests", show_requests))
    
    # Диалог заявки
    app.add_handler(ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND, request_name_entry)],
        states={
            REQUEST_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_phone)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    
    # Диалог добавления ученика
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(add_student_entry, pattern="^add_student$")],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_student_name)],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_student_phone)],
            TG_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_student_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    
    # Диалог добавления абонемента
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(membership_lessons_entry, pattern="^select_student_membership_")],
        states={
            LESSONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_membership_lessons)],
            DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_membership_days)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    
    # Диалог добавления группы
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(add_group_entry, pattern="^add_group$")],
        states={
            GROUP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_group_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    
    # ===== ИСПРАВЛЕННЫЙ ДИАЛОГ ПРОДЛЕНИЯ =====
    # Функция входа в диалог продления
    async def extend_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Вход в диалог продления"""
        return EXTEND_DAYS
    
    # Добавляем диалог продления с исправленной функцией
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(extend_entry, pattern="^extend_student_")],
        states={
            EXTEND_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, extend_days_input)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    ))
    # ===== КОНЕЦ ИСПРАВЛЕНИЯ =====
    
    # Обработчик всех callback-кнопок (должен быть последним)
    app.add_handler(CallbackQueryHandler(button_handler))

    # Планировщик задач
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_daily(check_expiring_memberships, time=dt.time(hour=10, minute=0))
        logger.info("⏰ Запланирована ежедневная проверка истекающих абонементов в 10:00")

    logger.info("🚀 Бот с исправленной заморозкой и системой заявок запущен")
    app.run_polling()

if __name__ == "__main__":
    main()

