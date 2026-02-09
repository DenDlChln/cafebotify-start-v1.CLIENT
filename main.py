import os
import json
import logging
import asyncio
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

import redis.asyncio as redis
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    ChatMemberUpdated,
)
from aiogram.filters import CommandStart, Command, StateFilter, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from aiogram.utils.deep_linking import create_start_link, create_startgroup_link

from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter
from aiogram.filters import IS_NOT_MEMBER, IS_MEMBER


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

MSK_TZ = timezone(timedelta(hours=3))
RATE_LIMIT_SECONDS = 60


def get_moscow_time() -> datetime:
    return datetime.now(MSK_TZ)


# -------------------------
# Config (multi-cafe)
# -------------------------

def load_config_file() -> Dict[str, Any]:
    path = os.getenv("CONFIG_PATH", "config.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("config root must be object")
            return data
    except FileNotFoundError as e:
        logger.error(f"❌ CONFIG not found: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"❌ CONFIG JSON invalid: {e}")
    except Exception as e:
        logger.error(f"❌ CONFIG load error: {e}")
    return {}


CONFIG = load_config_file()
CAFES = CONFIG.get("cafes", [])
if not isinstance(CAFES, list):
    CAFES = []

DEFAULT_CAFE = {
    "id": "default_cafe",
    "name": "Кофейня (дефолт) ☕",
    "phone": "+7 900 000-00-00",
    "admin_chat_id": 0,
    "work_start": 9,
    "work_end": 21,
    "menu": {
        "☕ Капучино": 250,
        "🥛 Латте": 270,
    },
}

NORMALIZED_CAFES: list[Dict[str, Any]] = []
for cafe in CAFES:
    if not isinstance(cafe, dict):
        continue
    c = dict(DEFAULT_CAFE)
    c.update(cafe)
    c["id"] = str(c.get("id", DEFAULT_CAFE["id"])).strip()
    c["name"] = str(c.get("name", DEFAULT_CAFE["name"]))
    c["phone"] = str(c.get("phone", DEFAULT_CAFE["phone"]))
    c["admin_chat_id"] = int(c.get("admin_chat_id", 0))
    c["work_start"] = int(c.get("work_start", DEFAULT_CAFE["work_start"]))
    c["work_end"] = int(c.get("work_end", DEFAULT_CAFE["work_end"]))
    c["menu"] = dict(c.get("menu", DEFAULT_CAFE["menu"]))
    if c["id"]:
        NORMALIZED_CAFES.append(c)

if not NORMALIZED_CAFES:
    NORMALIZED_CAFES = [DEFAULT_CAFE]

CAFES = NORMALIZED_CAFES
CAFES_BY_ID = {c["id"]: c for c in CAFES}
DEFAULT_CAFE_ID = CAFES[0]["id"]

SUPERADMIN_ID = int(CONFIG.get("superadmin_id") or 0)


# -------------------------
# Env / Webhook
# -------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "cafebot123")
HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
PORT = int(os.getenv("PORT", 10000))

if not HOSTNAME:
    HOSTNAME = "localhost"

WEBHOOK_PATH = f"/{WEBHOOK_SECRET}/webhook"
WEBHOOK_URL = f"https://{HOSTNAME}{WEBHOOK_PATH}"


router = Router()


class OrderStates(StatesGroup):
    waiting_for_quantity = State()
    waiting_for_confirmation = State()


# -------------------------
# Helpers (redis keys)
# -------------------------

async def get_redis_client():
    client = redis.from_url(REDIS_URL)
    try:
        await client.ping()
        return client
    except Exception:
        await client.aclose()
        raise


def _rate_limit_key(user_id: int) -> str:
    return f"rate_limit:{user_id}"


def _user_cafe_key(user_id: int) -> str:
    return f"user_cafe:{user_id}"


def _group_cafe_key(chat_id: int) -> str:
    return f"group_cafe:{chat_id}"


async def get_user_cafe_id(user_id: int) -> Optional[str]:
    r = await get_redis_client()
    try:
        v = await r.get(_user_cafe_key(user_id))
    finally:
        await r.aclose()
    if not v:
        return None
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


async def set_user_cafe_id(user_id: int, cafe_id: str) -> None:
    r = await get_redis_client()
    try:
        await r.set(_user_cafe_key(user_id), cafe_id)
    finally:
        await r.aclose()


async def set_group_cafe_id(chat_id: int, cafe_id: str) -> None:
    r = await get_redis_client()
    try:
        await r.set(_group_cafe_key(chat_id), cafe_id)
    finally:
        await r.aclose()


async def get_group_cafe_id(chat_id: int) -> Optional[str]:
    r = await get_redis_client()
    try:
        v = await r.get(_group_cafe_key(chat_id))
    finally:
        await r.aclose()
    if not v:
        return None
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


def get_cafe_or_default(cafe_id: Optional[str]) -> Dict[str, Any]:
    if cafe_id and cafe_id in CAFES_BY_ID:
        return CAFES_BY_ID[cafe_id]
    return CAFES_BY_ID[DEFAULT_CAFE_ID]


async def get_cafe_for_user(user_id: int) -> Dict[str, Any]:
    cafe_id = await get_user_cafe_id(user_id)
    return get_cafe_or_default(cafe_id)


def is_cafe_open(cafe: Dict[str, Any]) -> bool:
    ws = int(cafe["work_start"])
    we = int(cafe["work_end"])
    return ws <= get_moscow_time().hour < we


def get_work_status(cafe: Dict[str, Any]) -> str:
    ws = int(cafe["work_start"])
    we = int(cafe["work_end"])
    h = get_moscow_time().hour
    if ws <= h < we:
        remaining = max(0, we - h)
        return f"🟢 <b>Открыто</b> (ещё {remaining} ч.)"
    return f"🔴 <b>Закрыто</b>\n🕐 Открываемся: {ws}:00 (МСК)"


# -------------------------
# Keyboards
# -------------------------

def create_menu_keyboard(cafe: Dict[str, Any]) -> ReplyKeyboardMarkup:
    keyboard = [[KeyboardButton(text=drink)] for drink in cafe["menu"].keys()]
    keyboard.append([KeyboardButton(text="📞 Позвонить"), KeyboardButton(text="⏰ Часы работы")])
    keyboard.append([KeyboardButton(text="🛠 Админ")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def create_info_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📞 Позвонить"), KeyboardButton(text="⏰ Часы работы")],
            [KeyboardButton(text="🛠 Админ")],
        ],
        resize_keyboard=True,
    )


def create_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔗 Мои ссылки")],
            [KeyboardButton(text="👥 Подключить группу")],
            [KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="☕ Открыть меню")],
        ],
        resize_keyboard=True,
    )


def create_quantity_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1️⃣"), KeyboardButton(text="2️⃣"), KeyboardButton(text="3️⃣")],
            [KeyboardButton(text="4️⃣"), KeyboardButton(text="5️⃣"), KeyboardButton(text="🔙 Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def create_confirm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Подтвердить"), KeyboardButton(text="Меню")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# -------------------------
# Warm texts
# -------------------------

WELCOME_VARIANTS = [
    "Рад тебя видеть, {name}! Сегодня что-то классическое или попробуем новинку?",
    "{name}, добро пожаловать! Я уже грею молоко — выбирай, что приготовить.",
    "Заходи, {name}! Сейчас самое время для вкусного перерыва.",
    "{name}, привет! Устроим небольшой кофейный ритуал?",
    "Отлично, что заглянул, {name}! Давай подберём идеальный напиток под настроение.",
]

CHOICE_VARIANTS = [
    "Отличный выбор! Такое сейчас особенно популярно.",
    "Классика, которая никогда не подводит.",
    "Мне тоже нравится этот вариант — не прогадаешь.",
    "Прекрасный вкус, {name}! Это один из хитов нашего меню.",
    "Вот это да, {name}! Любители хорошего кофе тебя поймут.",
    "Смело! Такой выбор обычно делают настоящие ценители.",
    "{name}, ты знаешь толк в напитках.",
    "Звучит вкусно — уже представляю аромат.",
]

FINISH_VARIANTS = [
    "Спасибо за заказ! Буду рад увидеть тебя снова.",
    "Рад был помочь с выбором. Заглядывай ещё — всегда ждём.",
    "Отличный заказ! Надеюсь, это сделает день чуточку лучше.",
    "Спасибо, что выбрал именно нас. До следующей кофейной паузы!",
    "Заказ готовим с заботой. Возвращайся, когда захочется повторить.",
]


def get_user_name(message: Message) -> str:
    if message.from_user is None:
        return "друг"
    return message.from_user.first_name or "друг"


def get_closed_message(cafe: Dict[str, Any]) -> str:
    menu_text = " • ".join([f"<b>{drink}</b> {price}₽" for drink, price in cafe["menu"].items()])
    return (
        f"🔒 <b>{cafe['name']} сейчас закрыто!</b>\n\n"
        f"⏰ {get_work_status(cafe)}\n\n"
        f"☕ <b>Наше меню:</b>\n{menu_text}\n\n"
        f"📞 <b>Связаться:</b>\n<code>{cafe['phone']}</code>\n\n"
        f"✨ <i>До скорой встречи!</i>"
    )


def is_admin_of_cafe(user_id: int, cafe: Dict[str, Any]) -> bool:
    return user_id == int(cafe["admin_chat_id"]) or (SUPERADMIN_ID and user_id == SUPERADMIN_ID)


# -------------------------
# Group events + /bind
# -------------------------

@router.my_chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def bot_added_to_group(event: ChatMemberUpdated, bot: Bot):
    if event.chat.type not in ("group", "supergroup"):
        return
    await bot.send_message(
        event.chat.id,
        "Я добавлен в группу.\n\n"
        "Чтобы привязать группу к кафе, напишите:\n"
        "<code>/bind cafe_roma</code>\n\n"
        "Эту команду должен выполнить администратор кафе.",
    )


@router.message(Command("bind"))
async def bind_group(message: Message, command: CommandObject):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Команда /bind работает только в группе персонала.")
        return

    cafe_id = (command.args or "").strip()
    if not cafe_id:
        await message.answer("Формат: /bind cafe_roma")
        return

    if cafe_id not in CAFES_BY_ID:
        await message.answer("Неизвестный cafe_id. Пример: /bind cafe_roma")
        return

    cafe = CAFES_BY_ID[cafe_id]
    if message.from_user.id != int(cafe["admin_chat_id"]) and (not SUPERADMIN_ID or message.from_user.id != SUPERADMIN_ID):
        await message.answer("Только администратор этого кафе может привязать группу.")
        return

    await set_group_cafe_id(message.chat.id, cafe_id)
    await message.answer(f"✅ Группа привязана к кафе: <b>{cafe['name']}</b>")


# -------------------------
# Admin screens
# -------------------------

async def send_admin_start_screen(message: Message, cafe: Dict[str, Any]):
    guest_link = await create_start_link(message.bot, payload=cafe["id"], encode=False)
    staff_link = await create_startgroup_link(message.bot, payload=cafe["id"], encode=False)

    text = (
        f"🛠 <b>Режим администратора</b>\n"
        f"Кафе: <b>{cafe['name']}</b> (id=<code>{cafe['id']}</code>)\n\n"
        f"1) Гостевая ссылка (QR на столы):\n{guest_link}\n\n"
        f"2) Ссылка для группы персонала:\n{staff_link}\n\n"
        f"3) После добавления бота в группу напишите там:\n"
        f"<code>/bind {cafe['id']}</code>\n\n"
        f"Нажмите «☕ Открыть меню», чтобы посмотреть сценарий гостя."
    )

    await message.answer(text, reply_markup=create_admin_keyboard(), disable_web_page_preview=True)


# -------------------------
# User flow
# -------------------------

@router.message(CommandStart(deep_link=True))
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    await state.clear()

    user_id = message.from_user.id
    incoming = (command.args or "").strip() or None

    if incoming:
        if incoming not in CAFES_BY_ID:
            await message.answer("Ссылка устарела или кафе не найдено. Попросите актуальную ссылку у заведения.")
            return
        await set_user_cafe_id(user_id, incoming)
        cafe = CAFES_BY_ID[incoming]
    else:
        cafe = await get_cafe_for_user(user_id)
        if not await get_user_cafe_id(user_id):
            await set_user_cafe_id(user_id, cafe["id"])

    logger.info(f"👤 /start от {user_id} | cafe={cafe['id']} | is_admin={is_admin_of_cafe(user_id, cafe)}")

    if is_admin_of_cafe(user_id, cafe):
        await send_admin_start_screen(message, cafe)
        return

    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    if is_cafe_open(cafe):
        await message.answer(
            f"{welcome}\n\n"
            f"🏪 <b>{cafe['name']}</b>\n"
            f"🕐 <i>Московское время: {msk_time}</i>\n"
            f"🏪 {get_work_status(cafe)}\n\n"
            f"☕ <b>Выберите напиток:</b>",
            reply_markup=create_menu_keyboard(cafe),
        )
    else:
        await message.answer(get_closed_message(cafe), reply_markup=create_info_keyboard())


@router.message(F.text == "🛠 Админ")
async def admin_button(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return
    await send_admin_start_screen(message, cafe)


@router.message(F.text == "☕ Открыть меню")
async def open_menu_as_guest(message: Message, state: FSMContext):
    await state.clear()
    cafe = await get_cafe_for_user(message.from_user.id)
    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")
    welcome = random.choice(WELCOME_VARIANTS).format(name=name)

    if is_cafe_open(cafe):
        await message.answer(
            f"{welcome}\n\n"
            f"🏪 <b>{cafe['name']}</b>\n"
            f"🕐 <i>Московское время: {msk_time}</i>\n"
            f"🏪 {get_work_status(cafe)}\n\n"
            f"☕ <b>Выберите напиток:</b>",
            reply_markup=create_menu_keyboard(cafe),
        )
    else:
        await message.answer(get_closed_message(cafe), reply_markup=create_info_keyboard())


@router.message(F.text == "🔗 Мои ссылки")
async def my_links_button(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return
    await send_admin_start_screen(message, cafe)


@router.message(F.text == "👥 Подключить группу")
async def group_help_button(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        await message.answer("Доступно только администратору кафе.")
        return

    staff_link = await create_startgroup_link(message.bot, payload=cafe["id"], encode=False)
    text = (
        "👥 <b>Подключение группы персонала</b>\n\n"
        "1) Создайте группу (например “Кафе — персонал”).\n"
        "2) Добавьте туда бота по ссылке:\n"
        f"{staff_link}\n\n"
        f"3) В группе напишите:\n<code>/bind {cafe['id']}</code>\n"
    )
    await message.answer(text, disable_web_page_preview=True)


@router.message(F.text == "📊 Статистика")
async def stats_button(message: Message):
    await stats_command(message)


# -------------------------
# Ordering
# -------------------------

@router.message(F.text)
async def drink_selected(message: Message, state: FSMContext):
    if not message.text:
        return

    cafe = await get_cafe_for_user(message.from_user.id)
    menu = cafe["menu"]

    if message.text not in menu:
        return

    if not is_cafe_open(cafe):
        await message.answer(get_closed_message(cafe), reply_markup=create_info_keyboard())
        return

    drink = message.text
    price = int(menu[drink])

    await state.set_state(OrderStates.waiting_for_quantity)
    await state.set_data({"drink": drink, "price": price, "cafe_id": cafe["id"]})

    choice_text = random.choice(CHOICE_VARIANTS).format(name=get_user_name(message))
    await message.answer(
        f"{choice_text}\n\n"
        f"🥤 <b>{drink}</b>\n💰 <b>{price} ₽</b>\n\n📝 <b>Сколько порций?</b>",
        reply_markup=create_quantity_keyboard(),
    )


@router.message(StateFilter(OrderStates.waiting_for_quantity))
async def process_quantity(message: Message, state: FSMContext):
    cafe = await get_cafe_for_user(message.from_user.id)

    if message.text == "🔙 Отмена":
        await state.clear()
        await message.answer(
            "❌ Заказ отменён",
            reply_markup=create_menu_keyboard(cafe) if is_cafe_open(cafe) else create_info_keyboard(),
        )
        return

    try:
        quantity = int(message.text[0])
        if 1 <= quantity <= 5:
            data = await state.get_data()
            drink, price = data["drink"], int(data["price"])
            total = price * quantity

            await state.set_state(OrderStates.waiting_for_confirmation)
            await state.update_data(quantity=quantity, total=total)

            await message.answer(
                f"🥤 <b>{drink}</b> × {quantity}\n💰 Итого: <b>{total} ₽</b>\n\n✅ Правильно?",
                reply_markup=create_confirm_keyboard(),
            )
        else:
            await message.answer("❌ Выберите от 1 до 5", reply_markup=create_quantity_keyboard())
    except ValueError:
        await message.answer("❌ Нажмите на кнопку", reply_markup=create_quantity_keyboard())


@router.message(StateFilter(OrderStates.waiting_for_confirmation))
async def process_confirmation(message: Message, state: FSMContext):
    cafe = await get_cafe_for_user(message.from_user.id)
    user_id = message.from_user.id

    if message.text == "Подтвердить":
        try:
            r_client = await get_redis_client()
            last_order = await r_client.get(_rate_limit_key(user_id))
            if last_order and time.time() - float(last_order) < RATE_LIMIT_SECONDS:
                await message.answer(
                    f"⏳ Дай мне минутку: новый заказ можно оформить через {RATE_LIMIT_SECONDS} секунд после предыдущего.",
                    reply_markup=create_menu_keyboard(cafe),
                )
                await r_client.aclose()
                return
            await r_client.setex(_rate_limit_key(user_id), RATE_LIMIT_SECONDS, time.time())
            await r_client.aclose()
        except Exception:
            pass

        data = await state.get_data()
        drink = data["drink"]
        quantity = int(data["quantity"])
        total = int(data["total"])

        order_id = f"order:{int(time.time())}:{user_id}"
        order_num = order_id.split(":")[-1]
        user_name = message.from_user.username or message.from_user.first_name or "Клиент"

        try:
            r_client = await get_redis_client()
            await r_client.hset(
                order_id,
                mapping={
                    "user_id": user_id,
                    "username": user_name,
                    "drink": drink,
                    "quantity": quantity,
                    "total": total,
                    "timestamp": datetime.now().isoformat(),
                    "cafe_id": cafe["id"],
                },
            )
            await r_client.expire(order_id, 86400)
            await r_client.incr(f"stats:{cafe['id']}:total_orders")
            await r_client.incr(f"stats:{cafe['id']}:drink:{drink}")
            await r_client.aclose()
        except Exception:
            pass

        user_link = f'<a href="tg://user?id={user_id}">{user_name}</a>'
        admin_message = (
            f"🔔 <b>НОВЫЙ ЗАКАЗ #{order_num}</b> | {cafe['name']}\n\n"
            f"{user_link}\n"
            f"<code>{user_id}</code>\n\n"
            f"{drink}\n"
            f"{quantity} порций\n"
            f"<b>{total} ₽</b>\n\n"
            f"Нажми на имя, чтобы открыть чат и ответить клиенту."
        )

        await message.bot.send_message(int(cafe["admin_chat_id"]), admin_message, disable_web_page_preview=True)

        finish_text = random.choice(FINISH_VARIANTS)
        await message.answer(
            f"🎉 <b>Заказ #{order_num} принят!</b>\n\n"
            f"🥤 {drink} × {quantity}\n"
            f"💰 {total}₽\n\n"
            f"{finish_text}",
            reply_markup=create_menu_keyboard(cafe),
        )
        await state.clear()
        return

    if message.text == "Меню":
        await state.clear()
        await message.answer("☕ Меню:", reply_markup=create_menu_keyboard(cafe))
        return

    await message.answer("❌ Нажмите кнопку", reply_markup=create_confirm_keyboard())


# -------------------------
# Info buttons
# -------------------------

@router.message(F.text == "📞 Позвонить")
async def call_phone(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    name = get_user_name(message)

    if is_cafe_open(cafe):
        text = (
            f"{name}, буду рад помочь!\n\n"
            f"📞 <b>Телефон {cafe['name']}:</b>\n<code>{cafe['phone']}</code>\n"
        )
        await message.answer(text, reply_markup=create_menu_keyboard(cafe))
    else:
        text = (
            f"{name}, сейчас мы закрыты.\n\n"
            f"📞 <b>Телефон {cafe['name']}:</b>\n<code>{cafe['phone']}</code>\n\n"
            f"⏰ {get_work_status(cafe)}\n"
        )
        await message.answer(text, reply_markup=create_info_keyboard())


@router.message(F.text == "⏰ Часы работы")
async def show_hours(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    name = get_user_name(message)
    msk_time = get_moscow_time().strftime("%H:%M")

    text = (
        f"{name}, вот режим работы:\n\n"
        f"🕐 <b>Сейчас:</b> {msk_time} (МСК)\n"
        f"🏪 {get_work_status(cafe)}\n\n"
        f"📞 Телефон: <code>{cafe['phone']}</code>\n"
    )
    await message.answer(text, reply_markup=create_menu_keyboard(cafe) if is_cafe_open(cafe) else create_info_keyboard())


# -------------------------
# Stats / links / help / myid
# -------------------------

@router.message(Command("stats"))
async def stats_command(message: Message):
    cafe = await get_cafe_for_user(message.from_user.id)
    if not is_admin_of_cafe(message.from_user.id, cafe):
        return

    try:
        r_client = await get_redis_client()
        total_orders = int(await r_client.get(f"stats:{cafe['id']}:total_orders") or 0)
        stats_text = (
            f"📊 <b>Статистика заказов</b>\n"
            f"Кафе: <b>{cafe['name']}</b> (id={cafe['id']})\n\n"
            f"Всего заказов: <b>{total_orders}</b>\n\n"
        )
        for drink in cafe["menu"].keys():
            count = int(await r_client.get(f"stats:{cafe['id']}:drink:{drink}") or 0)
            if count > 0:
                stats_text += f"{drink}: {count}\n"
        await r_client.aclose()
        await message.answer(stats_text)
    except Exception:
        await message.answer("❌ Ошибка статистики")


@router.message(Command("links"))
async def links_command(message: Message):
    if not SUPERADMIN_ID or message.from_user.id != SUPERADMIN_ID:
        return

    parts = ["🔗 <b>Ссылки всех кафе</b>\n"]
    for cafe in CAFES:
        guest_link = await create_start_link(message.bot, payload=cafe["id"], encode=False)
        staff_link = await create_startgroup_link(message.bot, payload=cafe["id"], encode=False)
        parts.append(f"<b>{cafe['name']}</b> (id={cafe['id']}):\nГости: {guest_link}\nПерсонал: {staff_link}\n")

    await message.answer("\n".join(parts), disable_web_page_preview=True)


@router.message(Command("help"))
async def help_command(message: Message):
    text = (
        "CafeBotify.\n\n"
        "Гости: оформляйте заказ через меню.\n"
        "Админы: добавьте бота в группу персонала и выполните /bind.\n"
    )
    await message.answer(text)


@router.message(Command("myid"))
async def myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")


# -------------------------
# Startup / Webhook
# -------------------------

async def set_bot_commands(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Запуск бота"),
        BotCommand(command="myid", description="Показать мой Telegram ID"),
        BotCommand(command="stats", description="Статистика (админ)"),
        BotCommand(command="bind", description="Привязать группу к кафе (в группе)"),
        BotCommand(command="links", description="Ссылки всех кафе (суперадмин)"),
    ]
    await bot.set_my_commands(commands)


async def on_startup(bot: Bot) -> None:
    logger.info("🚀 Startup (MULTI-CAFE)...")
    logger.info(f"🏪 Cafes loaded: {len(CAFES)}")
    for c in CAFES:
        logger.info(f"CFG cafe={c['id']} admin={c['admin_chat_id']}")
    logger.info(f"🔗 Webhook target: {WEBHOOK_URL}")

    try:
        r_test = redis.from_url(REDIS_URL)
        await r_test.ping()
        await r_test.aclose()
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis error: {e}")

    try:
        await bot.set_webhook(WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
        logger.info("✅ Webhook set")
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")

    try:
        await set_bot_commands(bot)
        logger.info("✅ Commands set (no /help in menu)")
    except Exception as e:
        logger.error(f"❌ set_my_commands error: {e}")

    try:
        for cafe in CAFES:
            guest = await create_start_link(bot, payload=cafe["id"], encode=False)
            staff = await create_startgroup_link(bot, payload=cafe["id"], encode=False)
            logger.info(f"LINK guest [{cafe['id']}]: {guest}")
            logger.info(f"LINK staff  [{cafe['id']}]: {staff}")
    except Exception as e:
        logger.error(f"❌ Link generation error: {e}")


async def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return
    if not REDIS_URL:
        logger.error("❌ REDIS_URL not found")
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    storage = RedisStorage.from_url(REDIS_URL)
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    dp.startup.register(on_startup)

    app = web.Application()

    async def healthcheck(request: web.Request):
        return web.json_response({"status": "healthy", "bot": "ready"})

    app.router.add_get("/", healthcheck)

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
        handle_in_background=True,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)

    async def _on_shutdown(a: web.Application):
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        try:
            await storage.close()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info("🛑 Shutdown complete")

    app.on_shutdown.append(_on_shutdown)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info(f"🌐 Server running on 0.0.0.0:{PORT}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
