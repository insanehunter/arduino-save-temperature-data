import os
from enum import Enum
from typing import Optional

from influxdb import InfluxDBClient
from telegram import ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Updater, MessageHandler, Filters

from lib.data import set_watcher_enabled, get_watcher_enabled, get_furnace_status, FurnaceStatus, \
    get_current_temperature
from lib.giphy import Giphy

MESSAGE_STOP_NOTIFICATIONS = '🔕 Больше не хочу следить за печкой'
MESSAGE_START_NOTIFICATIONS = '🔔 Хочу следить за печкой'
MESSAGE_CHECK = '❕ Как там печка?'


class KeyboardType(Enum):
    NONE = 'NONE'
    WATCHER_ENABLED = 'WATCHER_ENABLED'
    WATCHER_DISABLED = 'WATCHER_DISABLED'


def _get_reply_markup(keyboard_type: KeyboardType) -> Optional[ReplyKeyboardMarkup]:
    if keyboard_type == KeyboardType.NONE:
        return None

    if keyboard_type == KeyboardType.WATCHER_ENABLED:
        return ReplyKeyboardMarkup(
            [[KeyboardButton(MESSAGE_CHECK)],
             [KeyboardButton(MESSAGE_STOP_NOTIFICATIONS)]],
            resize_keyboard=True, one_time_keyboard=True
        )

    if keyboard_type == KeyboardType.WATCHER_DISABLED:
        return ReplyKeyboardMarkup(
            [[KeyboardButton(MESSAGE_CHECK)],
             [KeyboardButton(MESSAGE_START_NOTIFICATIONS)]],
            resize_keyboard=True, one_time_keyboard=True
        )
    raise Exception(f'Unhandled case: {keyboard_type}')


def send_gif_message(chat_id: str, gif_url: str, caption: str, keyboard_type: KeyboardType):
    _telegram.bot.send_animation(
        chat_id, gif_url, caption=caption, parse_mode='Markdown', reply_markup=_get_reply_markup(keyboard_type)
    )


def on_message(update, context):
    chat_id = str(update.effective_chat.id)
    if chat_id not in os.getenv('TELEGRAM_RECIPIENT_CHAT_IDS', '').split(','):
        context.bot.send_message(chat_id=chat_id, text='Ты кто такой? Давай до свидания!')
        return

    influxdb = InfluxDBClient(host=os.getenv('INFLUXDB_HOST', 'localhost'), database='temperatures')

    if update.message.text == MESSAGE_STOP_NOTIFICATIONS:
        set_watcher_enabled(influxdb, chat_id, False)
        context.bot.send_message(
            chat_id=chat_id, text='Ок! Больше не буду беспокоить.',
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(MESSAGE_CHECK)],
                 [KeyboardButton(MESSAGE_START_NOTIFICATIONS)]],
                resize_keyboard=True, one_time_keyboard=True
            ))

    elif update.message.text == MESSAGE_START_NOTIFICATIONS:
        set_watcher_enabled(influxdb, chat_id, True)
        context.bot.send_message(
            chat_id=chat_id, text='Ок! Если она начнет остывать, я тебе сообщу.',
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(MESSAGE_CHECK)],
                 [KeyboardButton(MESSAGE_STOP_NOTIFICATIONS)]],
                resize_keyboard=True, one_time_keyboard=True
            ))

    elif update.message.text == MESSAGE_CHECK:
        furnace_status = get_furnace_status(influxdb)
        tag = 'random' if furnace_status == FurnaceStatus.NOT_TRENDING else \
            'heat' if furnace_status == FurnaceStatus.HEATING_UP else 'cold'
        message = '😒 Как на этой картинке.' if furnace_status == FurnaceStatus.NOT_TRENDING else \
            '🔥 Разгорается.' if furnace_status == FurnaceStatus.HEATING_UP else '❄ Остывает.'
        message += f' Температура - {get_current_temperature(influxdb):.1f}°C.'
        notif_button = MESSAGE_STOP_NOTIFICATIONS \
            if get_watcher_enabled(influxdb, chat_id) else MESSAGE_START_NOTIFICATIONS
        _telegram.bot.send_animation(
            chat_id, Giphy.random_video_url(tag), caption=message,
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(MESSAGE_CHECK)],
                 [KeyboardButton(notif_button)]],
                resize_keyboard=True, one_time_keyboard=True
            )
        )


_telegram = Updater(token=os.getenv('TELEGRAM_BOT_TOKEN'), use_context=True)
_telegram.dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), on_message))


def start_telegram_bot():
    _telegram.start_polling()
