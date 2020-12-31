import os
from datetime import datetime, timedelta, timezone

from dateutil import tz
from dotenv import load_dotenv
from flask import Flask, request
from influxdb import InfluxDBClient
from telegram import ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import MessageHandler, Filters
from telegram.ext import Updater

load_dotenv()

app = Flask(__name__)
influxdb = InfluxDBClient(host=os.getenv('INFLUXDB_HOST', 'localhost'), database='temperatures')
updater = Updater(token=os.getenv('TELEGRAM_BOT_TOKEN'), use_context=True)

MESSAGE_STOP_NOTIFICATIONS = '🔕 Больше не хочу следить за печкой'
MESSAGE_START_NOTIFICATIONS = '🔔 Хочу следить за печкой'


def check_alert():
    emas = list(influxdb.query(
        'SELECT EXPONENTIAL_MOVING_AVERAGE(value, 5) AS ema'
        ' FROM temperatures.autogen.temperature WHERE time > now()-5m'
    ).get_points())
    if not emas:
        return 'Ok (No data?)'

    difference = emas[-1]['ema'] - emas[0]['ema']
    max_diff = max([emas[i + 1]['ema'] - emas[0]['ema'] for i in range(len(emas) - 1)])
    if difference < 0 and max_diff < 0:
        results = list(influxdb.query('SELECT * FROM alert ORDER BY time DESC LIMIT 1').get_points())
        if results and results[0]['status'] == 'on':
            return 'Ok (Alarm already started)'

        for chat_id in os.getenv('TELEGRAM_RECIPIENT_CHAT_IDS', '').split(','):
            results = list(influxdb.query('SELECT * FROM watcher ORDER BY time DESC LIMIT 1').get_points())
            if not results or results[-1]['status'] != 'off':
                updater.bot.send_message(
                    chat_id, f'⚠ *Проверь печку!*\nЗа последние 5 минут температура упала на {-difference:.2f}°C',
                    parse_mode='Markdown', reply_markup=ReplyKeyboardMarkup(
                        [[KeyboardButton(MESSAGE_STOP_NOTIFICATIONS)]],
                        resize_keyboard=True, one_time_keyboard=True
                    )
                )
        influxdb.write_points([f'alert,status=on diff={difference}'], protocol='line', time_precision='ms')
        return 'Ok (Alarm!)'

    influxdb.write_points([f'alert,status=off diff={difference}'], protocol='line', time_precision='ms')
    return f'Ok (dT={difference:.2f})'


@app.route('/temperature', methods=['PUT'])
def put_temperature():
    temperatures = sorted(request.json['median_temps'].items(), key=lambda x: x[0], reverse=True)
    now = datetime.now().timestamp()

    # Writing data
    latest_clock_time = None
    data_points = []
    for key, temperature in temperatures:
        epoch, clock_time = key.split('_')
        clock_time = int(epoch) * (2 ** 32) + int(clock_time)
        if latest_clock_time is None:
            latest_clock_time = clock_time
        timestamp = int(now * 1000 - (latest_clock_time - clock_time))
        data_points.append(f'temperature value={temperature} {timestamp}')
    data_points.reverse()
    influxdb.write_points(data_points, protocol='line', time_precision='ms')
    return check_alert()


@app.route('/check', methods=['POST'])
def check():
    return check_alert()


@app.route('/status', methods=['GET'])
def status():
    timestamp_str = next(influxdb.query('SELECT * FROM temperature ORDER BY time DESC LIMIT 1').get_points())['time']
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    time_passed = datetime.now().astimezone(tz.tzlocal()) - timestamp
    if time_passed > timedelta(minutes=15):
        return 'Temperature readings are stale', 500
    return f'Ok (last data received {time_passed.seconds // 60}m ago)'


def on_message(update, context):
    chat_id = str(update.effective_chat.id)
    if chat_id not in os.getenv('TELEGRAM_RECIPIENT_CHAT_IDS', '').split(','):
        context.bot.send_message(chat_id=chat_id, text='Ты кто такой? Давай до свидания!')
        return
    if update.message.text == MESSAGE_STOP_NOTIFICATIONS:
        influxdb.write_points(['watcher,status=off value=0'], protocol='line', time_precision='ms')
        context.bot.send_message(
            chat_id=chat_id, text='Ок! Больше не буду беспокоить.',
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(MESSAGE_START_NOTIFICATIONS)]],
                resize_keyboard=True, one_time_keyboard=True
            ))
    elif update.message.text == MESSAGE_START_NOTIFICATIONS:
        influxdb.write_points(['watcher,status=on value=0'], protocol='line', time_precision='ms')
        context.bot.send_message(
            chat_id=chat_id, text='Ок! Если она начнет остывать, я тебе сообщу.',
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(MESSAGE_STOP_NOTIFICATIONS)]],
                resize_keyboard=True, one_time_keyboard=True
            ))


updater.dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), on_message))

if __name__ == '__main__':
    updater.start_polling()
    app.run(host='0.0.0.0', port=5000)
