import os
from datetime import datetime, timedelta

from dateutil.tz import tz
from dotenv import load_dotenv
from flask import Blueprint, request
from influxdb import InfluxDBClient

import lib.data
from lib.giphy import Giphy
from lib.telegram_bot import send_gif_message, KeyboardType

load_dotenv()

bp = Blueprint('temperature', __name__)


def check_alert(influxdb: InfluxDBClient) -> str:
    furnace_status = lib.data.get_furnace_status(influxdb)
    temperature = lib.data.get_current_temperature(influxdb)

    alert_is_on, alert_timestamp = lib.data.get_last_alert_status_and_timestamp(influxdb)
    if furnace_status == lib.data.FurnaceStatus.COOLING_DOWN:
        if alert_is_on:
            return 'Ok (Alarm already started)'

        delta = datetime.now().astimezone(tz.tzlocal()) - alert_timestamp
        if delta < timedelta(minutes=5):
            return f'Ok (Alarm suppressed - not enough time passed since previous one: {delta})'

        chat_ids_to_send = []
        for chat_id in os.getenv('TELEGRAM_RECIPIENT_CHAT_IDS', '').split(','):
            if lib.data.get_watcher_enabled(influxdb, chat_id):
                chat_ids_to_send.append(chat_id)

        if chat_ids_to_send:
            message = f'⚠️ *Пора проверить печку!*\nТемпература - {temperature:.1f}°C.'
            gif_url = Giphy.random_video_url('go')
            for chat_id in chat_ids_to_send:
                send_gif_message(chat_id, gif_url, message, KeyboardType.WATCHER_ENABLED)

        lib.data.save_alert_status(influxdb, True)
        return 'Ok (Alarm!)'

    if alert_is_on:
        lib.data.save_alert_status(influxdb, False)

    # Congratulation on reaching 40 degrees
    if temperature >= 40:
        timestamp = lib.data.get_last_congrat_timestamp(influxdb)
        delta = datetime.now().astimezone(tz.tzlocal()) - timestamp
        if delta > timedelta(hours=8):
            gif_url = Giphy.random_video_url('celebration')
            message = f'🔥🔥🔥🔥🔥🎉🎉🎉🎊🎊🎊\nУра! Печка разгорелась до {temperature:.1f}°C.'
            for chat_id in os.getenv('TELEGRAM_RECIPIENT_CHAT_IDS', '').split(','):
                send_gif_message(chat_id, gif_url, message, KeyboardType.NONE)
            lib.data.save_congrat(influxdb, temperature)

    return 'Ok'


@bp.route('/temperature', methods=['PUT'])
def put_temperature():
    temperatures = sorted(request.json['median_temps'].items(), key=lambda x: x[0], reverse=True)
    influxdb = InfluxDBClient(host=os.getenv('INFLUXDB_HOST', 'localhost'), database='temperatures')
    lib.data.save_measurements(influxdb, temperatures)
    return check_alert(influxdb)


@bp.route('/check', methods=['POST'])
def check():
    influxdb = InfluxDBClient(host=os.getenv('INFLUXDB_HOST', 'localhost'), database='temperatures')
    return check_alert(influxdb)


@bp.route('/status', methods=['GET'])
def status():
    influxdb = InfluxDBClient(host=os.getenv('INFLUXDB_HOST', 'localhost'), database='temperatures')
    timestamp = lib.data.get_last_measurement_timestamp(influxdb)
    time_passed = datetime.now().astimezone(tz.tzlocal()) - timestamp
    if time_passed > timedelta(minutes=15):
        return 'Temperature readings are stale', 500
    return f'Ok (last data received {time_passed.seconds // 60}m ago)'
