import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dateutil import tz
from dotenv import load_dotenv
from flask import Flask, request
from influxdb import InfluxDBClient

load_dotenv()

app = Flask(__name__)
influxdb = InfluxDBClient(host=os.getenv('INFLUXDB_HOST', 'localhost'), database='temperatures')


def check_alert():
    readings = list(influxdb.query(
        'SELECT (EXPONENTIAL_MOVING_AVERAGE(value, 1) - EXPONENTIAL_MOVING_AVERAGE(value, 10)) AS difference'
        ' FROM temperatures.autogen.temperature WHERE time > now()-15m'
    ).get_points())
    if not readings:
        return 'Ok (No data?)'

    last_difference = readings[-1]['difference']
    max_difference = max([r['difference'] for r in readings][-10:])
    if max_difference < 0:
        results = list(influxdb.query('SELECT * FROM alert ORDER BY time DESC LIMIT 1').get_points())
        if results and results[0]['status'] == 'on':
            return 'Ok (Alarm already started)'

        if results and results[0]['status'] == 'off':
            timestamp_str = results[0]['time']
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            if datetime.now().astimezone(tz.tzlocal()) - timestamp < timedelta(minutes=5):
                return 'Ok (Alarm muted)'

        influxdb.write_points([f'alert,status=on last_diff={last_difference}'], protocol='line', time_precision='ms')
        post_fields = {
            'm': f'Пора проверить печку, ΔT={last_difference}°C',
            's': 8,  # Buzzer
            'i': 83,  # House with fire icon
            'd': 'a',  # All devices
            'k': os.getenv('PUSHSAFER_SECRET_KEY')
        }
        req = Request('https://www.pushsafer.com/api', urlencode(post_fields).encode())
        response = json.loads(urlopen(req).read().decode())
        if response['status'] != 1:
            raise Exception(f'Failed to send alert notification: {response}')
        return 'Ok (Alarm!)'

    influxdb.write_points([f'alert,status=off last_diff={last_difference}'], protocol='line', time_precision='ms')
    return f'Ok (last {last_difference}, max {max_difference})'


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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
