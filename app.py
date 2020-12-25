from datetime import datetime, timedelta, timezone

from dateutil import tz
from flask import Flask, request
from influxdb import InfluxDBClient

app = Flask(__name__)
influxdb = InfluxDBClient(database='temperatures')


@app.route('/temperature', methods=['PUT'])
def put_temperature():
    temperatures = sorted(request.json['median_temps'].items(), key=lambda x: x[0], reverse=True)
    now = datetime.now().timestamp()

    latest_clock_time = None
    data_points = []
    for key, temperature in temperatures:
        epoch, clock_time = key.split('_')
        clock_time = int(epoch) * (2 ** 32) + int(clock_time)
        if latest_clock_time is None:
            latest_clock_time = clock_time
        timestamp = int(now * 1000 - (latest_clock_time - clock_time))
        data_points.append(f'temperature value={temperature} {timestamp}')

    influxdb.write_points(data_points, protocol='line', time_precision='ms')
    return 'Ok!'


@app.route('/status', methods=['GET'])
def check():
    timestamp_str = next(influxdb.query('SELECT * FROM temperature ORDER BY time DESC LIMIT 1').get_points())['time']
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    time_passed = datetime.now().astimezone(tz.tzlocal()) - timestamp
    if time_passed > timedelta(minutes=15):
        return 'Temperature readings are stale', 500
    return f'Ok (last data received {time_passed.seconds // 60}m ago)'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
