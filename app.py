from datetime import datetime

from filelock import FileLock
from flask import Flask, request, send_file

app = Flask(__name__)

DATABASE_PATH = 'data/temperatures.csv'
DATABASE_LOCK = FileLock(f'{DATABASE_PATH}.lock', timeout=1)


@app.route('/temperature', methods=['PUT'])
def put_temperature():
    content = request.json
    temperatures = content['temp_sma']
    timestamp = datetime.now().timestamp()
    rows = ''.join([f'{timestamp},{clock_time},{temp}\n' for clock_time, temp in temperatures.items()])
    with DATABASE_LOCK:
        with open(DATABASE_PATH, 'at') as f:
            f.write(rows)
    return 'Ok!'


@app.route('/csv', methods=['GET'])
def get_csv():
    with DATABASE_LOCK:
        return send_file(DATABASE_PATH, 'text/csv')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
