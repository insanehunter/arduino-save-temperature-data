from datetime import datetime

from filelock import FileLock
from flask import Flask, request, send_file, make_response

app = Flask(__name__)

DATABASE_PATH = 'data/temperatures.csv'


@app.route('/temperature', methods=['PUT'])
def put_temperature():
    content = request.json
    temperatures = content['temp_sma']
    timestamp = datetime.now().timestamp()
    rows = ''.join([f'{timestamp},{clock_time},{temp}\n' for clock_time, temp in temperatures.items()])
    with FileLock(f'{DATABASE_PATH}.lock', timeout=1):
        with open(DATABASE_PATH, 'at') as f:
            f.write(rows)
    return 'Ok!'


@app.route('/csv', methods=['GET'])
def get_csv():
    with FileLock(f'{DATABASE_PATH}.lock', timeout=1):
        response = make_response(send_file(DATABASE_PATH))
    response.headers['Content-Type'] = 'text/csv'
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
