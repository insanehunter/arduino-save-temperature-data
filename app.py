from dotenv import load_dotenv
from flask import Flask

from lib.blueprints import api
from lib.telegram_bot import start_telegram_bot

load_dotenv()

app = Flask(__name__)
app.register_blueprint(api.bp)
start_telegram_bot()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
