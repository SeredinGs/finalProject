"""Модуль бекэенда сервиса"""
# 2540 -1 6545 - 2 8650 - 0
import sys
sys.path.append('..')
from src.mongoconnection import mongoconnector
from logging.handlers import RotatingFileHandler
from time import strftime, time
import logging
import traceback
from src.ml_api import payload
from flask import Flask, request, jsonify


app = Flask(__name__)

# Logging
HANDLER = RotatingFileHandler('backend.log', maxBytes=100000, backupCount=5)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER)


@app.route("/")
def index():
    """Вывод стартовой страницы"""
    return "Welcome to API"


@app.route("/predict", methods=['POST'])
def predict():
    """функция при пост-запросе predict"""
    json_input = request.json
    user, pred = mongoconnector(json_input['user'])
    if user!=0000:
        if pred == 0:
            return {'text': f'Не выдаем кредит!'}
        elif pred == 1:
            return {'text': f'Выдаем кредит!..Но с осторожностью'}
        else:
            return {'text': f'Выдаем кредит! Уверенно'}
    else:
        return payload(str(json_input))


@app.errorhandler(Exception)
def exceptions(e):
    """Обработка ошибок"""
    current_datatime = strftime('[%Y-%b-%d %H:%M:%S]')
    error_message = traceback.format_exc()
    LOGGER.error('%s %s %s %s %s 5xx INTERNAL SERVER ERROR\n%s',
                 current_datatime,
                 request.remote_addr,
                 request.method,
                 request.scheme,
                 request.full_path,
                 error_message)
    return jsonify({'error': 'Internal Server Error'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0')

