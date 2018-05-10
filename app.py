import json
import sys
import websocket

import _thread as thread

from config import API_KEY, API_SECRET, ENABLE_TRACE, PING_SLEEP_SECONDS
from deribit_api import RestClient
from pipeline import pipeline, vwap_all
from time import sleep


def ping():
    def run(*args):
        while True:
            ws.send(json.dumps({
                "action": "/api/v1/public/ping"
            }))
            sleep(PING_SLEEP_SECONDS)
    thread.start_new_thread(run, ())


def on_message(ws, message):
    msg = json.loads(message)
    if 'notifications' not in msg:
        return

    for notification in msg['notifications']:
        if not notification['success']:
            continue

        for trade in notification['result']:
            pipeline(db, trade)

    vwap_all(db)


def on_error(ws, error):
    print(error)


def on_close(ws):
    pass


def on_open(ws):
    data = {
        "action": "/api/v1/private/subscribe",
        "arguments": {
            "instrument": ["all"],
            "event": ["trade"]
        }
    }
    data['sig'] = client.generate_signature(data['action'], data['arguments'])

    ws.send(json.dumps(data))
    ping()


db = {}
client = RestClient(API_KEY, API_SECRET)
websocket.enableTrace(ENABLE_TRACE)
ws = websocket.WebSocketApp("wss://www.deribit.com/ws/api/v1/",
                          on_message = on_message,
                          on_error = on_error,
                          on_close = on_close)
ws.on_open = on_open
ws.run_forever()
