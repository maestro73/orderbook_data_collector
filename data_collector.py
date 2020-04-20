import websocket
import zlib
import json
import threading
import time

import redis_upload


# decompress the received data
def inflate(data):
    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


# check the received message
def on_message(ws, message):
    try:
        inflated = inflate(message).decode('utf-8')
        if inflated == '{"event":"pong"}':
            print("Pong received.")
            return
        global order_book
        msg = json.loads(inflated)
        if "channel" in msg:  # check whether the correct channel is subscribed
            print(msg["channel"] + " subscribed.")
        if "table" in msg:
            order_book = msg['data']
            print(order_book)
            redis_upload.upload(msg['table'] + ":" + order_book[0]['instrument_id'], str(order_book))
    except Exception as e:
        print(e)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


# send request when connection is started
def on_open(ws):
    ws.send('{"op": "subscribe", "args": ["spot/depth5:BTC-USDT"]}')
    ws.send('{"op": "subscribe", "args": ["spot/depth5:ETH-USDT"]}')


# send heartbeat to maintain the connection
def sendHeartBeat(ws):
    ping = '{"event":"ping"}'
    while(True):
        time.sleep(30) # send request every 30 seconds
        sent = False
        while(sent is False): # keep trying if failed
            try:
                ws.send(ping)
                sent = True
                print("Ping sent.")
            except Exception as e:
                print(e)


# create websocket connection
def ws_main():
    websocket.enableTrace(True)
    host = "wss://real.OKEx.com:8443/ws/v3"
    ws = websocket.WebSocketApp(host,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    threading.Thread(target=sendHeartBeat, args=(ws,)).start()
    ws.run_forever()


if __name__ == "__main__":
    order_book = 0
    threading.Thread(target=ws_main).start()