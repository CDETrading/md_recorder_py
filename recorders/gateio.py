import websockets
import aiohttp
from .base_recorder import BaseRecorder, TimeSeriesData
import json
import time

class GateIORecorder(BaseRecorder):
    WS_URL = "wss://api.gateio.ws/ws/v4/"
    REST_URL = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    EXCHANGE_NAME = "gateio"
    BATCH_SIZE = 30
    def connect_ws(self):
        return websockets.connect(self.WS_URL)

    async def fetch_instruments(self) -> list[str]:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.REST_URL) as resp:
                    result = await resp.json()
                    symbols = []
                    for item in result:
                        symbol = item['id']
                        trade_status = item['trade_status']
                        if trade_status == "tradable":
                            symbols.append(symbol)
                        else:
                            print(f"Skipping {symbol} because trade_status is {trade_status}")
                        
                    return symbols
            except Exception as e:
                print(f"Failed to fetch instruments: {e}")
                return []

    async def subscribe(self, websocket, subscribe_message:str):
        print(f"Subscribing to {subscribe_message}")
        await websocket.send(subscribe_message)

    def generate_subscribe_messages(self, instruments:list[str]) -> list[str]:
        subscribe_messages = []
        for i in range(0, len(instruments), self.BATCH_SIZE):
            batch_instruments = instruments[i:i + self.BATCH_SIZE]
            subscribe_messages.append(json.dumps({
                "time": int(time.time()),
                "channel": "spot.trades_v2", 
                "event": "subscribe",
                "payload": batch_instruments
            }))
            subscribe_messages.append(json.dumps({
                "time": int(time.time()),
                "channel": "spot.book_ticker", 
                "event": "subscribe",
                "payload": batch_instruments
            }))
        return subscribe_messages
    
    def parse_message(self, string_message) -> TimeSeriesData:
        try:
            recv_ts_ns = time.time_ns()
            msg = json.loads(string_message)
            if not isinstance(msg, dict):
                print(f"Invalid message: {string_message}")
                return None

            event = msg.get("event")
            if event in ("subscribe", "unsubscribe"):
                print(f"Skipping event: {event}")
                return None
            
            channel = msg.get("channel")
            if channel == "spot.book_ticker":
                return self.on_bookticker_message(msg, recv_ts_ns)
            elif channel == "spot.trades_v2":
                return self.on_trades_message(msg, recv_ts_ns)
            else:
                print(f"Unknown channel: {channel}")
                return None
            
        except Exception as e:
            print(f"Failed to parse message: {e}")
            return None
    
    def on_bookticker_message(self, parsed_message, recv_ts_ns):
        channel = parsed_message.get("channel")
        payload = parsed_message.get("result")
        symbol = payload.get("s")
        if not symbol:
            return None

        full_channel = f"{channel}.{symbol}"
        
        return TimeSeriesData(
            data_recv_ts_ns=recv_ts_ns,
            channel=full_channel,
            data=parsed_message
        )

    def on_trades_message(self, parsed_message, recv_ts_ns):
        channel = parsed_message.get("channel")
        payload = parsed_message.get("result")
        symbol = payload.get("currency_pair")
        if not symbol:
            return None

        full_channel = f"{channel}.{symbol}"
        
        return TimeSeriesData(
            data_recv_ts_ns=recv_ts_ns,
            channel=full_channel,
            data=parsed_message
        )
