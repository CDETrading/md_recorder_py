# import asyncio
# import threading
# import uuid
# import websockets
# import json
# import datetime
# import logging
# import requests
# import pandas as pd
# import os
# import fastparquet
# import time
# from collections import defaultdict
# from queue import Queue


# # Set up logging
# class DailyRotatingLogHandler(logging.Handler):
#     def __init__(self, base_path):
#         super().__init__()
#         self.base_path = base_path
#         self.current_date = None
#         self.current_file = None

#     def emit(self, record):
#         today = datetime.datetime.now().strftime("%Y%m%d")
#         if today != self.current_date:
#             if self.current_file:
#                 self.current_file.close()
#             filename = f'{self.base_path}/mexc_recorder.{today}.log'
#             os.makedirs(os.path.dirname(filename), exist_ok=True)
#             self.current_file = open(filename, 'a')
#             self.current_date = today
        
#         msg = self.format(record)
#         self.current_file.write(msg + '\n')
#         self.current_file.flush()

# handler = DailyRotatingLogHandler('/log')
# handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
# logger = logging.getLogger()
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)

# global connection_lock
# global BATCH_SIZE
# connection_lock = asyncio.Lock()
# BATCH_SIZE = 100

# class MexcRecorder:
#     DERIBIT_WS = "wss://www.deribit.com/ws/api/v2"
#     DERIBIT_REST = "https://www.deribit.com/api/v2/public/get_instruments"
    
#     def __init__(self, currency="BTC", kind="option", api_key=None, api_secret=None):
#         """
#         Initialize the DeribitRecorder.
        
#         Args:
#             currency (str): Currency of the instruments to fetch (default "BTC").
#             kind (str): Instrument type (e.g. "option", "future"). Default is "option".
#             api_key (str): API key for authenticated endpoints (optional).
#             api_secret (str): API secret for authenticated endpoints (optional).
#         """
#         self.currency = currency
#         self.kind = kind
#         self.api_key = api_key
#         self.api_secret = api_secret

#         # Dictionary to hold queues of data per channel.
#         # Structure: {channel: [ [timestamp, data], [timestamp, data], ... ] }
#         self.data = defaultdict(Queue)

#         # Dictionary to hold tasks per channel so we do not duplicate subscriptions.
#         self.channel_tasks = {}

#         # To track active channels (set of channel names)
#         self.active_channels = set()
        
#         # Lock for modifying the shared data structure
#         self.data_lock = asyncio.Lock()

#     async def login(self, websocket):
#         """
#         If API credentials are provided, login on the websocket connection.
#         (Deribit login requires a specific JSON-RPC call.)
#         """
#         if self.api_key and self.api_secret:
#             login_message = {
#                 "jsonrpc": "2.0",
#                 "id": 1,
#                 "method": "public/auth",
#                 "params": {
#                     "grant_type": "client_credentials",
#                     "client_id": self.api_key,
#                     "client_secret": self.api_secret,
#                 }
#             }
#             await websocket.send(json.dumps(login_message))
#             # response = await websocket.recv()
#             # logging.info(f"Login response: {response}")
#             # You might add error handling based on the response.

#     async def run_ws_loop(self, channels: list[str]):
#         """
#         Opens a websocket connection, log in (if needed), subscribes to a channel,
#         and listens for messages, adding them to the data queue.
#         """
#         task_id = uuid.uuid4()
#         while True:
#             try:
                
#                 async with websockets.connect(self.DERIBIT_WS) as websocket:
#                     await self.on_open(websocket)
#                     await self.subscribe(websocket, channels)
#                     async for message in websocket:
#                         self.on_message(message)
#             except Exception as e:
#                 logging.error(f"Error in ws_subscribe_channel ({channels}): {e}")
#                 print(f"WebSocket error on id:{task_id} e:{e}. Reconnecting in 5 seconds...")
#                 await asyncio.sleep(5)  # Reconnect after a delay.
    
#     def on_message(self, message):
#         """
#         Handle incoming messages from the websocket.
#         """
#         try:
#             local_recv_ts_ns = time.time_ns()
#             data = json.loads(message)
            
#             # Extract message data
#             channel = data["params"]["channel"]
#             exchange_timestamp = data["params"]['data']["timestamp"]
#             exchange_timestamp_ns = exchange_timestamp * 1_000_000
            
#             # Calculate and log latency if significant
#             data_latency = local_recv_ts_ns - exchange_timestamp_ns
#             if data_latency > 1_000_000_000:  # 1 second in nanoseconds
#                 logging.warning(
#                     f"High latency detected - Channel: {channel}, "
#                     f"Latency: {data_latency/1_000_000:.2f}ms"
#                 )
            
#             # Store the message data
#             self.data[channel].put([local_recv_ts_ns, data])
            
#         except json.JSONDecodeError as e:
#             logging.error(f"Failed to decode JSON message: {message} - {e}")
#         except KeyError as e:
#             logging.error(f"Missing required field in message: {message} - {e}")
#         except Exception as e:
#             logging.error(f"Unexpected error processing message: {message} - {e}")
            
    
#     async def on_open(self, websocket):
#         """
#         Handle the opening of the websocket connection.
#         """
#         if self.api_key and self.api_secret:
#             await self.login(websocket)

#     async def subscribe(self, websocket, channels: list[str]):
#         """
#         Subscribe to a list of channels on the Deribit WebSocket API.
        
#         Args:
#             websocket: The WebSocket connection to send the subscription request
#             channels: List of channel names to subscribe to
            
#         Returns:
#             None
#         """
#         subscription_message = {
#             "jsonrpc": "2.0",
#             "id": 1,
#             "method": "public/subscribe",
#             "params": {
#                 "channels": channels
#             }
#         }
#         print(f"{datetime.datetime.now(datetime.UTC)} Subscribing to {channels}")
#         await websocket.send(json.dumps(subscription_message))
    

    
#     def fetch_instruments(self) -> list[str]:
#         """
#         Fetches instrument list from Deribit REST API and returns new ticker channels.
        
#         Returns:
#             list[str]: List of new ticker channel names to subscribe to
#         """
#         params = {
#             "currency": self.currency,
#             "kind": self.kind
#         }
        
#         try:
#             response = requests.get(self.DERIBIT_REST, params=params)
#             result = response.json()
            
#             if not result.get("result"):
#                 logging.error(f"Error fetching instruments: {result}")
#                 return []
                
#             new_channels = []
#             for instrument in result["result"]:
#                 if name := instrument.get("instrument_name"):
#                     channel = f"ticker.{name}.raw"
#                     if channel not in self.active_channels:
#                         new_channels.append(channel)
                        
#             return new_channels
            
#         except Exception as e:
#             logging.error(f"Exception in fetch_instruments: {e}")
#             return []
    
#     async def start_listening_to_all_channels(self) -> None:
#         """
#         Start listening to all batches.
        
#         Args:
#             batches: List of channel batch lists to listen to
            
#         Returns:
#             None
#         """
#         all_channels: list[str] = self.fetch_instruments()
#         new_channels: list[str] = [i for i in all_channels if i not in self.active_channels]
#         batches: list[list[str]] = [new_channels[i:i + BATCH_SIZE] for i in range(0, len(new_channels), BATCH_SIZE)]
#         for idx, batch in enumerate(batches):
#             await connection_lock.acquire()
#             task: asyncio.Task = asyncio.create_task(self.run_ws_loop(batch))
#             print(f"Batch {idx} started listening to {len(batch)} channels")
#             for ch in batch:
#                 self.active_channels.add(ch)
#                 self.channel_tasks[ch] = task
#             await asyncio.sleep(1)
#             connection_lock.release()
    
#     async def listen_to_trade_channel(self):
#         """
#         Listen to a trade channel.
#         """
#         trade_channel = f"trades.option.{self.currency}.raw"
#         await self.run_ws_loop([trade_channel])

#     async def instruments_scheduler(self):
#         """
#         Wait until 8am UTC, then fetch instruments. Then schedule fetches every 24 hours.
#         """
#         while True:
#             now = datetime.datetime.now(datetime.UTC)
#             # Create a datetime object for the next 8am UTC
#             next_run = now.replace(hour=8, minute=0, second=0, microsecond=0)
#             if now >= next_run:
#                 next_run += datetime.timedelta(days=1)
#             delay = (next_run - now).total_seconds()
#             logging.info(f"Next instrument fetch scheduled in {delay} seconds at {next_run.isoformat()} UTC")
#             await asyncio.sleep(delay)
#             await self.fetch_instruments()
#             # Continue fetching every 24 hours after the initial run.
#             await asyncio.sleep(24 * 3600 - delay)

#     def flush_data_loop(self):
#         """
#         Periodically flushes data from channel queues to parquet files.
#         Runs every 5 seconds and handles each channel's data separately.
#         """
#         while True:
#             time.sleep(5)
            
#             for channel in list(self.data.keys()):
#                 queue = self.data.get(channel)
#                 if queue is None or queue.empty():
#                     continue
                    
#                 try:
#                     # Collect all data from queue
#                     datas = []
#                     while not queue.empty():
#                         datas.append(queue.get())
                    
#                     if not datas:
#                         continue
                        
#                     # Create DataFrame and prepare file path
#                     df = pd.DataFrame(datas, columns=["timestamp", "data"])
#                     date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
#                     filename = f"./tmp/data/{date}/{self.currency}/{channel}_{date}.parquet"
#                     os.makedirs(os.path.dirname(filename), exist_ok=True)
                    
#                     # Write to parquet file
#                     if os.path.exists(filename):
#                         fastparquet.write(filename, df, append=True, file_scheme='simple')
#                     else:
#                         fastparquet.write(filename, df, file_scheme='simple')
                        
#                     logging.info(f"Successfully flushed {len(datas)} messages from {channel} to {filename}")
                    
#                 except Exception as e:
#                     logging.error(f"Failed to write parquet file for {channel}: {e}")
#                     # On error, clear the queue to prevent memory buildup
#                     while not queue.empty():
#                         queue.get()
                        
#     def start_background_flush_thread(self):
#         """
#         Start a background thread to flush data periodically.
#         """
#         t = threading.Thread(target=self.flush_data_loop, daemon=True)
#         t.start()

#     async def start(self):
#         """
#         Start the DeribitRecorder by launching the instrument scheduler,
#         the flush data task, and (if desired) an initial instruments fetch.
#         """
#         # Launch tasks concurrently.
#         self.start_background_flush_thread()
#         tasks = [
#             asyncio.create_task(self.start_listening_to_all_channels()),
#             asyncio.create_task(self.listen_to_trade_channel())
#         ]
#         # Wait for tasks to run indefinitely.
#         await asyncio.gather(*tasks)

# async def main():
#     btc_recorder = DeribitRecorder(currency="BTC", kind="option", api_key=api, api_secret=secret)
#     eth_recorder = DeribitRecorder(currency="ETH", kind="option", api_key=api, api_secret=secret)
#     try:
#         tasks = [
#             asyncio.create_task(btc_recorder.start()),
#             asyncio.create_task(eth_recorder.start())
#         ]
#         await asyncio.gather(*tasks)
#     except KeyboardInterrupt:
#         print("Shutting down DeribitRecorder. wait 10 seconds...")
#         await asyncio.sleep(10)
#         print("Shutting down DeribitRecorder. done.")


# if __name__ == "__main__":
#     api = '09oWCFYp'
#     secret = 'h0yAt_38xL4vL9gkki7GCteno6vezR8tGNHQkr2VCaA'
#     asyncio.run(main())