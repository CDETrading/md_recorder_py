import asyncio
from dataclasses import dataclass
import threading
import datetime
import logging
import os
import pandas as pd
import fastparquet
import time
import uuid
import json
from queue import Queue, Empty
from collections import defaultdict

logging.basicConfig(
    filename='recorder.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class TimeSeriesData:
    data_recv_ts_ns: int
    channel: str
    data: dict

class BaseRecorder:
    WS_URL = None  # to be defined in subclass
    REST_URL = None
    EXCHANGE_NAME = None
    BATCH_SIZE = None

    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret

        self.data = defaultdict(Queue)
        self.channel_tasks = {}
        self.active_channels = set()
        self.data_lock = asyncio.Lock()

    async def run_ws_loop(self, subscribe_message:str):
        task_id = uuid.uuid4()
        while True:
            try:
                async with self.connect_ws() as websocket:
                    await self.on_open(websocket)
                    await self.subscribe(websocket, subscribe_message)
                    async for message in websocket:
                        self.on_message(message)
            except Exception as e:
                logging.error(f"[{task_id}] Error in ws_subscribe_channel ({subscribe_message}): {e}")
                await asyncio.sleep(5)

    def on_message(self, message):
        try:
            parsed:TimeSeriesData = self.parse_message(message)
            if not parsed:
                return
            full_data = parsed.data
            channel = parsed.channel
            data_recv_ts_ns = parsed.data_recv_ts_ns
   
            self.data[channel].put([data_recv_ts_ns, full_data])
        except Exception as e:
            logging.error(f"on_message error: {e}")

    def flush_data_loop(self):
        while True:
            time.sleep(5)
            for channel in list(self.data.keys()):
                queue = self.data[channel]
                if queue.empty():
                    continue
                try:
                    datas = []
                    while True:
                        try:
                            datas.append(queue.get_nowait())
                        except Empty:
                            break
                    if not datas:
                        continue
                    df = pd.DataFrame(datas, columns=["timestamp", "data"])
                    date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
                    filename = f"./data/{self.EXCHANGE_NAME}/{date}/{channel}_{date}.parquet"
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    if os.path.exists(filename):
                        fastparquet.write(filename, df, append=True, file_scheme='simple')
                    else:
                        fastparquet.write(filename, df, file_scheme='simple')
                    logging.info(f"Flushed {len(datas)} from {channel} to {filename}")
                except Exception as e:
                    logging.error(f"Flush error on {channel}: {e}")

    def start_background_flush_thread(self):
        threading.Thread(target=self.flush_data_loop, daemon=True).start()

    async def start_listening(self):
        all_instruments = await self.fetch_instruments()
        subscribe_messages = self.generate_subscribe_messages(all_instruments)
        tasks = []
        for subscribe_message in subscribe_messages:
            task = asyncio.create_task(self.run_ws_loop(subscribe_message))
            self.channel_tasks[subscribe_message] = task
            self.active_channels.add(subscribe_message)
            tasks.append(task)
            await asyncio.sleep(0.2)
        return tasks

    async def start(self):
        self.start_background_flush_thread()
        tasks = await self.start_listening()
        # Wait for all tasks to complete
        await asyncio.gather(*self.channel_tasks.values())

    # ---------- Methods to implement in subclass ----------

    def generate_subscribe_messages(self, instruments:list[str]) -> list[str]:
        raise NotImplementedError

    async def fetch_instruments(self) -> list[str]:
        raise NotImplementedError

    def parse_message(self, raw_message) -> TimeSeriesData:
        raise NotImplementedError

    async def subscribe(self, websocket, subscribe_message:str):
        raise NotImplementedError

    async def on_open(self, websocket):
        pass

    def connect_ws(self):
        raise NotImplementedError