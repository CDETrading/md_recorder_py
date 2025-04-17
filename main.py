import asyncio
import os
import argparse
from recorders.base_recorder import BaseRecorder
from recorders.gateio import GateIORecorder

async def main():
    recorder = GateIORecorder()
    try:
        # Start the recorder and keep it running
        await recorder.start()  # This will now run until interrupted
    except KeyboardInterrupt:
        print("Shutting down recorder...")
        # Optionally implement graceful shutdown
        for task in recorder.channel_tasks.values():
            task.cancel()
        await asyncio.gather(*recorder.channel_tasks.values(), return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())

