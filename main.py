import time
import asyncio
import os

from pdr_predictoors.utils.process import process_block
from pdr_predictoors.utils.contract import w3


# TODO - check for all envs
assert os.environ.get("RPC_URL",None), "You must set RPC_URL environment variable"
assert os.environ.get("SUBGRAPH_URL",None), "You must set SUBGRAPH_URL environment variable"

async def log_loop(event_filter, poll_interval):
    last_block_time=0
    last_processed_block_no = 0
    avergage_time_between_blocks = 0
    while True:
        block = None
        for event in event_filter.get_new_entries():
            block_hash = event.hex()
            block = w3.eth.get_block(block_hash, full_transactions=False)
            if last_block_time>0:
                avergage_time_between_blocks = (avergage_time_between_blocks + (block["timestamp"] - last_block_time))/2
            last_block_time = block["timestamp"]
        """ Always handle latest block"""
        if block and block["number"]>last_processed_block_no:
            last_processed_block_no = block["number"]
            process_block(block,avergage_time_between_blocks)

        await asyncio.sleep(poll_interval)

def main():
    block_filter = w3.eth.filter('latest')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(block_filter, 1))
        )
    finally:
        loop.close()

if __name__ == '__main__':
    main()