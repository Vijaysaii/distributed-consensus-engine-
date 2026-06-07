# client.py
# submits concurrent transactions to the distributed ledger
# written by Vijay Sai Krishna Devabhakthuni - G25AI1016

import asyncio
import json
import os
import time
import sys

LEADER_HOST = os.environ.get("LEADER_HOST", "node0")
LEADER_PORT = int(os.environ.get("LEADER_PORT", "5000"))


async def send_transaction(tx_id: int):
    value = f"TX-{tx_id}-timestamp-{time.time():.3f}"
    msg   = {"type": "PROPOSE", "value": value}
    data  = json.dumps(msg).encode()
    try:
        reader, writer = await asyncio.open_connection(LEADER_HOST, LEADER_PORT)
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        print(f"sent: {value}")
    except Exception as e:
        print(f"error sending TX-{tx_id}: {e}")


async def main():
    # wait a bit for nodes to start up first
    print("client: waiting 5 seconds for cluster to be ready...")
    await asyncio.sleep(5)
    print(f"client: sending 20 transactions to {LEADER_HOST}:{LEADER_PORT}")
    tasks = [send_transaction(i) for i in range(1, 21)]
    await asyncio.gather(*tasks)
    print("client: all transactions sent")


if __name__ == "__main__":
    asyncio.run(main())
