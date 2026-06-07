# adversary.py
# byzantine adversary node - intentionally breaks the protocol to test PBFT
# written by Vijay Sai Krishna Devabhakthuni - G25AI1016

import asyncio
import json
import os
import random
import logging
import time

from crypto_utils import generate_keys, sign_message, serialize_public_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  ADVERSARY  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("adversary")

NODE_ID   = int(os.environ.get("NODE_ID", "5"))
NODE_PORT = int(os.environ.get("NODE_PORT", "5005"))
PEERS_RAW = os.environ.get("PEERS", "[]")
PEERS     = json.loads(PEERS_RAW)

private_key, public_key = generate_keys()
my_pubkey_str = serialize_public_key(public_key)


async def send_to(host, port, message: dict):
    try:
        data = json.dumps(message).encode()
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


async def equivocate(seq: int):
    # send different values to different peers - this is equivocation attack
    log.warning(f"ATTACK: equivocating for seq={seq} - sending conflicting values to peers")
    half = len(PEERS) // 2
    for i, (host, port) in enumerate(PEERS):
        # first half gets FORGED_TX_A, second half gets FORGED_TX_B
        fake_value = "FORGED_TX_A" if i < half else "FORGED_TX_B"
        msg = {
            "type":   "PBFT_PRE_PREPARE",
            "value":  fake_value,
            "seq":    seq,
            "from":   NODE_ID
        }
        # sign with our own key - honest nodes will still reject because
        # they wont have matching prepare votes from 3 honest nodes
        sig = sign_message(private_key, msg)
        msg["sig"]    = sig.hex()
        msg["pubkey"] = my_pubkey_str
        await send_to(host, port, msg)
        log.warning(f"ATTACK: sent {fake_value} to {host}:{port}")


async def drop_commits():
    # randomly suppress commit messages
    while True:
        await asyncio.sleep(random.uniform(2, 5))
        if random.random() < 0.6:
            log.warning("ATTACK: suppressing a commit message (doing nothing)")
            # intentionally not sending anything


async def attack_loop():
    seq = 100   # use high seq numbers so they dont conflict with real ones
    while True:
        await asyncio.sleep(random.uniform(4, 8))
        await equivocate(seq)
        seq += 1


async def handle_connection(reader, writer):
    # adversary just reads and ignores or responds badly
    try:
        raw_len = await reader.readexactly(4)
        length  = int.from_bytes(raw_len, "big")
        data    = await reader.readexactly(length)
        msg     = json.loads(data.decode())
        log.info(f"received {msg.get('type')} - will ignore or corrupt it")
    except Exception:
        pass
    finally:
        writer.close()


async def main():
    log.info(f"adversary node {NODE_ID} starting on port {NODE_PORT}")
    server = await asyncio.start_server(handle_connection, "0.0.0.0", NODE_PORT)
    async with server:
        await asyncio.gather(
            server.serve_forever(),
            attack_loop(),
            drop_commits()
        )


if __name__ == "__main__":
    asyncio.run(main())
