# node.py
# main node daemon - handles leader election, paxos (mode A) and PBFT (mode B)
# written by Vijay Sai Krishna Devabhakthuni - G25AI1016

import asyncio
import json
import os
import sys
import time
import logging

from crypto_utils import generate_keys, sign_message, verify_message, serialize_public_key, deserialize_public_key

# basic logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  NODE-%(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# read config from environment variables (set in docker-compose)
NODE_ID   = int(os.environ.get("NODE_ID", "0"))
NODE_PORT = int(os.environ.get("NODE_PORT", str(5000 + NODE_ID)))
MODE      = os.environ.get("MODE", "A")   # A = paxos,  B = pbft
PEERS_RAW = os.environ.get("PEERS", "[]")
PEERS     = json.loads(PEERS_RAW)   # list of [host, port]

log = logging.getLogger(str(NODE_ID))

# generate this nodes RSA keys at startup
private_key, public_key = generate_keys()
my_pubkey_str = serialize_public_key(public_key)

# store public keys of other nodes (filled when they connect)
peer_pubkeys = {}

# node state
ledger       = []       # list of committed transactions
leader_id    = None
current_term = 0
last_heartbeat_time = time.time()

# paxos state
paxos_promises  = {}   # proposal_id -> set of node ids who promised
paxos_accepted  = {}   # proposal_id -> value

# pbft state
pbft_prepare_votes = {}   # seq -> set of node ids
pbft_commit_votes  = {}   # seq -> set of node ids
pbft_committed     = set()


async def send_to_peer(host, port, message: dict):
    # send a json message to one peer node
    try:
        data = json.dumps(message).encode()
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        log.warning(f"could not reach {host}:{port} -> {e}")


async def broadcast(message: dict):
    # send a message to all peer nodes
    tasks = [send_to_peer(h, p, message) for h, p in PEERS]
    await asyncio.gather(*tasks, return_exceptions=True)


async def heartbeat_sender():
    # if i am the leader, send heartbeat every second
    while True:
        if leader_id == NODE_ID:
            await broadcast({
                "type": "HEARTBEAT",
                "from": NODE_ID,
                "term": current_term
            })
        await asyncio.sleep(1)


async def leader_watchdog():
    # check if leader is still alive
    # if no heartbeat for 3 seconds, start new election
    global leader_id, current_term, last_heartbeat_time
    while True:
        await asyncio.sleep(0.5)
        if leader_id != NODE_ID:
            elapsed = time.time() - last_heartbeat_time
            if elapsed > 3.0:
                log.info(f"no heartbeat for {elapsed:.1f}s - starting election")
                current_term += 1
                leader_id = NODE_ID
                log.info(f"i am now the leader, term={current_term}")
                await broadcast({
                    "type": "NEW_LEADER",
                    "from": NODE_ID,
                    "term": current_term
                })
                last_heartbeat_time = time.time()


async def run_paxos(value: str):
    # phase 1: send prepare to all nodes
    proposal_id = (current_term, NODE_ID)
    log.info(f"paxos PREPARE  proposal_id={proposal_id}  value={value}")
    await broadcast({
        "type":        "PAXOS_PREPARE",
        "proposal_id": list(proposal_id),
        "from":        NODE_ID
    })
    # phase 2 happens when we collect promises (see handle_message)
    # store the value so we can use it in phase 2
    key = str(proposal_id)
    paxos_accepted[key] = value


async def run_pbft(value: str):
    # send pre-prepare as the primary
    seq = len(ledger) + 1
    msg = {
        "type":  "PBFT_PRE_PREPARE",
        "value": value,
        "seq":   seq,
        "from":  NODE_ID
    }
    sig = sign_message(private_key, {k: v for k, v in msg.items() if k != "sig"})
    msg["sig"] = sig.hex()
    msg["pubkey"] = my_pubkey_str
    log.info(f"pbft PRE_PREPARE  seq={seq}  value={value}")
    log.info(f"signed with RSA-PSS SHA256  sig={sig.hex()[:8]}...{sig.hex()[-4:]}")
    await broadcast(msg)


async def handle_message(msg: dict):
    global leader_id, current_term, last_heartbeat_time

    mtype = msg.get("type", "")

    if mtype == "HEARTBEAT":
        last_heartbeat_time = time.time()
        if msg["term"] >= current_term:
            current_term = msg["term"]
            leader_id    = msg["from"]

    elif mtype == "NEW_LEADER":
        if msg["term"] > current_term:
            current_term = msg["term"]
            leader_id    = msg["from"]
            last_heartbeat_time = time.time()
            log.info(f"accepted new leader: node {leader_id}  term={current_term}")

    elif mtype == "PROPOSE":
        # client is asking leader to commit a transaction
        if leader_id == NODE_ID:
            if MODE == "A":
                await run_paxos(msg["value"])
            else:
                await run_pbft(msg["value"])
        else:
            log.warning("received PROPOSE but i am not the leader, ignoring")

    elif mtype == "PAXOS_PREPARE":
        # send promise back to proposer
        pid = msg["proposal_id"]
        log.info(f"paxos PROMISE  proposal_id={pid}  to node {msg['from']}")
        await send_to_peer(*PEERS[msg["from"]], {
            "type":        "PAXOS_PROMISE",
            "proposal_id": pid,
            "from":        NODE_ID
        }) if msg["from"] < len(PEERS) else None
        await broadcast({
            "type":        "PAXOS_PROMISE",
            "proposal_id": pid,
            "from":        NODE_ID
        })

    elif mtype == "PAXOS_PROMISE":
        pid_key = str(tuple(msg["proposal_id"]))
        if pid_key not in paxos_promises:
            paxos_promises[pid_key] = set()
        paxos_promises[pid_key].add(msg["from"])

        # if we got majority promises, send accept
        if len(paxos_promises[pid_key]) >= 3:
            value = paxos_accepted.get(pid_key, "unknown_tx")
            log.info(f"paxos majority promises received ({len(paxos_promises[pid_key])}/5) - sending ACCEPT")
            await broadcast({
                "type":        "PAXOS_ACCEPT",
                "proposal_id": msg["proposal_id"],
                "value":       value,
                "from":        NODE_ID
            })
            # clear so we dont send accept multiple times
            paxos_promises[pid_key] = set()

    elif mtype == "PAXOS_ACCEPT":
        value = msg["value"]
        if value not in ledger:
            ledger.append(value)
            log.info(f"paxos ACCEPTED  value={value}  ledger_size={len(ledger)}")
            # write to disk
            with open(f"/tmp/ledger_node{NODE_ID}.log", "a") as f:
                f.write(f"{value}\n")

    elif mtype == "PBFT_PRE_PREPARE":
        # verify signature first
        sender_pubkey_str = msg.get("pubkey", "")
        check_msg = {k: v for k, v in msg.items() if k not in ("sig", "pubkey")}
        try:
            sender_pub = deserialize_public_key(sender_pubkey_str)
            sig_bytes  = bytes.fromhex(msg["sig"])
            valid      = verify_message(sender_pub, check_msg, sig_bytes)
        if valid:
            log.info(f"SIGNATURE VERIFIED  from node {msg.get('from')}  ok=True")
        except Exception:
            valid = False

        if not valid:
            log.warning(f"PBFT PRE_PREPARE from node {msg.get('from')} has BAD SIGNATURE - ignoring")
            return

        # signature ok, send prepare vote
        seq = msg["seq"]
        vote = {
            "type": "PBFT_PREPARE",
            "seq":  seq,
            "from": NODE_ID
        }
        sig = sign_message(private_key, vote)
        vote["sig"]    = sig.hex()
        vote["pubkey"] = my_pubkey_str
        log.info(f"pbft PREPARE vote  seq={seq}")
        await broadcast(vote)

    elif mtype == "PBFT_PREPARE":
        seq = msg["seq"]
        # verify vote signature
        check_msg = {k: v for k, v in msg.items() if k not in ("sig", "pubkey")}
        try:
            sender_pub = deserialize_public_key(msg.get("pubkey", ""))
            sig_bytes  = bytes.fromhex(msg["sig"])
            valid      = verify_message(sender_pub, check_msg, sig_bytes)
        if valid:
            log.info(f"SIGNATURE VERIFIED  from node {msg.get('from')}  ok=True")
        except Exception:
            valid = False

        if not valid:
            log.warning(f"PBFT PREPARE from node {msg.get('from')} has BAD SIGNATURE - ignoring")
            return

        if seq not in pbft_prepare_votes:
            pbft_prepare_votes[seq] = set()
        pbft_prepare_votes[seq].add(msg["from"])

        if len(pbft_prepare_votes[seq]) >= 3:
            log.info(f"pbft PREPARE quorum for seq={seq} - sending COMMIT")
            commit = {
                "type": "PBFT_COMMIT",
                "seq":  seq,
                "from": NODE_ID
            }
            sig = sign_message(private_key, commit)
            commit["sig"]    = sig.hex()
            commit["pubkey"] = my_pubkey_str
            await broadcast(commit)
            pbft_prepare_votes[seq] = set()

    elif mtype == "PBFT_COMMIT":
        seq = msg["seq"]
        check_msg = {k: v for k, v in msg.items() if k not in ("sig", "pubkey")}
        try:
            sender_pub = deserialize_public_key(msg.get("pubkey", ""))
            sig_bytes  = bytes.fromhex(msg["sig"])
            valid      = verify_message(sender_pub, check_msg, sig_bytes)
        if valid:
            log.info(f"SIGNATURE VERIFIED  from node {msg.get('from')}  ok=True")
        except Exception:
            valid = False

        if not valid:
            log.warning(f"PBFT COMMIT from node {msg.get('from')} has BAD SIGNATURE - ignoring")
            return

        if seq not in pbft_commit_votes:
            pbft_commit_votes[seq] = set()
        pbft_commit_votes[seq].add(msg["from"])

        if len(pbft_commit_votes[seq]) >= 3 and seq not in pbft_committed:
            entry = f"pbft_tx_seq_{seq}"
            ledger.append(entry)
            pbft_committed.add(seq)
            log.info(f"pbft COMMITTED  seq={seq}  entry={entry}  ledger_size={len(ledger)}")
            with open(f"/tmp/ledger_node{NODE_ID}.log", "a") as f:
                f.write(f"{entry}\n")


async def tcp_server():
    async def handle_connection(reader, writer):
        try:
            raw_len = await reader.readexactly(4)
            length  = int.from_bytes(raw_len, "big")
            data    = await reader.readexactly(length)
            msg     = json.loads(data.decode())
            await handle_message(msg)
        except Exception as e:
            log.debug(f"connection error: {e}")
        finally:
            writer.close()

    server = await asyncio.start_server(
        handle_connection, "0.0.0.0", NODE_PORT
    )
    log.info(f"node {NODE_ID} listening on port {NODE_PORT}  mode={MODE}")
    async with server:
        await server.serve_forever()


async def main():
    log.info(f"starting node {NODE_ID}  mode={MODE}  peers={PEERS}")
    # node 0 starts as leader initially
    global leader_id
    if NODE_ID == 0:
        leader_id = 0
        log.info("i am the initial leader")

    await asyncio.gather(
        tcp_server(),
        heartbeat_sender(),
        leader_watchdog()
    )


if __name__ == "__main__":
    asyncio.run(main())
