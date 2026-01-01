import asyncio
import json
import sys
from dataclasses import asdict
from aiomqtt import Client

import messages
from utils.buildTopic import buildClientTopic, MASTER_PID, buildNodeTopic

BROKER_IP = "localhost"
BROKER_PORT = 1883

START_VALUES = {
    1: 108,
    2: 76,
    3: 12,
    4: 60,
    5: 36
}


def distribute_nodes(total_nodes: int, total_clients: int) -> list[int]:
    base = total_nodes // total_clients
    rest = total_nodes % total_clients
    return [base + 1 if i < rest else base for i in range(total_clients)]


def chunk_pids(pids: list[int], client_quantity: int) -> list[list[int]]:
    counts = distribute_nodes(len(pids), client_quantity)
    chunks: list[list[int]] = []
    start = 0
    for c in counts:
        chunks.append(pids[start:start + c])
        start += c
    return chunks


async def master():
    async with Client(BROKER_IP, BROKER_PORT) as client:
        await client.subscribe(buildNodeTopic(MASTER_PID))
        print("[MASTER] verbunden")

        pids = list(START_VALUES.keys())
        n = len(pids)

        client_quantity = 5

        pid_chunks = chunk_pids(pids, client_quantity)

        for client_id, pid_list in enumerate(pid_chunks, start=1):
            msg = messages.Message(
                type=messages.MessageType.SET_NODE_PIDS.value,
                value=json.dumps(pid_list)
            )
            await client.publish(buildClientTopic(client_id), json.dumps(asdict(msg)).encode())
            print(f"[MASTER] sende PIDs {pid_list} an Client {client_id}")


        await asyncio.sleep(1)


        for i, pid in enumerate(pids):
            left = pids[(i - 1) % n]
            right = pids[(i + 1) % n]

            msg_left = messages.Message(
                type=messages.MessageType.SET_NEIGHBOUR_LEFT.value,
                value=str(left),
            )
            msg_right = messages.Message(
                type=messages.MessageType.SET_NEIGHBOUR_RIGHT.value,
                value=str(right),
            )
            await client.publish(buildNodeTopic(pid), json.dumps(asdict(msg_left)).encode())
            await client.publish(buildNodeTopic(pid), json.dumps(asdict(msg_right)).encode())

            msg_m = messages.Message(
                type=messages.MessageType.SET_M.value,
                value=str(START_VALUES[pid]),
            )
            await client.publish(buildNodeTopic(pid), json.dumps(asdict(msg_m)).encode())

            print(f"[MASTER] pid={pid}: left={left} right={right} M0={START_VALUES[pid]}")

        # Laufzeit begrenzen (aktuell 5 s)
        await asyncio.sleep(5)

        # Ergebnisse abfragen
        get_msg = messages.Message(type=messages.MessageType.GET_M.value, value="")
        payload = json.dumps(asdict(get_msg)).encode()

        for pid in pids:
            await client.publish(buildNodeTopic(pid), payload)
            print(f"[MASTER] sende GET_M an pid={pid}")

        results: list[int] = []
        async for message in client.messages:
            data = json.loads(message.payload.decode())
            msg = messages.Message(**data)

            if msg.type == messages.MessageType.GET_M.value:
                m_val = int(msg.value)
                results.append(m_val)
                print(f"[MASTER] Antwort erhalten: {m_val}")

            # Wenn wir von allen Peers eine Antwort haben → raus aus der Schleife
            if len(results) >= len(pids):
                break

        print("\n[MASTER] Endergebnisse:")
        for i, m_val in enumerate(results, start=1):
            print(f"Antwort {i}: M = {m_val}")

if __name__ == "__main__":
    if sys.platform == "win32":
        from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy

        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

    asyncio.run(master())
