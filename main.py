import asyncio
import json
import sys
from asyncio import tasks
from dataclasses import asdict
from aiomqtt import Client

import messages
from client.peer import Peer
from utils.buildTopic import buildTopic, MASTER_PID

BROKER_IP = "localhost"
BROKER_PORT = 1883

START_VALUES ={
    1: 108,
    2: 76,
    3: 12,
    4: 60,
    5: 36
}
# Server

#ABlauf
# 1. Verbinde mit Broker
# 2. sende allen clients ihre nachbartopics
# 3. sende jedem client sein m
# 5. warte Zeit t
# 6. schicke allen ein Get_M
# 7. gebe m aus oder wähl das größte oder so

async def master():
    async with Client(BROKER_IP, BROKER_PORT) as client:

        await client.subscribe(buildTopic(MASTER_PID))
        print("Master verbundn")
        pids = list(START_VALUES.keys())
        n = len(pids)

        for i, pid in enumerate(pids):
            left = pids[(i - 1) % n]
            right = pids[(i + 1) % n]

            msg_left = messages.Message(
                type=messages.MessageType.SET_NEIGHBOUR_LEFT.value,
                value=str(left),
            )
            print(f"  Sende LEFT-Info an {buildTopic(pid)}: {msg_left}")
            msg_right = messages.Message(
                type=messages.MessageType.SET_NEIGHBOUR_RIGHT.value,
                value=str(right),
            )
            print(f"  Sende RIGHT-Info an {buildTopic(pid)}: {msg_right}")
            await client.publish(buildTopic(pid), json.dumps(asdict(msg_left)).encode())
            await client.publish(buildTopic(pid), json.dumps(asdict(msg_right)).encode())

            print("Master started to send M to clients")

            #for pid, m0 in START_VALUES.items():
            msg = messages.Message(
                type=messages.MessageType.SET_M.value,
                value=str(START_VALUES[pid]),
            )
            payload = json.dumps(asdict(msg)).encode()
            print("encoded")
            await client.publish(buildTopic(pid), payload)
            print("published")

        # Laufzeit begrenzen (aktuell 5 s)
        await asyncio.sleep(5)

        # Ergebnisse abfragen
        msg = messages.Message(
            type=messages.MessageType.GET_M.value,
            value=""
        )

        payload_clients = json.dumps(asdict(msg)).encode()

        for pid in START_VALUES.keys():
            await client.publish(buildTopic(pid), payload_clients)
            print("sende message an client {pid}")



        results: list[int] = []
        async for message in client.messages:
            data = json.loads(message.payload.decode())
            msg = messages.Message(**data)

            if msg.type == messages.MessageType.GET_M.value:
                m_val = int(msg.value)
                results.append(m_val)
                print(f"[MASTER] Antwort erhalten: {m_val}")

            # Wenn wir von allen Peers eine Antwort haben → raus aus der Schleife
            if len(results) >= len(START_VALUES):
                break

        print("\n[MASTER] Endergebnisse:")
        for i, m_val in enumerate(results, start=1):
            print(f"Antwort {i}: M = {m_val}")

if __name__ == "__main__":
    if sys.platform == "win32":
        from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy

        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

    asyncio.run(master())
