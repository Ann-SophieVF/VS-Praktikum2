import asyncio
import json
from asyncio import tasks
from dataclasses import asdict
from asyncio_mqtt import Client

import messages
from client.peer import Peer
from utils.buildTopic import buildTopic, MASTER_PID

BROKER_IP = "localhost"
BROKER_PORT = 1883

start_values ={
    1: 108,
    2: 76,
    3: 12,
    4: 60,
    5: 36
}
# Server

async def master():
    async with Client(BROKER_IP, BROKER_PORT) as client:
        print("Master verbundn")
        pids = list(start_values.keys())
        n = len(pids)

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
            await client.publish(buildTopic(pid), json.dumps(asdict(msg_left)).encode())
            await client.publish(buildTopic(pid), json.dumps(asdict(msg_right)).encode())

            print("Master started to send M to clients")

            for pid, m0 in START_VALUES.items():
                msg = messages.Message(
                    type=messages.MessageType.SET_M.value,
                    value=str(m0),
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

    for pid in start_values.keys():
        await client.publish(buildTopic(pid), payload_clients)
        print("sende message an client {pid}")

    await client.subscribe(buildTopic(MASTER_PID))

    results: list[int] = []
    async with client.messages() as messages_stream:
        while len(results) < len(start_values):
            message = await messages_stream.__anext__()
            data = json.loads(message.payload.decode())
            msg = messages.Message(**data)

            if msg.type == messages.MessageType.GET_M.value:
                m_val = int(msg.value) #cast damit zahl korrekt ausgegeben wird
                results.append(m_val)
            print(f"[MASTER] Antwort erhalten: {m_val}")

    print("\n[MASTER] Endergebnisse:")
    for i, m_val in enumerate(results, start=1):
        print(f"Antwort {i}: M = {m_val}")

if __name__ == "__main__":
    asyncio.run(master())
