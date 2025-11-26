import asyncio
import json
from asyncio import tasks
from dataclasses import asdict
from asyncio_mqtt import Client

import messages
from client.peer import Peer
from utils.buildTopic import buildTopic

BROKER_IP = "localhost"
BROKER_PORT = 1883

start_values ={
    #machen wir das als feldchen?
}
# Server
async def main():
    # Startwerte
    start_values = [108, 76, 12, 60, 36]
    peer_count = 5
    #peers = [Peer(f"P{i+1}", val) for i, val in enumerate(start_values)]

async def master():
    async with Client(BROKER_IP, BROKER_PORT) as client:
        for pid, m0 in start_values.items():
            msg = messages.Message(
                type=messages.MessageType.SET_M.value,
                value=str(m0)
            )
        print("Master started to send M to clients")

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

    payload = json.dumps(asdict(msg)).encode()

    await client.subscribe(buildTopic(MASTER_PID))
    # Tasks abbrechen
    for t in tasks:
        t.cancel()

if __name__ == "__main__":
    asyncio.run(master())
