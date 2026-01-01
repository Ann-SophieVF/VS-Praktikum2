import asyncio
import random
import json
import aiomqtt
from aiomqtt import Client
from dataclasses import dataclass, asdict

from utils.buildTopic import buildNodeTopic, MASTER_PID
import messages


class Peer:
    def __init__(self, broker_ip:str,pid: int, broker_port:int): #broker ip und port übergeben für die verbindung
        self.client = None
        self.pid = pid
        self.M = None
        self.m_ready = asyncio.Event()
        self.y_queue = asyncio.Queue()
        #self.broker_host = broker_host
        self.broker = { "port": broker_port, "ip": broker_ip }
        self.neighbour_topics = { "left": None, "right": None}


    async def start(self):
        async with Client(self.broker["ip"], self.broker["port"]) as client:
            self.client = client
            await self.client.subscribe(buildNodeTopic(self.pid))

            receiver = asyncio.create_task(self.handle_msg())
            worker = asyncio.create_task(self.run())

            await asyncio.gather(receiver, worker)

    async def handle_msg(self):
        async for message in self.client.messages:
            try:
                data = json.loads(message.payload.decode())
                msg = messages.Message( ** data)
                await self.exec_msg(msg)
            except Exception as e:
                print(f"[{self.pid}] Fehler beim Verarbeiten der Nachricht: {e}")
                print(f"[{self.pid}] payload war: {message.payload!r}")




    async def exec_msg(self, msg: messages.Message):
        match msg.type:
            case messages.MessageType.SET_NEIGHBOUR_LEFT.value:
                self.neighbour_topics["left"] = buildNodeTopic(int(msg.value))
            case messages.MessageType.SET_NEIGHBOUR_RIGHT.value:
                self.neighbour_topics["right"] = buildNodeTopic(int(msg.value))
            case messages.MessageType.SET_M.value:
                self.M = int(msg.value)
                self.m_ready.set()
            case messages.MessageType.SET_Y.value:
                await self.y_queue.put(int(msg.value))
            case messages.MessageType.GET_M.value:
                msg = messages.Message(
                    type=messages.MessageType.GET_M.value, value=str(self.M))
                payload = json.dumps(asdict(msg)).encode()
                await self.client.publish(buildNodeTopic(MASTER_PID), payload)
            case _ :
                print("komische messagi reingekommeni, ikannixi verarbeiten")


    async def send(self, value: int):
        msg = messages.Message(
            type = messages.MessageType.SET_Y.value, value = str(value) )
        payload = json.dumps(asdict(msg)).encode()
        # Asynchron an alle Nachbarn senden
        await self.client.publish(self.neighbour_topics["left"], payload)
        await self.client.publish(self.neighbour_topics["right"], payload)

    async def run(self):

        await self.m_ready.wait()

        print(f"[{self.pid}] gestartet mit M={self.M}")
        # Initialwert an Nachbarn senden
        await self.send(self.M)

        while True:
            y = await self.y_queue.get()
            if y < self.M:
                old = self.M
                self.M = ((self.M - 1) % y) + 1
                print(f"[{self.pid}] erhielt {y}, neues M={self.M} (alt={old})")
                await self.send(self.M)
            await asyncio.sleep(0)  # Event-Loop nicht blockieren
