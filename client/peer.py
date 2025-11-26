import asyncio
import random
import json
import asyncio
from asyncio_mqtt import Client
from dataclasses import dataclass, asdict

from utils.buildTopic import buildTopic, MASTER_PID
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
            await self.client.subscribe(buildTopic(self.pid))

            receiver = asyncio.create_task(self.handle_msg())
            worker = asyncio.create_task(self.run())

            await asyncio.gather(receiver, worker)

    async def handle_msg(self):
        async with self.client.messages() as messageList:
            async for message in messageList:
                data = json.loads(message)
                msg = messageList.Message(**data)

                await self.exec_msg(msg)

    async def exec_msg(self, msg: messages.Message):
        match msg.type:
            case messages.MessageType.SET_NEIGHBOUR_LEFT:
                self.neighbour_topics["left"] = buildTopic(int(msg.value))
            case messages.MessageType.SET_NEIGHBOUR_RIGHT:
                self.neighbour_topics["right"] = buildTopic(int(msg.value))
            case messages.MessageType.SET_M:
                self.M = msg.value
                self.m_ready.set()
            case messages.MessageType.SET_Y:
                await self.y_queue.put(msg.value)
            case messages.MessageType.GET_M:
                msg = messages.Message(
                    type=messages.MessageType.GET_M.value, value=str(self.M))
                payload = json.dumps(asdict(msg)).encode()
                await self.client.publish(buildTopic(MASTER_PID), payload)


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
