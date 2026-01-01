import asyncio
import json
from aiomqtt import Client

import messages
from peer import Peer
from utils.buildTopic import buildClientTopic


class ClientNode:
    def __init__(self, broker_ip: str, broker_port: int, client_id: int):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.client_id = client_id

        self.num_peers: int | None = None
        self.assigned_pids: list[int] = []

        self.config_ready = asyncio.Event()

        self._mqtt_client: Client | None = None
        self._peer_tasks: list[asyncio.Task] = []

    async def start(self):
        async with Client(self.broker_ip, self.broker_port) as client:
            self._mqtt_client = client
            print(f"[CLIENT {self.client_id}] Verbunden mit Broker {self.broker_ip}:{self.broker_port}")

            await client.subscribe(buildClientTopic(self.client_id))
            print(f"[CLIENT {self.client_id}] Warte auf Konfiguration auf Topic '{buildClientTopic(self.client_id)}'")

            config_listener = asyncio.create_task(self.handle_msg(), name=f"client-config-{self.client_id}")

            await self.config_ready.wait()

            print(f"[CLIENT {self.client_id}] Konfiguration erhalten: PIDs={self.assigned_pids}")

            await self._start_peers()

            config_listener.cancel()

            await asyncio.gather(*self._peer_tasks)

    async def handle_msg(self):
        assert self._mqtt_client is not None

        async for message in self._mqtt_client.messages:
            try:
                data = json.loads(message.payload.decode())
                msg = messages.Message(**data)
                await self.exec_msg(msg)
            except Exception as e:
                print(f"[CLIENT {self.client_id}] Fehler beim Verarbeiten der Nachricht: {e}")
                print(f"[CLIENT {self.client_id}] payload war: {message.payload!r}")

    async def exec_msg(self, msg: messages.Message):
        if msg.type == messages.MessageType.SET_NODE_PIDS.value:
            pids = json.loads(msg.value)
            self.assigned_pids = [int(x) for x in pids]
            self.num_peers = len(self.assigned_pids)
            self.config_ready.set()

        elif msg.type == messages.MessageType.SET_NODE_COUNT.value:
            count = int(msg.value)
            self.num_peers = count
            self.assigned_pids = list(range(1, count + 1))
            print(f"[CLIENT {self.client_id}] WARNUNG: SET_NODE_COUNT erzeugt ggf. PID-Kollisionen über mehrere Rechner!")
            self.config_ready.set()

        else:
            print(f"[CLIENT {self.client_id}] Unbekannter Nachrichtentyp: {msg.type}")

    async def _start_peers(self):
        for pid in self.assigned_pids:
            peer = Peer(
                broker_ip=self.broker_ip,
                pid=pid,
                broker_port=self.broker_port,
            )
            task = asyncio.create_task(peer.start(), name=f"peer-{pid}")
            self._peer_tasks.append(task)
            print(f"[CLIENT {self.client_id}] Peer mit pid={pid} gestartet")

