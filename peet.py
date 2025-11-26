import asyncio
import random

class Peer:
    def __init__(self, pid: str, start_value: int):
        self.pid = pid
        self.M = start_value
        self.inbox = asyncio.Queue()
        self.neighbors: list["Peer"] = []

    def connect(self, neighbors):
        self.neighbors = neighbors

    async def send(self, value: int):
        # Asynchron an alle Nachbarn senden
        for n in self.neighbors:
            await n.inbox.put(value)

    async def run(self):
        print(f"[{self.pid}] gestartet mit M={self.M}")
        # Initialwert an Nachbarn senden
        await self.send(self.M)

        while True:
            y = await self.inbox.get()
            if y < self.M:
                old = self.M
                self.M = ((self.M - 1) % y) + 1
                print(f"[{self.pid}] erhielt {y}, neues M={self.M} (alt={old})")
                await self.send(self.M)
            await asyncio.sleep(0)  # Event-Loop nicht blockieren
