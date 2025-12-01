import asyncio
import sys

import aiomqtt
import argparse

from peer import Peer   # falls die Klasse in derselben Datei ist, diesen Import weglassen

DEFAULT_BROKER_IP = "localhost"
DEFAULT_BROKER_PORT = 1883


async def async_main():
    parser = argparse.ArgumentParser(description="Starte einen Peer-Prozess im Ring.")
    parser.add_argument("--pid", type=int, required=True, help="ID dieses Peers")
    parser.add_argument("--broker-ip", type=str, default=DEFAULT_BROKER_IP, help="IP des MQTT-Brokers")
    parser.add_argument("--broker-port", type=int, default=DEFAULT_BROKER_PORT, help="Port des MQTT-Brokers")

    args = parser.parse_args()

    peer = Peer(
        broker_ip=args.broker_ip,
        pid=args.pid,
        broker_port=args.broker_port,
    )

    await peer.start()

async def main_short():

    peer = Peer(
        broker_ip="127.0.0.1",
        pid=1,
        broker_port=1883,
    )

    await peer.start()

    peer = Peer(
        broker_ip="127.0.0.1",
        pid=2,
        broker_port=1883,
    )

    await peer.start()

    peer = Peer(
        broker_ip="127.0.0.1",
        pid=3,
        broker_port=1883,
    )

    await peer.start()


def main():
    asyncio.run(main_short())


if __name__ == "__main__":
    if sys.platform == "win32":
        from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy

        asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
