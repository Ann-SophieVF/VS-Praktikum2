import asyncio
from peet import Peer


async def main():
    # Beispiel-Startwerte (wie in eurer Aufgabe)
    start_values = [108, 76, 12, 60, 36]
    peers = [Peer(f"P{i+1}", val) for i, val in enumerate(start_values)]

    # Ring aufbauen
    for i, p in enumerate(peers):
        prev_peer = peers[(i - 1) % len(peers)]
        next_peer = peers[(i + 1) % len(peers)]
        p.connect([prev_peer, next_peer])

    # Alle Peers als Tasks starten
    tasks = [asyncio.create_task(p.run()) for p in peers]

    # Laufzeit begrenzen (z. B. 5 s)
    await asyncio.sleep(5)

    # Ergebnisse abfragen
    for p in peers:
        print(f"Ergebnis {p.pid}: M = {p.M}")

    # Tasks abbrechen
    for t in tasks:
        t.cancel()

if __name__ == "__main__":
    asyncio.run(main())
