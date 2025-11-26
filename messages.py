# Messages

# Message: Initial Verteilung der Zahlen M vom Server

# Message: Startbedinungung: Seinem NAchbarn M mitteilen
# Messages: ganzer Algorihtmus durchgehend

# Messages: Endbedingung: Request: Server fragt M an
# Messages: Endbedingung: Response: alle CLients senden M zurück

from dataclasses import dataclass, asdict
import json

from enum import Enum

@dataclass
class Message:
    def __init__(self, type: str, value: str):
        self.type = type      # z.B. "update"
        self.value = value            # z.B. deine Zahl M

class MessageType(Enum):
    SET_M = "SET_M"
    SET_Y = "SET_Y"
    GET_M = "GET_M"
    SET_NEIGHBOUR_LEFT = "SET_NEIGHBOUR_LEFT"
    SET_NEIGHBOUR_RIGHT = "SET_NEIGHBOUR_RIGHT"

