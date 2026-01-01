from dataclasses import dataclass
from enum import Enum

@dataclass
class Message:
    type: str
    value: str

class MessageType(Enum):
    SET_NODE_COUNT = "SET_NODE_COUNT"
    SET_NODE_PIDS = "SET_NODE_PIDS"

    SET_M = "SET_M"
    SET_Y = "SET_Y"
    GET_M = "GET_M"

    SET_NEIGHBOUR_LEFT = "SET_NEIGHBOUR_LEFT"
    SET_NEIGHBOUR_RIGHT = "SET_NEIGHBOUR_RIGHT"
