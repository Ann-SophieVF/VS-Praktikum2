class Neighbour:
    def __init__(self, pid: str, ip: str, port: int):
        self.pid = pid
        self.ip = ip
        self.port = port
        self.connection = None

    def connect(self):
        #todo:stell connection her