MASTER_PID = 0

def buildNodeTopic(pid:int):     return f"ring/node{str(pid)}"
def buildClientTopic(cid:int):     return f"ring/client{str(cid)}"

