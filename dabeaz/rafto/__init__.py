# __init__.py

from .control import RaftControl
from .net import TCPRaftNet
from . import config

def make_server(n, apply):
    # Create a Raft server for node n. Returns the associated controller
    net = TCPRaftNet(n)
    control = RaftControl(net, apply)
    control.start()
    return control

def make_async_server(n, apply):
    from .acontrol import AsyncRaftControl
    from .anet import AsyncTCPRaftNet
    net = AsyncTCPRaftNet(n)
    control = AsyncRaftControl(net, apply)
    control.start()
    return control


    
