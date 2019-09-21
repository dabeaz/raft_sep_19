# test.py

from rafto.control import RaftControl
from rafto.net import TCPRaftNet

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('net').propagate = False

c = [ RaftControl(TCPRaftNet(n), lambda e, n=n: print(f"[{n}]: Applying {e}")) for n in range(5) ] 
for _ in c:
    _.start()

r = [ _.machine for _ in c ]

#r[0].become_candidate()
