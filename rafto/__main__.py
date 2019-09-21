# __main__.py

import sys
if len(sys.argv) != 2:
    raise SystemExit(f"Usage: python -i -m rafto serverno")

from .net import TCPRaftNet
serv = TCPRaftNet(int(sys.argv[1]))
serv.start()

