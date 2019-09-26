# test_kvserver.py
import subprocess
import time
import atexit
import os

procs = [ None ] * 5
def launch(n):
    if os.name != 'posix':
        p = subprocess.Popen(['python', 'raft_kvserver.py', str(n)],                        
                             creationflags=subprocess.CREATE_NEW_CONSOLE)
    else:
        p = subprocess.Popen(['python', 'raft_kvserver.py', str(n)])

    if procs[n]:
        procs[n].terminate
    procs[n] = p

def kill(n):
    procs[n].terminate()
    
for n in range(5):
    launch(n)

atexit.register(lambda: [p.terminate() for p in procs])





        
