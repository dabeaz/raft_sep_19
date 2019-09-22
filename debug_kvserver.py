# test_kvserver.py
import subprocess
import time
import atexit

procs = [ None ] * 5
def launch(n):
    p = subprocess.Popen(['python', 'raft_kvserver.py', str(n)],                        
                               creationflags=subprocess.CREATE_NEW_CONSOLE)
    if procs[n]:
        procs[n].terminate
    procs[n] = p

def kill(n):
    procs[n].terminate()
    
for n in range(5):
    launch(n)

atexit.register(lambda: [p.terminate() for p in procs])





        
