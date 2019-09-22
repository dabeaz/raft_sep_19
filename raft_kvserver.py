# raft_kvserver.py
#

import ratnet
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import json
import threading
import rafto

class KVStore:
    def __init__(self):
        self.data = { }
        
    def get(self, key):
        return self.data.get(key, '')

    def set(self, key, value):
        self.data[key] = value

    def delete(self, key):
        del self.data[key]

store = KVStore()

def apply_changes(action):
    meth, *args = action
    getattr(store, meth)(*args)
    
def handle_client(sock, control):
    # If not leader.  We're done
    if control.machine.state != 'LEADER':
        sock.close()
        return

    # Send an ack that we're okay as a client
    sock.sendall(b'OK')
    try:
        print("HANDLING CLIENT", sock)
        while True:
            request_msg = ratnet.recv_message(sock)
            print("GOT REQUEST:", request_msg)
            if control.machine.state != 'LEADER':
                break
            request = json.loads(request_msg.decode('utf-8'))
            if request['op'] == 'get':
                result = store.get(request['key'])
            elif request['op'] == 'delete':
                control.client_append_entries([('delete', request['key'])])
                result = 'ok'
            elif request['op'] == 'set':
                control.client_append_entries([('set', request['key'], request['value'])])
                result = 'ok'
            else:
                raise ConnectionError(f"Bad Request: {request['op']}")
            ratnet.send_message(sock, json.dumps({'result': result}).encode('utf-8'))
    except ConnectionError:
        print("Goodbye")
        
    sock.close()

def main(argv):
    import time
    import logging
    logging.basicConfig(level=logging.INFO)
    
    if len(argv) != 2:
        raise SystemExit("Usage: raft_kvstore.py serverN")
    # control = rafto.make_server(int(argv[1]), apply_changes)
    control = rafto.make_async_server(int(argv[1]), apply_changes)
    control.run_tcp_server(rafto.config.APPSERVERS[int(argv[1])], handle_client)
    print("KV Server running on", rafto.config.APPSERVERS[int(argv[1])])
    while True:
        time.sleep(10)
        
if __name__ == '__main__':
    import sys
    main(sys.argv)

