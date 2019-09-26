# kvclient.py
from socket import socket, AF_INET, SOCK_STREAM
import ratnet
import json
import rafto
import time


class KVClient:
    def __init__(self):
        self.sock = None

    def _connect(self):
        while not self.sock:
            for n, addr in rafto.config.APPSERVERS.items():
                sock = socket(AF_INET, SOCK_STREAM)
                try:
                    print("TRYING", addr)
                    sock.connect(addr)
                except OSError as e:
                    continue
                msg = sock.recv(2)
                #print("MSG:", msg)
                if msg != b'OK':
                    sock.close()
                    continue
                self.sock = sock
                break
            print("Could not connect. Retrying.")
        print('Connected to:', self.sock)
                
    def get(self, key):
        while True:
            if not self.sock:
                self._connect()
            try:
                msg = json.dumps({'op': 'get', 'key': key})
                ratnet.send_message(self.sock, msg.encode('utf-8'))
                resp = ratnet.recv_message(self.sock)
                result = json.loads(resp.decode('utf-8'))
                return result['result']
            except OSError as e:
                self.sock.close()
                self.sock = None
                print("Connection error:", e)
                
    def set(self, key, value):
        while True:
            if not self.sock:
                self._connect()
            try:
                msg = json.dumps({'op': 'set', 'key': key, 'value': value})
                ratnet.send_message(self.sock, msg.encode('utf-8'))
                resp = ratnet.recv_message(self.sock)
                result = json.loads(resp.decode('utf-8'))
                return result['result']
            except OSError as e:
                self.sock.close()
                self.sock = None
                print("Connection error:", e)

    def delete(self, key):
        while True:
            if not self.sock:
                self._connect()
            try:
                msg = json.dumps({'op': 'delete', 'key': key})
                ratnet.send_message(self.sock, msg.encode('utf-8'))
                resp = ratnet.recv_message(self.sock)
                result = json.loads(resp.decode('utf-8'))
                return result['result']
            except OSError as e:
                self.sock.close()
                self.sock = None
                print("Connection error:", e)

    
if __name__ == '__main__':
    kv = KVClient()
    
