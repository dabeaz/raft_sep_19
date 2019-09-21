# control.py

from queue import Queue
import pickle
import threading
import time
import random
import os

from .raftcore import Message, RaftMachine
from . import config
        
class RaftControl:
    def __init__(self, net, apply=None):
        # Some information about the network topology
        self.address = net.address
        self.net = net
        self.numservers = net.numservers
        self.quorum = (self.numservers // 2) + 1
        self.machine = RaftMachine(self, apply)
        self.events = Queue()
        self._alive = False

        # Persistent log
        self._restore_log()
        self._plog = open(f'{self.address}-plog.p', 'ab')
        self._plog.seek(0, os.SEEK_END)
        
        # Debugging/Testing.
        self.paused = False

    # --- Functions to be called by the underlying state machine
    
    def leader_alive(self):
        '''
        Resets the election timer by setting a flag that indicates the leader is alive.
        '''
        self._alive = True
        
    def send_message(self, msg):
        '''
        Sends a message.  The message destination is assumed to be encoded in the
        message itself as a "dest" attribute.
        '''
        self.net.send(msg.dest, pickle.dumps(msg))

    def client_append_entries(self, values):
        '''
        Function used by *clients* to append values to the log.  This only works
        if the underlying state machine is a leader. Returns True/False on success.
        '''
        if self.machine.state != 'LEADER':
            return False
        self.events.put(('append_entries', values))
        return True

    def persist(self, transaction):
        '''
        Write a transaction to the controller's persistent log.  This is used
        to manage persistent state required by the state machine.
        '''
        pickle.dump(transaction, self._plog)
        self._plog.flush()

    # --- Persistent log. Restored on startup
    
    def _restore_log(self):
        nentries = 0
        try:
            with open(f'{self.address}-plog.p', 'rb') as f:
                while True:
                    try:
                        transaction = pickle.load(f)
                        self.machine.restore(transaction)
                        nentries += 1
                    except EOFError:
                        break
        except IOError:
            pass
        print(f"Restored {nentries} transactions")
        
    # --- Runtime threads.  These manage the controller internal event queue.
    
    def _receiver(self):
        '''
        Receive any message from the network and put on the event queue.
        '''
        while True:
            msg = self.net.recv()
            self.events.put(('message', pickle.loads(msg)))

    def _leader_heartbeat(self):
        '''
        Timer thread that generates heartbeat events on a periodic interval.
        '''
        while True:
            time.sleep(config.LEADER_HEARTBEAT)
            self.events.put(('heartbeat', None))

    def _election_timeout(self):
        '''
        Timer thread that generates randomized election timeout events.
        '''
        while True:
            self._alive = False
            time.sleep(config.ELECTION_TIMEOUT_BASE
                       + random.random()*config.ELECTION_TIMEOUT_SPREAD)
            # An event is only generated if no calls to leader_alive() have been made.
            if not self._alive:
                self.events.put(('timeout', None))
            
    def _event_loop(self):
        '''
        Event loop for the controller.
        '''
        print(f"Controller {self.address} running")
        while True:
            evt, arg = self.events.get()
            if self.paused:
                continue
            if evt == 'heartbeat':
                self.machine.append_entries([])
            elif evt == 'timeout':
                self.machine.become_candidate()
            elif evt == 'message':
                self.machine.handle_Message(arg)
            elif evt == 'append_entries':
                self.machine.append_entries(arg)
            else:
                raise RuntimeError(f'Unknown event {evt}')

    def start(self):
        self.net.start()
        threading.Thread(target=self._receiver, daemon=True).start()
        threading.Thread(target=self._leader_heartbeat, daemon=True).start()
        threading.Thread(target=self._election_timeout, daemon=True).start()
        threading.Thread(target=self._event_loop, daemon=True).start()

    def run_tcp_server(self, address, handler):
        def _server():
            from socket import socket, SOCK_STREAM, AF_INET, SOL_SOCKET, SO_REUSEADDR
            # Run a TCP application server on a given address.
            # On connection. Call handler
            sock = socket(AF_INET, SOCK_STREAM)
            sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
            sock.bind(address)
            sock.listen(1)
            print("TCP SERVER RUNNING", sock)
            while True:
                client, addr = sock.accept()
                threading.Thread(target=handler, args=(client, self), daemon=True).start()
                
        threading.Thread(target=_server, daemon=True).start()

        
        
            

        

