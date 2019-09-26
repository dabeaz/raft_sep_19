# net.py
#
# Raft network layer.  Allows messages to be sent/received between
# different servers.  This is only for the Raft Server-Server communication.
# Not for application level networking. 

import logging
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import threading
import queue

from . import config
from . import message

class RaftNetBase:
    '''
    Abstract base class for networking layer
    '''
    def send(self, dest, msg):
        '''
        Send a message to a specified destination.  Does not wait for the
        message to be delivered. Does not guarantee message delivery.
        Returns immediately.
        '''
        raise NotImplementedError()

    def recv(self):
        '''
        Receive a message from any server.  Waits until a message arrives.
        Does not include any information about the message sender.  If this
        is desired, that information should be encoded as part of the message
        payload itself.
        '''
        raise NotImplementedError()
        
    def start(self):
        '''
        Start the networking layer.  If there are any background servers or
        other things that need to start in the background, launch them here.
        '''
        raise NotImplementedError()

    
class TCPRaftNet(RaftNetBase):
    def __init__(self, address):
        self.address = address
        self.numservers = len(config.SERVERS)
        self._outgoing = {n : queue.Queue() for n in config.SERVERS }
        self._socks = { n: None for n in config.SERVERS }  # The other servers
        self.server_sock = None
        self._msgqueue = queue.Queue()   # Incoming messages
        self.log = logging.getLogger(f'net.{address}')

    def send(self, dest, msg):
        self._outgoing[dest].put(msg)

    def _sender(self, dest):
        while True:
            msg = self._outgoing[dest].get()
            self.log.debug("Sending %r to %s", msg, dest)
            if self._socks[dest] is None:
                try:
                    self.log.debug("Trying connection to: %s - %s", dest, config.SERVERS[dest])
                    self._socks[dest] = socket(AF_INET, SOCK_STREAM)
                    # Discussion: Connecting to a remote machine might take a long time.
                    # Is send() supposed to wait for this to happen?  Or does send() try
                    # to return as fast as possible? 
                    self._socks[dest].connect(config.SERVERS[dest])   # Sore point
                    self.log.info("Connected to: %s - %s", dest, config.SERVERS[dest])
                except IOError as err:
                    self._socks[dest] = None
                    self.log.debug("Connection to: %s failed", exc_info=True)
            if self._socks[dest]:
                try:
                    # Discussion. You send a message, but send() blocks due to TCP flow
                    # control or some other buffering issue.  Is this supposed to happen
                    # or should send() return immediately?
                    message.send_message(self._socks[dest], msg)
                except IOError as err:
                    self.log.debug("Send to %s failed", dest, exc_info=True)
                    self._socks[dest].close()
                    self._socks[dest] = None
            else:
                self.log.info("Server %s offline", dest)

    def recv(self):
        return self._msgqueue.get()
                                
    def connection_server(self):
        '''
        Thread that runs in the background listening for connections
        from the other Raft servers. Delivers messages to internal message queue.
        '''
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(config.SERVERS[self.address])
        sock.listen(1)
        self.log.info("Server %s running on %s", self.address, sock)
        while True:
            client, addr = sock.accept()
            self.log.info("Received connection from: %s", addr)
            threading.Thread(target=self.handle_client,
                             args=(client,), daemon=True).start()
            
    def handle_client(self, sock):
        with sock:
            while True:
                msg = message.recv_message(sock)
                self.log.debug("Received message: %r", msg)
                self._msgqueue.put(msg)

    def start(self):
        threading.Thread(target=self.connection_server, daemon=True).start()
        for n in config.SERVERS:
            threading.Thread(target=self._sender, args=(n,), daemon=True).start()
            



        
        
