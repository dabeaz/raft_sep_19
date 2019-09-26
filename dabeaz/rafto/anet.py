# anet.py
#
# Raft network layer.  Allows messages to be sent/received between
# different servers.  This is only for the Raft Server-Server communication.
# Not for application level networking. 

import logging
from curio.socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import curio

from . import config

async def send_message_size(sock, nbytes):
    sz = b'%10d' % nbytes
    await sock.sendall(sz)
    
async def send_message(sock, msg):
    await send_message_size(sock, len(msg))
    await sock.sendall(msg)

async def recv_exactly(sock, nbytes):
    parts = [ ]
    while nbytes > 0:
        part = await sock.recv(nbytes)
        if not part:
            raise ConnectionError("Connection Closed")
        parts.append(part)
        nbytes -= len(part)
    return b''.join(parts)

async def recv_message_size(sock):
    sz = await recv_exactly(sock, 10)
    return int(sz)

async def recv_message(sock):
    # Need to know how big the message is in order to get it
    size = await recv_message_size(sock)
    return (await recv_exactly(sock, size))



class AsyncRaftNetBase:
    '''
    Abstract base class for networking layer
    '''
    async def send(self, dest, msg):
        '''
        Send a message to a specified destination.  Does not wait for the
        message to be delivered. Does not guarantee message delivery.
        Returns immediately.
        '''
        raise NotImplementedError()

    async def recv(self):
        '''
        Receive a message from any server.  Waits until a message arrives.
        Does not include any information about the message sender.  If this
        is desired, that information should be encoded as part of the message
        payload itself.
        '''
        raise NotImplementedError()
        
    async def start(self):
        '''
        Start the networking layer.  If there are any background servers or
        other things that need to start in the background, launch them here.
        '''
        raise NotImplementedError()

class AsyncTCPRaftNet(AsyncRaftNetBase):
    def __init__(self, address):
        self.address = address
        self.numservers = len(config.SERVERS)
        self._outgoing = {n : curio.Queue() for n in config.SERVERS }
        self._socks = { n: None for n in config.SERVERS }  # The other servers
        self.server_sock = None
        self._msgqueue = curio.Queue()   # Incoming messages
        self.log = logging.getLogger(f'net.{address}')

    async def send(self, dest, msg):
        await self._outgoing[dest].put(msg)

    async def _sender(self, dest):
        while True:
            msg = await self._outgoing[dest].get()
            self.log.debug("Sending %r to %s", msg, dest)
            if self._socks[dest] is None:
                try:
                    self.log.debug("Trying connection to: %s - %s", dest, config.SERVERS[dest])
                    self._socks[dest] = socket(AF_INET, SOCK_STREAM)
                    # Discussion: Connecting to a remote machine might take a long time.
                    # Is send() supposed to wait for this to happen?  Or does send() try
                    # to return as fast as possible? 
                    await self._socks[dest].connect(config.SERVERS[dest])   # Sore point
                    self.log.info("Connected to: %s - %s", dest, config.SERVERS[dest])
                except IOError as err:
                    self._socks[dest] = None
                    self.log.debug("Connection to: %s failed", exc_info=True)
            if self._socks[dest]:
                try:
                    # Discussion. You send a message, but send() blocks due to TCP flow
                    # control or some other buffering issue.  Is this supposed to happen
                    # or should send() return immediately?
                    await send_message(self._socks[dest], msg)
                except IOError as err:
                    self.log.debug("Send to %s failed", dest, exc_info=True)
                    await self._socks[dest].close()
                    self._socks[dest] = None
            else:
                self.log.info("Server %s offline", dest)

    async def recv(self):
        return await self._msgqueue.get()
                                
    async def connection_server(self):
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
            client, addr = await sock.accept()
            self.log.info("Received connection from: %s", addr)
            await curio.spawn(self.handle_client, client, daemon=True)
            
    async def handle_client(self, sock):
        async with sock:
            while True:
                msg = await recv_message(sock)
                self.log.debug("Received message: %r", msg)
                await self._msgqueue.put(msg)

    async def start(self):
        await curio.spawn(self.connection_server, daemon=True)
        for n in config.SERVERS:
            await curio.spawn(self._sender, n, daemon=True)

            



        
        
