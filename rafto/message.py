# message.py

def send_message_size(sock, nbytes):
    sz = b'%10d' % nbytes
    sock.sendall(sz)
    
def send_message(sock, msg):
    send_message_size(sock, len(msg))
    sock.sendall(msg)

def recv_exactly(sock, nbytes):
    parts = [ ]
    while nbytes > 0:
        part = sock.recv(nbytes)
        if not part:
            raise ConnectionError("Connection Closed")
        parts.append(part)
        nbytes -= len(part)
    return b''.join(parts)

def recv_message_size(sock):
    sz = recv_exactly(sock, 10)
    return int(sz)

def recv_message(sock):
    # Need to know how big the message is in order to get it
    size = recv_message_size(sock)
    return recv_exactly(sock, size)

def smoke_test():
    from socket import socketpair
    s1, s2 = socketpair()    # Creates a pair of sockets already connected to each other
    # Try to send a basic message. Make sure it works at all.
    send_message(s1, b"hello")
    assert recv_message(s2) == b"hello"
    s1.close()
    s2.close()

smoke_test()
