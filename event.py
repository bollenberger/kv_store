import select
import socket
import os

# Thrown if you try to read or write the same IO while the same operation is already pending. Don't do that.
class ReentrantIOException(Exception):
    pass
    
# A wrapper for IO objects that performs operations in an event driven manner using epoll
class IO(object):
    def __init__(self, events, fd):
        self.events = events
        self.fd = fd
        
        self.data_in = []
        self.on_in = None
        self.on_out = None
        
        def when_ready(event):
            if event & select.EPOLLIN and self.on_in is not None:
                self.on_in()
            if event & select.EPOLLOUT and self.on_out is not None:
                self.on_out()
            if event & select.EPOLLHUP:
                self.close()
        events.register(fd, select.EPOLLIN | select.EPOLLOUT, when_ready)
        
    def close(self):
        self.events.unregister(self.fd)
        self.do_close()
    
    def read(self, callback):
        if self.on_in is not None:
            raise ReentrantIOException()
        
        def on_in():
            self.on_in = None
            self.data_in.append(self.do_read())
            data = ''.join(self.data_in)
            self.data_in = []
            callback(data)
        
        self.on_in = on_in
    
    def write(self, data, callback):
        if self.on_out is not None:
            raise ReentrantIOException()
        
        offset = [0]
        def on_out():
            offset[0] += self.do_write(data[offset[0]:])
            if offset[0] >= len(data):
                self.on_out = None
                callback()
        self.on_out = on_out
    
    def readline(self, callback, newline = '\r\n'):
        def got_data(data):
            if newline in data:
                line, data = data.split(newline, 1)
                callback(line)
            else:
                self.read(got_data)
            self.data_in.append(data) # Put back partial line data.
        self.read(got_data)

class Socket(IO):
    def __init__(self, events, socket):
        self.socket = socket
        super(Socket, self).__init__(events, socket.fileno())
    
    def do_read(self):
        return self.socket.recv(1024)
    
    def do_write(self, data):
        return self.socket.send(data)
    
    def do_close(self):
        self.socket.close()

# Listen on an address and port
class ServerSocket(object):
    def __init__(self, events, address, port, callback):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((address, port))
        sock.listen(5)
        sock.setblocking(0)
    
        def do_accept(event):
            connection, address = sock.accept()
            connection.setblocking(0)
            callback(Socket(events, connection), address)
        events.register(sock.fileno(), select.EPOLLIN, do_accept)

class Events:
    def __init__(self):
        self.epoll = select.epoll()
        self.handlers = {}
    
    def register(self, fileno, mask, handler):
        self.handlers[fileno] = handler
        self.epoll.register(fileno, mask)
    
    def unregister(self, fileno):
        self.epoll.unregister(fileno)
        del self.handlers[fileno]
    
    def run(self):
        while True:
            for fileno, event in self.epoll.poll():
                self.handlers[fileno](event)
