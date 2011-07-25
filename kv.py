import event
import os

class Log:
    def __init__(self, filename):
        # Load log - assume we are the only one using that file
        self.data = {}
        log = None
        try:
            log = open(filename, 'r')
        except IOError:
            pass
        if log:
            line = log.readline()
            while line:
                command, key, value = line.split(None, 3)
                if command == 'SET':
                    self.data[key] = value
                else:
                    raise ValueError()
                line = log.readline()
            log.close()
        
        # Open log for append
        self.log = os.open(filename, os.O_CREAT | os.O_WRONLY | os.O_APPEND)
    
    def set(self, key, value, callback):
        self.data[key] = value
        line = 'SET ' + key + ' ' + value + '\r\n'
        line_len = len(line)
        offset = 0
        while offset < line_len:
            offset += os.write(self.log, line[offset:])
        os.fsync(self.log) # If we could put this on the epoll queue, we could avoid blocking the whole server. Alternatively, we could do this in another thread.
        callback() # I think we could even get away with calling callback() in the writer thread, since it ultimately just sets us up to read the next command from the client.
        
    def get(self, key):
        if key in self.data:
            return self.data[key]

class Server:
    def __init__(self, events, address = '0.0.0.0', port = 1234):
        log = Log('logfile.log')
        
        def accept_connection(connection, address):
            def readline():
                connection.readline(get_line)
            
            def write_response(response):
                connection.write(response + '\r\n', readline)
            
            def get_line(line):
                try:
                    command, args = line.split(None, 1)
                    if command == 'SET':
                        key, value = args.split(None, 2)
                        def success():
                            write_response('OK')
                        log.set(key, value, success)
                    elif command == 'GET':
                        key = args.split(None, 1)[0]
                        value  = log.get(key)
                        if value is not None:
                            write_response('OK\r\n' + log.get(key))
                        else:
                            write_response('MISSING')
                    elif command == 'COMPACT':
                        write_response('NOPE')
                    else:
                        raise ValueError()
                except ValueError:
                    write_response('ERROR')
            
            readline()
        event.ServerSocket(events, address, port, accept_connection)
        
def main():
    events = event.Events()
    Server(events)
    events.run()
    
if __name__ == "__main__":
    main()
