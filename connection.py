import asyncio, functools, socket, struct, time, traceback

from panda3d.core import Datagram, DatagramIterator
from panda3d.core import BufferedDatagramConnection, SocketAddress, SocketTCPListen

def blocking_execute(f):
    @functools.wraps(f)     
    async def inner(*args, **kwargs):         
        loop = asyncio.get_running_loop()        
        return await loop.run_in_executor(None, lambda: f(*args, **kwargs))     
    return inner

class Connection:
    '''
    This class manages the underlying connection for a Client or Server and its Clients.
    It's not reccomended to use this class directly and instead use the Client or Server classes instead.
    '''
    
    READ_BUFF_KB = 4096
    READ_BUFF = READ_BUFF_KB * 1000
    WRITE_BUFF_KB = 4096
    WRITE_BUFF = WRITE_BUFF_KB * 1000
    WRITE_FLUSH_POINT = 1400
    BACKLOG = 2800
    
    def __init__(self):
        self.address = None
        self.bufferconn = None
        self.closed = True
        self.listener = None
        self.server_address = None
        self.server_closed = True
        self.name = "NONE"
        self.datagrams = []
        
    @classmethod
    async def from_server(cls, bufferconn, address):
        self = cls()
        self.bufferconn = bufferconn
        self.address = address
        self.closed = False
        return self
        
    async def connect(self, addr, port):
        if not self.closed:
            return True # Already connected.

        self.address = SocketAddress(port)
        self.address.set_host(addr, port)
        
        self.bufferconn = BufferedDatagramConnection(self.READ_BUFF, self.WRITE_BUFF, self.WRITE_FLUSH_POINT)
        self.bufferconn.Reset()
        self.bufferconn.ClearAddresses()
        self.bufferconn.AddAddress(self.address)
        if not self.bufferconn.DoConnect():
            raise RuntimeError(f"Address '{self.address.get_ip_port()}' isn't valid or open for connection or is already in use by another application or connection.")
            return False
        
        connected = self.bufferconn.IsConnected()
        self.closed = not connected
        return connected

    async def listen(self, port, ipv6=False):
        self.server_address = SocketAddress(port)
        self.server_address.set_any_IP(port)
        if ipv6:
            self.server_address.set_any_IPv6(port)
        
        self.listener = SocketTCPListen()
        if ipv6:
            self.listener.SetV6Only(False)
            
        listening = self.listener.OpenForListen(self.server_address, self.BACKLOG)
        if listening:
            self.listener.SetNonBlocking()
            self.server_closed = False

        return listening

    async def close(self):
        if self.closed and self.server_closed:
            return

        if self.bufferconn:
            self.bufferconn.Reset()
            self.bufferconn.ClearAddresses()
        
        if self.listener:
            self.listener.Close()
            
        self.closed = True
        self.server_closed = True
            
        del self.address
        self.address = None

        del self.server_address
        self.server_address = None
            
        del self.bufferconn
        self.bufferconn = None
        
        del self.listener
        self.listener = None
        
        del self.datagrams
        self.datagrams = []
        
    def read_datagram(self):
        if not self.bufferconn:
            return False
            
        dg = Datagram()
        if not self.bufferconn.GetMessage(dg):
            return False
            
        self.datagrams.append(dg)
        return True
        
    async def read_datagrams(self):
        if self.closed:
            return

        while self.read_datagram():
            continue
            
        for dg in self.datagrams:
            await self.receive_datagram(dg)

        self.datagrams = []
        
    async def receive_datagram(self, dg):
        return
            
    async def send_datagram(self, dg):
        if self.closed or not self.bufferconn:
            return False

        return self.bufferconn.SendMessage(dg)
        
    @blocking_execute
    def accept_incoming_connection(self):
        if not self.listener:
            return None
        address = SocketAddress(0)
        bufferconn = BufferedDatagramConnection(self.READ_BUFF, self.WRITE_BUFF, self.WRITE_FLUSH_POINT)
        if not self.listener.GetIncomingConnection(bufferconn, address):
            return None
        bufferconn.SetNoDelay(True)
        bufferconn.SetNonBlocking()
        return (address, bufferconn)
        
    def get_address(self):
        if self.closed or not self.address:
            return None

        return (self.address.get_ip(), self.address.get_port())
        
    def get_server_address(self):
        if self.server_closed or not self.server_address:
            return None

        return (self.server_address.get_ip(), self.server_address.get_port())
        
    def set_name(self, name):
        self.name = name
        
    def get_name(self):
        return self.name
        
    def is_closed(self):
        return self.closed
        
    def is_server_closed(self):
        return self.server_closed

class Client(Connection):
    def __init__(self):
        super().__init__()
        
        self.timeout_period = 0
        self.set_name("CLIENT")
        
    @classmethod
    async def initialize(cls, addr, port):
        self = cls()
        await self.connect(addr, port)
        return self
        
    async def handle_lost_connection(self):
        await self.close()
            
    async def drain_interface(self):
        if self.closed or not self.bufferconn:
            return False
        return self.bufferconn.Flush()
            
    async def drain_interface_routine(self):
        while True:
            try:
                # Write out all of the data for all of our clients.
                await self.drain_interface()
                await asyncio.sleep(0.05)
                if self.closed: break
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        
    async def flush_interface(self):
        await self.read_datagrams()
        
    async def flush_interface_routine(self):
        while True:
            try:
                await self.flush_interface()
                await asyncio.sleep(0.05)
                if self.closed: break
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
                
    async def timeout(self):
        try:
            # Wait for our timeout period.
            await asyncio.sleep(self.timeout_period)
            
            # Don't do anything if we're already closed.
            if self.closed:
                return
            
            # Handle our lost connection.
            await self.handle_lost_connection()
        except KeyboardInterrupt as e:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            traceback.print_exception(e)
        
    drain_routine = drain_interface_routine
    flush = flush_interface
    flush_routine = flush_interface_routine
        
class Server(Connection):
    client_cls = Client
    
    def __init__(self):
        super().__init__()
        self.clients = []
        self.set_name("SERVER")
        
    @classmethod
    async def initialize(cls, port):
        self = cls()
        await self.start(port)
        return self
        
    async def start(self, port=0):
        if not self.server_closed:
            return
            
        await self.listen(port)
        self.server_closed = False
        
    async def close(self):
        if self.server_closed:
            return
            
        for i in range(0, len(self.clients)):
            client = self.clients[i]
            
            # Close the connection for the client.
            await client.close()
            
            # Remove the client from our list.
            del self.clients[i]
        
        del self.clients
        self.clients = []
        
        self.server_closed = True

    async def handle_client(self, bufferconn, address):
        client = await self.client_cls.from_server(bufferconn, address)
        self.clients.append(client)
        
        addr = client.get_address()
        print(f"[{self.name}]: Accepted connection from {addr[0]}:{addr[1]}!")
        
    async def drop_client(self, client):
        addr = client.get_address()
        print(f"[{self.name}]: Dropping connection from {addr[0]}:{addr[1]}!")
        
        # Handle the connection being lost.
        # The client will close itself on a lost connection.
        await client.handle_lost_connection()
        
        # We still do want to force it closed just in case though.
        if not client.is_closed():
            await client.close()
            
    async def remove_client(self, client):
        # Only remove the client if it's been closed.
        if not client.is_closed(): 
            return
            
        # Client is closed so we don't receive from or send to it.
        self.clients.remove(client)
            
    async def client_timeout(self, client):
        try:
            # Wait for our timeout period.
            await asyncio.sleep(client.timeout_period)
            
            # Don't do anything if we're already closed.
            if client.closed:
                return
                
            addr = client.get_address()
            print(f"[{self.name}]: Timing out connection for {addr[0]}:{addr[1]}.")
            
            # Handle the disconnection of the client.
            await self.drop_client(client)
        except KeyboardInterrupt as e:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            traceback.print_exception(e)
        
    async def flush_client(self, client):
        await client.read_datagrams()
        
    async def flush_server(self):
        # Iterate all of our clients and receive data for them.
        await asyncio.gather(*map(self.flush_client, self.clients))
        
        #for client in self.clients:
        #    await self.flush_client(client)
        
        # Remove all of our closed clients.
        await asyncio.gather(*map(self.remove_client, list(self.clients)))
        
        #for client in list(self.clients):
        #    await self.remove_client(client)
        
    async def flush_server_routine(self):
        while True:
            try:
                await self.flush_server()
                await asyncio.sleep(0.05)
                if self.server_closed: break
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        
    async def drain_client(self, client):
        await client.drain_interface()
        
    async def drain_server_routine(self):
        while True:
            try:
                # Write out all of the data for all of our clients.
                await asyncio.gather(*map(self.drain_client, self.clients))
                await asyncio.sleep(0.05)
                if self.server_closed: break
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
                
    async def listen_routine(self):
        # Try and get all of the incoming connections.
        while True:
            try:
                if self.server_closed: break
                
                connection = await self.accept_incoming_connection()
                if connection:
                    address, bufferconn = connection
                    await self.handle_client(bufferconn, address)
                    
                self.pending_connections = []
                await asyncio.sleep(0.05)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)

    drain_routine = drain_server_routine
    flush = flush_server
    flush_routine = flush_server_routine