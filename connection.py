import asyncio, socket, struct, time

from panda3d.core import Datagram, DatagramIterator

class Client:
    def __init__(self):
        self.writer = None
        self.reader = None
        self.buffer = bytearray()
        self.closed = True
        
    @classmethod
    async def initialize(cls, addr, port):
        self = cls()
        await self.connect(addr, port)
        return self
    
    @classmethod
    async def from_server(cls, reader, writer):
        self = cls()
        self.reader = reader
        self.writer = writer
        self.closed = False
        return self
        
    async def connect(self, addr, port):
        if not self.closed:
            return True # Already connected.

        self.reader, self.writer = await asyncio.open_connection(addr, port)
        if not self.reader or not self.writer:
            return False # Failed to connect.
            
        self.closed = False
        return True # Connected successfully.
        
    async def close(self):
        if self.closed:
            return
            
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            
        del self.buffer
        self.buffer = bytearray()
        del self.reader
        self.reader = None
        del self.writer
        self.writer = None
        
        self.closed = True

    async def read(self, n):
        if self.closed:
            return
            
        data = await self.reader.read(n)
        return data
        
    async def write(self, data):
        if self.closed:
            return
            
        self.writer.write(data)
        await self.writer.drain()
        
    async def receive_data(self, data):
        if self.closed:
            return
            
        self.buffer += data
        while len(self.buffer) >= 2:
            length = struct.unpack("<H", self.buffer[:2])[0]
            if len(self.buffer) < length + 2:
                break
                
            packet = self.buffer[2:length + 2]
            self.buffer = self.buffer[length + 2:]
            
            await self.receive_datagram(Datagram(bytes(packet)))
        
    async def send_data(self, data):
        await self.write(data)
        
    async def receive_datagram(self, dg):
        return
        
    async def send_datagram(self, dg):
        if self.closed:
            return

        buffer = bytearray()
        buffer += struct.pack("<H", dg.getLength())
        buffer += bytes(dg)
        await self.send_data(bytes(buffer))
        
    async def handle_lost_connection(self):
        await self.close()
        
    def is_closed(self):
        return self.closed
   
    def get_address(self):
        if self.closed:
            return None

        return self.writer.get_extra_info('peername')
        
class Server:
    client_cls = Client
    
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port

        self.server = None
        self.clients = []
        
        self.server_closed = True
        
    @classmethod
    async def initialize(cls, addr, port):
        if not self.server_closed:
            return

        self = cls(addr, port)
        await self.start()
        return self
        
    async def start(self):
        if not self.server_closed:
            return
            
        self.server = await asyncio.start_server(self.handle_client, self.addr, self.port)
        await self.server.start_serving()
        self.server_closed = False
        
    async def close(self):
        if self.server_closed:
            return

        if self.server:
            self.server.close()
            await self.server.wait_closed()
            
            del self.server
            self.server = None
            
        for i in range(0, len(self.clients)):
            client = self.clients[i]
            
            # Close the connection for the client.
            await client.close()
            
            # Remove the client from our list.
            del self.clients[i]
        
        del self.clients
        self.clients = []
        
        self.server_closed = True

    async def handle_client(self, reader, writer):
        client = await self.client_cls.from_server(reader, writer)
        self.clients.append(client)
        
    async def drop_client(self, client):
        print("Dropping client from %s!" % (str(client.get_address())))
        
        # Handle the connection being lost.
        # The client will close itself on a lost connection.
        await client.handle_lost_connection()
        
        # We still do want to force it closed just in case though.
        if not client.is_closed():
            await client.close()
        
    async def flush_client(self, client):
        try:
            data = await client.read(2048)
        except Exception as e:
            data = None
        
        if not data:
            # We have no data and have lost the connection for whatever reason.
            await self.drop_client(client)
            return
        
        await client.receive_data(data)
        
    async def flush(self):
        # Iterate all of our clients and receive data for them.
        await asyncio.gather(*map(self.flush_client, self.clients))
        
        '''
        for i in range(0, len(self.clients)):
            await self.flush_client(self.clients[i])
        '''
        
        # Remove all of our closed clients.
        for i in range(0, len(self.clients)):
            if self.clients[i].is_closed():
                del self.clients[i]