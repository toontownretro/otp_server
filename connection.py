import asyncio, socket, struct, time, traceback

from panda3d.core import Datagram, DatagramIterator

class Client:
    READ_IN_KiB = 16
    READ_IN = READ_IN_KiB * 1024
    
    def __init__(self):
        self.writer = None
        self.reader = None
        self.buffer = bytearray()
        self.closed = True
        
        self.timeout_task = None
        self.timeout_period = 20
        
        self.set_name("CLIENT")
        
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
        
        self.timeout_task = None

        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except ConnectionResetError as e:
                pass
            
        del self.buffer
        self.buffer = bytearray()
        del self.reader
        self.reader = None
        del self.writer
        self.writer = None
        
        self.closed = True

    async def read(self, n):
        if self.closed:
            return None
            
        data = await self.reader.read(n)
        return data
        
    async def write(self, data):
        if self.closed or not self.writer:
            return False
            
        try:
            self.writer.write(data)
            #await self.writer.drain()
            #await asyncio.sleep(0)
            return True
        except:
            return False
            
    def set_name(self, name):
        self.name = name
        
    def get_name(self):
        return self.name
        
    def is_closed(self):
        return self.closed
   
    def get_address(self):
        if self.closed:
            return None

        return self.writer.get_extra_info('peername')
            
    async def writer_drain(self):
        if self.closed or not self.writer:
            return

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
        return await self.write(data)
        
    async def receive_datagram(self, dg):
        return
        
    async def send_datagram(self, dg):
        if self.closed:
            return False

        buffer = bytearray()
        buffer += struct.pack("<H", dg.getLength())
        buffer += bytes(dg)
        return await self.send_data(bytes(buffer))
        
    async def handle_lost_connection(self):
        await self.close()
        
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
            
    async def drain_interface_routine(self):
        while (not self.closed):
            try:
                # Write out all of the data for all of our clients.
                await self.writer_drain()
                await asyncio.sleep(0.05)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        
    async def flush_interface(self):
        try:
            data = await self.read(self.READ_IN)
        except Exception as e:
            data = None
        
        if not data:
            if self.timeout_task: return
            
            # We have no data for whatever reason,
            # Create a timeout task for the client to drop with.
            self.timeout_task = asyncio.create_task(self.timeout(), name=f"{self.get_name()}-timeout")
            return
        elif self.timeout_task != None:
            # Cancel the disconnection task, We got data.
            self.timeout_task.cancel()
            self.timeout_task = None
        
        await self.receive_data(data)
        
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
        
    drain_routine = drain_interface_routine
    flush = flush_interface
    flush_routine = flush_interface_routine
        
class Server:
    client_cls = Client
    
    READ_IN_KiB = 256
    READ_IN = READ_IN_KiB * 1024
    
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port

        self.server = None
        self.clients = []
        
        self.server_closed = True
        
        self.name = "SERVER"
        
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
            
        self.server = await asyncio.start_server(self.handle_client, self.addr, self.port, start_serving=False, limit=self.READ_IN+256)
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
        try:
            data = await client.read(self.READ_IN)
        except Exception as e:
            data = None

        if not data:
            if client.timeout_task: return
            
            # We have no data for whatever reason,
            # Create a timeout task for the client to drop with.
            client.timeout_task = asyncio.create_task(self.client_timeout(client), name=f"{client.get_name()}-timeout")
            return
        elif client.timeout_task != None:
            # Cancel the disconnection task, We got data.
            client.timeout_task.cancel()
            client.timeout_task = None
        
        await client.receive_data(data)
        
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
        await client.writer_drain()
        
    async def drain_server_routine(self):
        while (not self.server_closed):
            try:
                # Write out all of the data for all of our clients.
                await asyncio.gather(*map(self.drain_client, self.clients))
                await asyncio.sleep(0.05)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)

    def set_name(self, name):
        self.name = name
        
    def get_name(self):
        return self.name

    drain_routine = drain_server_routine
    flush = flush_server
    flush_routine = flush_server_routine