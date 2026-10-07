import asyncio, functools, math, socket, struct, time, traceback

from panda3d.core import ConfigVariableInt, Datagram, DatagramIterator

from connection import Client, Server
from msgtypes import *

class MDMessage:
    def __init__(self, channels, code, sender, data):
        self.channels = channels
        self.code = code
        self.sender = sender
        self.data = data

class MDClient(Client):
    def __init__(self):
        super().__init__()
        
        self.connection_names = []
        self.connection_urls = []
        self.channels = set()
        self.post_removes = []
        self.route_messages = []
        
        self.timeout_period = 30
        
        self.ping_task = None
        self.ping_delta = 0.0
        self.ping_time = 0.0
        self.name = None
        
    async def close(self):
        if self.closed:
            return
            
        # Clear our ping task if it exists.
        if self.ping_task:
            self.ping_task.cancel()
        self.ping_task = None
        self.ping_delta = 0.0
        self.ping_time = 0.0

        # Try to handle any post remove datagrams.
        await self.handle_post_removes()
        
        # Close our connection.
        await super().close()
        
        # Cleanup all of our stored data and reset it,
        # The exception is route messages as the Message Director
        # will clear those out.
        
        del self.connection_names
        self.connection_names = []
        del self.connection_urls
        self.connection_urls = []
        del self.channels
        self.channels = set()
        del self.post_removes
        self.post_removes = []
        
    async def handle_message(self, di):
        if self.closed:
            return
            
        sender = di.getUint64()
        code = di.getUint16()
        
        if code == SERVER_PING:
            if sender != self.get_primary_channel():
                print(f"[{self.get_name()}]: Received ping message from channel {sender} not meant for us ({self.get_primary_channel()}).")
                return
                
            # Deconstruct ping message.
            sec = di.getUint32()
            usec = di.getUint32()
            url = di.getString()
            channel = di.getUint32()
            
            delta = time.time() - (sec + usec / 1000000.0)
            
            # Allow up to double of our timeout period for a extra leniency
            # with clients that are trying to ping.
            if delta > self.ping_delta and self.ping_delta <= self.timeout_period:
                self.ping_delta = delta
            
            print(f"[{self.get_name()}]: Received ping with delay of approximately {round(delta, 4)} seconds.")
            
            # Clear our timeout task if we have it.
            if self.timeout_task:
                self.timeout_task.cancel()
                self.timeout_task = None
                
            # Schedule our next server ping by creating a new ping task.
            self.ping_task = asyncio.create_task(self.schedule_ping())
            return
        
        addr = self.get_address()
        print(f"[{self.get_name()}]: Received unsupported code '{code}' for a message.")
        
    async def handle_control_message(self, di):
        if self.closed:
            return

        code = di.getUint16()
        
        if code == CONTROL_SET_CHANNEL:
            channel = di.getUint64()
            self.channels.add(channel)
            
            print(f"[{self.get_name()}]: Registered channel {channel}")
            
            # Clear our timeout task if we have it.
            if self.timeout_task:
                self.timeout_task.cancel()
                self.timeout_task = None
            
            # Start our server ping routine by creating a ping task.
            #if not self.ping_task:
            #    self.ping_task = asyncio.create_task(self.schedule_ping(delay=0))
            
        elif code == CONTROL_REMOVE_CHANNEL:
            channel = di.getUint64()
            
            if channel in self.channels:
                self.channels.remove(channel)
                print(f"[{self.get_name()}]: Unregistered channel {channel}")
            
            if not self.get_primary_channel():
                # A client without a primary channel can't be sent to or from.
                # So we begin a timeout countdown for it.
                # If it doesn't add a new primary channel within' that time,
                # We will drop this client.
            
                # Clear our ping task if we have it.
                if self.ping_task:
                    self.ping_task.cancel()
                    self.ping_task = None
                
                # Cancel the old timeout task if it exists.
                if self.timeout_task:
                    self.timeout_task.cancel()
                    self.timeout_task = None
                
                # Create our new timeout task.
                self.timeout_task = asyncio.create_task(self.timeout())
            
        # This is just a guess of what this control code actually did, We don't know in truth.
        # It could've also added all of the channels in-between a range of two channels. But I don't see any good reason
        # to do it that way.
        # It may of also been used for districts? (Doubt) But Toontown doesn't use this. And if Pirates did. Then we won't
        # know how unless Pirates leaks.
        elif code == CONTROL_ADD_RANGE:
            count = di.getInt16()
            if count <= 0 or not di.getRemainingSize() >= count * 8:
                return

            for _ in range(count):
                self.channels.add(di.getUint64())
                
        # See CONTROL_ADD_RANGE.
        elif code == CONTROL_REMOVE_RANGE:
            count = di.getInt16()
            if count <= 0 or not di.getRemainingSize() >= count * 8:
                return

            for _ in range(count):
                channel = di.getUint64()
                if not channel in self.channels: 
                    continue
                self.channels.remove(channel)
                
            if not self.get_primary_channel():
                # A client without a primary channel can't be sent to or from.
                # So we begin a timeout countdown for it.
                # If it doesn't add a new primary channel within' that time,
                # We will drop this client.
            
                # Clear our ping task if we have it.
                if self.ping_task:
                    self.ping_task.cancel()
                    self.ping_task = None
                
                # Cancel the old timeout task if it exists.
                if self.timeout_task:
                    self.timeout_task.cancel()
                    self.timeout_task = None
                
                # Create our new timeout task.
                self.timeout_task = asyncio.create_task(self.timeout())
            
        elif code == CONTROL_ADD_POST_REMOVE:
            message = di.getRemainingBytes()
            self.post_removes.append(message)
            
        elif code == CONTROL_CLEAR_POST_REMOVE:
            self.post_removes = []
            
        elif code == CONTROL_SET_CON_NAME:
            self.connection_names.append(di.getString())
            self.set_name(self.connection_names[0])
            
        elif code == CONTROL_SET_CON_URL:
            self.connection_urls.append(di.getString())

        else:
            raise NotImplementedError("CONTROL_MESSAGE", code)
        
        #print(self.connection_names[0], self.connection_urls, self.channels)
        
    async def route_message(self, channels, di):
        # Remove our own channels from the list of channels to send to.
        # This is mainly just a sanity safety check.
        r = self.channels.intersection(channels)
        channels = set(channels - r)
        
        sender = di.getUint64()
        code = di.getUint16()
        data = di.getRemainingBytes()
        
        # Make the message to add to our list.
        message = MDMessage(channels, code, sender, data)
        
        # Add it for later.
        self.route_messages.append(message)
        
    async def send_message(self, message):
        if self.closed:
            return False
        if not message: 
            return False
        
        # Make sure the message we received from the Message Director is
        # something we care about.
        if not self.channels.intersection(message.channels):
            return False
            
        # Construct the datagram from the message.
        dg = Datagram()
        dg.addUint8(len(message.channels))
        for channel in message.channels:
            dg.addUint64(channel)
        dg.addUint64(message.sender)
        dg.addUint16(message.code)
        dg.appendData(message.data)
        
        # Send our message.
        return await self.send_datagram(dg)
        
    async def receive_datagram(self, dg):
        di = DatagramIterator(dg)
        
        # First check if the datagram has anything in it.
        if not di.getRemainingSize() >= 1:
            #print("Received Datagram was truncated!")
            return
            
        # Get the amount of channels the datagram will be sent to.
        count = di.getUint8()
        if count <= 0 or not di.getRemainingSize() >= count * 8:
            #print("Received datagram has invalid amount of channels!")
            return
        
        # Get the channels we will send the datagram to.
        channels = set()
        for _ in range(count):
            # Check each loop to make sure we don't run out of size.
            # We can't have a size less then 8, Because that's how big
            # a 64 bit integer is at minimum.
            if not di.getRemainingSize() >= 8:
                #print("Received Datagram was truncated!")
                return
            channel = di.getUint64()
            channels.add(channel)
        
        # Handle the special case of a control message.
        if count == 1:
            if channel == CONTROL_MESSAGE:
                await self.handle_control_message(di)
                return
            elif channel == 20000000:
                await self.handle_message(di)
                return
        
        # Add the datagram to the routing wait list. The Message Director will pick them up and pass them along.
        await self.route_message(channels, di)
        
    async def handle_post_removes(self):
        for x in self.post_removes:
            await self.receive_datagram(Datagram(x))
            
    async def send_ping(self):
        if self.closed:
            return False
        if not self.get_primary_channel():
            return False
            
        self.ping_time = time.time()
        usec, sec = math.modf(self.ping_time)
            
        # Construct the data for SERVER_PING.
        dg = Datagram()
        dg.addUint32(int(sec))
        dg.addUint32(int(usec * 1000000))
        if len(self.connection_urls) >= 1:
            dg.addString(self.connection_urls[0])
        else:
            dg.addString("")
        dg.addUint32(self.get_primary_channel())
        
        di = DatagramIterator(dg)
    
        # Make the message to send.
        message = MDMessage([self.get_primary_channel()], SERVER_PING, 20000000, di.getRemainingBytes())
        
        # Send our message
        return await self.send_message(message)
        
    async def schedule_ping(self, delay=10):
        # Sleep for 10 seconds between each ping for the default.
        await asyncio.sleep(delay)
        
        # Verify we aren't already closed.
        if self.closed:
            return
            
        # Create a timeout task for the client to drop with.
        # This will trigger if we don't recieve a response ping in time.
        self.timeout_task = asyncio.create_task(self.timeout())
        
        # Send our ping to the server which is connected to us.
        await self.send_ping()
        
    async def handle_lost_connection(self):
        print(f"[{self.get_name()}]: Lost connection.")
        await self.close()
        
    async def timeout(self):
        try:
            # Wait for our timeout period.
            await asyncio.sleep(self.timeout_period + self.ping_delta)
            
            # Don't do anything if we're already closed.
            if self.closed:
                return
                
            delta = time.time() - self.ping_time
            print(f"[{self.get_name()}]: Timing out with no response within {round(delta, 4)} seconds.")
            
            # Handle our lost connection.
            await self.handle_lost_connection()
        except KeyboardInterrupt as e:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            traceback.print_exception(e)
            
    def get_name(self):
        addr = self.get_address()
        if self.name:
            return f"MD CLIENT ({self.name}) - {addr[0]}:{addr[1]}"
        return f"MD CLIENT - {addr[0]}:{addr[1]}"
        
    def get_primary_channel(self):
        if len(self.channels) <= 0:
            return 0
        return list(self.channels)[0]

class MessageDirector(Server):
    client_cls = MDClient
    
    def __init__(self, addr, port):
        super().__init__(addr, port)
        
        self.set_name("MESSAGE DIRECTOR")
        
    @classmethod
    async def initialize(cls, addr="0.0.0.0", port=6666):
        self = cls(addr, port)
        await self.start()
        return self
        
    async def start(self):
        await super().start()
        print(f"[{self.name}]: Now directing on {self.addr}:{self.port}.")
        
    async def handle_client(self, reader, writer):
        client = await self.client_cls.from_server(reader, writer)
        # Create a timeout task to boot the client if it doesn't do anything.
        client.timeout_task = asyncio.create_task(client.timeout())
        self.clients.append(client)
        
        addr = client.get_address()
        print(f"[{self.name}]: Accepted connection from {addr[0]}:{addr[1]}!")
        
    async def flush_client(self, client):
        try:
            data = await client.read(2048)
        except Exception as e:
            data = None
        
        # Just return, The Message Director manages ping messages for timeouts instead.
        if not data:
            return
        
        await client.receive_data(data)
        
    async def flush(self):
        # Before we take in the data from our clients,
        # Flush out the pending messages that we last received.
        # This is so clients that have disconencted can still pass along
        # certain messages. Such as Post Removes.
        
        # Collect of the messages to route from all of our clients.
        route_messages = []
        for client in self.clients:
            route_messages.extend(client.route_messages)
            client.route_messages = [] # Make sure to clear the list, so no duplicates happen!
        
        # Bring in all of the new messages from our clients.
        await super().flush()
        
        # Route all of the messages we've collected.
        await asyncio.gather(*map(self.route_message, route_messages))
        
    async def route_message(self, message):
        if not message: return
        
        # Prepare a list of partial coroutines for the clients.
        routines = [
            functools.partial(client.send_message, message)
            for client in self.clients  
        ]
        coroutines = [f() for f in routines]
        
        # Send out the message to all the clients wanting to receive it.
        await asyncio.gather(*coroutines)
        
        '''
        for client in self.clients:
            await client.send_message(message)
        '''
        
'''
if __name__ == "__main__":
    async def main():
        # Get the running loop inside an async function
        loop = asyncio.get_running_loop()
        
        md = await MessageDirector.initialize("0.0.0.0", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        try:
            loop.create_task(md.server.serve_forever())
        except asyncio.CancelledError:
            pass
        
        while True:
            try:
                await md.flush()
                await asyncio.sleep(0)
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
            
    asyncio.run(main())
'''