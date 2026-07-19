import asyncio, functools, socket, struct

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
        
    async def close(self):
        if self.closed:
            return

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

        code = di.getUint16()
        
        if code == CONTROL_SET_CHANNEL:
            channel = di.getUint64()
            self.channels.add(channel)
            #print("Registered channel %d for %s:%d" % (channel, self.addr[0], self.addr[1]))
            
        elif code == CONTROL_REMOVE_CHANNEL:
            channel = di.getUint64()
            self.channels.remove(channel)
            #print("Unregistered channel %d for %s:%d" % (channel, self.addr[0], self.addr[1]))
            
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
                self.channels.remove(di.getUint64())
            
        elif code == CONTROL_ADD_POST_REMOVE:
            message = di.getBlob()
            self.post_removes.append(message)
            
        elif code == CONTROL_CLEAR_POST_REMOVE:
            self.post_removes = []
            
        elif code == CONTROL_SET_CON_NAME:
            self.connection_names.append(di.getString())
            
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
            return
        if not message: 
            return
        
        # Make sure the message we recieved from the Message Director is
        # something we care about.
        if not self.channels.intersection(message.channels):
            return
            
        # Construct the datagram from the message.
        dg = Datagram()
        dg.addUint8(len(message.channels))
        for channel in message.channels:
            dg.addUint64(channel)
        dg.addUint64(message.sender)
        dg.addUint16(message.code)
        dg.appendData(message.data)
        
        # Send our message.
        await self.send_datagram(dg)
        
    async def receive_datagram(self, dg):
        di = DatagramIterator(dg)
        
        # First check if the datagram has anything in it.
        if not di.getRemainingSize() >= 1:
            #print("Recieved Datagram was truncated!")
            return
            
        # Get the amount of channels the datagram will be sent to.
        count = di.getUint8()
        if count <= 0 or not di.getRemainingSize() >= count * 8:
            #print("Recieved datagram has invalid amount of channels!")
            return
        
        # Get the channels we will send the datagram to.
        channels = set()
        for _ in range(count):
            # Check each loop to make sure we don't run out of size.
            # We can't have a size less then 8, Because that's how big
            # a 64 bit integer is at minimum.
            if not di.getRemainingSize() >= 8:
                #print("Recieved Datagram was truncated!")
                return
            channel = di.getUint64()
            channels.add(channel)
        
        # Handle the special case of a control message.
        if count == 1 and channel == CONTROL_MESSAGE:
            await self.handle_message(di)
            return
        
        # Add the datagram to the routing wait list. The Message Director will pick them up and pass them along.
        await self.route_message(channels, di)
        
    async def handle_post_removes(self):
        for x in self.post_removes:
            await self.receive_datagram(Datagram(x))
        
    def is_uberdog(self):
        if len(self.connection_names) <= 0:
            return False
        return self.connection_names[0] == "UberDog"
        
    def get_primary_channel(self):
        if len(self.channels) <= 0:
            return 0
        return list(self.channels)[0]

class MessageDirector(Server):
    client_cls = MDClient
    
    def __init__(self, addr, port):
        super().__init__(addr, port)
        
    @classmethod
    async def initialize(cls, addr="0.0.0.0", port=ConfigVariableInt("msg-director-port", 6666).getValue()):
        self = cls.super().initialize(addr, port)
        return self
        
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
            
        # Route all of the messages we've collected.
        await asyncio.gather(*map(self.route_message, route_messages))
        
        # Bring in all of the new messages from our clients.
        await super().flush()
        
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

    async def get_uberdog(self):
        for client in self.clients:
            if client.is_uberdog():
                return client
        
        return None