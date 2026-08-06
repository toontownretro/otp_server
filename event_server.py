import asyncio, os, socket, struct

from panda3d.core import Datagram, DatagramIterator, Filename

from direct.directnotify import RotatingLog

from msgtypes import *

class EventServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop, server):
        super().__init__()
        
        self.loop = loop
        self.server = server
        self.transport = None
        
        self.buffLen = {}
        self.buffDesc = {}
        
        self.current_tasks = {}
    
    def connection_made(self, transport):
        self.transport = transport
        
    def datagram_received(self, data, addr):
        dg = Datagram(bytes(data))
        di = DatagramIterator(dg)
        
        # Check if the datagram has anything in it.
        if di.getRemainingSize() < 2:
            return
            
        # If we already have a task. Wait on it first.
        if addr in self.current_tasks:
            await self.current_tasks[addr]
            
        # Schedule the actual work as a new task
        self.current_tasks[addr] = self.loop.create_task(self.handle_datagram(di, addr))
        
    async def handle_datagram(self, di, addr):
        # Get the length of the full event message data.
        length = di.getUint16()
        
        # Get the size for comparing with the length.
        packetSize = di.getRemainingSize()
        if packetSize < 8:
            return
        
        messageType = di.getUint16()
        serverType = di.getUint16()
        channel = di.getUint32()
        if messageType == 1: # Server Event
            eventType = di.getString()
            who = di.getString()
            description = di.getString()
            if (length > packetSize) or addr in self.buffLen:
                if not addr in self.buffLen:
                    # If we're not buffering a description, We start buffering it here.
                    self.buffDesc[addr] = description       
                    self.buffLen[addr] = length - packetSize
                elif self.buffLen[addr] - len(description) <= 0:
                    # We're done buffering a description.
                    description = self.buffDesc[addr] + description
                    del self.buffDesc[addr]
                    del self.buffLen[addr]

                    await self.server.queue.put((messageType, (serverType, channel, eventType, who, description))
                else:
                    # We're already buffering a description, Add to it.
                    self.buffDesc[addr] += description
                    self.buffLen[addr] -= len(description)
            else:
                await self.server.queue.put((messageType, (serverType, channel, eventType, who, description))
        elif messageType == 2: # Server Status
            who = di.getString()
            avatarCount = di.getUint32()
            objectCount = di.getUint32()
            await self.server.queue.put((messageType, (serverType, channel, who, avatarCount, objectCount))
        elif messageType == 3: # Server Status 2
            who = di.getString()
            pingChannel = di.getUint64()
            avatarCount = di.getUint32()
            objectCount = di.getUint32()
            await self.server.queue.put((messageType, (serverType, channel, pingChannel, who, avatarCount, objectCount))
            
        del self.current_tasks[addr]
        
    def connection_lost(self, exc):
        if not self.transport: return
        
        #self.transport.close()
        
        del self.transport
        self.transport = None

class EventServer:
    def __init__(self):
        super().__init__()
        
        self.transport = None
        self.protocol = None
        self.loop = None
        
        self.queue = asyncio.Queue()
        
        self.log_task = None
        
        self.init_log()
        
    def init_log(self):
        logDir = os.path.join(os.path.expandvars('$PLAYER'), "event_logs")
        if not os.path.isdir(logDir):
            print(f"EventServer: Didn't find the event log directory, Making it!")
            os.mkdir(logDir)

        logPath = os.path.join(logDir, "otpserver")
        self.log = RotatingLog.RotatingLog(logPath, hourInterval=24, megabyteLimit=256)
        
    async def start(self, addr='0.0.0.0', port=4343):
        # Get the running loop inside an async function
        self.loop = asyncio.get_running_loop()
        
        # Create the server endpoint
        self.transport, self.protocol = await self.loop.create_datagram_endpoint(
            lambda: EventServerProtocol(self.loop, self),
            local_addr=(addr, port)
        )
        
        try:
            self.loop.run_until_complete(self.start_logging)
        except asyncio.CancelledError:
            pass
        
    async def start_logging(self):
        # Only write to our log every 60 seconds.
        self.log_task = asyncio.ensure_future(logging_loop(60))
        await self.log_task
            
    async def logging_loop(self, interval):
        while True:
            await asyncio.gather(
                self.perform_logging(),
                #self.queue.join(), # Block while we're working on the queue.
                asyncio.sleep(interval),
            )
            
    async def perform_logging(self):
        while not self.queue.empty():
            log = await self.queue.get()
            self.queue.task_done()
    
    async def close(self):
        self.transport.close()
        
        if self.queue:
            self.queue.shutdown()
        
        if self.log_task:
            self.log_task.cancel()
            
        del self.queue
        self.queue = None
        
        del self.log_task
        self.log_task = None
        
        del self.protocol
        self.protocol = None
        del self.transport
        self.transport = None