import asyncio, os, socket, struct

from panda3d.core import Datagram, DatagramIterator, Filename

from direct.directnotify import RotatingLog

from msgtypes import *

class EventServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, log):
        super().__init__()
        
        self.log = log
        self.buffDesc = {}
        self.transport = None
    
    def connection_made(self, transport):
        self.transport = transport
        
    def datagram_received(self, data, addr):
        self.handle_datagram(Datagram(bytes(data)), addr)
        
    def handle_datagram(self, dg, addr):
        di = DatagramIterator(dg)
        
        # First check if the datagram has anything in it.
        remainingSize = di.getRemainingSize()
        if not remainingSize >= 1:
            #print("Event Logger datagram was truncated!")
            return
        
        length = di.getUint16()
        messageType = di.getUint16()
        serverType = di.getUint16()
        channel = di.getUint32()
        if messageType == 1: # Server Event
            eventType = di.getString()
            who = di.getString()
            description = di.getString()
            # If we're buffering a description, We buffer it here.
            if not addr in self.buffDesc:
                self.buffDesc[addr] = ""
            if (length > remainingSize) or self.buffDesc[addr]:
                self.buffDesc[addr] += description
            elif (length <= remainingSize) and self.buffDesc[addr]:
                self.buffDesc[addr] += description
                self.log.write("%d|%d|%s|%s|%s\n" % (channel, messageType, eventType, who, self.buffDesc[addr]))
                del self.buffDesc[addr]
            else:
                self.log.write("%d|%d|%s|%s|%s\n" % (channel, messageType, eventType, who, description))
        elif messageType == 2: # Server Status
            who = di.getString()
            avatarCount = di.getUint32()
            objectCount = di.getUint32()
            self.log.write("%d|%d|%s|%d|%d\n" % (channel, messageType, who, avatarCount, objectCount))
        elif messageType == 3: # Server Status 2
            who = di.getString()
            pingChannel = di.getUint64()
            avatarCount = di.getUint32()
            objectCount = di.getUint32()
            self.log.write("%d|%d|%d|%s|%d|%d\n" % (channel, pingChannel, messageType, who, avatarCount, objectCount))
        
    def connection_lost(self, exc):
        if not self.transport: return
        
        self.transport.close()
        
        del self.transport
        self.transport = None

class EventServer:
    def __init__(self):
        super().__init__()
        
        self.transport = None
        self.protocol = None
        
        self.init_log()
        
    def init_log(self):
        logDir = os.path.join(os.path.expandvars('$PLAYER'), "event_logs")
        if not os.path.isdir(logDir):
            print(f"EventServer: Didn't find the event log directory, Making it!")
            os.mkdir(logDir)

        logPath = os.path.join(logDir, "otpserver")
        self.log = RotatingLog.RotatingLog(logPath, hourInterval=24, megabyteLimit=8192)
        
    async def start(self, addr='0.0.0.0', port=4343):
        # Get the running loop inside an async function
        loop = asyncio.get_running_loop()
        
        # Create the server endpoint
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: EventServerProtocol(self.log),
            local_addr=(addr, port)
        )
    
    async def close(self):
        self.transport.close()
        
        del self.protocol
        self.protocol = None
        del self.transport
        self.transport = None