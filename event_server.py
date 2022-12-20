import os, socket, struct

from panda3d.core import Datagram, DatagramIterator, Filename

from direct.directnotify import RotatingLog

from msgtypes import *

class EventServer:
    def __init__(self, otp):
        # Main OTP
        self.otp = otp
        
        # ES Sock
        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 4343))
        
        self.buffer = bytearray()
        
        self.buffDesc = ""
        
        logDir = os.path.join(os.path.expandvars('$PLAYER'), "event_logs")
        
        if not os.path.isdir(logDir):
            print(f"EventServer: Didn't find the event log directory, Making it!")
            os.mkdir(logDir)
        
        logPath = os.path.join(logDir, "toon_otpserver")
        self.log = RotatingLog.RotatingLog(logPath, hourInterval=24, megabyteLimit=8192)
        
    def writeToLog(self, str):
        self.log.write(str)
        
    def onData(self, data):
        self.onDatagram(Datagram(bytes(data)))
            
    def onDatagram(self, dg):
        di = DatagramIterator(dg)
        
        # First check if the datagram has anything in it.
        remainingSize = di.getRemainingSize()
        if not remainingSize >= 1:
            print("Event Logger datagram was truncated!")
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
            if (length > remainingSize) or self.buffDesc:
                self.buffDesc += description
            elif (length <= remainingSize) and self.buffDesc:
                self.buffDesc += description
                self.log.write("%d|%d|%s|%s|%s\n" % (channel, messageType, eventType, who, self.buffDesc))
                self.buffDesc = ""
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
            self.log.write("%d|%d|%s|%d|%d\n" % (channel, messageType, who, avatarCount, objectCount))