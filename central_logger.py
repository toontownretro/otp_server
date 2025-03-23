from panda3d.core import Datagram
from panda3d.direct import DCPacker

from distributed_object import DistributedObject

class CentralLogger(DistributedObject):
    def __init__(self, otp, doId, dclass, parentId, zoneId):
        super().__init__(doId, dclass, parentId, zoneId)
        
        # Main OTP
        self.otp = otp
        
        # Quick access for ES
        self.eventServer = self.otp.eventServer

    def receiveField(self, sender, field, di):
        # We don't want a molecular field update.
        molecular = field.asMolecularField()
        if molecular: return
        
        packer = DCPacker()
        packer.setUnpackData(di.getRemainingBytes())
        
        packer.beginUnpack(field)
        value = field.unpackArgs(packer)
        category, eventString, targetDISLId, targetAvId = value
        
        packer.endUnpack()
            
        di.skipBytes(packer.getNumUnpackedBytes())
        
        self.eventServer.writeToLog("%d|%s|%s|%d|%d\n" % (sender, category, eventString, targetDISLId, targetAvId))