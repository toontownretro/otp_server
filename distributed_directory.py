from panda3d.core import Datagram
from panda3d.direct import DCPacker

from distributed_object import DistributedObject
    
class DistributedDirectory(DistributedObject):

    def __init__(self, doId, dclass, parentId, zoneId):
        super().__init__(doId, dclass, parentId, zoneId)
        
        FIELD_SETPARENTINGRULES_ID = self.dclass.getFieldByName("setParentingRules").getNumber()
        
    def receiveField(self, field, di):
        if field.asMolecularField():
            return self.receiveMolecularField(field, di)
            
        field_id = field.getNumber()
        if field_id == FIELD_SETPARENTINGRULES_ID:
            packer = DCPacker()
            packer.setUnpackData(di.getRemainingBytes())
            
            packer.beginUnpack(field)
            value = field.unpackArgs(packer)
            packer.endUnpack()
            di.skipBytes(packer.getNumUnpackedBytes())
            
            type, rule = value
            return self.setParentingRules(type, rule)

        return super().receiveField(field, di)
            
    def setParentingRules(self, type="Stated", rule=""):
        pass