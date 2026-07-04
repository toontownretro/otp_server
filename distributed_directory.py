from panda3d.core import Datagram
from panda3d.direct import DCPacker

from distributed_object import DistributedObject
    
class DistributedDirectory(DistributedObject):

    def __init__(self, doId, dclass, parentId, zoneId):
        super().__init__(doId, dclass, parentId, zoneId)