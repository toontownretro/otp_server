import traceback

from panda3d.core import Datagram
from panda3d.direct import DCPacker

from msgtypes import *
    
class DistributedObject:

    def __init__(self, doId, dclass, parentId=BAD_DO_ID, zoneId=BAD_ZONE_ID):
        self.doId = doId
        self.dclass = dclass
        self.parentId = parentId
        self.ownerId = BAD_DO_ID
        self.zoneId = zoneId
        
        self.senders = []
        self.senderId = None
        
        self.children = set()
        
        self.fields = {}
        
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if (field.isRequired() or field.isRam()) and field.asAtomicField():
                self.fields[field.getNumber()] = None
                
    def get(self, field, default=None):
        field = self.dclass.getFieldByName(field)
        if not field:
            return default
        return self.fields.get(field.getNumber(), default)

    def update(self, field, *values):
        self.fields[self.dclass.getFieldByName(field).getNumber()] = values
        
    def packDetails(self, dg, fields):
        # Pack required fields.
        fieldPacker = DCPacker()
        for i in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(i)
            if not field.isRequired() or field.asMolecularField():
                continue

            k = field.getName()
            v = fields.get(k, None)

            fieldPacker.beginPack(field)
            if not v:
                fieldPacker.packDefaultValue()
            else:
                field.packArgs(fieldPacker, v)

            fieldPacker.endPack()

        dg.appendData(fieldPacker.getBytes())
        return dg
        
    def packField(self, dg, field):
        packer = DCPacker()
        packer.beginPack(field)
        
        index = field.getNumber()
        if index in self.fields and self.fields[index] != None:
            field.packArgs(packer, self.fields[field.getNumber()])
        else:
            packer.packDefaultValue()
            
        packer.endPack()
        dg.appendData(packer.getBytes())
        return dg

    def packRequired(self, dg):
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if field.isRequired() and field.asAtomicField():
                self.packField(dg, field)
                
        return dg

    def packRequiredBroadcast(self, dg):
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if (field.isRequired() and field.isBroadcast()) and field.asAtomicField():
                self.packField(dg, field)

        return dg

    def packOther(self, dg):
        dg2 = Datagram()
        count = 0
        
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if field.isBroadcast() and not field.isRequired() and self.fields.get(field.getNumber(), None) is not None:
                count += 1
                
                dg2.addUint16(field.getNumber())
                self.packField(dg2, field)
                
        dg.addUint16(count)
        dg.appendData(dg2.getMessage())
        return dg
        
    def receiveMolecularField(self, field, di):
        molecular = field.asMolecularField()
        if not molecular:
            return None
            
        packer = DCPacker()
        packer.setUnpackData(di.getRemainingBytes())

        res = []
        for n in range(molecular.getNumAtomics()):
            atomic = molecular.getAtomic(n)
            
            try:
                packer.beginUnpack(atomic)
                value = atomic.unpackArgs(packer)
                packer.endUnpack()
            except Exception as e:
                traceback.print_exception(e)
                print(f"FATAL ERROR: Failed to unpack field '{field.getName()}'. See above for exception.")
                break
            
            if not atomic.getNumber() in self.fields:
                res.append(False)
                continue
                
            self.fields[atomic.getNumber()] = value
            res.append(True)
            
        try:
            di.skipBytes(packer.getNumUnpackedBytes())
        except:
            return [False] * molecular.getNumAtomics()
        return res
        
    def receiveField(self, field, di):
        if field.asMolecularField():
            return self.receiveMolecularField(field, di)
        
        packer = DCPacker()
        packer.setUnpackData(di.getRemainingBytes())
        
        try:
            packer.beginUnpack(field)
            value = field.unpackArgs(packer)
            packer.endUnpack()
            di.skipBytes(packer.getNumUnpackedBytes())
        except Exception as e:
            traceback.print_exception(e)
            print(f"FATAL ERROR: Failed to unpack field '{field.getName()}'. {packer.getNumUnpackedBytes()} bytes remain unpacked. See above for exception.")
            value = None
        
        # Value failed to unpack for one reason or another.
        if not value:
            return False
        
        if not field.getNumber() in self.fields:
            return False
            
        self.fields[field.getNumber()] = value
        return True
        
    def receiveRequired(self, di):
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if field.isRequired() and field.asAtomicField():
                self.receiveField(field, di)
        
    def receiveRequiredBroadcast(self, di):
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if field.isRequired() or field.asAtomicField():
                self.receiveField(field, di)
        
    def receiveOther(self, di):
        for n in range(di.getUint16()):
            index = di.getUint16()
            
            field = self.dclass.getFieldByIndex(index)
            self.receiveField(field, di)

    def __repr__(self):
        return "<" + self.dclass.getName() + " instance at " + str(self.doId) + ", in " + str(self.parentId) + " zone " + str(self.zoneId) + ">"