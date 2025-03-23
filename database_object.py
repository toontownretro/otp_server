from panda3d.direct import DCPacker

import uuid
from pprint import pformat

class DatabaseObject:
    # This is our current version for database objects.
    # This can be used in future to add backwards compatibility support for later revisions.
    majVer = 1
    minVer = 0
    subVer = 0
    supMajVer = 1
    supMinVer = 0
    supSubVer = 0
    # Tuple of full version.
    version = (majVer, minVer, subVer)
    minVersion = (supMajVer, supMinVer, supSubVer)
    
    def __init__(self, dbm, doId, uuId, dclass):
        self.dbm = dbm
        self.doId = doId
        self.uuId = uuId
        self.dclass = dclass
        self.fields = {}
        self.dcObjectType = 0
        
    def packRequired(self, dg):
        packer = DCPacker()
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if field.isRequired():
                packer.beginPack(field)
                if field.getName() in self.fields:
                    field.packArgs(packer, self.fields[field.getName()])
                else:
                    packer.packDefaultValue()
                    
                packer.endPack()
                
        dg.appendData(packer.getBytes())
        
        
    def packOther(self, dg):
        packer = DCPacker()
        count = 0
        
        for index in range(self.dclass.getNumInheritedFields()):
            field = self.dclass.getInheritedField(index)
            if field.isDb() and not field.isRequired() and field.getName() in self.fields:
                packer.rawPackUint16(field.getNumber())
                packer.beginPack(field)
                fieldValue = self.fields[field.getName()]
                if not fieldValue:
                    print("Failed to pack other field '%s' in dcclass '%s' for doId %d!" % (field.getName(), self.dclass.getName(), self.doId))
                    continue
                field.packArgs(packer, fieldValue)
                packer.packDefaultValue()
                packer.endPack()
                count += 1
                
        dg.addUint16(count)
        dg.appendData(packer.getBytes())
    
    def packField(self, fieldName, value):
        field = self.dclass.getFieldByName(fieldName)
        if not field:
            return None
        
        packer = DCPacker()
        packer.beginPack(field)
        field.packArgs(packer, value)
        packer.endPack()
        
        return packer.getBytes()
        
    def unpackField(self, fieldName, data):
        packer = DCPacker()

        if not data:
            return None
            
        packer.setUnpackData(data)
        
        field = self.dclass.getFieldByName(fieldName)
        if not field:
            return None
            
        packer.beginUnpack(field)
        value = field.unpackArgs(packer)
        packer.endUnpack()

        return value
        
    def setField(self, fieldName, value):
        field = self.dclass.getFieldByName(fieldName)
        if not field:
            return
        
        if not field.isDb():
            print("Setting server only field %r." % field.getName())
            
        self.fields[field.getName()] = value
        
    def getField(self, fieldName):
        return self.fields.get(fieldName, None)
        
    def setFields(self, fieldsData):
        for fieldName, value in fieldsData.items():
            field = self.dclass.getFieldByName(fieldName)
            if not field:
                continue
            
            if not field.isDb():
                print("Setting server only field %r." % field.getName())
                
            self.fields[field.getName()] = value
        
    def getFields(self):
        return self.fields
        
    def receiveField(self, field, di):
        packer = DCPacker()
        packer.setUnpackData(di.getRemainingBytes())
        
        molecular = field.asMolecularField()
        if molecular:
            for n in range(molecular.getNumAtomics()):
                atomic = molecular.getAtomic(n)
                
                packer.beginUnpack(atomic)
                value = atomic.unpackArgs(packer)
                
                if atomic.isDb():
                    self.fields[atomic.getName()] = value
                    
                packer.endUnpack()
                
        else:
            packer.beginUnpack(field)
            value = field.unpackArgs(packer)
            
            if field.isDb():
                self.fields[field.getName()] = value
            
            packer.endUnpack()
            
        di.skipBytes(packer.getNumUnpackedBytes())
        
        # This isn't very optimized, but we wanna make sure we don't lose anything
        self.dbm.saveDatabaseObject(self)
        
        
    def update(self, field, *values):
        # "Manual" update
        field = self.dclass.getFieldByName(field)
        
        if field.asAtomicField():
            self.fields[field.getName()] = values
            
        elif field.asMolecularField():
            raise Exception("No.")
            
        elif field.asParameter():
            if len(values) != 1:
                raise Exception("Arg count mismatch")
                
            self.fields[field.getName()] = values[0]
            
        self.dbm.saveDatabaseObject(self)
        
    def unsafe_update(self, field, *values):
        """
        This does the same as update(), But doesn't we don't save them to database
        on appliance. 
        """
        
        # "Manual" update
        field = self.dclass.getFieldByName(field)
        
        if field.asAtomicField():
            self.fields[field.getName()] = values
            
        elif field.asMolecularField():
            raise Exception("No.")
            
        elif field.asParameter():
            if len(values) != 1:
                raise Exception("Arg count mismatch")
                
            self.fields[field.getName()] = values[0]