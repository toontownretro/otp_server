import uuid
from panda3d.direct import DCPacker
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
    
    def __init__(self, dbss, doId, uuId, dclass):
        self.dbss = dbss
        self.doId = doId
        self.uuId = uuId
        self.dclass = dclass
        self.fields = {}
        
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
                field.packArgs(packer, self.fields[field.getName()])
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
        
    def getField(self, fieldName):
        return self.fields.get(fieldName, None)
        
    def getFields(self):
        return self.fields
        
    @classmethod
    def fromBinary(cls, dbss, data):
        if data[:16] == b"# DatabaseObject":
            dclassName, version, doId, uuId, fieldsData = eval(data)
            
            minVersion = cls.minVersion
            lastVersion = cls.version
            
            # Check for our minimum supported version.
            if version < minVersion or version > lastVersion:
                raise Exception("Tried to read database object with version %d.%d.%d, But only %d.%d.%d through %d.%d.%d is supported!" % (version[0], version[1], version[2], minVersion[0], minVersion[1], minVersion[2], lastVersion[0], lastVersion[1], lastVersion[2]))

            # Convert the string back into a UUID instance.
            uuId = uuid.UUID(uuId)
            
            dclass = dbss.dc.getClassByName(dclassName)
            
            self = cls(dbss, doId, uuId, dclass)
            for fieldName, value in fieldsData.items():
                field = dclass.getFieldByName(fieldName)
                
                if not field.isDb():
                    print("Reading server only field %r." % field.getName())
                    
                self.fields[field.getName()] = value
                    
            return self
        else:
            packer = DCPacker()
            packer.setUnpackData(data)
            
            # Get our version from our packed object.
            majVer = packer.rawUnpackUint8()
            minVer = packer.rawUnpackUint8()
            subVer = packer.rawUnpackUint8()
            version = (majVer, minVer, subVer)
            
            minVersion = cls.minVersion
            lastVersion = cls.version
            
            # Check for our minimum supported version.
            if version < minVersion or version > lastVersion:
                raise Exception("Tried to read database object with version %d.%d.%d, But only %d.%d.%d through %d.%d.%d is supported!" % (version[0], version[1], version[2], minVersion[0], minVersion[1], minVersion[2], lastVersion[0], lastVersion[1], lastVersion[2]))
                
            dclass = dbss.dc.getClassByName(packer.rawUnpackString())
            doId = packer.rawUnpackUint32()
            
            # Convert the string back into a UUID instance.
            uuId = uuid.UUID(packer.rawUnpackString())
            
            self = cls(dbss, doId, uuId, dclass)
            
            # We get every field
            while packer.getUnpackLength() > packer.getNumUnpackedBytes():
                field = dclass.getFieldByName(packer.rawUnpackString())
                
                packer.beginUnpack(field)
                value = field.unpackArgs(packer)
                packer.endUnpack()
                
                if not field.isDb():
                    print("Reading server only field %r." % field.getName())
                    
                self.fields[field.getName()] = value
                    
            return self
        
        
    def toBinary(self):
        if True:
            # Special readable format. We probably should benchmark this,
            # it's perhaps faster
            
            return b"# DatabaseObject\n" + pformat((self.dclass.getName(), self.version, self.doId, str(self.uuId), self.fields), width=-1, sort_dicts=True).encode("utf8")
        else:
            data = bytearray()
            packer = DCPacker()
            
            # Pack our version.
            packer.rawPackUint8(self.majVer)
            packer.rawPackUint8(self.minVer)
            packer.rawPackUint8(self.subVer)
            
            # Pack our DC object.
            packer.rawPackString(self.dclass.getName())
            packer.rawPackUint32(self.doId)
            packer.rawPackString(str(self.uuId))
            
            # We get every field
            for fieldName, value in self.fields.items():
                field = self.dclass.getFieldByName(fieldName)
                
                if field.isDb():
                    packer.rawPackString(field.getName())
                    packer.beginPack(field)
                    field.packArgs(packer, self.fields[field.getName()])
                    packer.endPack()
                    
            return packer.getBytes()
        
        
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
        self.dbss.saveDatabaseObject(self)
        
        
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
            
        self.dbss.saveDatabaseObject(self)