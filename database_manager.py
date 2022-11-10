import base64, hashlib, os, threading, traceback, uuid

from datetime import datetime

dbmType = "gnu"
try:
    # If we can, Use semidbm as a fast db access method.
    import semidbm as dbm
    dbmType = "semidbm"
except:
    # Anydbm was made into dbm.ndbm but we'd rather use dbm.gnu anyways.
    import dbm.gnu as dbm

from pprint import pformat

# Use pymysql for our SQL connection.
import pymysql as MySQLdb

from panda3d.core import ConfigVariableInt, ConfigVariableString, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem
from panda3d.direct import DCPacker

from database_object import DatabaseObject
from distributed_object import DistributedObject
from msgtypes import *

class DatabaseBackend:
    def __init__(self, manager):
        self.manager = manager
        
        # DC File
        self.dc = self.manager.dc
            
        self._mutexLock = threading.RLock()

    def addToAccountServer(self, key, value):
        """
        Add a value to our database storage, If we don't have one.
        An exception will be raised.
        """
        raise Exception("Tried to add value to account server, But we don't have one!")
        
    def getFromAccountServer(self, key):
        """
        Get the value of a key in our database storage.
        If we don't have a storage, We always return None.
        """
        return None
    
    def inAccountServer(self, key):
        """
        Return if a key is within' our databases storage.
        If we don't have a storage, This is always False.
        """
        return False
    
    def hasAccountServer(self):
        """
        Check if we have a file or server for account database storage.
        """
        return False
        
    def load(self, doId):
        """
        Safely loads the data from database using a mutex lock,
        so the process is thread safe...
        """

        with self._mutexLock:
            return self.handleLoad(doId)
            
    def handleLoad(self, doId):
        """
        Loads the data from database to memory safely.
        """
    
    def save(self, do):
        """
        Safely saves the data to database using a mutex lock,
        so the process is thread safe...
        """

        with self._mutexLock:
            self.handleSave(do)
            
    def handleSave(self, do):
        """
        Dumps the data from memory out to database safely.
        """
        
    def exists(self, doId):
        """
        Return if the specified doId exists in the database.
        """
        return False
        
    def getNextDoId(self):
        """
        Get the next open doId for the backend we're using.
        """
        return None
        
class DatabaseBackendFile(DatabaseBackend):
    def __init__(self, manager):
        DatabaseBackend.__init__(self, manager)
        
        # Database Configs
        self.databaseDirectory = Filename(os.path.expandvars(ConfigVariableString('database-directory', "database").getValue()))
        self.databaseExtension = ConfigVariableString('database-extension', ".bin").getValue()
        self.databaseStoreFile = ConfigVariableString('database-storage', "game-accounts-%s.db" % (dbmType)).getValue()
        self.databaseStore = dbm.open(self.databaseDirectory + "/" + self.databaseStoreFile, 'c')
        
        # Get our Panda3D Virtual File System, And keep a reference.
        self.vfs = VirtualFileSystem.getGlobalPtr()
        
        if not self.vfs.exists(self.databaseDirectory):
            self.vfs.makeDirectoryFull(self.databaseDirectory)

class DatabaseBackendRaw(DatabaseBackendFile):
    def __init__(self, manager):
        DatabaseBackendFile.__init__(self, manager)
        
    def addToAccountServer(self, key, value):
        """
        Add a value to our database storage, If we don't have one.
        An exception will be raised.
        """
        if not self.hasAccountServer():
            raise Exception("Tried to add value to account server, But we don't have one!")
            
        self.databaseStore[str(key).encode("utf-8")] = str(value)
        
        # If our database has syncing. Then let's sync now.
        if getattr(self.databaseStore, 'sync', None):
            self.databaseStore.sync()
        
    def getFromAccountServer(self, key):
        """
        Get the value of a key in our database storage.
        If we don't have a storage, We always return None.
        """
        if not self.hasAccountServer(): 
            return None
            
        return self.databaseStore[str(key).encode("utf-8")]
    
    def inAccountServer(self, key):
        """
        Return if a key is within' our databases storage.
        If we don't have a storage, This is always False.
        """
        if not self.hasAccountServer(): 
            return False
        
        return str(key).encode("utf-8") in self.databaseStore.keys()
    
    def hasAccountServer(self):
        """
        Check if we have a file or server for account database storage.
        """
        return self.databaseStore != None
            
    def handleLoad(self, doId):
        """
        Loads the data from database to memory safely.
        """
        with open(os.path.join(self.databaseDirectory, str(doId) + self.databaseExtension), "rb") as file:
            data = file.read()
            
            if data[:16] != b"# DatabaseObject":
                raise Exception("Invalid header for Database Object!")
                
            dclassName, version, doId, uuId, fieldsData = eval(data)
            
            minVersion = DatabaseObject.minVersion
            lastVersion = DatabaseObject.version
            
            # Check for our minimum supported version.
            if version < minVersion or version > lastVersion:
                raise Exception("Tried to read database object with version %d.%d.%d, But only %d.%d.%d through %d.%d.%d is supported!" % (version[0], version[1], version[2], minVersion[0], minVersion[1], minVersion[2], lastVersion[0], lastVersion[1], lastVersion[2]))

            # Convert the string back into a UUID instance.
            uuId = uuid.UUID(uuId)
            
            dclass = self.dc.getClassByName(dclassName)
            
            do = DatabaseObject(self.manager, doId, uuId, dclass)
            do.setFields(fieldsData)
            return do
            
        print("ERROR: Failed to load Database Object %d!" % (doId))
        return None
            
    def handleSave(self, do):
        """
        Dumps the data from memory out to database safely.
        """
        with open(os.path.join(self.databaseDirectory, str(do.doId) + self.databaseExtension), "wb") as file:
            data = b"# DatabaseObject\n" + pformat((do.dclass.getName(), do.version, do.doId, str(do.uuId), do.fields), width=-1, sort_dicts=True).encode("utf8")
            file.write(data)
        
    def exists(self, doId):
        """
        Return if the specified doId exists in the database.
        """
        return self.vfs.isRegularFile(Filename(os.path.join(self.databaseDirectory.getFullpath(), str(doId) + self.databaseExtension)))
        
    def getNextDoId(self):
        """
        Get the next open doId for the backend we're using.
        """
        
        # We get a doId
        files = os.listdir(self.databaseDirectory)
        
        if sum(filename.endswith(self.databaseExtension) for filename in files) == 0:
            return 10000000
            
        return max([int(filename[:-4]) for filename in files if filename.endswith(self.databaseExtension)]) + 1
        
class DatabaseBackendPacked(DatabaseBackendFile):
    def __init__(self, manager):
        DatabaseBackendFile.__init__(self, manager)
        
    def addToAccountServer(self, key, value):
        """
        Add a value to our database storage, If we don't have one.
        An exception will be raised.
        """
        if not self.hasAccountServer():
            raise Exception("Tried to add value to account server, But we don't have one!")
            
        self.databaseStore[str(key).encode("utf-8")] = str(value)
        
        # If our database has syncing. Then let's sync now.
        if getattr(self.databaseStore, 'sync', None):
            self.databaseStore.sync()
        
    def getFromAccountServer(self, key):
        """
        Get the value of a key in our database storage.
        If we don't have a storage, We always return None.
        """
        if not self.hasAccountServer(): 
            return None
            
        return self.databaseStore[str(key).encode("utf-8")]
    
    def inAccountServer(self, key):
        """
        Return if a key is within' our databases storage.
        If we don't have a storage, This is always False.
        """
        if not self.hasAccountServer(): 
            return False

        return str(key).encode("utf-8") in self.databaseStore.keys()
    
    def hasAccountServer(self):
        """
        Check if we have a file or server for account database storage.
        """
        return self.databaseStore != None
            
    def handleLoad(self, doId):
        """
        Loads the data from database to memory safely.
        """
        with open(os.path.join(self.databaseDirectory, str(doId) + self.databaseExtension), "rb") as file:
            data = file.read()
            
            packer = DCPacker()
            packer.setUnpackData(data)
            
            # Get our version from our packed object.
            majVer = packer.rawUnpackUint8()
            minVer = packer.rawUnpackUint8()
            subVer = packer.rawUnpackUint8()
            version = (majVer, minVer, subVer)
            
            minVersion = DatabaseObject.minVersion
            lastVersion = DatabaseObject.version
            
            # Check for our minimum supported version.
            if version < minVersion or version > lastVersion:
                raise Exception("Tried to read database object with version %d.%d.%d, But only %d.%d.%d through %d.%d.%d is supported!" % (version[0], version[1], version[2], minVersion[0], minVersion[1], minVersion[2], lastVersion[0], lastVersion[1], lastVersion[2]))
                
            dclass = self.manager.dbss.dc.getClassByName(packer.rawUnpackString())
            doId = packer.rawUnpackUint32()
            
            # Convert the string back into a UUID instance.
            uuId = uuid.UUID(packer.rawUnpackString())
            
            do = DatabaseObject(self.manager, doId, uuId, dclass)
            
            # We get every field
            while packer.getUnpackLength() > packer.getNumUnpackedBytes():
                field = dclass.getFieldByName(packer.rawUnpackString())
                
                packer.beginUnpack(field)
                value = field.unpackArgs(packer)
                packer.endUnpack()
                
                if not field.isDb():
                    print("Reading server only field %r." % field.getName())
                    
                do.fields[field.getName()] = value
                
            return do
            
        print("ERROR: Failed to load Database Object %d!" % (doId))
        return None
            
    def handleSave(self, do):
        """
        Dumps the data from memory out to database safely.
        """
        with open(os.path.join(self.databaseDirectory, str(do.doId) + self.databaseExtension), "wb") as file:
            packer = DCPacker()
            
            # Pack our version.
            packer.rawPackUint8(do.majVer)
            packer.rawPackUint8(do.minVer)
            packer.rawPackUint8(do.subVer)
            
            # Pack our DC object.
            packer.rawPackString(do.dclass.getName())
            packer.rawPackUint32(do.doId)
            packer.rawPackString(str(do.uuId))
            
            # We get every field
            for fieldName, value in do.fields.items():
                field = do.dclass.getFieldByName(fieldName)
                
                if field.isDb():
                    packer.rawPackString(field.getName())
                    packer.beginPack(field)
                    field.packArgs(packer, do.fields[field.getName()])
                    packer.endPack()

            data = packer.getBytes()
            file.write(data)
        
    def exists(self, doId):
        """
        Return if the specified doId exists in the database.
        """
        return self.vfs.isRegularFile(Filename(os.path.join(self.databaseDirectory.getFullpath(), str(doId) + self.databaseExtension)))
        
    def getNextDoId(self):
        """
        Get the next open doId for the backend we're using.
        """
        
        # We get a doId
        files = os.listdir(self.databaseDirectory)
        
        if sum(filename.endswith(self.databaseExtension) for filename in files) == 0:
            return 10000000
            
        return max([int(filename[:-4]) for filename in files if filename.endswith(self.databaseExtension)]) + 1
        
class DatabaseBackendMySQL(DatabaseBackend):
    # Types for reading our field datagrams.
    T_NONE = 0
    T_BOOL = 1
    T_UINT = 2
    T_INT = 3
    T_FLOAT = 4
    T_STRING = 5
    T_BLOB = 6
    T_TUPLE = 7
    T_LIST = 8
    T_DICT = 9

    def __init__(self, manager):
        DatabaseBackend.__init__(self, manager)

        # Get the config variables for our MySQL database.
        self.host = ConfigVariableString("mysql-host", "localhost").getValue()
        self.port = ConfigVariableInt("mysql-port", 3306).getValue()
        self.user = ConfigVariableString("mysql-user", "").getValue()
        self.passwd = ConfigVariableString("mysql-passwd", "").getValue()
        self.db = None
        
        # Get our language for any language specifc database, Then get the name.
        language = ConfigVariableString("language", "english").getValue()
        self.dbName = "toontownTopDb"
        if language == 'castillian':
            self.dbName = "es_toontownTopDb"
        elif language == "japanese":
            self.dbName = "jp_toontownTopDb"
        elif language == "german":
            self.dbName = "de_toontownTopDb"
        elif language == "french":
            self.dbName = "french_toontownTopDb"
        elif language == "portuguese":
            self.dbName = "br_toontownTopDb"
        
        # Try connecting to our MySQL database.
        self.connect(self.host, self.port, self.user, self.passwd)
        
    def connect(self, host, port, user, passwd):
        # Try to connect to our MySQL database at the host.
        try:
            self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd)
        except MySQLdb.OperationalError as e:
            raise Exception("Failed to connect to MySQL db=%s at %s:%d."% (self.dbName, host, port))
            return
            
        print("Connected to gamedb=%s at %s:%d." % (self.dbName, host, port))
        
        # Temp hack for developers, Create DB structure if it doesn't exist already.
        cursor = self.db.cursor()
        try:
            cursor.execute("CREATE DATABASE `%s`" % self.dbName)
            if __debug__:
                print("Database '%s' did not exist, created a new one!" % self.dbName)
        except MySQLdb.ProgrammingError as e:
            # print('%s' % str(e))
            pass
        except MySQLdb.OperationalError as e:
            # print('%s' % str(e))
            pass
            
        # We don't want our data to auto-commit, We want to rollback any errors.
        self.db.autocommit(False)
            
        cursor.execute("USE `%s`" % self.dbName)
        if __debug__:
            print("Using database '%s'" % self.dbName)
            
        # We've connected to our database! Now we want to create our tables if we need to.
        # Let's check for them all.
        self.checkTables()
            
    def reconnect(self):
        if not self.db:
            # For some reason, We weren't connected to begin with! Try to connect to the server!
            #print("MySQL server was missing, attempting to reconnect.")
            #self.db.close()
            self.db = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd)
            # We don't want our data to auto-commit, We want to rollback any errors.
            self.db.autocommit(False)
        else:
            # Ping the server, And attempt to reconnect to the host.
            self.db.ping(True)

        cursor = self.db.cursor()
        cursor.execute("USE `%s`" % self.dbName)
        print("Reconnected to MySQL server at %s:%d." % (self.host, self.port))

    def disconnect(self):
        if self.db:
            self.db.close()
            self.db = None
            
    def checkTables(self):
        if not self.db:
            print("Could not check the SQL tables because we don't have a MYSQL server connection! Attempting to reconnect.")
            # Reconnect if we can.
            self.reconnect()
            # Retry our check.
            self.checkTables()
            return

        cursor = self.db.cursor()
        dictCursor = MySQLdb.cursors.DictCursor(self.db)
        try:
            self.db.begin() # Start transaction
            
            # Check our "database server" accounts table. 
            cursor.execute("Show tables like 'accounts';")
            if not cursor.rowcount:
                # We know the accounts table doesn't exist correctly, create it again
                cursor.execute("""
                DROP TABLE IF EXISTS accounts;
                """)

                cursor.execute("""
                CREATE TABLE accounts(
                  accountName       VARCHAR(10) NOT NULL,
                  doId              BIGINT NOT NULL,
                  PRIMARY KEY (accountName),
                  UNIQUE INDEX uidx_doId(doId)
                )
                ENGINE=Innodb
                DEFAULT CHARSET=utf8;
                """)
            
            # Check the table which stores all the root DC objects, (Only central info, No fields)
            cursor.execute("Show tables like 'objects';")
            if not cursor.rowcount:
                # We know the objects table doesn't exist correctly, create it again
                cursor.execute("""
                DROP TABLE IF EXISTS objects;
                """)

                cursor.execute("""
                CREATE TABLE objects(
                  dcClass       VARCHAR(32) NOT NULL,
                  doId          BIGINT NOT NULL,
                  uuId          VARCHAR(36) NOT NULL,
                  PRIMARY KEY (doId),
                  UNIQUE INDEX uidx_uuid(uuId)
                )
                ENGINE=Innodb
                DEFAULT CHARSET=utf8;
                """)
                
            # Check our field tables which store all the fields for our DC Objects. (No central info, Only fields.)
            for i in range(0, self.dc.getNumClasses()):
                dcc = self.dc.getClass(i)
                dcName = dcc.getName()
                
                cursor.execute("Show tables like '%s_field';" % (dcName))
                if cursor.rowcount: break
                
                ss = """CREATE TABLE IF NOT EXISTS %s_fields(
                  doId        BIGINT NOT NULL PRIMARY KEY""" % (dcName)
                
                numFields = 0
                for j in range(0, dcc.getNumInheritedFields()):
                    field = dcc.getInheritedField(j)
                    if field.isDb() and not field.asMolecularField():
                        # TODO: See if you can't find a convenient way to get the max length of
                        #       for example a string field, and use a VARCHAR(len) instead of MEDIUMBLOB.
                        #       Same for blobs with VARBINARY.
                        ss += ",%s MEDIUMBLOB" % field.getName()
                        numFields += 1
                
                ss += """)
                         ENGINE=Innodb
                         DEFAULT CHARSET=utf8;
                      """
                
                # If more then one field exists to store, Then we store the table.
                # Otherwise, It's a waste of space.
                if numFields > 0: cursor.execute(ss)
                    
                
            self.db.commit() # End transaction
        except MySQLdb.OperationalError as e:
            self.notify.warning("Unknown error when creating tables, retrying:\n%s" % str(e))
            self.db.rollback() # Revert transaction
        except Exception as e:
            # Attempt to revert transaction.
            try: self.db.rollback()
            except: pass
            
            # Output our error.
            traceback.print_exc()
            
    def addToAccountServer(self, key, value):
        """
        Add a value to our database storage, If we don't have one.
        An exception will be raised.
        """
        if not self.hasAccountServer():
            raise Exception("Tried to add value to account server, But we don't have one!")
            
        cursor = self.db.cursor()
        try:
            self.db.begin() # Start transaction
            
            # Check our "database server" accounts table. 
            cursor.execute("Show tables like 'accounts';")
            if not cursor.rowcount:
                self.db.rollback() # Revert transaction
                raise Exception("Tried to add value to account server, But the table for our accounts doesn't exist!")
                return
                
            if self.getFromAccountServer(key) == value:
                raise Exception("Tried to add value to account server, But the table for this account already exists!")
                return

            cursor.execute("INSERT INTO accounts (accountName, doId) VALUES (%s, %s)", (key, value))
            
            self.db.commit() # End transaction
        except MySQLdb.OperationalError as e:
            self.db.rollback() # Revert transaction
        except Exception as e:
            self.db.rollback() # Revert transaction
            
            # Output our error.
            traceback.print_exc()
        
    def getFromAccountServer(self, key):
        """
        Get the value of a key in our database storage.
        If we don't have a storage, We always return None.
        """
        if not self.hasAccountServer(): 
            return None

        cursor = MySQLdb.cursors.DictCursor(self.db)
        try:
            # Check our "database server" accounts table. 
            cursor.execute("Show tables like 'accounts';")
            if not cursor.rowcount: return None
            
            cursor.execute("SELECT doId FROM accounts where accountName=%s", (key,))
            res = cursor.fetchone()
            
            if not res: return None
            return res["doId"]
        except MySQLdb.OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exc()
    
    def inAccountServer(self, key):
        """
        Return if a key is within' our databases storage.
        If we don't have a storage, This is always False.
        """
        if not self.hasAccountServer(): 
            return False

        return self.getFromAccountServer(key) != None
    
    def hasAccountServer(self):
        """
        Check if we have a file or server for account database storage.
        """
        
        if not self.db: return False

        cursor = self.db.cursor()
        try:
            # Check our "database server" accounts table. 
            cursor.execute("Show tables like 'accounts';")
            if cursor.rowcount: return True
        except MySQLdb.OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exc()
            
        return False
        
    def __unpackValue(self, value=None, field=None, dgi=None):
        if isinstance(value, str): # Make sure we're working with a bytes object.
            value = value.encode("utf-8")
            
        if not dgi:
            dg = Datagram(value)
            dgi = DatagramIterator(dg)
        
        typeCode = dgi.getUint8()
        
        if typeCode == self.T_NONE:
            value = None
        elif typeCode == self.T_BOOL:
            value = dgi.getBool()
        elif typeCode == self.T_UINT:
            value = dgi.getUint64()
        elif typeCode == self.T_INT:
            value = dgi.getInt64()
        elif typeCode == self.T_FLOAT:
            value = dgi.getFloat64()
        elif typeCode == self.T_STRING:
            value = dgi.getString32()
        elif typeCode == self.T_BLOB:
            value = dgi.getBlob32()
        elif typeCode == self.T_TUPLE:
            value = ()
            size = dgi.getUint32()
            for i in range(0, size):
                value += (self.__unpackValue(field=field, dgi=dgi),)
        elif typeCode == self.T_LIST:
            value = []
            size = dgi.getUint32()
            for i in range(0, size):
                value.append(self.__unpackValue(field=field, dgi=dgi))
        elif typeCode == self.T_DICT:
            value = {}
            size = dgi.getUint32()
            for i in range(0, size):
                # Dicts have both an key and a value. We pack them one after another.
                key = self.__unpackValue(field=field, dgi=dgi)
                item = self.__unpackValue(field=field, dgi=dgi)
                value[key] = item
        else:
            print("Failed to unpack unknown typecode for field '%s'!" % (field.getName()))

        return value
        
    def handleLoad(self, doId):
        """
        Loads the data from database to memory safely.
        """

        cursor = MySQLdb.cursors.DictCursor(self.db)
        try:
            if not self.exists(doId):
                return None # If the doId doesn't exist. Just return nothing.
                
            # Check our databases dc object table. 
            cursor.execute("Show tables like 'objects';")
            if not cursor.rowcount:
                print("Can't load a database object because the object table is missing!")
                return None # If the table doesn't exist. Just return the default.
            
            cursor.execute("SELECT * FROM objects where doId=%s", (doId,))
            objData = cursor.fetchone()
            if not objData: 
                print("Can't load a database object because the object does not exist!")
                return None # If we got no result, There is no objects.
            
            dcClassName = objData["dcClass"]
            dcClass = self.dc.getClassByName(dcClassName)
            if not dcClass:
                print("Can't load a database object because the objects dcclass does not exist!")
                return None # If we got no result, There is no valid class.

            # Create our Database Object.
            do = DatabaseObject(self.manager, objData["doId"], uuid.UUID(objData["uuId"]), dcClass)
            
            ss = "SELECT * FROM %s_fields where doId=%%s" % (dcClassName)
            cursor.execute(ss, (doId,))
            res = cursor.fetchone()
            if not res:
                print("Can't load a database object because the object does not have fields!")
                return None # If we got no result, There is no valid fields.
            
            del res["doId"] # This isn't needed or used here.
            
            fields = {}
            
            # Go through all the results and unpack them.
            for fieldName, value in res.items():
                field = dcClass.getFieldByName(fieldName)
                if not field: continue
                
                # Unpack our field.
                value = self.__unpackValue(value=value, field=field)
                
                fields[fieldName] = value 
                
            # Set our fields!
            do.setFields(fields)
            
            return do
        except MySQLdb.OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exc()
            
        return None
        
    def __packValue(self, value, field, root=True):
        dg = Datagram()
        dgi = DatagramIterator(dg)
        
        if value == None:
            if root and field and field.hasDefaultValue():
                # Unpack the default value so we can use it.
                packer = DCPacker()
                packer.setUnpackData(valueData)
                packer.beginUnpack()
                blob = self.__packValue(packer.unpackArgs(), field, root=False) # Specially pack the value for our interest.
                packer.endUnpack()
                # Append the packed data to our dg.
                dg.appendData(blob)
            else:
                dg.addUint8(self.T_NONE)
        elif isinstance(value, bool):
            dg.addUint8(self.T_BOOL)
            dg.addBool(value)
        elif isinstance(value, int):
            if value >= 0:
                dg.addUint8(self.T_UINT)
                dg.addUint64(value)
            else:
                dg.addUint8(self.T_INT)
                dg.addInt64(value)
        elif isinstance(value, float):
            dg.addUint8(self.T_FLOAT)
            dg.addFloat64(value)
        elif isinstance(value, str):
            dg.addUint8(self.T_STRING)
            dg.addString32(value)
        elif isinstance(value, bytes):
            dg.addUint8(self.T_BLOB)
            dg.addBlob32(value)
        elif isinstance(value, tuple):
            dg.addUint8(self.T_TUPLE)
            dg.addUint32(len(value))
            for i in range(0, len(value)):
                blob = self.__packValue(value[i], field, root=False)
                # Append the packed data to our dg.
                dg.appendData(blob)
        elif isinstance(value, list):
            dg.addUint8(self.T_LIST)
            dg.addUint32(len(value))
            for i in range(0, len(value)):
                blob = self.__packValue(value[i], field, root=False)
                # Append the packed data to our dg.
                dg.appendData(blob)
        elif isinstance(value, dict):
            dg.addUint8(self.T_DICT)
            dg.addUint32(len(value))
            for i, j in value.items():
                keyBlob = self.__packValue(i, field, root=False)
                valueBlob = self.__packValue(k, field, root=False)
                # Append the packed data to our dg.
                dg.appendData(keyBlob)
                dg.appendData(valueBlob)
        else:
            print("Failed to pack value '%s' for %s!" % (value, field.getName()))
            dg.addUint8(self.T_NONE)

        value = dgi.getRemainingBytes() # .decode("latin-1")
        return value
        
    def handleSave(self, do):
        """
        Dumps the data from memory out to database safely.
        """
        

        cursor = self.db.cursor()
        try:
            self.db.begin() # Start transaction
            
            # Check our "database server" accounts table. 
            cursor.execute("Show tables like 'objects';")
            if not cursor.rowcount:
                self.db.rollback() # Revert transaction
                raise Exception("Tried to add database object to database, But the table for our objects doesn't exist!")
                return
            
            if not self.exists(do.doId):
                # Create the dc object handler for our newly saved database object.
                cursor.execute("INSERT INTO objects (dcClass, doId, uuId) VALUES (%s, %s, %s);", (do.dclass.getName(), do.doId, str(do.uuId)))
                
                # Create the fields table for our newly handled dc object and fill them.
                ss = "INSERT INTO %s_fields (doId) VALUES (%%s);" % (do.dclass.getName())
                cursor.execute(ss, (do.doId,))
                
                # Go over all our fields and save them all into our fields.
                for fieldName, value in do.getFields().items():
                    field = do.dclass.getFieldByName(fieldName)
                    if not field or not field.isDb():
                        continue
                    value = self.__packValue(value, field)
                    # Some types of value need different packing then others.
                    if isinstance(value, str):
                        fs = "UPDATE %s_fields SET %s='%s' WHERE doId=%s;" % (do.dclass.getName(), fieldName, MySQLdb.converters.escape_string(value), str(do.doId))
                        cursor.execute(fs)
                    else:
                        fs = "UPDATE %s_fields SET %s=%%s WHERE doId=%s;" % (do.dclass.getName(), fieldName, str(do.doId))
                        cursor.execute(fs, (value,))
            else:
                # Just update and save our fields!
                for fieldName, value in do.getFields().items():
                    field = do.dclass.getFieldByName(fieldName)
                    if not field or not field.isDb():
                        continue
                    value = self.__packValue(value, field)
                    # Some types of value need different packing then others.
                    if isinstance(value, str):
                        fs = "UPDATE %s_fields SET %s='%s' WHERE doId=%s;" % (do.dclass.getName(), fieldName, value, str(do.doId))
                        cursor.execute(fs)
                    else:
                        fs = "UPDATE %s_fields SET %s=%%s WHERE doId=%s;" % (do.dclass.getName(), fieldName, str(do.doId))
                        cursor.execute(fs, (value,))
                
            
            self.db.commit() # End transaction
        except MySQLdb.OperationalError as e:
            self.db.rollback() # Revert transaction
        except Exception as e:
            self.db.rollback() # Revert transaction
            
            # Output our error.
            traceback.print_exc()
        
    def exists(self, doId):
        """
        Return if the specified doId exists in the database.
        """

        cursor = MySQLdb.cursors.DictCursor(self.db)
        try:
            # Check our databases dc object table. 
            cursor.execute("Show tables like 'objects';")
            if not cursor.rowcount: return False
            
            cursor.execute("SELECT uuId FROM objects where doId=%s", (doId,))
            res = cursor.fetchone()
            
            if not res: return False
            return res.get("uuId", None) != None
        except MySQLdb.OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exc()
            
        return False
        
    def getNextDoId(self):
        """
        Get the next open doId for the backend we're using.
        """
        cursor = MySQLdb.cursors.DictCursor(self.db)
        try:
            # Check our databases dc object table. 
            cursor.execute("Show tables like 'objects';")
            if not cursor.rowcount: return 10000000 # If the table doesn't exist. Just return the default.
            
            cursor.execute("SELECT * FROM objects")
            res = cursor.fetchall()
            
            if not res: return 10000000 # If we got no result, There is no objects.
            return 10000000 + len(res) # Add the nujmber of objects to the base id.
        except MySQLdb.OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exc()
            
        return 10000000

class DatabaseManager:
    def __init__(self, dbss):
        # Database Server
        self.dbss = dbss
        
        # DC File
        self.dc = self.dbss.dc
        
        # Cached DBObjects
        self.cache = {}
        
        # DBS Objects
        self.dcObjectTypes = self.dbss.dcObjectTypes
        self.dcObjectTypeFromName = self.dbss.dcObjectTypeFromName
        
        # Get our Panda3D Virtual File System, And keep a reference.
        self.vfs = VirtualFileSystem.getGlobalPtr()
        
        # Get our backend.
        self.backend = None
        self.backendName = ConfigVariableString('database-backend', "raw").getValue()
        if self.backendName == "raw":
            self.backend = DatabaseBackendRaw(self)
        elif self.backendName == "packed":
            self.backend = DatabaseBackendPacked(self)
        elif self.backendName == "sql":
            self.backend = DatabaseBackendMySQL(self)
        else: # Default to raw.
            self.backend = DatabaseBackendRaw(self)
        
    def createDatabaseObject(self, dcObjectType, fields={}):
        """
        Create a database object with the dclass and default fields
        """
        
        # We look for the dclass by getting it from our dc object type dict.
        dclass = self.dcObjectTypes.get(dcObjectType, None)
        if not dclass:
            raise ValueError(dcObjectType)

        # Get the next available doId.
        doId = self.backend.getNextDoId()

        # Generate a unique indentifier for the database object.
        m = hashlib.md5()
        m.update(("%s-%d-%s" % (str(dclass.getName()), doId, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))).encode('utf-8'))

        doUuId = uuid.UUID(m.hexdigest(), version=4)

        # We generate the DatabaseObject
        do = DatabaseObject(self, doId, doUuId, dclass)
        
        # We set default values
        packer = DCPacker()
        for n in range(dclass.getNumInheritedFields()):
            field = dclass.getInheritedField(n)
            if field.isDb():
                packer.setUnpackData(field.getDefaultValue())
                packer.beginUnpack(field)
                do.fields[field.getName()] = field.unpackArgs(packer)
                packer.endUnpack()
                
                # Unpack our default value then insert it into our dos fields.
                #value = do.unpackField(field.getName(), field.getDefaultValue())
                #do.fields[field.getName()] = value #do.packField(field.getName(), value)
                
        # Now we set the fields to the ones we got early.
        for fieldName, *values in fields.items():
            field = dclass.getFieldByName(fieldName)
            
            if field.asAtomicField():
                do.fields[field.getName()] = values
                
            elif field.asMolecularField():
                raise Exception("No.")
                
            elif field.asParameter():
                if len(values) != 1:
                    raise Exception("Arg count mismatch")
                    
                do.fields[field.getName()] = values[0]
            else:
                print("Skipping field '%s' for saving!" % (field.getName()))

        # Set our DC Object Type if we have it!
        if dclass.getName() in list(self.dcObjectTypeFromName.keys()):
            do.fields["DcObjectType"] = dclass.getName()

        # We save the object
        self.saveDatabaseObject(do)
        return do
        
    def createDatabaseObjectFromName(self, dclassName, fields={}):
        # We look for the dclass and make sure it exists.
        if not self.dc.getClassByName(dclassName):
            raise NameError(dclassName)
        
        # Make sure the dcclass can be stored in our database.
        # Any dcclass without a dc object type, Can not be stored.
        if not dclassName in list(self.dcObjectTypeFromName.keys()):
            raise NameError(dclassName)
        
        # Create a database object from the type we caculated.
        dcObjectType = self.dcObjectTypeFromName[dclassName]
        return self.createDatabaseObject(dcObjectType, fields=fields)
        
    def hasDatabaseObject(self, doId):
        """
        Check if a database object exists.
        """
        return self.backend.exists(doId)
        
    def saveDatabaseObject(self, do):
        """
        Save a database object
        """
        self.backend.save(do)

    def loadDatabaseObject(self, doId):
        """
        Load a database object by its id
        """
        if not doId in self.cache:
            self.cache[doId] = self.backend.load(doId)

        return self.cache[doId]