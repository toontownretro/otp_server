import base64, hashlib, os, threading, traceback, uuid

from datetime import datetime

from pprint import pformat

# Use MariaDB for our SQL connection.
import mariadb
from mariadb import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    PoolError,
    ProgrammingError,
    Warning,
)
from mariadb_shared.constants.ERR import *

from panda3d.core import ConfigVariableInt, ConfigVariableString, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem
from panda3d.direct import DCPacker

from database_object import DatabaseObject
from distributed_object import DistributedObject
from msgtypes import *

'''
We use MariaDB for our SQL servers.
Toontown Online used MySQL but MySQL has fallen behind in every way and that
is besides the licensing issues.

We currently use MariaDB 2.0 which is in pre-release so you can install the connector with pip below.

python.exe -m pip install --pre mariadb[binary,pool]
'''

class DatabaseSQL:
    # SQL Errors
    DbAlreadyExists = ER_DB_CREATE_EXISTS
    TableAlreadyExists = ER_TABLE_EXISTS_ERROR
    ServerShuttingDown = ER_SERVER_SHUTDOWN
    ServerGoneAway = CR_SERVER_GONE_ERROR
    ServerLost = CR_SERVER_LOST
    
    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = None
        self.db_name = db
        
        self._mutex_lock = threading.RLock()
        
    async def connect(self):
        # Try to connect to our SQL database at the host.
        try:
            self.db = await mariadb.asyncConnect(host=self.host, port=self.port, user=self.user, passwd=self.passwd)
        except OperationalError as e:
            raise Exception(f"Failed to connect to SQL db={self.db_name} at {self.host}:{self.port}.")
            return
            
        print(f"Connected to database={self.db_name} at {self.host}:{self.port}.")
        
        # Temp hack for developers, Create DB structure if it doesn't exist already.
        cursor = self.db.cursor()
        try:
            await cursor.execute(f"CREATE DATABASE `{self.db_name}`")
            if __debug__:
                print(f"Database '{self.db_name}' did not exist, created a new one!")
        except ProgrammingError as e:
            pass
        except OperationalError as e:
            pass
            
        try:
            await cursor.execute(f"USE `{self.db_name}`")
            if __debug__:
                print(f"Using database '{self.db_name}'")
        finally:
            await cursor.close()
            
        # We don't want our data to auto-commit, We want to rollback any errors.
        self.db.autocommit(False)
        
    async def reconnect(self):
        if not self.db: return False

        # Ping the server, If we failed attempt to reconnect to the host.
        try:
            await self.db.ping(True)
        except:
            try:
                await self.db.reconnect()
            except Exception as e:
                return False

            cursor = self.db.cursor()
            
            try:
                await cursor.execute(f"CREATE DATABASE `{self.db_name}`")
                if __debug__:
                    print(f"Database '{self.db_name}' did not exist, created a new one!")
            except ProgrammingError as e:
                pass
            except OperationalError as e:
                pass
            
            try:
                await cursor.execute(f"USE `{self.db_name}`")
            finally:
                await cursor.close()
                
            # We don't want our data to auto-commit, We want to rollback any errors.
            self.db.autocommit(False)
            
        print(f"Reconnected to SQL server at {self.host}:{self.port} using database {self.db_name}")
        return True
        
    async def is_connected(self):
        if not self.db: return False
        
        try:
            await self.db.ping(True)
            return True
        except:
            return False

    async def disconnect(self):
        if self.db:
            await self.db.close()
            self.db = None
            
    async def begin(self):
        if not self.db:
            return
        await self.db.begin()
            
    async def commit(self):
        if not self.db:
            return
        await self.db.commit()
        
    async def rollback(self):
        if not self.db:
            return
        await self.db.rollback()
            
    def get_cursor(self):
        if not self.db:
            return None
        return self.db.cursor()
        
    def get_binary_cursor(self):
        if not self.db:
            return None
        return self.db.cursor(binary=True)

    def get_dict_cursor(self):
        if not self.db:
            return None
        return self.db.cursor(dictionary=True)
           
class DCDatabase(DatabaseSQL):
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

    def __init__(self, manager, host, port, user, passwd, db):
        super().__init__(host, port, user, passwd, db)
        
        self.manager = manager
        
        # DC File
        self.dc = self.manager.dc
        self.dc_hash = self.dc.get_hash()
        
    async def connect(self):
        await super().connect()
            
        # We've connected to our database! Now we want to create our tables if we need to.
        # Let's check for them all.
        await self.check_tables()
            
    async def check_tables(self):
        if not self.db:
            return

        cursor = self.get_cursor()
        try:
            await self.begin() # Start transaction
            
            # Check the table which stores all the DC objects
            await cursor.execute("Show tables like 'DCObject';")
            if not cursor.rowcount:
                # We know the dc objects table doesn't exist correctly, create it again.
                await cursor.execute("""
                DROP TABLE IF EXISTS DCObject;
                """)
                
                # A DC object (A collection of information regarding a class in a .dc file for saving)
                # is meant to be a self-contained collection of information about the object in question.
                # This includes the doId, a unique indentifer for the object, and the data for all of the
                # fields marked for storage.
                #
                # A hash for the DCFile instance is also included because if changes occur in the .dc files;
                # Field data that was formerly valid may be become invalid or fields may disappear if the .dc files are changed in any way.
                #
                # The dclass is stored within the FieldData itself as the field 'DcObjectType' and this field is stored
                # in a special manner for easy handling.
                await cursor.execute("""
                CREATE TABLE DCObject(
                  DoId          BIGINT NOT NULL PRIMARY KEY,
                  UUID          VARCHAR(36) NOT NULL,
                  DCHash        INT,
                  FieldData     MEDIUMBLOB,
                  UNIQUE INDEX uidx_uuid(uuId)
                )
                ENGINE=Innodb
                DEFAULT CHARSET=utf8;
                """)
                
            await self.commit() # End transaction
        except OperationalError as e:
            self.notify.warning("Unknown error when creating tables, retrying:\n%s" % str(e))
            await self.rollback() # Revert transaction
        except Exception as e:
            # Attempt to revert transaction.
            try: await self.rollback()
            except: pass
            
            # Output our error.
            traceback.print_exception(e)
        finally:
            await cursor.close()
        
    async def load(self, doId):
        """
        Safely loads the data from database using a mutex lock,
        so the process is thread safe...
        """

        with self._mutex_lock:
            return await self.handle_load(doId)
        
    async def handle_load(self, doId):
        """
        Loads the data from database to memory safely.
        """

        cursor = self.get_dict_cursor()
        try:
            if not await self.exists(doId):
                return None # If the doId doesn't exist. Just return nothing.
                
            # Check our databases dc object table. 
            await cursor.execute("Show tables like 'DCObject';")
            if not cursor.rowcount:
                print("Can't load a database object because the object table is missing!")
                return None # If the table doesn't exist. Just return the default.
            
            await cursor.execute("SELECT * FROM DCObject where DoId=%s", (doId,))
            data = await cursor.fetchone()
            if not data: 
                print("Can't load a database object because the object does not exist!")
                return None # If we got no result, There is no objects.
                
            doId = data["DoId"]
            UUID = uuid.UUID(data["UUID"])
            dc_hash = data["DCHash"]
            field_data = data["FieldData"]
            
            # Prepare to handle the stored field data.
            fields = {}
            
            dg = Datagram(field_data)
            di = DatagramIterator(dg)
            
            # Extract DcObjectType from the fields.
            dc_object_type_field_name = di.get_string()
            dclass_name = di.get_string()
            fields[dc_object_type_field_name] = dclass_name
            
            # Load our dclass to unpack the fields.
            dclass = self.dc.get_class_by_name(dclass_name)
            if not dclass:
                print("Can't load a database object because the objects dclass does not exist!")
                return None # If we got no result, There is no valid class.
                
            # Unpack all of our fields.
            count = di.get_uint32()
            for i in range(0, count):
                field_name = di.get_string()
                field = dclass.get_field_by_name(field_name)
                if not field: continue
                
                packer = DCPacker()
                packer.set_unpack_data(di.get_remaining_bytes())
                packer.begin_unpack(field)
                try:
                    value = field.unpack_args(packer)
                except Exception as e:
                    value = None
                finally:
                    packer.end_unpack()
                
                di.skip_bytes(packer.get_num_unpacked_bytes())
                
                fields[field_name] = value
                
            # Create our Database Object.
            do = DatabaseObject(self.manager, doId, UUID, dclass)
            # Set our fields!
            do.setFields(fields)
            return do
        except OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exception(e)
        finally:
            await cursor.close()
            
        return None
        
    async def save(self, do):
        """
        Safely saves the data to database using a mutex lock,
        so the process is thread safe...
        """

        with self._mutex_lock:
            await self.handle_save(do)
        
    async def handle_save(self, do):
        """
        Dumps the data from memory out to database safely.
        """
        
        cursor = self.get_cursor()
        try:
            await self.begin() # Start transaction
            
            # Check our "database server" accounts table. 
            await cursor.execute("Show tables like 'DCObject';")
            if not cursor.rowcount:
                await self.rollback() # Revert transaction
                raise Exception("Tried to add database object to database, But the table for our objects doesn't exist!")
                return
                
            dg = Datagram()
            di = DatagramIterator(dg)
            
            fields = do.getFields()
            
            # Manually save this field only.
            dg.add_string("DcObjectType")
            dclass_name = fields.get("DcObjectType", do.dclass.get_name())
            dg.add_string(dclass_name)
            if "DcObjectType" in fields:
                del fields["DcObjectType"]
            
            # Extract all of the database fields.
            db_fields = {}
            for fieldName, value in fields.items():
                field = do.dclass.get_field_by_name(fieldName)
                if not field or not field.is_db():
                    continue
                if field.as_molecular_field():
                    continue
                
                db_fields[field] = value
            
            # Pack all of the database fields.
            dg.add_uint32(len(db_fields))
            for field, value in db_fields.items():
                packer = DCPacker()
                # Pack the fields name.
                packer.raw_pack_string(field.get_name())
                # Pack the args to the field.
                packer.begin_pack(field)
                if value != None:
                    field.pack_args(packer, value)
                else:
                    packer.pack_default_value()
                packer.end_pack()
                
                # Append the packed data to our Datagram.
                dg.append_data(packer.get_bytes())
                
            # Get all of our field data as a binary string.
            fieldData = di.get_remaining_bytes()
            
            # Save our DC Object.
            if not await self.exists(do.doId):
                # Create a new DC Object entry in our database.
                cmd = f"INSERT INTO DCObject (DoId, UUID, DCHash, FieldData) VALUES ({do.doId}, {str(do.uuId)}, {self.dc_hash}, %%s);"
                await cursor.execute(cmd, (fieldData,))
            else:
                # Update the existing entry in our database.
                cmd = f"UPDATE DCObject SET FieldData=%%s WHERE DoId={do.doId};"
                await cursor.execute(cmd, (fieldData,))
                cmd = f"UPDATE DCObject SET DCHash={self.dc_hash} WHERE DoId={do.doId};"
                await cursor.execute(cmd, ())
            
            await self.commit() # End transaction
        except OperationalError as e:
            await self.rollback() # Revert transaction
        except Exception as e:
            await self.rollback() # Revert transaction
            
            # Output our error.
            traceback.print_exception(e)
        finally:
            await cursor.close()
        
    async def exists(self, doId):
        """
        Return if the specified doId exists in the database.
        """
        
        data = {}
        
        cursor = self.get_dict_cursor()
        try:
            # Check our databases dc object table. 
            await cursor.execute("Show tables like 'DCObject';")
            if not cursor.rowcount: return False
            
            await cursor.execute("SELECT UUID FROM DCObject where DoId=%s", (doId,))
            data = await cursor.fetchone()
            if not data: data = {}
        except OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exception(e)
        finally:
            await cursor.close()
            
        return data.get("UUID", None) != None
        
    async def get_next_doId(self):
        """
        Get the next open doId for the backend we're using.
        """
        
        doId = 100000000
        
        cursor = self.get_dict_cursor()
        try:
            # Check our databases dc object table. 
            await cursor.execute("Show tables like 'DCObject';")
            
             # If the table doesn't exist. Just return the default.
            if cursor.rowcount:
                await cursor.execute("SELECT * FROM DCObject")
                data = await cursor.fetchall() # If we got no result, There is no objects.
                if data: doId = 100000000 + len(data) # Add the number of objects to the base id.
        except OperationalError as e:
            pass
        except Exception as e:
            # Output our error.
            traceback.print_exception(e)
        finally:
            await cursor.close()
            
        return doId
        
    def __unpack_value(self, value=None, field=None, dgi=None):
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
                value += (self.__unpack_value(field=field, dgi=dgi),)
        elif typeCode == self.T_LIST:
            value = []
            size = dgi.getUint32()
            for i in range(0, size):
                value.append(self.__unpack_value(field=field, dgi=dgi))
        elif typeCode == self.T_DICT:
            value = {}
            size = dgi.getUint32()
            for i in range(0, size):
                # Dicts have both an key and a value. We pack them one after another.
                key = self.__unpack_value(field=field, dgi=dgi)
                item = self.__unpack_value(field=field, dgi=dgi)
                value[key] = item
        else:
            print(f"Failed to unpack unknown typecode for field '{field.getName()}'!")

        return value
        
    def __pack_value(self, value, field, root=True):
        dg = Datagram()
        dgi = DatagramIterator(dg)
        
        if value == None:
            if root and field and field.hasDefaultValue():
                # Unpack the default value so we can use it.
                packer = DCPacker()
                packer.setUnpackData(valueData)
                packer.beginUnpack()
                blob = self.__pack_value(packer.unpackArgs(), field, root=False) # Specially pack the value for our interest.
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
                blob = self.__pack_value(value[i], field, root=False)
                # Append the packed data to our dg.
                dg.appendData(blob)
        elif isinstance(value, list):
            dg.addUint8(self.T_LIST)
            dg.addUint32(len(value))
            for i in range(0, len(value)):
                blob = self.__pack_value(value[i], field, root=False)
                # Append the packed data to our dg.
                dg.appendData(blob)
        elif isinstance(value, dict):
            dg.addUint8(self.T_DICT)
            dg.addUint32(len(value))
            for i, j in value.items():
                keyBlob = self.__pack_value(i, field, root=False)
                valueBlob = self.__pack_value(k, field, root=False)
                # Append the packed data to our dg.
                dg.appendData(keyBlob)
                dg.appendData(valueBlob)
        else:
            print(f"Failed to pack value '{value}' for {field.getName()}!")
            dg.addUint8(self.T_NONE)

        value = dgi.getRemainingBytes() # .decode("latin-1")
        return value

class DatabaseManager:
    def __init__(self, dc):
        # DC File
        self.dc = dc
        
        # Cached DBObjects
        self.cache = {}
        
        # Get our Panda3D Virtual File System, And keep a reference.
        self.vfs = VirtualFileSystem.getGlobalPtr()
        
        self.database_channel_from_dclass_name = {
            #"Account": ACCOUNT_DB_CHANNEL_ID,
            #"DistributedAvatar": AVATAR_DB_CHANNEL_ID,
            #"DistributedPlayer": AVATAR_DB_CHANNEL_ID,
        }
        
        self.host = ConfigVariableString("mysql-host", "localhost").getValue()
        self.port = ConfigVariableInt("mysql-port", 3306).getValue()
        self.user = ConfigVariableString("mysql-user", "").getValue()
        self.passwd = ConfigVariableString("mysql-passwd", "").getValue()
        
        # Get the branch flavor for use with our databases.
        db_salt = ""
        '''
        if __dev__:
            db_salt = ConfigVariableString("dev-branch-flavor", "").getValue()
            if db_salt:
                db_salt = db_salt + '_'
        '''
        
        # Get our language for any language specific database
        language = ConfigVariableString("language", "english").getValue()
        
        # Get the corresponding default db name based on the language
        db_lang = ""
        if language == 'castillian':
            db_lang = "es_"
        elif language == "japanese":
            db_lang = "jp_"
        elif language == "german":
            db_lang = "de_"
        elif language == "french":
            db_lang = "french_"
        elif language == "portuguese":
            db_lang = "br_"
            
        # For now, We assume the default db name is for Toontown by default.
        default_db_name = ConfigVariableString("sql-default-db", "toontownTopDb").getValue()
        
        self.databases = {}
        self.databases[DEFAULT_DB_CHANNEL_ID] = DCDatabase(self, self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}{default_db_name}")
        self.databases[ACCOUNT_DB_CHANNEL_ID] = DCDatabase(self, self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}accounts")
        self.databases[AVATAR_DB_CHANNEL_ID] = DCDatabase(self, self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}avatars")
        #self.databases[AVATAR_FRIENDS_DB_CHANNEL_ID] = FriendDatabase(self.host, self.port, user, self.passwd, f"{db_salt}{db_lang}avatar_friends")
        self.databases[AVATAR_ACCESSORIES_DB_CHANNEL_ID] = DCDatabase(self, self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}avatar_accessories")
        self.databases[AWARDS_DB_CHANNEL_ID] = DatabaseSQL(self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}awards")
        self.databases[CODE_REDEMPTION_DB_CHANNEL_ID] = DatabaseSQL(self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}code_redemption")
        self.databases[GUILDS_DB_CHANNEL_ID] = DatabaseSQL(self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}guilds")
        self.databases[HOLIDAY_SCHEDULES_DB_CHANNEL_ID] = DatabaseSQL(self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}holidayschedules")
        self.databases[STATUS_DB_CHANNEL_ID] = DatabaseSQL(self.host, self.port, self.user, self.passwd, f"{db_salt}{db_lang}status")
        
    async def initialize(self):
        for channel, db in self.databases.items():
            await db.connect()
            
    async def create_dc_object(self, dclass, fields={}):
        """
        Create a DC database object with the dclass, fields with default values
        and any manually specified values for fields.
        """
        
        if not dclass: return None
        
        assert(dclass.get_field_by_name("DcObjectType") != None, f"Tried to create a DC Object in our databases but it's missing the 'DcObjectType' field!")
        
        time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        doId = await self.databases[DEFAULT_DB_CHANNEL_ID].get_next_doId()
        
        # Generate a unique identifier for the database object.
        m = hashlib.md5()
        m.update(f"{dclass.get_name()}{doId}{time_str}".encode('utf-8'))
        uuId = uuid.UUID(m.hexdigest(), version=4)
        
        # Create the DatabaseObject.
        do = DatabaseObject(self, doId, uuId, dclass)
        
        # We set default values
        packer = DCPacker()
        for n in range(dclass.get_num_inherited_fields()):
            field = dclass.get_inherited_field(n)
            if not field.is_db(): continue

            packer.set_unpack_data(field.get_default_value())
            packer.begin_unpack(field)
            do.fields[field.get_name()] = field.unpack_args(packer)
            packer.end_unpack()
                
        # Now we set the fields to the ones we got early.
        for field_name, *values in fields.items():
            field = dclass.get_field_by_name(field_name)
            if not field:
                continue
            
            if field.as_atomic_field():
                do.fields[field_name] = values
                
            elif field.as_molecular_field():
                continue
                
            elif field.as_parameter():
                if len(values) != 1:
                    raise Exception("Arg count mismatch")
                    
                do.fields[field_name] = values[0]
            else:
                print(f"Skipping field '{field_name}' for saving!")
        
        # Save the newly created DC database object.
        await self.save_dc_object(do)
        
        return do
        
    async def save_dc_object(self, do):
        if not do: return
        
        # Get the channel for saving the DC database object.
        db_channel = self.database_channel_from_dclass_name.get(do.dclass.get_name(), DEFAULT_DB_CHANNEL_ID)
        # Get the database from the channel.
        db = self.databases.get(db_channel, None)
        if not db:
            raise Exception("Failed to save dc object to a database!")
            return
        
        await db.save(do)
        
    async def load_dc_object(self, doId):
        """
        Load a DC database object by its id,
        If it doesn't exist we return None.
        """

        db_channel = await self.has_dc_object(doId)
        if not db_channel:
            return None

        do = self.cache.get(doId, None)
        if not do:
            db = self.databases[db_channel]
            do = await db.load(doId)
            self.cache[doId] = do
            
        return do
        
    async def has_dc_object(self, doId):
        """
        Check if a DC database object exists,
        If it does, return the database channel it exists in.
        """
        
        if not doId: return 0
        
        for channel, db in self.databases.items():
            if not isinstance(db, DCDatabase):
                continue
                
            if await db.exists(doId): 
                return channel
            
        return 0