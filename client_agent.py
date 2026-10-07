import asyncio, functools, os, socket, ssl, struct, time, traceback

from panda3d.core import ConfigVariableInt, ConfigVariableBool, ConfigVariableString, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem

from panda3d.toontown import DNAStorage, loadDNAFileAI

from central_logger import CentralLogger
from connection import Server
from client import Client
from distributed_object import DistributedObject
from distributed_directory import DistributedDirectory
from server_interface import ServerInterface
from msgtypes import *

class ClientAgent(ServerInterface, Server):
    client_cls = Client
    
    def __init__(self, addr="0.0.0.0", port=6667, channel=ConfigVariableInt("client-agent-id", 20200000).getValue()):
        ServerInterface.__init__(self)
        Server.__init__(self, addr, port)
        
        self.channel = channel
        
        # SSL Context
        #context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        #context.load_cert_chain('secure/server.cert', 'secure/server.key')
        
        self.visgroups = {}
            
        self.name_dictionary = {}
        
        self.load_dc()
        self.load_dna()
        self.load_namemaster()
        
        # These are a local reflection of the objects on the Stateserver,
        # We can only see what the Stateserver shares.
        # We also store objects we only need to have for our clients here too.
        self.objects = {}
        
        self.__context = 0
        
        # Special fields IDs (cache)
        self.setTalkFieldId = self.dc.getClassByName("TalkPath_owner").getFieldByName("setTalk").getNumber()
        
        self.set_name("CLIENTAGENT")
        
    @classmethod
    async def initialize(cls, addr, port, channel):
        return cls(addr, port, channel)
        
    async def connect(self, addr, port):
        connected = await ServerInterface.connect(self, addr, port)
        
        # Failed to connect to the Message Director!
        if not connected:
            return False
            
        # Setup our information on the Message Director.
        await self.register_for_channel(self.channel)
        await self.register_for_channel(CLIENTAGENT_ID)
        await self.set_connection_name(self.name)
        
        print(f"[{self.name}]: Connected and running on channel {self.channel}.")
        return True
        
    async def close_interface(self):
        # Close our connection.
        await ServerInterface.close(self)
        
    async def close_server(self):
        # Close our server.
        await Server.close(self)
        
    async def close(self):
        await self.close_interface()
        await self.close_server()
        
    async def handle_client(self, reader, writer):
        client = await self.client_cls.from_server(self, reader, writer)
        self.clients.append(client)
        
        addr = client.get_address()
        print(f"[{self.name}]: Accepted client from address {addr[0]}:{addr[1]}.")
        
    async def flush_client(self, client):
        try:
            data = await client.read(2048)
        except Exception as e:
            data = None
            
        if not data or client.is_closed():
            return
            
        addr = client.get_address()
        try:
            await client.receive_data(data)
            #print(f"[{self.name}]: Recieved data from client at address {addr[0]}:{addr[1]}.")
        except Exception as e:
            traceback.print_exception(e)
            print(f"[{self.name}]: Failed to recieve data from client at address {addr[0]}:{addr[1]}.")
        
    async def flush_interface(self):
        await ServerInterface.flush_interface(self)
        
    async def flush_server(self):
        await Server.flush_server(self)

    async def receive_datagram(self, dg):
        di = DatagramIterator(dg)
        
        # First check if the datagram has anything in it.
        if not di.getRemainingSize() >= 1:
            return
            
        # Get the amount of channels the datagram wants to be routed too.
        count = di.getUint8()
        if count <= 0:
            return
            
        # Extract all of the channels from the datagram.
        if not di.getRemainingSize() >= 8 * count:
            return
        channels = set()
        for _ in range(count):
            channels.add(di.getUint64())
            
        # Get the sender for the datagram and it's 'code' (Identifier for what type of datagram it is)
        if not di.getRemainingSize() >= 10:
            return
        sender = di.getUint64()
        code = di.getUint16()
        
        # Extract all of the remaining data into it's own datagram.
        data = di.getRemainingBytes()
        dg = Datagram(bytes(data))
        
        # Check if our channel is inside.
        if self.channel in channels or CLIENTAGENT_ID in channels:
            await self.handle_internal_channel(sender, code, dg)
                
        # Otherwise, We'll distribute the channel check for each client indivdually.
        args = []
        for client in self.clients:
            args.append((client, channels, sender, code, dg))
        
        await asyncio.gather(*map(self.handle_datagram_for_client, args))
        
    async def handle_internal_channel(self, sender, code, dg):
        di = DatagramIterator(dg)
        
        if code == SERVER_PING:
            if di.getRemainingSize() <= 10:
                return

            sec = di.getUint32()
            usec = di.getUint32()
            url = di.getString()
            channel = di.getUint32()
            
            # Respond
            dg = Datagram()
            dg.addUint32(sec)
            dg.addUint32(usec)
            dg.addString(url)
            dg.addUint32(channel)
            
            await self.send_message([sender], self.channel, SERVER_PING, dg)
        elif code in (STATESERVER_OBJECT_GENERATE_WITH_REQUIRED, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER):
            parentId = di.getUint32()
            zoneId = di.getUint32()
            classId = di.getUint16()
            doId = di.getUint32()
            
            if not doId in self.objects:
                # We get the dclass
                dclass = self.dc.getClass(classId)
                
                # We create the object
                do = DistributedObject(doId, dclass, parentId, zoneId)
                do.senders.append(sender)
                
                # We save the object
                self.objects[doId] = do
            else:
                do = self.objects[doId]
                do.parentId = parentId
                do.zoneId = zoneId
                do.senders.append(sender)

            # We update the object
            do.receiveRequired(di)
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                do.receiveOther(di)
            
            # Prepare a list of partial coroutines for the clients.
            routines = [
                functools.partial(client.receive_create, do, sender, code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
                for client in self.clients  
            ]
            coroutines = [f() for f in routines]
        
            # Have all of our clients recieve our create for the object.
            await asyncio.gather(*coroutines)
            
        elif code == STATESERVER_OBJECT_DELETE_RAM:
            # We are asked to delete an object.
            
            doId = di.getUint32()
            
            if not doId in self.objects:
                return

            do = self.objects[doId]
            
            # Prepare a list of partial coroutines for the clients.
            routines = [
                functools.partial(client.receive_delete, do, sender)
                for client in self.clients  
            ]
            coroutines = [f() for f in routines]
            
            # Have all of our clients recieve our delete for the object.
            await asyncio.gather(*coroutines)
            
            # Remove the object from our dict.
            del self.objects[doId]
            
        elif code == STATESERVER_OBJECT_SET_ZONE:
            # We are asked to move an object.
            
            doId = di.getUint32()

            if not doId in self.objects:
                return

            parentId = di.getUint32()
            zoneId = di.getUint32()
                
            do = self.objects[doId]
            
            # We get the previous zone
            prevParentId, prevZoneId = do.parentId, do.zoneId
            
            # We set the new zone
            do.parentId = parentId
            do.zoneId = zoneId
            
            # Prepare a list of partial coroutines for the clients.
            routines = [
                functools.partial(client.receive_move, do, prevParentId, prevZoneId, sender)
                for client in self.clients
            ]
            coroutines = [f() for f in routines]
        
            # Have all of our clients recieve our move for the object.
            await asyncio.gather(*coroutines)
            
        elif code == STATESERVER_OBJECT_UPDATE_FIELD:
            # We are asked to update an object.
            
            doId = di.getUint32()
            
            if not doId in self.objects:
                return
            
            do = self.objects[doId]
            
            # Now let's update our object field.
            fieldId = di.getUint16()
            
            # The remaining data is field data
            data = di.getRemainingBytes()
            
            # We apply the update
            field = do.dclass.getFieldByIndex(fieldId)
            
            # Receieve the update onto the object.
            do.receiveField(field, di)
            
            # Prepare a list of partial coroutines for the clients.
            routines = [
                functools.partial(client.receive_update, do, field, data, sender)
                for client in self.clients
            ]
            coroutines = [f() for f in routines]
        
            # Have all of our clients recieve our update for the object.
            await asyncio.gather(*coroutines)
        else:
            print(f"[{self.name}]: Unexpected message on internal channels (code {code})")
        
    async def handle_datagram_for_client(self, *args):
        client, channels, sender, code, datagram = args
        await client.handle_agent_datagram(channels, sender, code, datagram)
        
    def load_dna(self):
        # Get our Panda3D Virtual File System.
        vfs = VirtualFileSystem.getGlobalPtr()
        
        # Look for our file locations and read them in.
        searchPath = DSearchPath()
        
        # In other environments, including the dev environment, look here:
        ttmodelsPath = os.path.expandvars('$TTMODELS') or './ttmodels'
        searchPath.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(ttmodelsPath + '/src/dna')))
        
        # Every DNA file with visgroups. We don't care about all of them.
        dnaFiles = [
            "cog_hq_cashbot_sz.dna",
            
            "cog_hq_lawbot_sz.dna",
            
            "cog_hq_sellbot_11200.dna",
            "cog_hq_sellbot_sz.dna",
            
            # For now just use English
            "donalds_dock_1100_english.dna",
            "donalds_dock_1200_english.dna",
            "donalds_dock_1300_english.dna",
            
            "toontown_central_2100_english.dna",
            "toontown_central_2200_english.dna",
            "toontown_central_2300_english.dna",
            
            "the_burrrgh_3100_english.dna",
            "the_burrrgh_3200_english.dna",
            "the_burrrgh_3300_english.dna",
            
            "minnies_melody_land_4100_english.dna",
            "minnies_melody_land_4200_english.dna",
            "minnies_melody_land_4300_english.dna",
            
            "daisys_garden_5100_english.dna",
            "daisys_garden_5200_english.dna",
            "daisys_garden_5300_english.dna",
            
            "donalds_dreamland_9100_english.dna",
            "donalds_dreamland_9200_english.dna",
        ]
        
        # We cache the visgroups
        dnaStore = DNAStorage()
        
        for filename in dnaFiles:
            # This might be problematic for prebuilt
            # maybe use built instead?
            filepath = Filename(filename)
            vfs.resolveFilename(filepath, searchPath)
            loadDNAFileAI(dnaStore, str(filepath))
            
        for i in range(0, dnaStore.getNumDNAVisGroupsAI()):
            visgroup = dnaStore.getDNAVisGroupAI(i)
            visibles = []
            for j in range(0, visgroup.getNumVisibles()):
                visibles.append(visgroup.getVisibleName(j))
            self.visgroups[int(visgroup.name)] = visibles
            
    def load_namemaster(self):
        # Let's read our NameMaster
        
        # Get our Panda3D Virtual File System.
        vfs = VirtualFileSystem.getGlobalPtr()
        
        # Look for our file locations and read them in.
        searchPath = DSearchPath()
        
        # In other environments, including the dev environment, look here:
        toontownPath = os.path.expandvars('$TOONTOWN') or './toontown'
        searchPath.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(toontownPath + '/src/configfiles')))
        
        # Check which language should be used, defaults to English
        # Perhaps look for product code instead of language?
        language = ConfigVariableString("language", "english").getValue()
        
        name_master = "NameMasterEnglish.txt"
        if language == 'castillian':
            name_master = "NameMaster_castillian.txt"
        elif language == "japanese":
            name_master = "NameMaster_japanese.txt"
        elif language == "german":
            name_master = "NameMaster_german.txt"
        elif language == "french":
            name_master = "NameMaster_french.txt"
        elif language == "portuguese":
            name_master = "NameMaster_portuguese.txt"
            
        filepath = Filename(name_master)
        vfs.resolveFilename(filepath, searchPath)

        with open(filepath, "r") as file:
            for line in file:
                if line.startswith("#"):
                    continue
                    
                nameId, nameCategory, name = line.split("*", 2)
                self.name_dictionary[int(nameId)] = (int(nameCategory), name.strip())
                
    async def allocate_context(self):
        self.__context += 1
        if self.__context >= (1 << 32):
            self.__context = 0
        return self.__context
        
'''
if __name__ == "__main__":
    async def main():
        # Get the running loop inside an async function
        loop = asyncio.get_running_loop()
        
        ca = await ClientAgent.initialize("0.0.0.0", ConfigVariableInt("msg-director-port", 6666).getValue(), ConfigVariableInt("client-agent-id", 20200000).getValue())
        await ca.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        try:
            loop.create_task(ca.server.serve_forever())
        except asyncio.CancelledError:
            pass
        
        while True:
            try:
                await ca.flush()
                await asyncio.sleep(0)
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
            
    asyncio.run(main())
'''