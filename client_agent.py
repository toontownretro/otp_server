import asyncio, functools, socket, ssl, struct, time

from panda3d.core import ConfigVariableInt, ConfigVariableBool, Datagram, DatagramIterator

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
        
        # Special fields IDs (cache)
        self.setTalkFieldId = self.dc.getClassByName("TalkPath_owner").getFieldByName("setTalk").getNumber()
        
    @classmethod
    async def initialize(cls, addr, port, channel):
        return cls(addr, port, channel)
        
    async def connect(self, addr, port):
        connected = await ServerInterface.connect(self, addr, port)
        
        # Failed to connect to the Message Director!
        if not connected:
            return False
            
        # Setup our information on the Message Director.
        self.channel = channel
        await self.register_for_channel(self.channel)
        await self.register_for_channel(CLIENTAGENT_ID)
        await self.set_connection_name("ClientAgent")
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
            di = DatagramIterator(dg)
            await self.handle_internal_channel(sender, code, di)
                
        # Otherwise, We'll distribute the channel check for each client indivdually.
        args = []
        for client in self.clients:
            args.append((client, channels, sender, code, dg))
        
        await asyncio.gather(*map(self.handle_datagram_for_client, args))
        
    async def handle_datagram_for_client(self, client, channels, sender, code, datagram):
        if client.avatarId is None:
            return
        if not client.avatarId + (1 << 32) in channels:
            return

        if code == STATESERVER_OBJECT_UPDATE_FIELD:
            await client.send_message(channels, sender, CLIENT_OBJECT_UPDATE_FIELD, datagram)
        elif code == CLIENT_SET_FIELD_SENDABLE:
            dgi = DatagramIterator(datagram)
            doId = dgi.getUint32()
            
            # We do it like this because we don't add a size check.
            fields = []
            while dgi.getRemainingSize() >= 2:
                fields.append(dgi.getUint16())
            
            # Set the clsend fields for object in our client.
            await client.set_clsend_fields(doId, fields)
        else:
            print("Unexpected message on Puppet channel %d (code %d)" % (client.avatarId + (1 << 32), code))
        
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
            loadDNAFile(dnaStore, filepath)
            
        for visgroup in dnaStore.visGroups:
            self.visgroups[int(visgroup.name)] = [int(i) for i in visgroup.visibles]
            
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