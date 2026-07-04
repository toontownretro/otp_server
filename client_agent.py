import os, socket, select, ssl, time

from panda3d.core import ConfigVariableString, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem

from dnaparser import loadDNAFile, DNAStorage
from msgtypes import *

class ClientAgent:
    def __init__(self, otp):
        # Main OTP
        self.otp = otp
        
        # DC File
        self.dc = self.otp.dc
        
        # GameServer Sock
        sock = socket.socket()
        sock.bind(("0.0.0.0", 6667))
        sock.listen(5)
        
        # SSL Context
        #context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        #context.load_cert_chain('secure/server.cert', 'secure/server.key')

        # GameServer sock and clients
        self.sock = sock #context.wrap_socket(sock, server_side=True)
        self.clients = []
        
        self.visgroups = {}
            
        self.nameDictionary = {}
                
        # Special fields IDs (cache)
        self.setTalkFieldId = self.dc.getClassByName("TalkPath_owner").getFieldByName("setTalk").getNumber()
        
        self.readFiles()
        
    def readFiles(self):
        # Get our Panda3D Virtual File System.
        vfs = VirtualFileSystem.getGlobalPtr()
        
        # Look for our file locations and read them in.
        searchPath = DSearchPath()
        
        # In other environments, including the dev environment, look here:
        toontownPath = os.path.expandvars('$TOONTOWN') or './toontown'
        searchPath.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(toontownPath + '/src/configfiles')))
        
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
            
        # Let's read our NameMaster
        
        # Check which language should be used, defaults to English
        # Perhaps look for product code instead of language?
        language = ConfigVariableString("language", "english").getValue()
        
        self.NameMaster = "NameMasterEnglish.txt"
        if language == 'castillian':
            self.NameMaster = "NameMaster_castillian.txt"
        elif language == "japanese":
            self.NameMaster = "NameMaster_japanese.txt"
        elif language == "german":
            self.NameMaster = "NameMaster_german.txt"
        elif language == "french":
            self.NameMaster = "NameMaster_french.txt"
        elif language == "portuguese":
            self.NameMaster = "NameMaster_portuguese.txt"
            
        filepath = Filename(self.NameMaster)
        vfs.resolveFilename(filepath, searchPath)

        with open(filepath, "r") as file:
            for line in file:
                if line.startswith("#"):
                    continue
                    
                nameId, nameCategory, name = line.split("*", 2)
                self.nameDictionary[int(nameId)] = (int(nameCategory), name.strip())
            
    def announceCreate(self, do, sender):
        # We send to the interested clients that they have access to a brand new object!
        dg = Datagram()
        dg.addUint32(do.parentId)
        dg.addUint32(do.zoneId)
        dg.addUint16(do.dclass.getNumber())
        dg.addUint32(do.doId)
        do.packRequiredBroadcast(dg)
        do.packOther(dg)
        
        for client in self.clients:
            # No echo pls
            if client.avatarId == sender:
                continue
                
            # We send the object creation if we're the owner or if we're interested.
            if client.hasInterest(do.parentId, do.zoneId) or do.doId == client.avatarId:
                client.sendMessage(CLIENT_CREATE_OBJECT_REQUIRED_OTHER, dg)
        
        
    def announceDelete(self, do, sender):
        # We're deleting an object
        dg = Datagram()
        dg.addUint32(do.doId)
        
        for client in self.clients:
            # Not retransmitting
            if client.avatarId == sender:
                continue
                
            # If the client is the owner, we're in a special case and we're not sending the packet
            if do.doId == client.avatarId:
                client.onAvatarDelete()
            
            # We tell the client that it's disabled only if they're interested or the owner.
            # (Please note this last condition here is useless but it's meant to be replaced if owner view is implemented some day)
            elif client.hasInterest(do.parentId, do.zoneId) or do.doId == client.avatarId:
                client.sendMessage(CLIENT_OBJECT_DISABLE, dg)
        
        
    def announceMove(self, do, prevParentId, prevZoneId, sender):
        """
        Send CLIENT_OBJECT_LOCATION to interested clients,
        or CLIENT_OBJECT_DISABLE / CLIENT_CREATE_OBJECT_REQUIRED_OTHER
        """
        # Disable Message
        dg1 = Datagram()
        dg1.addUint32(do.doId)
        
        # Location Message
        dg2 = Datagram()
        dg2.addUint32(do.doId)
        dg2.addUint32(do.parentId)
        dg2.addUint32(do.zoneId)
        
        # Create Object Message
        dg3 = Datagram()
        dg3.addUint32(do.parentId)
        dg3.addUint32(do.zoneId)
        dg3.addUint16(do.dclass.getNumber())
        dg3.addUint32(do.doId)
        do.packRequiredBroadcast(dg3)
        do.packOther(dg3)
        
        for client in self.clients:
            # We are not transmitting back our own updates
            if client.avatarId == sender:
                continue
                
            # If we're the owner, we must receive it in any case
            if client.avatarId == do.doId:
                client.sendMessage(CLIENT_OBJECT_LOCATION, dg2)
                
            # If we're interested in the previous area
            elif client.hasInterest(prevParentId, prevZoneId):
                # If we're interested in the new area,
                # we can just tell the client that the object moved
                if client.hasInterest(do.parentId, do.zoneId):
                    client.sendMessage(CLIENT_OBJECT_LOCATION, dg2)
                else:   
                    # If we're not, we ask them to disable the object
                    client.sendMessage(CLIENT_OBJECT_DISABLE, dg1)
                    
            # If we're only interested in the new area,
            # we ask them to create the object
            elif client.hasInterest(do.parentId, do.zoneId):
                client.sendMessage(CLIENT_CREATE_OBJECT_REQUIRED_OTHER, dg3)
                
                
    def announceUpdate(self, do, field, data, sender):
        """
        Send CLIENT_OBJECT_UPDATE_FIELD to interested clients
        """
        # This field has no reason to be transmitted if it's not ownrecv or broadcast
        if not (field.isOwnrecv() or field.isBroadcast()):
            return
            
        # We generate the field update
        dg = Datagram()
        dg.addUint32(do.doId)
        dg.addUint16(field.getNumber())
        dg.appendData(data)
        
        for client in self.clients:
            # We are not transmitting back our own updates
            if client.avatarId == sender:
                continue
                
            # Can this client receive this update?
            # TODO: is broadcast check required?
            if (field.isOwnrecv() or not field.isBroadcast()) and client.avatarId != do.doId:
                continue
                
            # If we're interested OR owner, we send the update
            if client.hasInterest(do.parentId, do.zoneId) or client.avatarId == do.doId:
                client.sendMessage(CLIENT_OBJECT_UPDATE_FIELD, dg)
                
        
    def handle(self, channels, sender, code, datagram):
        """
        Handle a message
        """
        for channel in channels:
            for client in self.clients:
                if client.avatarId is None:
                    continue
                    
                if channel == client.avatarId + (1 << 32):
                    if code == STATESERVER_OBJECT_UPDATE_FIELD:
                        client.sendMessage(CLIENT_OBJECT_UPDATE_FIELD, datagram)
                    elif code == CLIENT_SET_FIELD_SENDABLE:
                        print("Recieved messsage type CLIENT_SET_FIELD_SENDABLE.")
                        
                        dgi = DatagramIterator(datagram)
                        
                        doId = dgi.getUint32()
                        
                        fields = []
                        
                        # We do it like this because we don't add a size check.
                        while dgi.getRemainingSize() >= 2:
                            fields.append(dgi.getUint16())
                        
                        # Set the clsend fields for object in our client.
                        client.setClsendFields(doId, fields)
                    else:
                        raise Exception("Unexpected message on Puppet channel (code %d)" % code)