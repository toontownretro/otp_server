import asyncio, functools, math, socket, struct, time, traceback

from panda3d.core import ConfigVariableInt, ConfigVariableBool, Datagram, DatagramIterator

from central_logger import CentralLogger
from distributed_object import DistributedObject
from distributed_directory import DistributedDirectory
from server_interface import ServerInterface
from msgtypes import *

class StateServer(ServerInterface):
    def __init__(self, channel):
        super().__init__()
        
        self.channel = channel
            
        # Distributed Objects
        self.objects = {}
        
        self.load_dc()
        self.create_objects()
        
        # Fast timeout period when trying to contact the Message Director.
        # If we can't communicate with it fast. It's probably just down.
        self.timeout_period = 5
        
        self.set_name("STATESERVER")

    @classmethod
    async def initialize(cls, addr, port, channel):
        self = cls(channel)
        await self.connect(addr, port)
        return self
        
    async def connect(self, addr, port):
        connected = await super().connect(addr, port)
        
        # Failed to connect to the Message Director!
        if not connected:
            return False
            
        # Setup our information on the Message Director.
        await self.register_for_channel(self.channel)
        await self.set_connection_name(self.name)
        
        # Register that we want updates for all of the objects we're handling.
        channels = set()
        for object in self.objects.values():
            channels.add(object.doId)
        await self.add_control_range(channels)
        
        # Annouce our objects to anybody ready to listen.
        for object in self.objects.values():
            await self.announce_object_generate_with_other(object, self.channel)
        
        print(f"[{self.name}]: Connected and running on channel {self.channel}.")
        return True
        
    async def timeout(self):
        try:
            # Wait for our timeout period.
            await asyncio.sleep(self.timeout_period)
            
            # Don't do anything if we're already closed.
            if self.closed:
                return
            
            # Handle our lost connection.
            await self.handle_lost_connection()
        except KeyboardInterrupt as e:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            traceback.print_exception(e)
        
    async def close(self):
        if self.closed:
            return
            
        # Delete all of our objects.
        for object in self.objects.values():
            await self.delete_object(object, self.channel)
            
        # Make sure to unregister our channel.
        await self.unregister_for_channel(self.channel)
        
        # Close our connection.
        await super().close()
        
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
        
        # Iterate over all our channels and handle the datagram accordingly.
        for channel in channels:
            di = DatagramIterator(dg)
            
            # Before we try to handle an object. Make sure we aren't recieving a stateserver message.
            # If we are handling a stateserver message. Handle it!
            if channel == self.channel:
                await self.handle_stateserver_channel(sender, code, di)
                continue
                
            # Verify the channel/object exists before trying to handle a message from it.
            if not channel in self.objects:
                #print("Received message from from sender %d for object %d which doesn't exist! Skipping message!" % (sender, channel))
                continue
                
            # Process the object message.
            await self.handle_object_channel(channel, sender, code, di)
            
    async def handle_lost_connection(self):
        # If we lost connection, Then we'll just close the connection locally.
        # We can't unregister any channels if it won't reach the Message Director.
        #
        # The Message Director will unregister us itself anyways, So no need to worry.
        print(f"[{self.name}]: Lost connection to the Message Director.")
        await super().close()
        
    async def handle_stateserver_channel(self, sender, code, di):
        if code in (STATESERVER_OBJECT_GENERATE_WITH_REQUIRED, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER):
            # We are asked to create an object
            parentId = di.getUint32()
            zoneId = di.getUint32()
            classId = di.getUint16()
            doId = di.getUint32()
            
            if not doId in self.objects:
                # We create the object
                do = self.create_object_from_dclass_id(doId, parentId, zoneId, classId)
                assert do != None
            else:
                do = self.objects[doId]
                do.parentId = parentId
                do.zoneId = zoneId
            
            # Add the current sender to our senders.
            do.senders.append(sender)
            
            #print(f"[{self.name}]: Generating {do.dclass.getName()} object {do.doId} under {do.parentId} at {do.zoneId} from {sender}.")
            
            # We update the object
            do.receiveRequired(di)
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                do.receiveOther(di)
                
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                await self.announce_object_generate_with_other(do, sender)
                return
            
            await self.announce_object_generate(do, sender)
            
        elif code == STATESERVER_OBJECT_UPDATE_FIELD:
            # We are asked to update an object field.
            
            # This packet can be sent to the StateServer
            # or the object channel.
            
            # Check and see if the object exists.
            doId = di.getUint32()
            
            # Does this object exist?
            if not doId in self.objects:
                print(f"[{self.name}]: Failed to update field for non-existent object {doId} for sender {sender}!")
                return
                
            # Get our object.
            do = self.objects[doId]
                
            # Now let's update our object field.
            fieldId = di.getUint16()
                
            # The remaining data is field data
            data = di.getRemainingBytes()
            
            # We apply the update
            field = do.dclass.getFieldByIndex(fieldId)
            
            # Handle internal CentralLogger specially.
            if isinstance(do, CentralLogger):
                do.receiveField(sender, field, di)
            else:
                do.receiveField(field, di)
                
            # We announce to clients too, The ClientAgent will manage how.
            channels = [CLIENTAGENT_ID]
            
            # We transmit the update if it was not sent by the owner
            channels += self.get_interested(do, sender)
            
            # We did not implement airecv fields yet so let's do it.
            # TODO: Figure out how to exclude Uberdog?
            for senderId in do.senders:
                # If the AI isn't going to recieve it, Remove it.
                if senderId in channels and not field.isAirecv():
                    channels.remove(senderId)
                    
            # Don't send it back to yourself you fucking dumbass!
            # We don't want any of your fucking infinite loops.
            if do.doId in channels:
                channels.remove(do.doId)
            if sender in channels:
                channels.remove(sender)

            dg = Datagram()
            dg.addUint32(doId)
            dg.addUint16(fieldId)
            dg.appendData(data)

            await self.send_message(channels, sender, STATESERVER_OBJECT_UPDATE_FIELD, dg)
            
        elif code == STATESERVER_OBJECT_DELETE_RAM:
            # We are asked to delete an object.
            
            # This packet can be sent to the StateServer
            # or the object channel.
            
            doId = di.getUint32()
            
            # Does this object exist?
            if not doId in self.objects:
                # We answer it was not found
                dg = Datagram()
                dg.addUint32(doId)
                await self.send_message([sender], self.channel, STATESERVER_OBJECT_NOTFOUND, dg)
                print(f"[{self.name}]: Object {doId} was non-existent in deletion request, Sending notfound response to {sender}.")
                return
                
            # Get our object.
            do = self.objects[doId]

            await self.delete_object(do, self.channel)
                
        elif code == STATESERVER_SHARD_REST:
            # Shard is going down.
            # We gotta delete its objects.
            shardId = di.getUint64()
            
            # We get every object to delete,
            # which means we look for the objects created by this shard,
            # or every object parented to it.
            objects = []
            for do in self.objects.values():
                if shardId in do.senders or (do.parentId in self.objects and shardId in self.objects[do.parentId].senders):
                    objects.append(do)
            
            # We got all the objects, we can now delete them.
            # The state server deletes the object, so we set the sender to ourself.
            for do in objects:
                await self.delete_object(do, self.channel)
        elif code == SERVER_PING:
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
        else:
            print(f"[{self.name}]: Received unsupported message {code} on stateserver channel from {sender}, Ignoring.")
            return
                
        if di.getRemainingSize():
            raise Exception("Data remaining on stateserver: code %d has %d bytes left", (code, di.getRemainingBytes()))
            
    async def handle_object_channel(self, channel, sender, code, di):
        assert channel in self.objects
        do = self.objects[channel]
            
        if code in (STATESERVER_OBJECT_GENERATE_WITH_REQUIRED, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER):
            # We are asked to create an object
            parentId = di.getUint32()
            zoneId = di.getUint32()
            classId = di.getUint16()
            doId = di.getUint32()
                
            if not do:
                if doId != channel:
                    print(f"[{self.name}]: Got mismatching generate request for object {doId}, Object {channel} recieved it instead!")
                    return
                    
                # We create the object
                do = self.create_object_from_dclass_id(doId, parentId, zoneId, classId)
                assert do != None
            else:
                if do.doId != doId:
                    print(f"[{self.name}]: Got mismatching generate request for object {doId}, Object {do.doId} recieved it instead!")
                    return

                do.parentId = parentId
                do.zoneId = zoneId

            # Add the current sender to our senders.
            do.senders.append(sender)
            
            #print(f"[{self.name}]: Generating {do.dclass.getName()} object {do.doId} under {do.parentId} at {do.zoneId} from {sender}.")
            
            # We update the object
            do.receiveRequired(di)
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                do.receiveOther(di)
                
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                await self.announce_object_generate_with_other(do, sender)
                return
            
            await self.announce_object_generate(do, sender)

        elif code == STATESERVER_OBJECT_DELETE_RAM:
            # We are asked to delete an object.
            
            # This packet can be sent to the StateServer
            # or the object channel.
            
            # We must check if doId matches, and if it doesn't,
            # it means it was sent to the wrong channel or was meant for the SS channel.
            
            doId = di.getUint32()
            if do.doId != doId:
                print(f"[{self.name}]: Received delete object request to delete {doId} on object {do.doId}! Ignoring invalid request.")
                return

            # It was sent directly to the object, which means it was found
            await self.delete_object(do, sender)
            
        elif code == STATESERVER_OBJECT_UPDATE_FIELD:
            # We are asked to update an object field.
            
            # This packet can be sent to the StateServer
            # or the object channel.
            
            # We must check if doId matches, and if it doesn't,
            # it means it was sent to the wrong channel or was meant for the SS channel.
            
            # We are asked to update a field
            doId = di.getUint32()
            fieldId = di.getUint16()
            
            # Is this sent to the correct object?
            if doId != do.doId:
                print(f"[{self.name}]: Received update field request for object {do.doId} but specified update doId {doId} does not match our object! Ignoring invalid request.")
                return
                
            # The remaining data is field data
            data = di.getRemainingBytes()
            
            # We apply the update
            field = do.dclass.getFieldByIndex(fieldId)
            
            # Handle internal CentralLogger specially.
            if isinstance(do, CentralLogger):
                do.receiveField(sender, field, di)
            else:
                do.receiveField(field, di)
                
            # We announce to clients too, The ClientAgent will manage how.
            channels = [CLIENTAGENT_ID]
            
            # We transmit the update if it was not sent by the owner
            channels += self.get_interested(do, sender)
            
            # We did not implement airecv fields yet so let's do it.
            # TODO: Figure out how to exclude Uberdog?
            for senderId in do.senders:
                # If the AI isn't going to recieve it, Remove it.
                if senderId in channels and not field.isAirecv():
                    channels.remove(senderId)
                    
            # Don't send it back to yourself you fucking dumbass!
            # We don't want any of your fucking infinite loops.
            if do.doId in channels:
                channels.remove(do.doId)
            if sender in channels:
                channels.remove(sender)
                
            dg = Datagram()
            dg.addUint32(doId)
            dg.addUint16(fieldId)
            dg.appendData(data)
            
            await self.send_message(channels, sender, STATESERVER_OBJECT_UPDATE_FIELD, dg)
            
        elif code == STATESERVER_QUERY_OBJECT_ALL:
            # Someone is asking info about us.
            context = di.getUint32()
            
            # We're sending our REQUIRED and OTHER fields.
            dg = Datagram()
            dg.addUint32(context)
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.get_number())
            dg.addUint32(do.doId)
            do.packRequired(dg)
            do.packOther(dg) # TODO: Should we check for airecv?
            
            await self.send_message([sender], self.channel, STATESERVER_QUERY_OBJECT_ALL_RESP, dg)
                
        elif code == STATESERVER_OBJECT_SET_ZONE:
            # We are asked to move an object.
            parentId = di.getUint32()
            zoneId = di.getUint32()
            
            # Store our previous parent information.
            prevParentId, prevZoneId = do.parentId, do.zoneId
            
            # Remove ourself from our old parent.
            prevParent = None
            if prevParentId in self.objects:
                prevParent = self.objects[prevParentId]
                prevParent.children.remove(do.doId)
                
            prevParentChannel = prevParent.senders[0] if prevParent else None
                
            # Try and get our parent if they exist.
            parent = None
            if parentId in self.objects:
                parent = self.objects[parentId]
                # Add ourself to our new parent.
                parent.children.add(do.doId)
                
            # I don't think this was correct..?
            #prevParentChannel = parent.senders[0] if parent else None
            
            # We set the new zone
            do.parentId = parentId
            do.zoneId = zoneId
            
            # We announce the object was moved if it was not asked by the "owner"
            if parent and not sender in parent.senders:
                if prevParentId == do.parentId:
                    # Parent id is the same: just send the update to the old a new zone
                    channels = self.get_interested(do, sender)
                    if channels:
                        dg = Datagram()
                        dg.addUint32(do.doId)
                        dg.addUint32(do.parentId)
                        dg.addUint32(do.zoneId)
                        dg.addUint32(prevParentId)
                        dg.addUint32(prevZoneId)
                        
                        await self.send_message(channels, sender, STATESERVER_OBJECT_CHANGE_ZONE, dg)
                else:
                    # Parent id changed: we must remove it and add it back
                    if prevParentChannel:
                        dg = Datagram()
                        dg.addUint32(do.doId)
                        
                        await self.send_message([prevParentChannel], sender, STATESERVER_OBJECT_LEAVING_AI_INTEREST, dg)
                    
                    channels = self.get_interested(do, sender)
                    if channels:
                        dg = Datagram()
                        dg.addUint32(do.parentId)
                        dg.addUint32(do.zoneId)
                        dg.addUint16(do.dclass.getNumber())
                        dg.addUint32(do.doId)
                        do.packRequired(dg)
                        do.packOther(dg) # TODO Should we check for airecv?
                        
                        await self.send_message(channels, sender, STATESERVER_OBJECT_ENTERZONE_WITH_REQUIRED_OTHER, dg)
                        
            # We announce to clients too, The ClientAgent will manage how.
            dg = Datagram()
            dg.addUint32(do.doId)
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            
            await self.send_message([CLIENTAGENT_ID], sender, STATESERVER_OBJECT_SET_ZONE, dg)
        else:
            print(f"[{self.name}]: Received unsupported message {code} on stateserver object channel from {sender}, Ignoring.")
            return
        
        if di.getRemainingSize():
            raise Exception("Data remaining on stateserver: code %d has %d bytes left", (code, di.getRemainingBytes()))
            
    def add_object(self, object):
        '''
        Simple function to help streamline object generation.
        '''
        if not object: return False
        
        # If we have a parent that is a object, Make sure it knows we're
        # a child of it.
        parentId = object.parentId
        if parentId and parentId in self.objects:
            parent = self.objects[parentId]
            parent.children.add(object.doId)
            
        # Add the object to our object dict.
        self.objects[object.doId] = object
        
        return True
        
    def create_object(self, doId, parentId, zoneId, dclassName, objclass=DistributedObject):
        '''
        Simple function to help streamline object generation.
        '''
        
        # Get our dclass from the name.
        dclass = self.dc.getClassByName(dclassName)
        if not dclass:
            return None
        
        # Initialize the object from the class.
        object = objclass(doId, dclass, parentId, zoneId)
        
        # Add our distributed object to our list.
        self.add_object(object)
        return object
        
    def create_object_from_dclass_id(self, doId, parentId, zoneId, dclassId, objclass=DistributedObject):
        '''
        Simple function to help streamline object generation.
        '''
        
        # Get our dclass from the id.
        dclass = self.dc.getClass(dclassId)
        if not dclass:
            return None
        
        # Initialize the object from the class.
        object = objclass(doId, dclass, parentId, zoneId)
        
        # Add our distributed object to our list.
        self.add_object(object)
        return object
        
    def create_objects(self):
        # Format for object hierarchy:
        # doId, parentId, zoneId

        # Make our StateServer Object which is the root of the whole OTP. 
        # OTP_SERVER_ROOT_DO_ID, 0, OTP_ZONE_ID_INVALID
        self.objectServer = self.create_object(OTP_SERVER_ROOT_DO_ID, 0, OTP_ZONE_ID_INVALID, "ObjectServer")
        self.objectServer.update("setName", "PyOTP")
        self.objectServer.update("setDcHash", self.dc.getHash())
        self.objectServer.update("setDateCreated", int(time.time()))
        
        # CentralLogger
        # OTP_DO_ID_CENTRAL_LOGGER, OTP_SERVER_ROOT_DO_ID, OTP_ZONE_ID_INVALID
        self.centralLogger = self.create_object(OTP_DO_ID_CENTRAL_LOGGER, OTP_SERVER_ROOT_DO_ID, OTP_ZONE_ID_INVALID, "CentralLogger", objclass=CentralLogger)
        
        # Make our game root for Toontown
        # OTP_DO_ID_TOONTOWN, OTP_SERVER_ROOT_DO_ID, OTP_ZONE_ID_MANAGEMENT
        self.toontownDirectory = self.create_object(OTP_DO_ID_TOONTOWN, OTP_SERVER_ROOT_DO_ID, OTP_ZONE_ID_MANAGEMENT, "DistributedDirectory", objclass=DistributedDirectory)
        
        # Make our global objects for Toontown.
        
        # OTP_DO_ID_TOONTOWN_SPEEDCHAT_RELAY, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownSpeedchatRelay = self.create_object(OTP_DO_ID_TOONTOWN_SPEEDCHAT_RELAY, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "TTSpeedchatRelay")
        
        # OTP_DO_ID_TOONTOWN_DELIVERY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownDeliveryManager = self.create_object(OTP_DO_ID_TOONTOWN_DELIVERY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedDeliveryManager")
        
        # OTP_DO_ID_TOONTOWN_MAIL_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownMailManager = self.create_object(OTP_DO_ID_TOONTOWN_MAIL_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedMailManager")
        
        # OTP_DO_ID_TOONTOWN_PARTY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownPartyManager = self.create_object(OTP_DO_ID_TOONTOWN_PARTY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedPartyManager")
        
        if ConfigVariableBool('want-code-redemption', 1).getValue():
            # OTP_DO_ID_TOONTOWN_CODE_REDEMPTION_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
            self.toontownCodeRedemptionManager = self.create_object(OTP_DO_ID_TOONTOWN_CODE_REDEMPTION_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "TTCodeRedemptionMgr")
            
        # OTP_DO_ID_TOONTOWN_NON_REPEATABLE_RANDOM_SOURCE, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownNonRepeatableRandomSource = self.create_object(OTP_DO_ID_TOONTOWN_NON_REPEATABLE_RANDOM_SOURCE, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "NonRepeatableRandomSource")
        
        if ConfigVariableBool('want-ddsm', 1).getValue():
            # OTP_DO_ID_TOONTOWN_TEMP_STORE_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
            self.toontownDataStoreManager = self.create_object(OTP_DO_ID_TOONTOWN_TEMP_STORE_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedDataStoreManager")
            
        # OTP_DO_ID_TOONTOWN_RAT_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownRATManager = self.create_object(OTP_DO_ID_TOONTOWN_RAT_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "RATManager")
        
        # OTP_DO_ID_TOONTOWN_AWARD_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownAwardManager = self.create_object(OTP_DO_ID_TOONTOWN_AWARD_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "AwardManager")
        
        # OTP_DO_ID_TOONTOWN_IN_GAME_NEWS_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownInGameNewsMgr = self.create_object(OTP_DO_ID_TOONTOWN_IN_GAME_NEWS_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedInGameNewsMgr")
        
        # OTP_DO_ID_TOONTOWN_WHITELIST_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownWhitelistManager = self.create_object(OTP_DO_ID_TOONTOWN_WHITELIST_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedWhitelistMgr")
        
        # OTP_DO_ID_TOONTOWN_CPU_INFO_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownCpuInfoManager = self.create_object(OTP_DO_ID_TOONTOWN_CPU_INFO_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedCpuInfoMgr")
        
        # OTP_DO_ID_TOONTOWN_SECURITY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownSecurityManager = self.create_object(OTP_DO_ID_TOONTOWN_SECURITY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID, "DistributedSecurityMgr")
        
    async def delete_object(self, do, sender):
        """
        Delete an object and transmits the deletion to all interested parties.
        """
        
        # Yeah no.
        if not do:
            return
        
        # We don't have the object in our object collection. Reject the delete request.
        if not do.doId in self.objects:
            print(f"[{self.name}]: Tried to delete {do.dclass.getName()} {do.doId} under {do.parentId} at {do.zoneId}, But we don't manage this object!")
            return

        #print(f"[{self.name}]: Deleting {do.dclass.getName()} {do.doId} under {do.parentId} at {do.zoneId}.")
        
        # We can delete the object
        del self.objects[do.doId]
            
        await self.unregister_for_channel(do.doId)
        
        # If we have a parent that is a object, Make sure it knows we're
        # no longer a child of it.
        parentId = do.parentId
        if parentId and parentId in self.objects:
            parent = self.objects[parentId]
            parent.children.remove(do.doId)
        
        # We should tell everyone the object is gone,
        # Write the delete ram packet.
        dg = Datagram()
        dg.addUint32(do.doId)
        
        # We announce to clients too, The ClientAgent will manage how.
        channels = [CLIENTAGENT_ID]
        # We send the update to the interested OTP clients.
        channels += self.get_interested(do, sender)
        # We want to reflect the delete back to the AI, 
        # Otherwise the deleted object channel in question will not be cleaned up.
        channels.append(sender)

        await self.send_message(channels, sender, STATESERVER_OBJECT_DELETE_RAM, dg)
        
    async def announce_object_generate(self, do, sender):
        if not do: return False
        
        #print(f"[{self.name}]: Announcing generate from {sender} for {do.dclass.getName()} {do.doId} under {do.parentId} at {do.zoneId}.")

        # We announce the object was created if it was not created by the owner.
        channels = self.get_interested(do, sender)
        if channels:
            dg = Datagram()
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.getNumber())
            dg.addUint32(do.doId)
            do.packRequired(dg)
            do.packOther(dg) # TODO Should we check for airecv?

            await self.send_message(channels, sender, STATESERVER_OBJECT_ENTERZONE_WITH_REQUIRED_OTHER, dg)
            
        # We announce to clients too, The Client Agent will manage how.
        dg = Datagram()
        dg.addUint32(do.parentId)
        dg.addUint32(do.zoneId)
        dg.addUint16(do.dclass.getNumber())
        dg.addUint32(do.doId)
        do.packRequired(dg)
        
        await self.send_message([CLIENTAGENT_ID], sender, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED, dg)
        return True
        
    async def announce_object_generate_with_other(self, do, sender):
        if not do: return False
        
        #print(f"[{self.name}]: Announcing generate (other) from {sender} for {do.dclass.getName()} {do.doId} under {do.parentId} at {do.zoneId}.")

        # We announce the object was created if it was not created by the owner.
        channels = self.get_interested(do, sender)
        if channels:
            dg = Datagram()
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.getNumber())
            dg.addUint32(do.doId)
            do.packRequired(dg)
            do.packOther(dg) # TODO Should we check for airecv?

            await self.send_message(channels, sender, STATESERVER_OBJECT_ENTERZONE_WITH_REQUIRED_OTHER, dg)
            
        # We announce to clients too, The Client Agent will manage how.
        dg = Datagram()
        dg.addUint32(do.parentId)
        dg.addUint32(do.zoneId)
        dg.addUint16(do.dclass.getNumber())
        dg.addUint32(do.doId)
        do.packRequired(dg)
        do.packOther(dg)
        
        await self.send_message([CLIENTAGENT_ID], sender, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER, dg)
        return True
        
    def get_interested(self, do, sender):
        """
        Get channels interested in those do updates.
        In a full working otp, this should include a channel for zones.
        """
        channels = set()
        
        for senderId in do.senders:
            #print("Adding do sender channel %d to channels." % (senderId))
            channels.add(senderId)
        
        if do.parentId in self.objects:
            parent = self.objects[do.parentId]
            for parentSender in parent.senders:
                #print("Adding channel %d from parent." % (parentSender))
                channels.add(parentSender)
        
        if sender in channels:
            #print("Remove sender %d from channels." % (sender))
            channels.remove(sender)
            
        return list(channels)
        
if __name__ == "__main__":
    async def main():
        ss = await StateServer.initialize("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue(), ConfigVariableInt("state-server-id", 20100000).getValue())
        while True:
            try:
                await ss.flush()
                await asyncio.sleep(0)
                if ss.is_closed(): break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
            
    asyncio.run(main())