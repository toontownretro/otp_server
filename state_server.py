import asyncio, functools, socket, struct, time

from panda3d.core import ConfigVariableInt, ConfigVariableBool, Datagram, DatagramIterator

from central_logger import CentralLogger
from distributed_object import DistributedObject
from distributed_directory import DistributedDirectory
from server_interface import ServerInterface
from msgtypes import *

class StateServer(ServerInterface):
    def __init__(self, channel=None):
        super().__init__()
        
        if not channel:
            self.channel = ConfigVariableInt("state-server-id", 20100000).getValue()
        else:
            self.channel = channel
            
        # Distributed Objects
        self.objects = {}
        
        # Database Distributed Objects
        self.db_objects = {}
        
        self.load_dc()
        self.create_objects()
        
    def create_objects(self):
        # Format for object hierarchy:
        # doId, parentId, zoneId

        # Make our StateServer Object which is the root of the whole OTP. 
        # OTP_DO_ID_SERVER_ROOT, 0, OTP_ZONE_ID_INVALID
        self.objectServer = DistributedObject(OTP_SERVER_ROOT_DO_ID, self.dc.getClassByName("ObjectServer"), 0, 0)
        self.objectServer.update("setName", "PyOTP")
        self.objectServer.update("setDcHash", 798635679)
        self.objectServer.update("setDateCreated", int(time.time()))
        self.objects[self.objectServer.doId] = self.objectServer
        
        # CentralLogger
        # OTP_DO_ID_CENTRAL_LOGGER, OTP_DO_ID_SERVER_ROOT, OTP_ZONE_ID_INVALID
        self.centralLogger = CentralLogger(self.otp, 4688, self.dc.getClassByName("CentralLogger"), OTP_SERVER_ROOT_DO_ID, 0)
        self.objects[self.centralLogger.doId] = self.centralLogger
        
        # Make our game root for Toontown, OTP_DO_ID_TOONTOWN, OTP_DO_ID_SERVER_ROOT, OTP_ZONE_ID_MANAGEMENT
        self.toontownDirectory = DistributedDirectory(4618, self.dc.getClassByName("DistributedDirectory"), OTP_SERVER_ROOT_DO_ID, 2)
        self.objects[self.toontownDirectory.doId] = self.toontownDirectory
        
        # Make our global objects for Toontown.
        
        # OTP_DO_ID_TOONTOWN_SPEEDCHAT_RELAY, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownSpeedchatRelay = DistributedObject(4712, self.dc.getClassByName("TTSpeedchatRelay"), 4618, 0)
        self.objects[self.toontownSpeedchatRelay.doId] = self.toontownSpeedchatRelay
        
        # OTP_DO_ID_TOONTOWN_DELIVERY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownDeliveryManager = DistributedObject(4683, self.dc.getClassByName("DistributedDeliveryManager"), 4618, 0)
        self.objects[self.toontownDeliveryManager.doId] = self.toontownDeliveryManager
        
        # OTP_DO_ID_TOONTOWN_MAIL_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownMailManager = DistributedObject(4690, self.dc.getClassByName("DistributedMailManager"), 4618, 0)
        self.objects[self.toontownMailManager.doId] = self.toontownMailManager
        
        # OTP_DO_ID_TOONTOWN_PARTY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownPartyManager = DistributedObject(4691, self.dc.getClassByName("DistributedPartyManager"), 4618, 0)
        self.objects[self.toontownPartyManager.doId] = self.toontownPartyManager
        
        if ConfigVariableBool('want-code-redemption', 1).getValue():
            # OTP_DO_ID_TOONTOWN_CODE_REDEMPTION_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
            self.toontownCodeRedemptionManager = DistributedObject(4695, self.dc.getClassByName("TTCodeRedemptionMgr"), 4618, 0)
            self.objects[self.toontownCodeRedemptionManager.doId] = self.toontownCodeRedemptionManager
            
        # OTP_DO_ID_TOONTOWN_NON_REPEATABLE_RANDOM_SOURCE, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownNonRepeatableRandomSource = DistributedObject(4697, self.dc.getClassByName("NonRepeatableRandomSource"), 4618, 0)
        self.objects[self.toontownNonRepeatableRandomSource.doId] = self.toontownNonRepeatableRandomSource
        
        if ConfigVariableBool('want-ddsm', 1).getValue():
            # OTP_DO_ID_TOONTOWN_TEMP_STORE_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
            self.toontownDataStoreManager = DistributedObject(4684, self.dc.getClassByName("DistributedDataStoreManager"), 4618, 0)
            self.objects[self.toontownDataStoreManager.doId] = self.toontownDataStoreManager
            
        # OTP_DO_ID_TOONTOWN_RAT_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownRATManager = DistributedObject(4692, self.dc.getClassByName("RATManager"), 4618, 0)
        self.objects[self.toontownRATManager.doId] = self.toontownRATManager
        
        # OTP_DO_ID_TOONTOWN_AWARD_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownAwardManager = DistributedObject(4694, self.dc.getClassByName("AwardManager"), 4618, 0)
        self.objects[self.toontownAwardManager.doId] = self.toontownAwardManager
        
        # OTP_DO_ID_TOONTOWN_IN_GAME_NEWS_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownInGameNewsMgr = DistributedObject(4696, self.dc.getClassByName("DistributedInGameNewsMgr"), 4618, 0)
        self.objects[self.toontownInGameNewsMgr.doId] = self.toontownInGameNewsMgr
        
        # OTP_DO_ID_TOONTOWN_WHITELIST_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownWhitelistManager = DistributedObject(4699, self.dc.getClassByName("DistributedWhitelistMgr"), 4618, 0)
        self.objects[self.toontownWhitelistManager.doId] = self.toontownWhitelistManager
        
        # OTP_DO_ID_TOONTOWN_CPU_INFO_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownCpuInfoManager = DistributedObject(4713, self.dc.getClassByName("DistributedCpuInfoMgr"), 4618, 0)
        self.objects[self.toontownCpuInfoManager.doId] = self.toontownCpuInfoManager
        
        # OTP_DO_ID_TOONTOWN_SECURITY_MANAGER, OTP_DO_ID_TOONTOWN, OTP_ZONE_ID_INVALID
        self.toontownSecurityManager = DistributedObject(4714, self.dc.getClassByName("DistributedSecurityMgr"), 4618, 0)
        self.objects[self.toontownSecurityManager.doId] = self.toontownSecurityManager

    @classmethod
    async def initialize(cls, addr, port, channel=None):
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
        await self.set_connection_name("Stateserver")
        
        # Register that we want updates for all of the objects we're handling.
        channels = set()
        for object in self.objects:
            channels.add(object.doId)
        for db_object in self.db_objects:
            channels.add(db_object.doId)
        await self.add_control_range(channels)
        
        return True
        
    async def close(self):
        if self.closed:
            return
            
        # Before we actually close out our connection, Make sure to unregister our channels.
        await self.unregister_for_channel(self.channel)
        
        # Unregister all of the objects that we were handling.
        channels = set()
        for object in self.objects:
            channels.add(object.doId)
        for db_object in self.db_objects:
            channels.add(db_object.doId)
        await self.remove_control_range(channels)
        
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
            if not channel in self.objects and not channel in self.db_objects:
                #print("Received message from from sender %d for object %d which doesn't exist! Skipping message!" % (sender, channel))
                continue
                
            # Process the object message.
            await self.handle_object_channel(channel, sender, code, di)
            
    async def handle_lost_connection(self):
        # If we lost connection, Then we'll just close the connection locally.
        # We can't unregister any channels if it won't reach the Message Director.
        #
        # The Message Director will unregister us itself anyways, So no need to worry.
        await super().close()
        
    async def handle_stateserver_channel(self, sender, code, di):
        if code in (STATESERVER_OBJECT_GENERATE_WITH_REQUIRED, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER):
            # We are asked to create an object
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
                await self.register_for_channel(doId)
            else:
                if doId in self.db_objects:
                    do = self.db_objects[doId]
                else:
                    do = self.objects[doId]
                do.parentId = parentId
                do.zoneId = zoneId
                do.senders.append(sender)
            
            #print("Generating %s object %d at (%d, %d) from %d" % (do.dclass.getName(), do.doId, do.parentId, do.zoneId, sender))
            
            # We update the object
            do.receiveRequired(di)
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                do.receiveOther(di)
                
            # We announce the object was created if it was not created by the owner.
            channels = [CLIENTAGENT_ID] # We announce to clients too, The ClientAgent will manage how.
            channels.append(self.get_interested(do, sender))
            
            dg = Datagram()
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.getNumber())
            dg.addUint32(do.doId)
            do.packRequired(dg)
            do.packOther(dg) # TODO Should we check for airecv?
            
            await self.send_message(channels, sender, STATESERVER_OBJECT_ENTERZONE_WITH_REQUIRED_OTHER, dg)
            
        elif code == STATESERVER_OBJECT_UPDATE_FIELD:
            # We are asked to update an object field.
            
            # This packet can be sent to the StateServer
            # or the object channel.
            
            # Check and see if the object exists.
            doId = di.getUint32()
            
            # Does this object exist?
            if not doId in self.objects and not doId in self.db_objects:
                print("Failed to update field for non-existent object %d for sender %d!" % (doId, sender))
                return
                
            # Get our object.
            if doId in self.db_objects:
                do = self.db_objects[doId]
            else:
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
            channels.append(self.get_interested(do, sender))
            
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
            if not doId in self.objects and not doId in self.db_objects:
                # We answer it was not found
                dg = Datagram()
                dg.addUint32(doId)
                await self.send_message([sender], self.channel, STATESERVER_OBJECT_NOTFOUND, dg)
                return
                
            # Get our object.
            if doId in self.db_objects:
                do = self.db_objects[doId]
            else:
                do = self.objects[doId]
                
            # This is very likely a uninitialized DB object.
            # We don't want to delete these yet as they are used
            # for a generate in the future.
            if do.zoneId == 0 and do.parentId == 0:
                return

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

            for do in self.db_objects.values():
                if shardId in do.senders or (do.parentId in self.db_objects and shardId in self.db_objects[do.parentId].senders):
                    objects.append(do)
            
            # We got all the objects, we can now delete them.
            # The state server deletes the object, so we set the sender to ourself.
            for do in objects:
                await self.delete_object(do, self.channel)
        else:
            print("Received unsupported message %d on stateserver channel from %d, Ignoring." % (code, sender))
            return
                
        if di.getRemainingSize():
            raise Exception("Data remaining on stateserver: code %d has %d bytes left", (code, di.getRemainingBytes()))
            
    def handle_object_channel(self, channel, sender, code, di):
        do = None
        if channel in self.db_objects:
            do = self.db_objects[channel]
        elif channel in self.objects:
            do = self.objects[channel]
            
        if code in (STATESERVER_OBJECT_GENERATE_WITH_REQUIRED, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER):
            # We are asked to create an object
            parentId = di.getUint32()
            zoneId = di.getUint32()
            classId = di.getUint16()
            doId = di.getUint32()
                
            if not do:
                if doId != channel:
                    print("Got mismatching generate request for object %d, Object %d recieved it instead!" % (doId, channel))
                    return
                    
                # We get the dclass
                dclass = self.dc.getClass(classId)
                
                # We create the object
                do = DistributedObject(doId, dclass, parentId, zoneId)
                do.senders.append(sender)
                
                # We save the object
                self.objects[doId] = do
                await self.register_for_channel(doId)
            else:
                if do.doId != doId:
                    print("Got mismatching generate request for object %d, Object %d recieved it instead!" % (doId, do.doId))
                    return

                do.parentId = parentId
                do.zoneId = zoneId
                do.senders.append(sender)
            
            #print("Generating %s object %d at (%d, %d)" % (do.dclass.getName(), do.doId, do.parentId, do.zoneId))
            
            # We update the object
            do.receiveRequired(di)
            if code == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
                do.receiveOther(di)
                
            # We announce to clients too, The ClientAgent will manage how.
            channels = [CLIENTAGENT_ID]
                
            # We announce the object was created if it was not created by the owner.
            channels.append(self.get_interested(do, sender))
            
            dg = Datagram()
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.getNumber())
            dg.addUint32(do.doId)
            do.packRequired(dg)
            do.packOther(dg) # TODO Should we check for airecv?
            
            await self.send_message(channels, sender, STATESERVER_OBJECT_ENTERZONE_WITH_REQUIRED_OTHER, dg)

        elif code == STATESERVER_OBJECT_DELETE_RAM:
            # We are asked to delete an object.
            
            # This packet can be sent to the StateServer
            # or the object channel.
            
            # We must check if doId matches, and if it doesn't,
            # it means it was sent to the wrong channel or was meant for the SS channel.
            
            doId = di.getUint32()
            if do.doId != doId:
                raise Exception("Received stateserver delete object message for an object! (channel %d doId %d)" % (channel, doId))
                
            # This is very likely a uninitialized DB object.
            # We don't want to delete these yet as they are used
            # for a generate in the future.
            if do.zoneId == 0 and do.parentId == 0:
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
                raise Exception("Object %d does not match channel %d" % (doId, do.doId))
                
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
            channels.append(self.get_interested(do, sender))
            
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
            # Someone is asking info about us
            context = di.getUint32()
            
            # We're sending our REQUIRED and OTHER fields
            dg = Datagram()
            dg.addUint32(context)
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.getNumber())
            dg.addUint32(do.doId)
            do.packRequired(dg)
            do.packOther(dg) # TODO Should we check for airecv?
            
            await self.send_message([sender], self.ssId, STATESERVER_QUERY_OBJECT_ALL_RESP, dg)
                
        elif code == STATESERVER_OBJECT_SET_ZONE:
            # We are asked to move an object.
            parentId = di.getUint32()
            zoneId = di.getUint32()
            
            # Try and get our parent if they exist.
            parent = None
            if parentId in self.db_objects:
                parent = self.db_objects[parentId]
            elif parentId in self.objects:
                parent = self.objects[parentId]
            
            # We get the previous zone
            prevParentChannel = parent.senders[0] if parent else None
            prevParentId, prevZoneId = do.parentId, do.zoneId
            
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
            dg.addUint32(prevParentId)
            dg.addUint32(prevZoneId)
            
            await self.send_message([CLIENTAGENT_ID], sender, STATESERVER_OBJECT_SET_ZONE, dg)
        else:
            print("Received unsupported message %d on stateserver object channel from %d, Ignoring." % (code, sender))
            return
        
        if di.getRemainingSize():
            raise Exception("Data remaining on stateserver: code %d has %d bytes left", (code, di.getRemainingBytes()))
        
    async def delete_object(self, do, sender):
        """
        Delete an object and transmits the deletion to all interested parties.
        """
        
        # Yeah no.
        if not do:
            return
        
        # We don't have the object in either! Nothing to do here.
        if not do.doId in self.db_objects and not do.doId in self.objects:
            #print("Tried to delete do %d that wasn't in objects anymore!" % (do.doId))
            return
            
        #print("Deleting do %d in objects!" % (do.doId))
            
        if not do.doId in self.db_objects:
            assert self.objects[do.doId] == do, "wrong object"
            
            # We can delete the object
            del self.objects[do.doId]
        else:
            assert self.db_objects[do.doId] == do, "wrong database object"
            
            # We can delete the object
            del self.db_objects[do.doId]
            
        await self.unregister_for_channel(do.doId)
        
        # We should tell everyone the object is gone,
        # Write the delete ram packet.
        dg = Datagram()
        dg.addUint32(do.doId)
        
        # We announce to clients too, The ClientAgent will manage how.
        channels = [CLIENTAGENT_ID]
        # We send the update to the interested OTP clients.
        channels.append(self.get_interested(do, sender))
        # We want to reflect the delete back to the AI, 
        # Otherwise the deleted object channel in question will not be cleaned up.
        channels.append(sender)

        await self.send_message(channels, sender, STATESERVER_OBJECT_DELETE_RAM, dg)
        
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
                
        if do.parentId in self.db_objects:
            parent = self.db_objects[do.parentId]
            for parentSender in parent.senders:
                #print("Adding channel %d from parent." % (parentSender))
                channels.add(parentSender)
        
        if sender in channels:
            #print("Remove sender %d from channels." % (sender))
            channels.remove(sender)
            
        return list(channels)