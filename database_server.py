import hashlib, os, sys, uuid, string, random

from datetime import datetime, timedelta

try:
    # Try to use simplejson if we can, Otherwise just use normal json.
    import simplejson as json
except:
    import json

from panda3d.core import ConfigVariableString, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem
from panda3d.direct import DCPacker

from database_manager import DatabaseManager
from database_object import DatabaseObject
from distributed_object import DistributedObject
from server_interface import ServerInterface
from msgtypes import *

class DatabaseServer(ServerInterface):
    def __init__(self, channel):
        super().__init__()
        
        self.channel = channel
        
        self.load_dc()
        
        # Dictionaries containing info relating to all of our DC Objects with the DcObjectType field.
        self.dc_object_types = {}
        
        # Create our Database Manager. 
        self.manager = DatabaseManager(self.dc)
        
        self.rngSeed = None
        self.secretFriendCodes = {}
        
        self.set_name("DATABASESERVER")

    @classmethod
    async def initialize(cls, addr, port, channel):
        self = cls(channel)
        await self.connect(addr, port)
        return self
        
    async def connect(self, addr, port):
        connected = await super().connect(addr, port)
        
        # Failed to connect to the Message Director!
        if not connected:
            print(f"[{self.name}]: Failed to connect to the Message Director.")
            return False
            
        # Setup our information on the Message Director.
        await self.register_for_channel(self.channel)
        await self.set_connection_name(self.name)
        
        print(f"[{self.name}]: Connected and running on channel {self.channel}.")
        return True
        
    async def close(self):
        if self.closed:
            return
            
        # Make sure to unregister our channel.
        await self.unregister_for_channel(self.channel)
        
        # Close our connection.
        await super().close()
        
    async def handle_lost_connection(self):
        # If we lost connection, Then we'll just close the connection locally.
        # We can't unregister any channels if it won't reach the Message Director.
        #
        # The Message Director will unregister us itself anyways, So no need to worry.
        print(f"[{self.name}]: Lost connection to the Message Director.")
        await super().close()
        
    async def receive_datagram(self, dg):
        di = DatagramIterator(dg)
        
        # First check if the datagram has anything in it.
        if not di.get_remaining_size() >= 1:
            return
            
        # Get the amount of channels the datagram wants to be routed too.
        count = di.get_uint8()
        if count <= 0:
            return
            
        # Extract all of the channels from the datagram.
        if not di.get_remaining_size() >= 8 * count:
            return
        channels = set()
        for _ in range(count):
            channels.add(di.get_uint64())
            
        # Get the sender for the datagram and it's 'code' (Identifier for what type of datagram it is)
        if not di.get_remaining_size() >= 10:
            return
        sender = di.get_uint64()
        code = di.get_uint16()
        
        # Extract all of the remaining data into it's own datagram.
        data = di.get_remaining_bytes()
        dg = Datagram(bytes(data))
        
        # Iterate over all our channels and handle the datagram accordingly.
        for channel in channels:
            di = DatagramIterator(dg)
            
            # Before we try to handle an object. Make sure we aren't recieving a stateserver message.
            # If we are handling a stateserver message. Handle it!
            if channel == self.channel:
                await self.handle_internal_channel(sender, code, di)
                continue
                
            # Verify the channel/object exists before trying to handle a message from it.
            if not channel in self.manager.cache:
                continue
                
            # Process the object message.
            await self.handle_object_channel(channel, sender, code, di)
            
    async def handle_internal_channel(self, sender, code, di):
        if code == SERVER_PING:
            await self.handle_ping(sender, di)
        elif code == DBSERVER_GET_STORED_VALUES:
            await self.handle_get_stored_values(sender, di)
        elif code == DBSERVER_SET_STORED_VALUES:
            await self.handle_set_stored_values(sender, di)
        elif code == DBSERVER_CREATE_STORED_OBJECT:
            await self.handle_create_stored_object(sender, di)
        elif code == DBSERVER_DELETE_STORED_OBJECT:
            print("DBSERVER_DELETE_STORED_OBJECT")
        elif code == DBSERVER_GET_ESTATE:
            await self.handle_get_estate(sender, di)
        elif code == DBSERVER_MAKE_FRIENDS:
            await self.handle_make_friends(sender, di)
        elif code == DBSERVER_REQUEST_SECRET:
            print("DBSERVER_REQUEST_SECRET")
            #self.requestSecret(sender, datagram)
        elif code == DBSERVER_SUBMIT_SECRET:
            print("DBSERVER_SUBMIT_SECRET")
            #self.submitSecret(sender, datagram)
        else:
            print(f"[{self.name}]: Received unsupported message {code} on internal channel from {sender}, Ignoring.")
            return
            
    async def handle_object_channel(self, channel, sender, code, di):
        do = self.manager.cache[channel]
        
        if code == STATESERVER_OBJECT_UPDATE_FIELD:
            # We are asked to update a field
            doId = di.get_uint32()
            fieldId = di.get_uint16()
            
            # Is this sent to the correct object?
            if doId != do.doId:
                raise Exception("Object %d does not match channel %d" % (doId, do.doId))
            
            # We apply the update
            field = do.dclass.get_field_by_index(fieldId)
            do.receiveField(field, di)
            
    async def handle_ping(self, sender, di):
        """
        Handles and returns a ping request sent from the sender.
        """
        
        if di.get_remaining_size() < 12:
            return
            
        sec = di.get_uint32()
        usec = di.get_uint32()
        url = di.get_string()
        channel = di.get_uint32()
        
        # Respond
        dg = Datagram()
        dg.add_uint32(sec)
        dg.add_uint32(usec)
        dg.add_string(url)
        dg.add_uint32(channel)
        
        await self.send_message([sender], self.channel, SERVER_PING, dg)
            
    async def handle_get_stored_values(self, sender, di):
        """
        Get the stored field values from the object specified in the datagram.
        """
        
        if di.get_remaining_size() < 10:
            return
        
        # Get the context.
        context = di.get_uint32()
        
        # The doId we want to get the fields from.
        doId = di.get_uint32()
        
        # The number of fields we're going to search for.
        num_fields = di.get_uint16()
        
        # Get all of the field names we want to work with!
        field_names = []
        for i in range(0, num_fields):
            field_names.append(di.getString())
            
        num_fields = len(field_names)
        
        dg = Datagram()
        dg.addUint32(context) # Rain or shine. We want the context.
        dg.addUint32(doId) # They'll need to know what doId this was for!
        dg.addUint16(num_fields) # Send back the number of fields we searched for.
        
        # Add all of our field names.
        for i in range(0, num_fields):
            dg.addString(field_names[i])
        
        # Make sure our database object even exists first.
        if not await self.manager.has_dc_object(doId):
            # Failed to get our object. So we just add our response code.
            dg.add_uint8(1)
            # Send out our response.
            await self.send_message([sender], self.channel, DBSERVER_GET_STORED_VALUES_RESP, dg)
            return
        
        # Load our database object.
        do = await self.manager.load_dc_object(doId)
        if not do:
            # Failed to get our object. So we just add our response code.
            dg.add_uint8(1)
            # Send out our response.
            await self.send_message([sender], self.channel, DBSERVER_GET_STORED_VALUES_RESP, dg)
            return
        
        values = []
        found = []
        
        dg.add_uint8(0)
        
        # Add our field values.
        for i in range(0, num_fields):
            field_name = field_names[i]
            if field_name in do.fields: # Success
                values.append(do.packField(field_name, do.fields[field_name]).decode('ISO-8859-1'))
                found.append(True)
                continue
            # Failure, The field doesn't exist.
            #print("Couldn't find field %s for do %s!" % (field_name, str(do.doId)))
            values.append("DEADBEEF")
            found.append(False)
            
        # Add our values.
        for i in range(0, num_fields):
            dg.add_string(values[i])
        
        # Add the list of our found field values.
        for i in range(0, num_fields):
            dg.add_uint8(found[i])

        # Send out our response.
        await self.send_message([sender], self.channel, DBSERVER_GET_STORED_VALUES_RESP, dg)
        
    async def handle_set_stored_values(self, sender, di):
        """
        Set the values of the fields for the object specified in the datagram.
        """
        
        if di.get_remaining_size() < 8:
            return
        
        # The doId we want to set the fields for.
        doId = di.get_uint32()
        
        # Make sure our database object even exists before
        # we try and attempt to set fields.
        if not await self.manager.has_dc_object(doId):
            return
        
        # The number of fields we're going to set.
        num_fields = di.get_uint32()
        
        field_names = []
        field_values = []
        
        # Get all of our field names.
        for i in range(0, num_fields):
            field_names.append(di.get_string())
        
        # Get all of our field values.
        for i in range(0, num_fields):
            field_values.append(di.get_string())

        # Load our database object.
        do = await self.manager.load_dc_object(doId)
        
        # Unpack and assign the field values.
        for i in range(0, num_fields):
            field_name = field_names[i]
            field_value = field_values[i]
            
            if not do.dclass.get_field_by_name(field_name):
                # We can't set a field that doesn't exist!
                continue
            
            unpacked_value = do.unpackField(field_name, field_value)
            if unpacked_value:
                do.fields[field_name] = unpacked_value
        
        # Save the database object to make sure we don't lose our changes.
        await self.manager.save_dc_object(do)
        
    async def handle_create_stored_object(self, sender, di):
        """
        Create a Database Object from the database object index.
        """
        
        if di.get_remaining_size() < 10:
            return
        
        # Get the context.
        context = di.get_uint32()
        
        # Name of the dclass; This is used if we don't have a valid database object ID provided.
        dclass_name = di.get_string()
        
        # This is our database object ID.
        db_object_type = di.get_uint16()
        
        # The amount of fields we have.
        num_fields = di.get_uint16()
        
        field_names = []
        field_values = []
        
        # Get all of our field names
        for i in range(0, num_fields):
            field_names.append(di.getString())
        
        # Get all of our field values.
        for i in range(0, num_fields):
            field_values.append(di.getString().encode('ISO-8859-1'))
        
        if not db_object_type in self.dc_object_types and not self.dc.get_class_by_name(dclass_name):
            print(f"[{self.name}]: Failed to create stored object with invalid paramters for the dclass: {dclass_name}, {db_object_type}")
                        
            dg = Datagram()
            # Add our context.
            dg.add_uint32(context)
            # We failed, So add a response code of 1.
            dg.add_uint8(1)
            
            # Send out our response.
            await self.send_message([sender], self.channel, DBSERVER_CREATE_STORED_OBJECT_RESP, dg)
            return
            
        dclass = self.dc_object_types.get(db_object_type, None)
        if not dclass:
            dclass = self.dc.get_class_by_name(dclass_name)
            
        # Unpack and assign the field values.
        fields = {}
        for i in range(0, num_fields):
            field_name = field_names[i]
            field_value = field_values[i]
            
            field = dclass.get_field_by_name(field_name)
            if not field:
                # We can't set a field our dcclass doesn't have!
                continue

            if not field_value:
                continue
            
            # Try to attempt and unpack the value passed for creation.
            # The data may be malformed for one reason or another so if it's no good;
            # Just discard and move on.
            try:
                # Create our packer and set the raw data for the field as the data
                # to unpack.
                packer = DCPacker()
                packer.set_unpack_data(field_value)
                    
                # Unpack the data in the field.
                packer.begin_unpack(field)
                unpacked_value = field.unpack_args(packer)
                packer.end_unpack()
            except:
                continue
            
            # Store our now unpacked value.
            if unpacked_value:
                fields[field_name] = unpacked_value
        
        # Create a database object from our dc object type with our specified fields.
        object = await self.manager.create_dc_object(dclass, fields)
        
        dg = Datagram()
        
        # Add our context.
        dg.add_uint32(context)
        
        # We successfully created and set the fields of the database object.
        dg.add_uint8(0)
        
        # Add the resulting object doId.
        dg.add_uint32(object.doId)
        
        # Send out our response.
        await self.send_message([sender], self.channel, DBSERVER_CREATE_STORED_OBJECT_RESP, dg)

    async def handle_get_estate(self, sender, di):
        """
        Return the database values for the Estate and fields specified, 
        If some parts of the Estate aren't created. They are here.
        """
        
        if di.get_remaining_size() < 8:
            return
        
        # Get the context for sending back.
        context = di.get_uint32()
        
        # The avatar which has the estate.
        doId = di.get_uint32()
        
        dg = Datagram()
        
        # Rain or shine. We want the context.
        dg.add_uint32(context)
        
        if not await self.manager.has_dc_object(doId):
            dg.add_uint8(1) # Failed to get our avatar, So we can't get their houses either!
            await self.send_message([sender], self.channel, DBSERVER_GET_ESTATE_RESP, dg)
            return
            
        avatar = await self.manager.load_dc_object(doId)
        
        # Somehow we don't have an account!
        if not 'setDISLid' in avatar.fields:
            dg.add_uint8(1) # Avatar had invalid fields, So we can't get their houses.
            await self.send_message([sender], self.channel, DBSERVER_GET_ESTATE_RESP, dg)
            return
            
        account_id = avatar.fields['setDISLid'][0]
        
        # Our account doesn't exist!?
        if not await self.manager.has_dc_object(account_id):
            dg.add_uint8(1) # Failed to get the account for our avatar, So we can't get their houses either!
            await self.send_message([sender], self.channel, DBSERVER_GET_ESTATE_RESP, dg)
            return
            
        account = await self.manager.load_dc_object(account_id)
        
        # Pre-define this here.
        estate = None
        estate_id = account.fields.get('ESTATE_ID', 0)
        house_ids = [0, 0, 0, 0, 0, 0]
        
        # We need to create an Estate!
        if not 'ESTATE_ID' in account.fields or estate_id == 0:
            dclass = self.dc.get_class_by_name("DistributedEstate")
            estate = await self.manager.create_dc_object(dclass)
            account.update("ESTATE_ID", estate.doId)
            account.update("HOUSE_ID_SET", house_ids)
        else:
            estate = await self.manager.load_dc_object(estate_id)
            house_ids = account.fields["HOUSE_ID_SET"]

        avatars = account.fields["ACCOUNT_AV_SET"]
        
        houses = []
        
        # First create all our blank houses.
        for i in range(0, len(house_ids)):
            house_id = house_ids[i]
            if house_id == 0:
                dclass = self.dc.get_class_by_name("DistributedHouse")
                house = await self.manager.create_dc_object(dclass)
                house.update("setName", "")
                house.update("setAvatarId", 0)
                house.update("setColor", i)
                house_ids[i] = house.doId
                houses.append(house)
            else: # If the house already exists... Just generate and store it.
                house = await self.manager.load_dc_object(house_id)
                house.update("setColor", i)
                houses.append(house)
                
        pets = []
                
        # Time to update our existing houses and pets!
        for i in range(0, len(avatars)):
            av_doId = avatars[i]
            
            # If we're missing the avatar for some reason... Skip!
            if not await self.manager.has_dc_object(av_doId):
                continue
                
            # Load in our avatar.
            avatar = await self.manager.load_dc_object(av_doId)
            
            # Load our pet for this avatar in question in.
            if "setPetId" in avatar.fields and avatar.fields["setPetId"][0] != 0:
                pet = await self.manager.load_dc_object(avatar.fields["setPetId"][0])
                pets.append(pet)
                
            av_position_index = avatar.fields["setPosIndex"][0]
            # If for some reason theres no house here... Create one!
            if houseIds[av_position_index] == 0:
                dclass = self.dc.get_class_by_name("DistributedHouse")
                house = await self.manager.create_dc_object(dclass)
                house.update("setName", avatar.fields["setName"][0])
                house.update("setAvatarId", avatar.doId)
                house.update("setColor", av_position_index)
                houseIds[av_position_index] = house.doId
            else: # Update our houses info just in case ours changed!
                house = await self.manager.load_dc_object(houseIds[av_position_index])
                house.update("setName", avatar.fields["setName"][0])
                house.update("setAvatarId", avatar.doId)
                house.update("setColor", av_position_index)
            
        # Update our ids just in case a new house was made.
        account.update("HOUSE_ID_SET", houseIds)
        
        # Make sure our account saved it's changes.
        await self.manager.save_dc_object(account)
        
        # We've succeeded in loading everything we need to, So we add this indicating success.
        dg.add_uint8(0)
        
        # Add our estate doId
        dg.add_uint32(estate.doId)
        
        # Add the amount of fields in our estate.
        dg.add_uint16(len(estate.fields))
        
        # Add our field values. This in theory isn't needed at all.
        for name, value in estate.fields.items():
            try:
                dg.add_string(name)
                dg.add_string(estate.packField(name, value).decode('ISO-8859-1'))
                dg.add_uint8(True)
            except:
                dg.add_string("DEADBEEF")
                dg.add_string("DEADBEEF")
                dg.add_uint8(False)
                
        # Add the number of houses we have.
        dg.add_uint16(len(houses))
        
        # Add all of our house doIds.
        for i in range(0, len(houses)):
            dg.add_uint32(houses[i].doId)
        
        house_data = {}
        
        for name in list(houses[0].fields.keys()):
            house_data[name] = []
        
        # Make a our lists of field names and values. 
        for i in range(0, len(houses)):
            house = houses[i]
            for name, value in house.fields.items():
                house_data[name].append(house.packField(name, value))

        # Add the number of house keys we have.
        dg.add_uint16(len(house_data))
        
        # Add our house keys.
        for name in list(house_data.keys()):
            dg.add_string(name)
            
        # Add the number of house values we have.
        dg.add_uint16(len(house_data))
        
        # Add our house values.
        for name, data in house_data.items():
            dg.add_uint16(houseLen) # Why the fuck is this needed Disney.
            for i in range(0, len(data)):
                value = data[i]
                dg.add_string(value.decode('ISO-8859-1'))

        # The amount of houses we got successfully,
        # It's not checked anymore. So it's safe to say it was scrapped.
        dg.add_uint16(len(houses))
        
        # Add in if we found a house or not, We don't really check this as of rn.
        # We've either failed earlier or gotten to this point.
        for i in range(0, len(house_data)):
            dg.add_uint16(0) #hvLen, This isn't used anymore either.
            for j in range(0, houseLen):
                dg.add_uint8(1)
            
        # Add the number of pets we have.
        dg.add_uint16(len(pets))
        
        # Add our pet doIds.
        for i in range(0, len(pets)):
            dg.add_uint32(pets[i].doId)
        
        # We can FINALLY send our message.
        await self.send_message([sender], self.channel, DBSERVER_GET_ESTATE_RESP, dg)

    async def handle_make_friends(self, sender, di):
        if di.get_remaining_size() < 13:
            return
            
        # The first person who wants to make friends.
        friend_idA = di.get_uint32()
        
        # The second person who wants to make to friends.
        friend_idB = di.get_uint32()
        
        # The flags for this friendship.
        flags = di.get_uint8()
        
        # Get the context for sending back.
        context = di.get_uint32()
        
        dg = Datagram()
        
        # If one or neither of the database objects exist. They can NOT become friends.
        if not await self.manager.has_dc_object(friend_idA) or not await self.manager.has_dc_object(friend_idB):
            dg.add_uint8(False)
            dg.add_uint32(context)
            # Send out our response.
            await self.send_message([sender], self.channel, DBSERVER_MAKE_FRIENDS_RESP, dg)
            return
            
        # Load the database objects for our friends.
        friendA = await self.manager.load_dc_object(friend_idA)
        friendB = await self.manager.load_dc_object(friend_idB)
        
        # If one or either can't possibly make friends, We will respond with a failure.
        if not friendA.dclass.get_field_by_name("setFriendsList") or not friendB.dclass.get_field_by_name("setFriendsList"):
            dg.add_uint8(False)
            dg.add_uint32(context)
            # Send out our response.
            await self.send_message([sender], self.channel, DBSERVER_MAKE_FRIENDS_RESP, dg)
            return
            
        # Make sure we have the field already.
        if not "setFriendsList" in friendA.fields:
            friendA.fields["setFriendsList"] = ([],)
        if not "setFriendsList" in friendB.fields:
            friendB.fields["setFriendsList"] = ([],)
            
        friends_listA = friendA.fields["setFriendsList"][0]
        friends_listB = friendB.fields["setFriendsList"][0]
        
        # To know if we had the corresponding friends or not.
        has_friendA = False
        has_friendB = False
        
        # Check if we already have friend B in friend As list;
        # And update it if we don't already.
        for i in range(0, len(friends_listA)):
            friend_pair = friends_listA[i]
            if friend_pair[0] == friendB.doId:
                # We did.  Update the code.
                friends_listA[i] = (friendB.doId, flags)
                has_friendA = True
                break
                
        if not has_friendA:
            # We didn't already have this friend so add them to our list.
            friends_listA.append((friendB.doId, flags))
            
        # Check if we already have friend A in friend Bs list;
        # And update it if we don't already.
        for i in range(0, len(friends_listB)):
            friend_pair = friends_listB[i]
            if friend_pair[0] == friendA.doId:
                # We did.  Update the code.
                friends_listB[i] = (friendA.doId, flags)
                has_friendB = True
                break
                
        if not has_friendB:
            # We didn't already have this friend so add them to our list.
            friends_listB.append((friendA.doId, flags))
        
        # Save the database objects to make sure we don't lose our changes.
        await self.manager.save_dc_object(friendA)
        await self.manager.save_dc_object(friendB)
        
        # We succesfully added them as a friend!
        dg.add_uint8(True)
        dg.add_uint32(context)
        await self.send_message([sender], self.channel, DBSERVER_MAKE_FRIENDS_RESP, dg)
        
    def saveSecretCodes(self):
        with open(os.path.join(self.databaseDirectory, "friend_access.dat"), "w") as file:
            json.dump((self.rngSeed, self.secretFriendCodes), file, ensure_ascii=False, sort_keys=True, indent=2)
            # Close our file, Our data is now written.
            file.close()
            
    def loadSecretCodes(self):
        with open(os.path.join(self.databaseDirectory, "friend_access.dat"), "w") as file:
            self.rngSeed, self.secretFriendCodes = json.load(file)
            file.close()
		
    def requestSecret(self, sender, datagram):
        if not self.rngSeed: self.rngSeed = random.randrange(sys.maxsize)
        
        random.seed(self.rngSeed)
        
        def id_generator(size=3, chars=string.ascii_lowercase + string.digits):
            return ''.join(random.Random().choice(chars) for _ in range(size))

        di = DatagramIterator(datagram)

        # The person who wants to get a secret.
        requesterId = di.getUint32()
        
        responseCode = 1
        if not self.secretFriendCodes.get(requesterId, None):
            self.secretFriendCodes[requesterId] = []

        if len(self.secretFriendCodes[requesterId]) >= 11:
            secret = ""
            responseCode = 0
        else:
            secret = "%s %s" % (id_generator(3), id_generator(3))
            expireDt = datetime.now() + timedelta(hours=48)
            self.secretFriendCodes[requesterId].append((secret, expireDt.strftime("%Y-%m-%d %H:%M:%S")))
            # This is a cheeky way to shuffle the seed.
            # We don't want SF code repeats, So we reseed each time to prevent them.
            self.rngSeed += requesterId
            random.seed(self.rngSeed)
            
            # Save our new secret codes so we don't need to worry about them.
            self.saveSecretCodes()

        dg = Datagram()
        dg.addUint8(responseCode)
        dg.addString(secret)
        dg.addUint32(requesterId)

        self.messageDirector.sendMessage([sender], DBSERVER_ID, DBSERVER_REQUEST_SECRET_RESP, dg)
        
    def submitSecret(self, sender, datagram):
        di = DatagramIterator(datagram)

        # The person who wants to get a secret.
        requesterId = di.getUint32()
        
        # The secret itself.
        secret = di.getString()
        
        responseCode = 0
        avId = 0
        sSecret = ""
        
        for avId, secrets in dict(self.secretFriendCodes).items():
            for i in range(0, len(secrets)):
                sSecret, time = secrets[i]
                
                # Compare the secrets. If they don't match, Just move on.
                if secret != sSecret:
                    continue
                
                dt = datetime.now()
                expireDt = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
                
                # TODO: Check the friends list of somebody to see
                # if they are over the limit.
                
                # If a secret code is expired, Set our response code.
                if dt >= expireDt:
                    responseCode = 0
                # The requester and creator of the secret match,
                # We don't accept matching avatar ids for a secret.
                elif requesterId == avId:
                    responseCode = 3
                # Our code is valid, We found the secret and it passed all checks.
                else:
                    responseCode = 1
                    
                del self.secretFriendCodes[avId][i]
                break
            
        dg = Datagram()
        dg.addUint8(responseCode)
        dg.addString(sSecret)
        dg.addUint32(requesterId)
        dg.addUint32(avId)

        self.messageDirector.sendMessage([sender], DBSERVER_ID, DBSERVER_SUBMIT_SECRET_RESP, dg)