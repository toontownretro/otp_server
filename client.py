import math, os, struct, time, pytz, traceback
from datetime import datetime, timezone

from panda3d.core import Datagram, DatagramIterator
from panda3d.direct import DCPacker

import connection
from distributed_object import DistributedObject
from zone_util import getCanonicalZoneId, getTrueZoneId
from msgtypes import *
from security import *

class Client(connection.Client):
    def __init__(self):
        super().__init__()
        
        self.agent = None
        
        self.interests = {}
        
        self.account = None

        self.avatar = None
        self.avatar_deleted = True
        
        self.avatars = [None, None, None, None, None, None]
        
        self.callbacks = {}
        self.generate_callbacks = {}
        
        self.db_callbacks = {}
        self.db_callback_objects = {}
        
        # For any callback that needs mulitple things to load,
        # you can count what's loaded with this.
        self.callback_counters = {}
        
        self.__authorized = False
        
        # Cache for interests, so we don't have to iterate through all interests
        # every time an object updates.
        # This could be optimized in other languages, but we're using Python so
        # we're just gonna use a dict with sets
        self.__interestCache = {}
        
        # This is used to store the clsend field overrides sent by CLIENT_SET_FIELD_SENDABLE.
        self.__doId2ClsendOverrides = {}
        
    @classmethod
    async def initialize(cls, agent, addr, port):
        self = cls()
        await self.connect(addr, port)
        self.agent = agent
        return self
    
    @classmethod
    async def from_server(cls, agent, reader, writer):
        self = cls()
        self.agent = agent
        self.reader = reader
        self.writer = writer
        self.closed = False
        return self
        
    async def close(self, index=153, reason="Lost connection."):
        await self.handle_disconnect(index, reason)
        await super().close()
        
    disconnect = close
        
    async def handle_disconnect(self, index, reason):
        datagram = Datagram()
        if index:
            datagram.addUint16(index)
            datagram.addString(reason)
            
        # Tell our connected client to go get lost.
        await self.send_message(CLIENT_GO_GET_LOST, datagram)
        
        # We are no longer authorized.
        self.__authorized = False
        
        # Remove our avatar if it exists.
        await self.remove_avatar()
        
        # Save all of our changes to our avatars to the Database
        for pos, avatar in enumerate(list(self.avatars)):
            if not avatar:
                continue
            
            # Save our avatar.
            await self.database_update_from_object(avatar)
            
            # Remove it from the list.
            self.avatars[pos] = None
        
        # Save any changes to the Account
        if self.account:
            await self.database_update_from_object(self.account)
        
        del self.account
        self.account = None
        
        # Clear these all out.
        del self.callbacks
        self.callbacks = {}
        
        del self.callback_counters
        self.callback_counters = {}
        
        del self.db_callbacks
        self.db_callbacks = {}
        
        del self.db_callback_objects
        self.db_callback_objects = {}
        
        del self.generate_callbacks
        self.generate_callbacks = {}
        
        del self.interests
        self.interests = {}
        
        del self.__interestCache
        self.__interestCache = {}
        
        del self.__doId2ClsendOverrides
        self.__doId2ClsendOverrides = {}
        
    async def handle_lost_connection(self):
        await self.close()
        
    async def receive_datagram(self, dg):
        di = DatagramIterator(dg)

        if not di.getRemainingSize() >= 2:
            print("Received truncated datagram from connection: %s!" % (self.get_address()))
            await self.disconnect(200, "") # Internal error in the clients state machine.  Contact the developers for correction.
            return
        
        code = di.getUint16()
        
        if self.__authorized:
            await self.handle_authenticated_datagram(code, di)
            return
            
        await self.handle_datagram(code, di)
        
    async def send_message(self, code, datagram):
        if self.closed:
            return
            
        # Construct our datagram which the game client will receive.
        dg = Datagram()
        dg.addUint16(code)
        data = bytes(datagram) # Convert the Datagram to it's raw data.
        dg.appendData(data) # Append it.
        
        # Send our message.
        await self.send_datagram(dg)
        
    async def handle_authenticated_datagram(self, code, di):
        if code == CLIENT_HEARTBEAT:
            await self.handle_heartbeat(di)
        elif code == CLIENT_DISCONNECT:
            # Luckily for us, Super simple.
            await self.disconnect(2, "")
        elif code == CLIENT_CREATE_AVATAR:
            await self.handle_create_avatar(di)
        elif code == CLIENT_DELETE_AVATAR:
            await self.handle_delete_avatar(di)
        elif code == CLIENT_SET_NAME_PATTERN:
            await self.prepare_set_name_pattern(di)
        elif code == CLIENT_SET_WISHNAME:
            await self.prepare_set_wishname(di)
        elif code == CLIENT_GET_AVATARS:
            await self.handle_get_avatars(di)
        elif code == CLIENT_SET_AVATAR:
            await self.handle_set_avatar(di)
        elif code == CLIENT_ADD_INTEREST:
            await self.handle_add_interest(di)
        elif code == CLIENT_REMOVE_INTEREST:
            await self.handle_remove_interest(di)
        elif code == CLIENT_OBJECT_LOCATION:
            await self.handle_object_location(di)
        else:
            print("Received unexpected/unknown messagetype %d from connection: %s!" % (code, self.get_address()))
            await self.disconnect(220, "") # Internal error in the client state machine.  Contact the developers for correction.
        
    async def handle_datagram(self, code, di):
        if code == CLIENT_HEARTBEAT:
            await self.handle_heartbeat(di)
        elif code == CLIENT_DISCONNECT:
            # Luckily for us, Super simple.
            await self.disconnect(2, "")
        elif code == CLIENT_LOGIN_2:
            await self.handle_login_2(di)
        elif code == CLIENT_LOGIN_TOONTOWN:
            await self.handle_login_toontown(di)
        else:
            print("Received unexpected/unknown messagetype %d from connection: %s!" % (code, self.get_address()))
            await self.disconnect(220, "") # Internal error in the client state machine.  Contact the developers for correction.
            
    async def handle_heartbeat(self, di):
        # TODO: Keep track of heartbeats.
        data = di.getRemainingBytes()
        await self.send_message(CLIENT_HEARTBEAT, Datagram(bytes(data)))
            
    async def handle_login_2(self, di):
        playToken = di.getString()
        serverVersion = di.getString()
        hashVal = di.getUint32()
        tokenType = di.getUint32()
        validateDownload = di.getString()
        wantMagicWords = di.getString()
        
        tokenInfo = await self.parse_play_token(playToken.encode("utf-8"), tokenType)
        
        returnCode = tokenInfo["returnCode"]
        if returnCode != 0:
            return

        accountDoId = 0
        if tokenInfo["accountNumber"] != 0: accountDoId = tokenInfo["accountNumber"]

        # If we have an existing account.
        if accountDoId != 0:
            # Load our existing account.
            context = await self.agent.allocate_context()
            self.db_callbacks[context] = (self.handle_login_2_db_resp, (tokenInfo, serverVersion, hashVal, validateDownload, wantMagicWords))
            await self.database_request_object("Account", accountDoId, context)
            return
            
        # Get our current time in UTC.
        now = datetime.now()
        #now = now.astimezone(tz=pytz.UTC)

        # Fill out our account fields that have no default value.
        fields = {"ACCOUNT_AV_SET": [0, 0, 0, 0, 0, 0,],
                  "ACCOUNT_AV_SET_DEL": [,],
                  "pirateAvatars": [0, 0, 0, 0, 0, 0,],
                  "HOUSE_ID_SET": [0, 0, 0, 0, 0, 0,],
                  "ESTATE_ID": 0,
                  "PLAYED_MINUTES": "",
                  "PLAYED_MINUTES_PERIOD": "",
                  "CREATED": now.strftime("%Y-%m-%d %H:%M:%S"),
                  "LAST_LOGIN": now.strftime("%Y-%m-%d %H:%M:%S")}
                 
        # We create an Account
        context = await self.agent.allocate_context()
        self.db_callbacks[context] = (self.handle_login_2_db_resp, (tokenInfo, serverVersion, hashVal, validateDownload, wantMagicWords))
        await self.database_create_object("Account", fields, context)
            
    async def handle_login_2_db_resp(self, object, args):
        if not object or not args:
            return
            
        tokenInfo, serverVersion, hashVal, validateDownload, wantMagicWords = *args
        
        # These arguments are things we need from our token read response.
        returnCode = tokenInfo["returnCode"]
        responseStr = tokenInfo["respString"]
        createFriendsWithChat = "YES" if tokenInfo["createFriendsWithChat"] else "NO"
        chatCodeCreationRule = "YES" if tokenInfo["chatCodeCreationRule"] else "NO"
        userName = tokenInfo["userName"] if tokenInfo["userName"] != None else ""
        whiteListChat = "YES" if tokenInfo["whitelistChat"] else "NO"
        
        # We have our account!
        self.account = object
        
        # Get our current time in UTC.
        now = datetime.now()
        #now = now.astimezone(tz=pytz.UTC)
            
        # Check if the account has the creation date.
        if not self.account.fields.get("CREATED", None):
            self.account.update("CREATED", now.strftime("%Y-%m-%d %H:%M:%S"))
            
        # Update our last login time.
        self.account.update("LAST_LOGIN", now.strftime("%Y-%m-%d %H:%M:%S"))
                
        # Calculate the amount of days since our account was created.
         
        # Get our creation time from the stored date string.
        creation_time = datetime.strptime(self.account.fields.get("CREATED"), "%Y-%m-%d %H:%M:%S")
         
        # Calculate the difference in dates.
        delta_time = now - creation_time
         
        # Get the difference in days, That's how many days our account has been created.
        accountDays = abs(delta_time.days)

        datagram = Datagram()
        datagram.addInt8(returnCode) # returnCode
        datagram.addString(responseStr) # errorString
        datagram.addString(userName) # userName - not saved in our db so we're just putting the playToken
        datagram.addUint8(tokenInfo["openChatEnabled"]) # canChat

        usec, sec = math.modf(time.time())
        datagram.addUint32(int(sec))
        datagram.addUint32(int(usec * 1000000))

        datagram.addUint8(tokenInfo["paid"]) # isPaid
        datagram.addInt32(1000 * 60 * 60) # minutesRemaining

        datagram.addString("") # familyStr, unused
        datagram.addString(whiteListChat) # whiteListChatEnabled
        datagram.addInt32(accountDays) # accountDays
        datagram.addString(now.strftime("%Y-%m-%d %H:%M:%S")) # lastLoggedInStr
        
        self.__authorized = True
        
        await self.send_message(CLIENT_LOGIN_2_RESP, datagram)
        
    async def handle_login_toontown(self, di):
        playToken = di.getString()
        serverVersion = di.getString()
        hashVal = di.getUint32()
        tokenType = di.getInt32()
        wantMagicWords = di.getString()
        
        tokenInfo = self.parse_play_token(playToken.encode("utf-8"), tokenType)
        
        returnCode = tokenInfo["returnCode"]
        if returnCode != 0:
            return
        
        accountDoId = tokenInfo["accountNumber"] if tokenInfo["accountNumber"] else 0
        if accountDoId != 0:
            # Load our existing account.
            context = await self.agent.allocate_context()
            self.db_callbacks[context] = (self.handle_login_toontown_db_resp, (tokenInfo, serverVersion, hashVal, wantMagicWords))
            await self.database_request_object("Account", accountDoId, context)
            return
            
        # Get our current time in UTC.
        now = datetime.now()
        #now = now.astimezone(tz=pytz.UTC)

        # Fill out our account fields.
        fields = {"ACCOUNT_AV_SET": [0, 0, 0, 0, 0, 0,],
                  "ACCOUNT_AV_SET_DEL": [,],
                  "pirateAvatars": [0, 0, 0, 0, 0, 0,],
                  "HOUSE_ID_SET": [0, 0, 0, 0, 0, 0,],
                  "ESTATE_ID": 0,
                  "PLAYED_MINUTES": "",
                  "PLAYED_MINUTES_PERIOD": "",
                  "CREATED": now.strftime("%Y-%m-%d %H:%M:%S"),
                  "LAST_LOGIN": now.strftime("%Y-%m-%d %H:%M:%S")}
                  
        # We create an Account
        context = await self.agent.allocate_context()
        self.db_callbacks[context] = (self.handle_login_toontown_db_resp, (tokenInfo, serverVersion, hashVal, wantMagicWords))
        await self.database_create_object("Account", fields, context)
        
    async def handle_login_toontown_db_resp(self, object, args):
        if not object or not args:
            return

        tokenInfo, serverVersion, hashVal, wantMagicWords = *args

        # These arguments are things we need from our token read response.
        returnCode = tokenInfo["returnCode"]
        responseStr = tokenInfo["respString"]
        accountName = tokenInfo["accountName"] if tokenInfo["accountName"] else ""
        openChatEnabled = "YES" if tokenInfo["openChatEnabled"] else "NO"
        createFriendsWithChatFlags = {0: "NO", 1: "CODE", 2: "YES"} # Index to result.
        createFriendsWithChat = createFriendsWithChatFlags.get(tokenInfo["createFriendsWithChat"], "NO")
        chatCodeCreationRuleFlags = {0: "NO", 1: "PARENT", 2: "YES"} # Index to result.
        chatCodeCreationRule = chatCodeCreationRuleFlags.get(tokenInfo["chatCodeCreationRule"], "NO")
        paid = "FULL" if tokenInfo["paid"] else "VELVET_ROPE"
        userName = tokenInfo["userName"] if tokenInfo["userName"] != None else ""
        whiteListChat = "YES" if tokenInfo["whitelistChat"] else "NO"
        
        # We have our account!
        self.account = object
        
        # Get our current time in UTC.
        now = datetime.now()
        #now = now.astimezone(tz=pytz.UTC)
            
        # Check if the account has the creation date.
        if not self.account.fields.get("CREATED", None):
            self.account.update("CREATED", now.strftime("%Y-%m-%d %H:%M:%S"))
            
        # Update our last login time.
        self.account.update("LAST_LOGIN", now.strftime("%Y-%m-%d %H:%M:%S"))
                
        # Calculate the amount of days since our account was created.
         
        # Get our creation time from the stored date string.
        creation_time = datetime.strptime(self.account.fields.get("CREATED"), "%Y-%m-%d %H:%M:%S")
         
        # Calculate the difference in dates.
        delta_time = now - creation_time
         
        # Get the difference in days, That's how many days our account has been created.
        accountDays = abs(delta_time.days)
            
        datagram = Datagram()
        datagram.addInt8(returnCode) # returnCode
        datagram.addString(responseStr) # respString (in case of error)
        datagram.addUint32(self.account.doId) # DISL ID
        datagram.addString(accountName) # accountName - not saved in our db so we're just putting the playToken
        datagram.addUint8(tokenInfo["accountNameApproved"]) # account name approved
        datagram.addString(openChatEnabled) # openChatEnabled
        datagram.addString(createFriendsWithChat) # createFriendsWithChat
        datagram.addString(chatCodeCreationRule) # chatCodeCreationRule

        usec, sec = math.modf(time.time())
        datagram.addUint32(int(sec))
        datagram.addUint32(int(usec * 1000000))

        datagram.addString(paid) # access
        datagram.addString(whiteListChat) # whiteListChat
        datagram.addString(now.strftime("%Y-%m-%d %H:%M:%S")) # lastLoggedInStr
        datagram.addInt32(accountDays) # accountDays
        datagram.addString("NO_PARENT_ACCOUNT")
        datagram.addString(userName) # userName - not saved in our db so we're just putting a placeholder
        
        self.__authorized = True
        
        await self.send_message(CLIENT_LOGIN_TOONTOWN_RESP, datagram)
        
    async def handle_create_avatar(self, di):
        # Client wants to create an avatar

        # We read av info
        contextId = di.getUint16()
        dnaString = di.getBlob()
        avPosition = di.getUint8()

        # Is avPosition valid?
        if not 0 <= avPosition < 6:
            #print("Client sent an invalid av position")
            await self.disconnect(351, "") # The client tried to load an invalid avatar position in CLIENT_CREATE_AVATAR.
            return

        # Doesn't it already have an avatar at this slot?
        accountAvSet = self.account.fields["ACCOUNT_AV_SET"]

        if accountAvSet[avPosition] != 0:
            #print("Client tried to overwrite an avatar")
            await self.disconnect(350, "") # The client tried to overwrite an avatar in CLIENT_CREATE_AVATAR.
            return
            
        fields = {}

        # OwningAccount is the field used internally for figuring out which 
        # local accounts own the Avatar.
        fields["OwningAccount"] = self.account.doId
        
        # We currently don't have a way to get a account name from a account,
        # So just leave it as a specialized internal dev one..
        fields["setAccountName"] = "internal_%s" % str(hex(self.account.doId))
        
        # DISL is an acronym for Disney Integrated Services Layer.
        # This server must of had it's own set of accounts which included names
        # and ids separate from the OTP Server.
        # Since we have no such server, The fields are unused for us.
        
        # The DISL Name is the name of the Disney XD Account (Global Account).
        # We don't know how their names were stored or looked like.
        fields["setDISLname"] = "unknown"
        # The DISL Id is the id for a Disney XD Account (Global Account).
        # We don't know how any of these look, So we just use our local account ID for now.
        fields["setDISLid"] = self.account.doId
        
        fields["setDNAString"] = dnaString
        fields["setPosIndex"] = avPosition
        
        # We create the avatar.
        context = await self.agent.allocate_context()
        self.db_callbacks[context] = (self.handle_create_avatar_db_resp, (contextId, avPosition))
        await self.database_create_object("DistributedToon", fields, context)
        
    async def handle_create_avatar_db_resp(self, object, args):
        if not object or not args:
            return
            
        contextId, avPosition = *args

        # We save the avatar in the account
        accountAvSet[avPosition] = object.doId
        self.account.update("ACCOUNT_AV_SET", accountAvSet)
        
        # Make sure to store the newly created avatar in our list.
        self.avatars[avPosition] = object

        # We tell the client their new avId!
        dg = Datagram()
        dg.addUint16(contextId)
        dg.addUint8(0) # returnCode
        dg.addUint32(object.doId)
        await self.send_message(CLIENT_CREATE_AVATAR_RESP, dg)
        
    async def handle_delete_avatar(self, di):
        # Client wants to delete one of his avatars.
        # That's sad but let it be.
        avId = di.getUint32()

        # Is that even our avatar?
        accountAvSet = self.account.fields["ACCOUNT_AV_SET"]
        if not avId in accountAvSet:
            return

        # We remove the avatar from our account.
        avPosition = accountAvSet.index(avId)
        accountAvSet[avPosition] = 0
        self.account.update("ACCOUNT_AV_SET", accountAvSet)
        
        # Get our current time in UTC.
        now = datetime.now()
        #now = now.astimezone(tz=pytz.UTC)
        
        # Add the avatar to the list of pending avatars to delete in the Database.
        accountAvDelList = self.account.fields["ACCOUNT_AV_SET_DEL"]
        accountAvDelList.append((avId, int(now.timestamp())))
        
        # Remove the avatar from our list.
        self.avatars[avPosition] = None

        # We tell him it's done and we send him his new av list.
        dg = Datagram()
        dg.addUint8(0)
        await self.write_avatar_list(dg)
        await self.send_message(CLIENT_DELETE_AVATAR_RESP, dg)
            
    async def prepare_set_name_pattern(self, di):
        # Client sets his name
        # We may only allow this if we don't already have a name,
        # or only have a default name.
        
        # But for now, we don't care. TODO
        
        assert self.account != None, f"Account is None in CLIENT_SET_NAME_PATTERN, which should not be possible."
        
        # Make sure we actually own the avatar.
        avId = di.getUint32()
        if not avId in self.account.fields["ACCOUNT_AV_SET"]:
            await self.disconnect(603, "") # The CLIENT_SET_WISHNAME_CLEAR or CLIENT_SET_NAME_PATTERN has passed a DOID that is not in the account list of valid avatars.
            return
            
        await self.handle_set_name_pattern(avId, di)
            
    async def handle_set_name_pattern(self, avId, di):
        accountAvSet = self.account.fields["ACCOUNT_AV_SET"]
        
        # Make sure the avatar is actually loaded.
        avPosition = accountAvSet.index(avId)
        if not self.avatars[avPosition]:
            await self.load_avatar_list(self.handle_set_name_pattern, (avId, di))
            return
            
        avatar = self.avatars[avPosition]

        nameIndices = []
        nameFlags = []

        nameIndices.append(di.getInt16())
        nameFlags.append(di.getInt16())
        nameIndices.append(di.getInt16())
        nameFlags.append(di.getInt16())
        nameIndices.append(di.getInt16())
        nameFlags.append(di.getInt16())
        nameIndices.append(di.getInt16())
        nameFlags.append(di.getInt16())

        # TODO: Check if the name is valid (incl KeyError)
        # King King KingKing is NOT a valid name.

        name = ""
        for index in range(4):
            indice, flag = nameIndices[index], nameFlags[index]
            if indice != -1:
                namePartType, namePart = self.agent.nameDictionary[indice]
                if flag:
                    namePart = namePart.capitalize()

                # %s %s %s%s
                if index != 3:
                    name += " "

                name += namePart
                
        # We set the toon's name
        avatar.update("setName", name.strip())

        # We tell the client that their new name is accepted
        dg = Datagram()
        dg.addUint32(avatar.doId)
        dg.addUint8(0)
        await self.send_message(CLIENT_SET_NAME_PATTERN_ANSWER, dg)
        
    async def prepare_set_wishname(self, di):
        # Client sets his name
        # We may only allow this if we don't already have a name,
        # or only have a default name.

        # But for now, we don't care. TODO
        
        assert self.account != None, f"Account is None in CLIENT_SET_WISHNAME, which should not be possible."

        avId = di.getUint32()
        name = di.getString()

        if avId == 0:
            # Client just wants to check the name
            dg = Datagram()
            dg.addUint32(0)
            dg.addUint16(0)
            dg.addString("")
            dg.addString(name)
            dg.addString("")

            await self.send_message(CLIENT_SET_WISHNAME_RESP, dg)
            return

        if not avId in self.account.fields["ACCOUNT_AV_SET"]:
            await self.disconnect(602, "") # The CLIENT_SET_WISHNAME has passed a DOID that is not in the account list of valid avatars.
            return

        await self.handle_set_wishname(avId, name, di)

    async def handle_set_wishname(self, avId, name, di):
        # Make sure the avatar is actually loaded.
        avPosition = accountAvSet.index(avId)
        if not self.avatars[avPosition]:
            await self.load_avatar_list(self.handle_set_wishname, (avId, name, di))
            return
    
        avatar = self.avatars[avPosition]
        
        # Client wants to set the name and we're just gonna
        # allow him to.
        avatar.update("setName", name)

        dg = Datagram()
        dg.addUint32(avatar.doId)
        dg.addUint16(0)
        dg.addString("")
        dg.addString(name)
        dg.addString("")
        
        await self.send_message(CLIENT_SET_WISHNAME_RESP, dg)
        
    async def handle_get_avatars(self, di):
        # Client asks us their avatars.
        
        # This should be impossible. So if it happens, Assert.
        assert self.account != None, f"Account is None in CLIENT_GET_AVATARS, which should not be possible."
        
        # Make sure all of our avatars are loaded in.
        wait = await self.load_avatar_list(self.handle_get_avatars, (di))
        if wait: return
            
        # Give the avatar list to the client.
        dg = Datagram()
        dg.addUint8(0) # returnCode
        await self.write_avatar_list(dg)
        await self.send_message(CLIENT_GET_AVATARS_RESP, dg)
        
    async def handle_set_avatar(self, di):
        # Client picked an avatar.
        
        assert self.account != None, f"Account is None in CLIENT_SET_AVATAR, which should not be possible."
        
        avId = di.get_uint32()
        
        # If avId is 0, they either disconnected or want to remove their avatar.
        if not avId:
            await self.remove_avatar()
            return
            
        # Make sure we own the avatar we're trying to set ourselves as.
        if not avId in self.account.fields["ACCOUNT_AV_SET"]:
            await self.disconnect(601, "") # The CLIENT_SET_AVATAR has passed a DOID that is not in the account list of valid avatars.
            return
            
        # If we already have a avatar, Remove it.
        # This patches the old method of cloning.
        if self.avatar:
            await self.remove_avatar()
            
        await self.set_avatar(avId)
        
    async def handle_add_interest(self, di):
        # Client wants to add or replace an interest
        handle = di.getUint16()
        contextId = di.getUint32()
        parentId = di.getUint32()

        # We get every zone in the interest, including visibles zones from our visgroup
        zones = set()
        while di.getRemainingSize():
            zoneId = di.getUint32()
            if zoneId == 1:
                # No we don't want you Quiet Zone
                continue

            zones.add(zoneId)

            # We add visibles
            canonicalZoneId = getCanonicalZoneId(zoneId)

            if canonicalZoneId in self.agent.visgroups:
                for visZoneId in self.agent.visgroups[canonicalZoneId]:
                    zones.add(getTrueZoneId(visZoneId, zoneId))

                # We want to add the "main" zone, i.e 2200 for 2205, etc
                zones.add(zoneId - zoneId % 100)

        # This is set to an empty tuple because it's only defined if
        # it's overwriting an interest, but needed anyway.
        oldZones = ()

        if handle in self.interests:
            # Our interest is overwriting another interest:
            #
            # - if the parent id is different, we're just gonna remove it by
            #   disabling every object present in the old zones if we're not interested
            #   in them anymore.
            #
            # - if it's the same parent id, we're gonna do some intersection stuff and
            #   only send from the new zones and remove from the old zones,
            #   basically not sending anything for the zones in the intersection

            # We get the old interest
            oldParentId, oldZones = self.interests[handle]

            # We remove it
            del self.interests[handle]
            await self.update_interest_cache()
            
            # Store if we still have interest in the old parent or not.
            is_interested_in_parent = await self.has_interest_in_parent(oldParentId)
            
            # Objects in the same parent are just disabled,
            # But if the parent is changed and we don't have the old parent
            # any longer, Then we will delete the old objects instead,
            # But only if we don't still have interest in that parent anymore.
            if oldParentId == parentId:
                # We gotta disable the objects we can't see anymore,
                for do in self.agent.objects.values():
                    # If we still have interest in the object, Then don't disable or delete it.
                    is_interested = await self.has_interest(do.parentId, do.zoneId)
                    if is_interested:
                        continue
                        
                    # If the object is not visible anymore, we disable it
                    # (it's in the removed interest zones, but not in the new interest (or any current interest) zones)
                    if do.parentId == parentId and do.zoneId in oldZones and not do.zoneId in zones:
                        dg = Datagram()
                        dg.addUint32(do.doId)
                        await self.send_message(CLIENT_OBJECT_DISABLE, dg)
            else:
                # We gotta disable or delete the objects we can't see anymore.
                for do in self.agent.objects.values():
                    # If we still have interest in the object, Then don't disable or delete it.
                    is_interested = await self.has_interest(do.parentId, do.zoneId)
                    if is_interested:
                        continue

                    # If the object isn't in our the old parent or isn't in the old zones.
                    # It can't be what we're looking for.
                    if do.parentId != oldParentId or not do.zoneId in oldZones:
                        continue
                        
                    # Do we still have interest in the old parent whatsoever?
                    if is_interested_in_parent:
                        # If we still do-just disable the object, It could enter a interest we still do have.
                        dg = Datagram()
                        dg.addUint32(do.doId)
                        await self.send_message(CLIENT_OBJECT_DISABLE, dg)
                        continue

                    # If the object is not visible anymore, we delete it
                    # (it's in the removed interest zones and parents, but not in the new interest (or any current interest) zones or parents)
                    dg = Datagram()
                    dg.addUint32(do.doId)
                    await self.send_message(CLIENT_OBJECT_DELETE, dg)

                # We set oldZones to an empty tuple
                # (because we're ignoring them as the parentId is different)
                oldZones = ()

        # We send the newly visible objects
        newZones = []
        for zoneId in zones:
            is_interested = await self.has_interest(parentId, zoneId)
            if not zoneId in oldZones and not is_interested:
                newZones.append(zoneId)

        # We have got a new zone list, we can finally send the objects.
        await self.send_objects(parentId, newZones)

        # We save the interest
        self.interests[handle] = (parentId, zones)
        await self.update_interest_cache()

        # We tell the client we're done
        dg = Datagram()
        dg.addUint16(handle)
        dg.addUint32(contextId)
        await self.send_message(CLIENT_DONE_INTEREST_RESP, dg)
        
    async def handle_remove_interest(self, di):
        # Client wants to remove an interest
        handle = di.getUint16()
        contextId = di.getUint32() # Might be optional

        # Did the interest exist?
        if not handle in self.interests:
            return

        # We get what the interest was
        oldParentId, oldZones = self.interests[handle]

        # We remove the interest
        del self.interests[handle]
        await self.update_interest_cache()
        
        # Store if we still have interest in the old parent or not.
        is_interested_in_parent = await self.has_interest_in_parent(oldParentId)

        # We disable or delete all the objects we're no longer interested in.
        for do in self.agent.objects.values():
            # If we have interest in the object, Then don't disable or delete it.
            is_interested = await self.has_interest(do.parentId, do.zoneId)
            if is_interested:
                continue
            
            # If the object isn't in our the old parent or isn't in the old zones.
            # It can't be what we're looking for.
            if do.parentId != oldParentId or not do.zoneId in oldZones:
                continue
                
            # Do we still have interest in the old parent whatsoever?
            if is_interested_in_parent:
                # If we still do-just disable the object, It could enter a interest we still do have.
                dg = Datagram()
                dg.addUint32(do.doId)
                await self.send_message(CLIENT_OBJECT_DISABLE, dg)
                continue

            # Otherwise delete the object, If it enters a interest we have under a new parent.
            # It will be regenerated.
            dg = Datagram()
            dg.addUint32(do.doId)
            await self.send_message(CLIENT_OBJECT_DELETE, dg)

        # We tell the client we're done
        dg = Datagram()
        dg.addUint16(handle)
        dg.addUint32(contextId)
        await self.send_message(CLIENT_DONE_INTEREST_RESP, dg)
        
    async def handle_object_location(self, di):
        # Client wants to move an object
        doId = di.getUint32()
        parentId = di.getUint32()
        zoneId = di.getUint32()
        
        if not doId in self.agent.objects:
            #print("Client tried to move an object that doesn't exist!")
            return
            
        object = self.agent.objects[doId]
        
        # Can we move it?
        # This will make more sense when Owner View support is added
        # and we change this.
        if not self.avatar or self.avatar.doId != object.doId:
            #print("Client wants to move an object it doesn't own")
            return
        
        # We tell the State Server that we're moving an object.
        dg = Datagram()
        dg.addUint32(parentId)
        dg.addUint32(zoneId)
        await self.agent.send_message(([object.doId], self.agent.channel, STATESERVER_OBJECT_SET_ZONE, dg)
        
        # Toontown Game Specific Code
        
        # If the object isn't our avatar (Toon), Then skip this.
        if not self.avatar or self.avatar.doId != object.doId:
            return
            
        # We don't care for dynamic zones, And won't save them.
        if zoneId == 0 or zoneId >= 61000:
            return
            
        fields = {}
        fields["setDefaultShard"] = (parentId,)

        defaultZoneId = zoneId - (zoneId % 1000)
        
        # If we're in Welcome Valley, Then we ignore it's sub zone changes. Including for Goofy Speedway.
        # Instead our default zone id will be for the Welcome Valley Token.
        if defaultZoneId >= 22000 and defaultZoneId < 61000:
            defaultZoneId = 0 # Set the default zone id to 0, Which is the Welcome Valley zone token.
 
        fields["setDefaultZone"] = (defaultZoneId,)

        canonZoneId = zoneId
        canonHoodId = zoneId
        
        # Get our canonical zone id.
        if canonZoneId >= 22000 and canonZoneId < 61000:
            canonZoneId = (canonZoneId % 2000)
            if canonZoneId < 1000:
                canonZoneId = canonZoneId + 2000
            else:
                canonZoneId = canonZoneId - 1000 + 8000
                
        # Get our hood id from it.
        canonHoodId = canonZoneId - (canonZoneId % 1000)

        fields["setLastHood"] = (canonHoodId,)
        
        zonesVisited = self.avatar.get("setZonesVisited", [])
        if not canonHoodId in zonesVisited:
            zonesVisited.append(canonHoodId)
            fields["setZonesVisited"] = (zonesVisited,)

        hoodsVisited = self.avatar.get("setHoodsVisited", [])
        if not canonHoodId in hoodsVisited:
            hoodsVisited.append(canonHoodId)
            fields["setHoodsVisited"] = (hoodsVisited,)
        
        await self.stateserver_update_object_fields(self.avatar, fields)
        
        
    async def get_unloaded_avatars(self):
        """
        Get the avatars in our account avatar list which are not yet loaded.
        """
        
        assert self.account != None, f"Account is None while checking which avatars we don't have loaded, which should not be possible."
        
        accountAvSet = self.account.fields["ACCOUNT_AV_SET"]
            
        # Don't load empty avatar slots,
        # and don't reload already loaded avatars.
        avts_to_load = {}
        for pos, avId in enumerate(accountAvSet):
            # Empty slot or Already Loaded
            if avId == 0 or self.avatars[pos] != None: 
                continue
            # Sanity check to make sure we can't just load the avatar from the ClientAgent.
            if avId in self.agent.objects:
                self.avatars[avId] = self.agent.objects[avId]
                continue
            avts_to_load[pos] = avId
            
        return avts_to_load
        
    async def load_avatar_list(self, callback=None, args=None):
        """
        Load the client avatar list from the database.
        """
            
        avts_to_load = await self.get_unloaded_avatars()
        
        # There's no avatars to load.
        if len(avts_to_load) <= 0:
            return False
            
        context = -1
        # If we have a callback, Properly set it up.
        if callback:
            context = await self.agent.allocate_context()
            self.callbacks[context] = (callback, args)
            self.callback_counters[context] = 0
            
        # Let's load the avatar slots we know are supposed to exist.
        for pos, avId in avts_to_load.items():
            avatar_context = await self.agent.allocate_context()
            self.db_callbacks[avatar_context] = (self.load_avatar_list_db_resp, (context, pos, len(avts_to_load)))
            await self.database_request_object("DistributedToon", avId, avatar_context)
            
        return True
            
    async def load_avatar_list_db_resp(self, object, args):
        context, pos, max_count = *args
        
        # If we didn't get an object. The avatar doesn't exist.
        # Let's remove the invalid avatar from the account.
        if not object:
            accountAvSet = self.account.fields["ACCOUNT_AV_SET"]
            accountAvSet[pos] = 0
            self.account.update("ACCOUNT_AV_SET", accountAvSet)
        else:
            self.avatars[pos] = object
        
        # We don't do anything else if there isn't a callback to later call.
        if context == -1 or not context in self.callbacks:
            return
            
        # Check our count, If it's reached the threshold then call the callback,
        # otherwise increment and continue waiting.
        current_count = self.callback_counters[context]
        if current_count < max_count:
            self.callback_counters[context] += 1
            return
        
        # Call our callback.
        callback, args = *self.callbacks[context]
        await callback(*args)
        
        # Remove the callback now that it's been called.
        del self.callbacks[context]
        
    async def write_avatar_list(self, datagram=None):
        if not datagram:
            datagram = Datagram()

        # This is each blob of data for the avatars we have managed to load.
        avatar_blobs = []

        # We send every avatar we have loaded.
        for avatar in self.avatars:
            if not avatar:
                continue

            dg = Datagram()
            di = DatagramIterator(dg)
            
            # PotentialAvatar
            dg.add_uint32(avatar.doId) # DoId
            dg.add_string(avatar.fields["setName"][0]) # Name
            dg.add_string("") # Wish Name
            dg.add_string("") # Approved Wish Name
            dg.add_string("") # Rejected Wish Name
            dg.add_blob(avatar.fields["setDNAString"][0]) # DNA
            dg.add_uint8(pos) # Position
            dg.add_uint8(0) # Naming Allowed - For if the avatar is available for naming.

            avatar_blobs.append(di.get_remaining_bytes())

        # Avatar count
        datagram.add_uint16(len(avatar_blobs)) # avatarTotal

        # Append each avatar blob to the datagram.
        for blob in avatar_blobs:
            datagram.append_data(blob)
            
        return datagram
        
    async def set_avatar(self, avId):
        """
        Choose an avatar
        """
        
        # Make sure all of our avatars are loaded in.
        wait = await self.load_avatar_list(self.set_avatar, (avId))
        if wait: return
        
        accountAvSet = self.account.fields["ACCOUNT_AV_SET"]
        avPosition = accountAvSet.index(avId)
        
        self.avatar = self.avatars[avPosition]
        
        # Put our avatar on the ClientAgent ahead of time.
        # This will allow us to recieve the generate as an update to the object.
        self.agents.objects[avatar.doId] = self.avatar
        
        self.generate_callbacks[avatar.doId] = (self.set_avatar_finish, ())
        
        # We ask STATESERVER to create our object
        dg = Datagram()
        dg.addUint32(0)
        dg.addUint32(0)
        dg.addUint16(avatar.dclass.get_number())
        dg.addUint32(avatar.doId)
        avatar.packRequired(dg)
        avatar.packOther(dg)
        await self.agent.send_message([20100000], avatar.doId, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER, dg)
        
    async def set_avatar_finish(self, object, args):
        self.avatar_deleted = False # This a mark to prevent resending a delete to the StateServer.
        
        # We can send that we are the proud owner of a DistributedToon!
        dg = Datagram()
        dg.addUint32(self.avatar.doId)
        dg.addUint8(0)
        self.avatar.packRequired(dg)
        await self.send_message(CLIENT_GET_AVATAR_DETAILS_RESP, dg)
        
        if not "setFriendsList" in self.avatar.fields:
            return
            
        # If we have friends... We should probably let them know we're online!
            
        friendsList = self.avatar.fields["setFriendsList"][0]

        # Get all of our friend ids.
        friendIds = []
        for i in range(0, len(friendsList)):
            friendIds.append(friendsList[i][0])
        
        # Send the online message to all of our friends.
        for client in self.agent.clients:
            # If the id is in the list, It means this friend is online,
            # Otherwise. We'll skip this client.
            if not client.avatar or not client.avatar.doId in friendIds:
                continue
                
            # Our friend is online, Let them know we are too!
            dg = Datagram()
            dg.add_uint32(self.avatar.doId)
            await client.send_message(CLIENT_FRIEND_ONLINE, dg)
            
    async def remove_avatar(self):
        if not self.avatar:
            return
            
        # Grab the fields we may need from the avatar before we delete it.
        avatarDoId = self.avatar.doId
        friendsList = self.avatar.fields["setFriendsList"][0] if "setFriendsList" in self.avatar.fields else None
        
        # Remove the avatar locally.
        del self.avatar
        self.avatar = None
        
        # We ask the StateServer to delete our object.
        if not self.avatar_deleted:
            dg = Datagram()
            dg.add_uint32(avatarDoId)
            await self.agent.send_message([20100000], avatarDoId, STATESERVER_OBJECT_DELETE_RAM, dg)
            self.avatar_deleted = True # This a mark to prevent resending a delete to the StateServer.

        if not friendsList:
            return
        
        # If we have friends... We should probably let them know we're heading off.

        # Get all of our friend ids.
        friendIds = []
        for i in range(0, len(friendsList)):
            friendIds.append(friendsList[i][0])
        
        # Send the offline message to all of our friends.
        for client in self.agent.clients:
            # If the id is in the list, It means this friend is online,
            # Otherwise. We'll skip this client.
            if not client.avatar or not client.avatar.doId in friendIds:
                continue

            # Our friend is online, Let them know we are heading off!
            dg = Datagram()
            dg.add_uint32(avatarDoId)
            await client.send_message(CLIENT_FRIEND_OFFLINE, dg)
    
    async def receive_create(self, do, sender, other):
        # We send the object creation if we're the owner or if we're interested.
        is_interested = await self.has_interest(do.parentId, do.zoneId)
        if not is_interested and do.doId != self.avatar.doId:
            return
            
        # Call our generate callback if we have one.
        if do.doId in self.generate_callbacks:
            callback, args = *self.generate_callbacks[do.doId]
            await callback(do, args)
            
        # Don't echo the message.
        if self.avatar.doId == sender:
            return
            
        # Pack the datagram for creating the object on the client.
        dg = Datagram()
        dg.add_uint32(do.parentId)
        dg.add_uint32(do.zoneId)
        dg.add_uint16(do.dclass.get_number())
        dg.add_uint32(do.doId)
        do.packRequiredBroadcast(dg)
        
        if other:
            do.packOther(dg)
            await self.send_message(CLIENT_CREATE_OBJECT_REQUIRED_OTHER, dg)
            return
        
        await self.send_message(CLIENT_CREATE_OBJECT_REQUIRED, dg)
        
    async def receive_delete(self, do, sender):
        # Not retransmitting.
        if self.avatar.doId == sender:
            return
            
        # An AI likely dropped dead and our avatar got deleted,
        # Disconnect our client with a lost connection message.
        if do.doId == self.avatar.doId:
            self.avatar_deleted = True # This a mark to prevent resending a delete to the StateServer.
            await self.disconnect(153, "Lost connection.")
            return
            
        # We tell the client that it's disabled only if they're interested or the owner.
        # (Please note this last condition here is useless but it's meant to be replaced if owner view is implemented some day)
        is_interested = await self.has_interest(do.parentId, do.zoneId)
        if not is_interested and do.doId != self.avatar.doId:
            return

        # We're deleting an object.
        # See receive_move(), handle_add_interest() and handle_remove_interest()
        # for more references to how objects and disabled and deleted and as to how and when.
        dg = Datagram()
        dg.addUint32(do.doId)
        await self.send_message(CLIENT_OBJECT_DELETE, dg)
        
    async def receive_move(self, do, prevParentId, prevZoneId, sender):
        # Don't echo the message.
        if self.avatar.doId == sender:
            return
            
        # If we're the owner, we must receive it in any case.
        if self.avatar.doId == do.doId:
            dg = Datagram()
            dg.add_uint32(do.doId)
            dg.add_uint32(do.parentId)
            dg.add_uint32(do.zoneId)
            await self.send_message(CLIENT_OBJECT_LOCATION, dg)
            return
            
        old_area_interest = await self.has_interest(prevParentId, prevZoneId)
        new_area_interest = await self.has_interest(do.parentId, do.zoneId)
            
        # We have no interest in either the old or new locations, Ignore this request for movement.
        if not old_area_interest and not new_area_interest:
            return
        
        # If we're interested in the previous area
        if old_area_interest:
            # If we're interested in the new area,
            # we can just tell the client that the object moved
            if new_area_interest:
                dg = Datagram()
                dg.add_uint32(do.doId)
                dg.add_uint32(do.parentId)
                dg.add_uint32(do.zoneId)
                await self.send_message(CLIENT_OBJECT_LOCATION, dg)
                return

            # If we're not, we ask them to disable the object if the parent is the same.
            # We don't need the object if it's moved elsewhere.
            if do.parentId == prevParentId:
                dg = Datagram()
                dg.add_uint32(do.doId)
                await self.send_message(CLIENT_OBJECT_DISABLE, dg)
                return
            
            # The parent is different, Delete the object instead.
            # We will send a generate for the client if it comes back.
            dg = Datagram()
            dg.add_uint32(do.doId)
            await self.send_message(CLIENT_OBJECT_DELETE, dg)
            return
            
        # If we're only interested in the new area,
        # we ask them to create the object
        dg = Datagram()
        dg.add_uint32(do.parentId)
        dg.add_uint32(do.zoneId)
        dg.add_uint16(do.dclass.get_number())
        dg.add_uint32(do.doId)
        do.packRequiredBroadcast(dg)
        do.packOther(dg)
        await self.send_message(CLIENT_CREATE_OBJECT_REQUIRED_OTHER, dg)
        
    async def receive_update(self, do, field, data, sender):
        # This field has no reason to be transmitted if it's not ownrecv or broadcast
        if not (field.isOwnrecv() or field.isBroadcast()):
            return
        
        # We are not transmitting back our own updates.
        if self.avatar and self.avatar.doId == sender:
            return
            
        # If we're the owner, We should always recieve the update.
        if self.avatar and self.avatar.doId == do.doId:
            # We generate the field update
            dg = Datagram()
            dg.addUint32(do.doId)
            dg.addUint16(field.get_number())
            dg.appendData(data)
            await self.send_message(CLIENT_OBJECT_UPDATE_FIELD, dg)
            return
            
        # Can this client receive this update?
        # Are we interested in this object?
        # TODO: Is the broadcast check required?
        
        interested = await self.has_interest(do.parentId, do.zoneId)
        if not interested or (field.isOwnrecv() or not field.isBroadcast())
            return
            
        # We generate the field update
        dg = Datagram()
        dg.addUint32(do.doId)
        dg.addUint16(field.get_number())
        dg.appendData(data)
        await self.send_message(CLIENT_OBJECT_UPDATE_FIELD, dg)
        
    async def receive_database_create_object_resp(self, context, di):
        return_code = di.get_uint8()
        if return_code != 0:
            print(f"Failed to create database object in context '{context}' with error code {return_code}")
            if context in self.db_callbacks:
                del self.db_callbacks[context]
            if context in self.db_callback_objects:
                del self.db_callback_objects[context]
            return
        
        # The newly created doId to assign to our object.
        doId = di.get_uint32()
        
        # Get the object and assign its doId
        object = self.db_callback_objects[context]
        object.doId = doId
        
        # Call our callback!
        callback, args = *self.db_callbacks[context]
        await callback(object, args)
        
        # These are no longer needed.
        del self.db_callbacks[context]
        del self.db_callback_objects[context]
        
    async def receive_database_request_object_resp(self, context, di):
        doId = di.get_uint32()
        num_fields = di.get_uint16()
        
        field_names = []
        for i in range(0, num_fields):
            field_names.append(di.get_string())
            
        return_code = di.get_uint8()
        if return_code != 0:
            print(f"Failed to receive database object {doId} in context '{context}' with error code {return_code}")
            if context in self.db_callbacks:
                del self.db_callbacks[context]
            if context in self.db_callback_objects:
                del self.db_callback_objects[context]
            return

        values = []
        for i in range(0, num_fields):
            value = di.get_string().encode('ISO-8859-1')
            values.append(value)

        for i in range(0, num_fields):
            found = di.get_uint8()
            if found: continue
            
            del field_names[i]
            del values[i]
            
        # Get the object from our callback obects.
        object = self.db_callback_objects[context]
        
        # Receive the fields from the resulting data onto our object.
        for i in range(0, num_fields):
            field_name = field_names[i]
            field = object.dclass.get_field_by_name(field_name)
            if not field: continue
            
            dg = Datagram(values[i])
            di = DatagramIterator(dg)
            object.receiveField(field, di)
            
        # Call our callback!
        callback, args = *self.db_callbacks[context]
        await callback(object, args)
        
        # These are no longer needed.
        del self.db_callbacks[context]
        del self.db_callback_objects[context]
        
    async def has_interest(self, parentId, zoneId):
        """
        Check if we're interested in a zone
        """
        # Do we have this interest cached?
        if not parentId in self.__interestCache:
            return False
        return zoneId in self.__interestCache[parentId]
        
    async def has_interest_in_parent(self, parentId):
        """
        Check if we're interested in a parent/shard
        """
        return parentId in self.__interestCache
        
    async def update_interest_cache(self):
        del self.__interestCache
        self.__interestCache = {}

        for handle in self.interests:
            parentId, zones = self.interests[handle]
            
            if not parentId in self.__interestCache:
                self.__interestCache[parentId] = set()

            for zoneId in zones:
                self.__interestCache[parentId].add(zoneId)

        return True
        
    async def send_objects(self, parentId, zones):
        objects = []
        for do in self.agent.objects.values():
            # We're not sending our own object because
            # we already know who we are (we are the owner)
            if self.avatar and self.avatar.doId == do.doId:
                continue

            # If the object is in one of the new interest zones, we get it
            if do.parentId == parentId and do.zoneId in zones:
                objects.append(do)

        # We sort them by dclass (fix some issues)
        objects.sort(key = lambda x: x.dclass.get_number())

        # We send every object
        for do in objects:
            dg = Datagram()
            dg.add_uint32(do.parentId)
            dg.add_uint32(do.zoneId)
            dg.add_uint16(do.dclass.getNumber())
            dg.add_uint32(do.doId)
            do.packRequiredBroadcast(dg)
            do.packOther(dg)
            await self.send_message(CLIENT_CREATE_OBJECT_REQUIRED_OTHER, dg)
            
    async def set_clsend_fields(self, doId, fields):
        self.__doId2ClsendOverrides[doId] = fields

    async def stateserver_update_object_field(self, object, field_name, *value):
        # Validate that the object is a valid one.
        if not object or not object.dclass:
            return
        # If our Client Agent doesn't have it, Assume it doesn't exist on the State Server.
        if not object.doId in self.agent.objects:
            return
        # There's nothing to update if we have no fields or value.
        if not field_name or not value:
            return
        
        # Apply the field locally and update it on the State Server.
        
        # Make sure the field exists.
        field = object.dclass.get_field_by_name(field_name)
        if not field:
            return
            
        # Make sure to update the object.
        object.fields[field.get_number()] = value
        
        # Pack the datagram to update the field for this object on the State Server.
        dg = Datagram()
        dg.add_uint32(object.doId)
        dg.add_uint16(field.get_number())
        object.packField(dg, field)
        
        # Send our message to State Server
        await self.agent.send_message([object.doId], self.agent.channel, STATESERVER_OBJECT_UPDATE_FIELD, dg)
        
    async def stateserver_update_object_fields(self, object, fields):
        # Validate that the object is a valid one.
        if not object or not object.dclass:
            return
        # If our Client Agent doesn't have it, Assume it doesn't exist on the State Server.
        if not object.doId in self.agent.objects:
            return
        # There's nothing to update if we have no fields.
        if not fields:
            return
        
        # Apply the fields locally and update them on the State Server.
        for field_name, *values in fields:
            field = object.dclass.get_field_by_name(field_name)
            if not field:
                continue
                
            # Make sure to update the object.
            object.fields[field.get_number()] = values
            
            # Pack the datagram to update the field for this object on the State Server.
            dg = Datagram()
            dg.add_uint32(object.doId)
            dg.add_uint16(field.get_number())
            object.packField(dg, field)
            
            # Send our message to State Server
            await self.agent.send_message([object.doId], self.agent.channel, STATESERVER_OBJECT_UPDATE_FIELD, dg)
        
    async def database_create_object(self, dclass_name, fields, context):
        dclass = self.agent.dc.get_class_by_name(dclass_name)
        if not dclass:
            return
            
        packer = DCPacker()
        packed_fields = {}
        for index in range(dclass.get_num_inherited_fields()):
            field = dclass.get_inherited_field(index)
            # We only want DB fields.
            if not field or not field.is_db() or field.as_molecular_field():
                continue
                
            # Get the name for the field.
            field_name = field.get_name()
                
            # If we don't have a value for the field and it's not required. Skip it.
            # Required fields MUST have default values but unrequired fields don't have to.
            if not field_name in fields and (not field.has_default_value() and not field.is_required()):
                continue
            
            # Begin packing.
            packer.beginPack(field)
            # Pack the field.
            if field_name in fields:
                field.packArgs(packer, fields[field_name])
            else:
                packer.packDefaultValue()
            # Packing over.
            packer.endPack()
                
            # Add our field to the dict.
            packed_fields[field_name] = Datagram(packer.getBytes())
            
        # Create a local object to store the fields.
        object = DistributedObject(-1, dclass, -1, -1)
            
        # Update our object with the newly packed fields.
        for name, value in packed_fields:
            field = object.dclass.get_field_by_name(name)
            assert field != None
            
            object.receiveField(field, DatagramIterator(value))
            
        # Store our now created locally object for later.
        # It should match our future created db object field wise.
        self.db_callback_objects[context] = object
        
        # Construct our datagram for requesting the creation of our object.
        dg = Datagram()
        dg.add_uint32(context)
        dg.add_string(dclass_name)
        dg.add_uint16(0)
        dg.add_uint16(len(packed_fields))
        for field in list(packed_fields.keys()):
            dg.add_string(field)
        for value in list(packed_fields.values()):
            dg.add_string(value.get_message())

        # Send our message to Database Server
        self.agent.send_message([DBSERVER_ID], self.agent.channel, DBSERVER_CREATE_STORED_OBJECT, dg)
    
    async def database_request_object(self, dclass_name, doId, context):
        dclass = self.agent.dc.get_class_by_name(dclass_name)
        if not dclass:
            return
            
        field_names = []
        for index in range(dclass.get_num_inherited_fields()):
            field = dclass.get_inherited_field(index)
            # We only want DB fields.
            if not field or not field.is_db() or field.as_molecular_field():
                continue

            field_names.append(field.get_name())
            
        # Create a local object to store the response
        # and store our now created locally object for later.
        object = DistributedObject(doId, dclass, -1, -1)
        self.db_callback_objects[context] = object
            
        # Construct our datagram for requesting the creation of our object.
        dg = Datagram()
        dg.add_uint32(context)
        dg.add_uint32(doId)
        dg.add_uint16(len(field_names))
        for name in field_names:
            dg.add_string(name)
            
        # Send our message to Database Server
        self.agent.send_message([DBSERVER_ID], self.agent.channel, DBSERVER_GET_STORED_VALUES, dg)
        
    async def database_update_object(self, dclass_name, doId, fields):
        dclass = self.agent.dc.get_class_by_name(dclass_name)
        if not dclass:
            return
            
        packer = DCPacker()
        packed_fields = {}
        for index in range(dclass.get_num_inherited_fields()):
            field = dclass.get_inherited_field(index)
            # We only want DB fields.
            if not field or not field.is_db() or field.as_molecular_field():
                continue
                
            # Get the name for the field.
            field_name = field.get_name()

            # If we specified a value for this field.
            if not field_name in fields:
                # Don't update a field we don't have a value for.
                continue 

            # Begin packing.
            packer.beginPack(field)
            # Pack the field.
            field.packArgs(packer, fields[field_name])
            # Packing over.
            packer.endPack()
                
            # Add our field to the dict.
            packed_fields[field_name] = Datagram(packer.getBytes())
            
        # Construct our datagram for requesting the creation of our object.
        dg = Datagram()
        dg.add_uint32(doId)
        dg.add_uint16(len(packed_fields))
        for field in list(packed_fields.keys()):
            dg.add_string(field)
        for value in list(packed_fields.values()):
            dg.add_string(value.get_message())

        # Send our message to Database Server
        self.agent.send_message([DBSERVER_ID], self.agent.channel, DBSERVER_SET_STORED_VALUES, dg)
        
    async def database_update_from_object(self, object, fields=None):
        if not object or not object.dclass:
            return
            
        packer = DCPacker()
        packed_fields = {}
        for index, value in object.fields:
            field = object.dclass.get_field_by_index(index)
            # We only want DB fields and we want them to have a value.
            if value == None or not field or not field.is_db() or field.as_molecular_field():
                continue
                
            field_name = field.get_name()
            
            # Don't include any fields not specified if we did specify them.
            if fields and not field_name in fields:
                continue
                
            # Begin packing.
            packer.beginPack(field)
            # Pack the field.
            field.packArgs(packer, value)
            # Packing over.
            packer.endPack()
                
            # Add our field to the dict.
            packed_fields[field_name] = Datagram(packer.getBytes())
            
        # Construct our datagram for requesting the creation of our object.
        dg = Datagram()
        dg.add_uint32(object.doId)
        dg.add_uint16(len(packed_fields))
        for field in list(packed_fields.keys()):
            dg.add_string(field)
        for value in list(packed_fields.values()):
            dg.add_string(value.get_message())

        # Send our message to Database Server
        self.agent.send_message([DBSERVER_ID], self.agent.channel, DBSERVER_SET_STORED_VALUES, dg)