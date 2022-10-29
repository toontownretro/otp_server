import math, os, struct, time, pytz, traceback
from datetime import datetime, timezone

from panda3d.core import Datagram, DatagramIterator
from panda3d.direct import DCPacker
from zone_util import getCanonicalZoneId, getTrueZoneId
from msgtypes import *
from security import *

class Client:
    def __init__(self, agent, sock, addr):
        self.agent = agent
        self.sock = sock
        self.addr = addr

        # Quick access for OTP
        self.otp = self.agent.otp
        self.messageDirector = self.otp.messageDirector
        self.databaseServer = self.otp.databaseServer
        self.stateServer = self.otp.stateServer

        # State stuff
        self.buffer = bytearray()
        self.interests = {}

        # Account stuff
        self.avatarId = 0
        self.account = None
        
        # This is for if we're authorized or not yet. Some messages can only be sent when authorized.
        self.__authorized = False

        # Cache for interests, so we don't have to iterate through all interests
        # every time an object updates.
        # This could be optimized in other languages, but we're using Python so
        # we're just gonna use a set
        self.__interestCache = set()

        # This is used to store the clsend field overrides sent by CLIENT_SET_FIELD_SENDABLE.
        self.__doId2ClsendOverrides = {}
        
        
    def disconnect(self, index=None, reason=""):
        datagram = Datagram()
        if index:
            datagram.addUint16(index)
            datagram.addString(reason)
            
        # Tell our connected client to go get lost.
        self.sendMessage(CLIENT_GO_GET_LOST, datagram)
        
        # We are no longer authorized.
        self.__authorized = False
        
        # Now we disconnect the client
        self.sock.close()

        # This is not gonna call itself
        self.onLost()

        # Now we delete the client from OTP
        del self.otp.clients[self.sock]
        self.agent.clients.remove(self)

    def onAvatarDelete(self):
        # Our avatar got deleted
        self.avatarId = 0
        self.disconnect(153, "Lost connection.")

    def onLost(self):
        # We remove the avatar if we're disconnecting. Bye!
        if self.avatarId:
            self.removeAvatar()

        # We are no longer authorized.
        self.__authorized = False

    def onData(self, data):
        self.buffer += data
        while len(self.buffer) >= 2:
            length = struct.unpack("<H", self.buffer[:2])[0]
            if len(self.buffer) < length+2:
                break

            packet = self.buffer[2:length+2]
            self.buffer = self.buffer[length+2:]

            self.onDatagram(Datagram(bytes(packet)))

    def onDatagram(self, msgDg):
        di = DatagramIterator(msgDg)
        
        try:
            msgType = di.getUint16()
        except:
            print("Received truncated datagram from connection: %s:%d!" % (self.addr[0], self.addr[1]))
            self.disconnect(200) # Internal error in the clients state machine.  Contact Developers for correction.
            
        # If it's a heartbeat, Respond directly. Otherwise handle our datagram.
        if msgType == CLIENT_HEARTBEAT:
            # TODO: Keep track of heartbeats.
            self.sendMessage(CLIENT_HEARTBEAT, msgDg)
        else:
            # Handle the datagram.
            self.handle_datagram(msgType, di)
            
    def handle_datagram(self, msgType, di):
        if msgType == CLIENT_DISCONNECT:
            # Luckily for us, Super simple.
            self.disconnect()
            
        elif msgType == CLIENT_LOGIN_2:
            print("CLIENT_LOGIN_2")
            playToken = di.getString()
            serverVersion = di.getString()
            hashVal = di.getUint32()
            tokenType = di.getUint32()
            validateDownload = di.getString()
            wantMagicWords = di.getString()
            
            tokenInfo = self.parse_play_token(playToken.encode("utf-8"), tokenType)
            
            # These arguments are things we need from our token read response.
            returnCode = tokenInfo["returnCode"]
            responseStr = tokenInfo["respString"]
            accountDoId = None
            accountName = ""
            createFriendsWithChat = "YES"
            chatCodeCreationRule = "YES"
            userName = ""
            
            if tokenInfo["accountName"] != None: accountName = tokenInfo["accountName"]
            
            if tokenInfo["accountNumber"] != None: accountDoId = tokenInfo["accountNumber"]
            
            createFriendsWithChat = "YES" if tokenInfo["createFriendsWithChat"] else "NO"
            
            chatCodeCreationRule = "YES" if tokenInfo["chatCodeCreationRule"] else "NO"

            whiteListChat = "YES" if tokenInfo["whitelistChat"] else "NO"
            
            if tokenInfo["userName"] != None:
                userName = tokenInfo["userName"]
                
                accFile = os.path.join("database", userName + ".txt")
                if os.path.isfile(accFile):
                    with open(accFile, "r") as f:
                        self.account = self.databaseServer.loadDatabaseObject(int(f.read()))
                        accountDoId = self.account.doId
                else:
                    # We create an Account
                    self.account = self.databaseServer.createDatabaseObject("Account")
                    accountDoId = self.account.doId
                    with open(accFile, "w") as f:
                        f.write(str(self.account.doId))
                        
            # Get our current time in UTC.
            now = datetime.now()
            #now = now.astimezone(tz=pytz.UTC)
            
            # By default, We say it's existed for 0 days. 
            accountDays = 0
                 
            # If we found the account then we'll do required field checks.
            if self.account:
                 # Check if the account has the creation date.
                 if not self.account.fields.get("CREATED", None):
                    self.account.update("CREATED", now.strftime("%Y-%m-%d %H:%M:%S"))

                 # Update our last login time.
                 self.account.update("LAST_LOGIN", now.strftime("%Y-%m-%d %H:%M:%S"))
                 
                 # Caculate the amount of days since our account was created.
                 
                 # Get our creation time from the stored date string.
                 creation_time = datetime.strptime(self.account.fields.get("CREATED", now.strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S")
                 
                 # Caculate the difference in dates.
                 delta_time = now - creation_time
                 
                 # Get the difference in days, That's how many days our account has been created.
                 accountDays = abs(delta_time.days)
                 
            # If no errors occured and we got our account, Then we authorize this client to use the other messages.
            if returnCode == 0 and self.account: self.__authorized = True

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
            self.sendMessage(CLIENT_LOGIN_2_RESP, datagram)

        elif msgType == CLIENT_LOGIN_TOONTOWN:
            print("CLIENT_LOGIN_TOONTOWN")
            playToken = di.getString()
            serverVersion = di.getString()
            hashVal = di.getUint32()
            tokenType = di.getInt32()
            wantMagicWords = di.getString()
            
            tokenInfo = self.parse_play_token(playToken.encode("utf-8"), tokenType)
                        
            # These arguments are things we need from our token read response.
            returnCode = tokenInfo["returnCode"]
            responseStr = tokenInfo["respString"]
            accountDoId = 0
            accountName = ""
            openChatEnabled = "YES"
            createFriendsWithChat = "YES"
            chatCodeCreationRule = "YES"
            paid = "VELVET_ROPE"
            whiteListChat = "YES"
            userName = ""
            
            if tokenInfo["accountName"] != None: accountName = tokenInfo["accountName"]
            
            if tokenInfo["accountNumber"] != None: accountDoId = tokenInfo["accountNumber"]
          
            openChatEnabled = "YES" if tokenInfo["openChatEnabled"] else "NO"
            
            createFriendsWithChatFlags = {0: "NO", 1: "CODE", 2: "YES"} # Index to result.
            createFriendsWithChat = createFriendsWithChatFlags.get(tokenInfo["createFriendsWithChat"], "NO")
            
            chatCodeCreationRuleFlags = {0: "NO", 1: "PARENT", 2: "YES"} # Index to result.
            chatCodeCreationRule = chatCodeCreationRuleFlags.get(tokenInfo["chatCodeCreationRule"], "NO")
            
            paid = "FULL" if tokenInfo["paid"] else "VELVET_ROPE"
            
            whiteListChat = "YES" if tokenInfo["whitelistChat"] else "NO"
            
            if tokenInfo["userName"] != None:
                userName = tokenInfo["userName"]
                
                accFile = os.path.join("database", userName + ".txt")
                if os.path.isfile(accFile):
                    with open(accFile, "r") as f:
                        self.account = self.databaseServer.loadDatabaseObject(int(f.read()))
                        accountDoId = self.account.doId
                else:
                    # We create an Account
                    self.account = self.databaseServer.createDatabaseObject("Account")
                    accountDoId = self.account.doId
                    with open(accFile, "w") as f:
                        f.write(str(self.account.doId))
                        
            # Get our current time in UTC.
            now = datetime.now()
            #now = now.astimezone(tz=pytz.UTC)
            
            # By default, We say it's existed for 0 days. 
            accountDays = 0
                 
            # If we found the account then we'll do required field checks.
            if self.account:
                 # Check if the account has the creation date.
                 if not self.account.fields.get("CREATED", None):
                    self.account.update("CREATED", now.strftime("%Y-%m-%d %H:%M:%S"))

                 # Update our last login time.
                 self.account.update("LAST_LOGIN", now.strftime("%Y-%m-%d %H:%M:%S"))
                 
                 # Caculate the amount of days since our account was created.
                 
                 # Get our creation time from the stored date string.
                 creation_time = datetime.strptime(self.account.fields.get("CREATED", now.strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S")
                 
                 # Caculate the difference in dates.
                 delta_time = now - creation_time
                 
                 # Get the difference in days, That's how many days our account has been created.
                 accountDays = abs(delta_time.days)
                 
            # If no errors occured and we got our account, Then we authorize this client to use the other messages.
            if returnCode == 0 and self.account: self.__authorized = True

            datagram = Datagram()
            datagram.addInt8(returnCode) # returnCode
            datagram.addString(responseStr) # respString (in case of error)
            datagram.addUint32(accountDoId) # DISL ID
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
            self.sendMessage(CLIENT_LOGIN_TOONTOWN_RESP, datagram)

        elif self.__authorized:
            self.handle_authenticated_datagram(msgType, di)

        else:
            print("Received unexpected/unknown messagetype %d from connection: %s:%d!" % (msgType, self.addr[0], self.addr[1]))
            self.disconnect(220) # Internal error in the clients state machine. Contact Developers for correction.
        
        
    def handle_authenticated_datagram(self, msgType, di):
        if msgType == CLIENT_CREATE_AVATAR:
            # Client wants to create an avatar

            # We read av info
            contextId = di.getUint16()
            dnaString = di.getBlob()
            avPosition = di.getUint8()

            # Is avPosition valid?
            if not 0 <= avPosition < 6:
                print("Client sent an invalid av position")
                return

            # Doesn't it already have an avatar at this slot?
            accountAvSet = self.account.fields["ACCOUNT_AV_SET"]

            if accountAvSet[avPosition]:
                print("Client tried to overwrite an avatar")
                return

            # We can create the avatar
            avatar = self.databaseServer.createDatabaseObject("DistributedToon")
            
            # OwningAccount is the field used internally for figuring out which 
            # local accounts own the Avatar.
            avatar.update("OwningAccount", self.account.doId)
            
            # We currently don't have a way to get a account name from a account,
            # So just leave it as a specialized internal dev one..
            avatar.update("setAccountName", "internal_%s" % str(hex(self.account.doId)))
            
            # DISL likely stood for Disney Internal Server Login. 
            # This server must of had it's own set of accounts which included names
            # and ids seperate from the OTP Server. 
            # Since we have no such server, The fields are unused for us.
            
            # The DISL Name is the name of the Disney XD Account (Global Account).
            # We don't know how their names were stored or looked like.
            avatar.update("setDISLname", "unknown")
            # The DISL Id is the id for a Disney XD Account (Global Account).
            # We don't know how any of these look, So we just use our local account ID for now.
            avatar.update("setDISLid", self.account.doId)
            
            avatar.update("setDNAString", dnaString)
            avatar.update("setPosIndex", avPosition)

            # We save the avatar in the account
            accountAvSet[avPosition] = avatar.doId
            self.account.update("ACCOUNT_AV_SET", accountAvSet)

            # We tell the client their new avId!
            datagram = Datagram()
            datagram.addUint16(contextId)
            datagram.addUint8(0) # returnCode
            datagram.addUint32(avatar.doId)
            self.sendMessage(CLIENT_CREATE_AVATAR_RESP, datagram)


        elif msgType == CLIENT_SET_NAME_PATTERN:
            # Client sets his name
            # We may only allow this if we don't already have a name,
            # or only have a default name.

            # But for now, we don't care. TODO
            if not self.account:
                raise Exception("Client has no account")

            nameIndices = []
            nameFlags = []

            avId = di.getUint32()
            if not avId in self.account.fields["ACCOUNT_AV_SET"]:
                raise Exception("Client sets the name of another Toon")

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

            # Make sure the requested object exists.
            if not self.databaseServer.hasDatabaseObject(avId):
                return

            # We set the toon's name
            avatar = self.databaseServer.loadDatabaseObject(avId)
            avatar.update("setName", name.strip())

            # We tell the client that their new name is accepted
            datagram = Datagram()
            datagram.addUint32(avatar.doId)
            datagram.addUint8(0)
            self.sendMessage(CLIENT_SET_NAME_PATTERN_ANSWER, datagram)


        elif msgType == CLIENT_SET_WISHNAME:
            # Client sets his name
            # We may only allow this if we don't already have a name,
            # or only have a default name.

            # But for now, we don't care. TODO
            if not self.account:
                print("Client has no account")
                return

            avId = di.getUint32()
            if avId and not avId in self.account.fields["ACCOUNT_AV_SET"]:
                print("Client tried to set the name of another Toon!")
                return

            name = di.getString()

            if avId == 0:
                # Client just wants to check the name
                datagram = Datagram()
                datagram.addUint32(0)
                datagram.addUint16(0)
                datagram.addString("")
                datagram.addString(name)
                datagram.addString("")

                self.sendMessage(CLIENT_SET_WISHNAME_RESP, datagram)
                return

            # Make sure the requested object exists.
            if not self.databaseServer.hasDatabaseObject(avId):
                return

            # Client wants to set the name and we're just gonna
            # allow him to.
            avatar = self.databaseServer.loadDatabaseObject(avId)
            avatar.update("setName", name)

            datagram = Datagram()
            datagram.addUint32(avatar.doId)
            datagram.addUint16(0)
            datagram.addString("")
            datagram.addString(name)
            datagram.addString("")

            self.sendMessage(CLIENT_SET_WISHNAME_RESP, datagram)


        elif msgType == CLIENT_DELETE_AVATAR:
            # Client wants to delete one of his avatars.
            # That's sad but let it be.
            avId = di.getUint32()

            # Is that even our avatar?
            accountAvSet = self.account.fields["ACCOUNT_AV_SET"]
            if not avId in accountAvSet:
                raise Exception("Client tries to delete an avatar it doesnt own!")

            # We remove the avatar
            accountAvSet[accountAvSet.index(avId)] = 0
            self.account.update("ACCOUNT_AV_SET", accountAvSet)

            # We tell him it's done and we send him his new av list.
            datagram = Datagram()
            datagram.addUint8(0)
            self.writeAvatarList(datagram)
            self.sendMessage(CLIENT_DELETE_AVATAR_RESP, datagram)

        elif msgType == CLIENT_ADD_INTEREST:
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
                self.updateInterestCache()

                # We gotta disable the objects we can't see anymore,
                if oldParentId == parentId:
                    for do in self.stateServer.objects.values():
                        # If the object is not visible anymore, we disable it
                        # (it's in the removed interest zones, but not in the new interest (or any current interest) zones)
                        if do.parentId == parentId and do.zoneId in oldZones and not (do.zoneId in zones or self.hasInterest(do.parentId, do.zoneId)):
                            dg = Datagram()
                            dg.addUint32(do.doId)
                            self.sendMessage(CLIENT_OBJECT_DISABLE, dg)

                    for do in self.stateServer.dbObjects.values():
                        # If the object is not visible anymore, we disable it
                        # (it's in the removed interest zones, but not in the new interest (or any current interest) zones)
                        if do.parentId == parentId and do.zoneId in oldZones and not (do.zoneId in zones or self.hasInterest(do.parentId, do.zoneId)):
                            dg = Datagram()
                            dg.addUint32(do.doId)
                            self.sendMessage(CLIENT_OBJECT_DISABLE, dg)

                else:
                    # We only check if we're no longer interested in
                    for do in self.stateServer.objects.values():
                        if do.parentId == oldParentId and do.zoneId in oldZones and not self.hasInterest(do.parentId, do.zoneId):
                            dg = Datagram()
                            dg.addUint32(do.doId)
                            self.sendMessage(CLIENT_OBJECT_DISABLE, dg)

                    # We only check if we're no longer interested in
                    for do in self.stateServer.dbObjects.values():
                        if do.parentId == oldParentId and do.zoneId in oldZones and not self.hasInterest(do.parentId, do.zoneId):
                            dg = Datagram()
                            dg.addUint32(do.doId)
                            self.sendMessage(CLIENT_OBJECT_DISABLE, dg)

                    # We set oldZones to an empty tuple
                    # (because we're ignoring them as parentId is difference)
                    oldZones = ()

            # We send the newly visible objects
            newZones = []
            for zoneId in zones:
                if not zoneId in oldZones and not self.hasInterest(parentId, zoneId):
                    newZones.append(zoneId)

            # We have got a new zone list, we can finally send the objects.
            self.sendObjects(parentId, newZones)

            # We save the interest
            self.interests[handle] = (parentId, zones)
            self.updateInterestCache()

            # We tell the client we're done
            dg = Datagram()
            dg.addUint16(handle)
            dg.addUint32(contextId)
            self.sendMessage(CLIENT_DONE_INTEREST_RESP, dg)


        elif msgType == CLIENT_REMOVE_INTEREST:
            # Client wants to remove an interest
            handle = di.getUint16()
            contextId = di.getUint32() # Might be optional

            # Did the interest exist?
            if not handle in self.interests:
                print("Client tried to remove an unexisting interest")
                return

            # We get what the interest was
            oldParentId, oldZones = self.interests[handle]

            # We remove the interest
            del self.interests[handle]
            self.updateInterestCache()

            # We disable all the objects we're no longer interested in
            for do in self.stateServer.objects.values():
                if do.parentId == oldParentId and do.zoneId in oldZones and not self.hasInterest(do.parentId, do.zoneId):
                    dg = Datagram()
                    dg.addUint32(do.doId)
                    self.sendMessage(CLIENT_OBJECT_DISABLE, dg)

            for do in self.stateServer.dbObjects.values():
                if do.parentId == oldParentId and do.zoneId in oldZones and not self.hasInterest(do.parentId, do.zoneId):
                    dg = Datagram()
                    dg.addUint32(do.doId)
                    self.sendMessage(CLIENT_OBJECT_DISABLE, dg)

            # We tell the client we're done
            dg = Datagram()
            dg.addUint16(handle)
            dg.addUint32(contextId)
            self.sendMessage(CLIENT_DONE_INTEREST_RESP, dg)

        elif msgType == CLIENT_GET_AVATARS:
            # Client asks us their avatars.
            if not self.account:
                # TODO Should we boot the client out or just set a bad returnCode?
                # For now we'll throw an exception as this should never happen.
                print("Client asked avatars with no account")
                return

            dg = Datagram()
            dg.addUint8(0) # returnCode
            self.writeAvatarList(dg)
            self.sendMessage(CLIENT_GET_AVATARS_RESP, dg)

        elif msgType == CLIENT_SET_AVATAR:
            # Client picked an avatar.
            # If avId is 0, it disconnected.
            avId = di.getUint32()

            self.handleSetAvatar(avId)

        elif msgType == CLIENT_OBJECT_UPDATE_FIELD:
            # Client wants to update a do object
            doId = di.getUint32()
            fieldId = di.getUint16()

            dg = Datagram()
            dg.addUint32(doId)
            dg.addUint16(fieldId)
            dg.appendData(di.getRemainingBytes())

            # Can we send this field? If not just return.
            if not doId in self.stateServer.objects and not doId in self.stateServer.dbObjects:
                print("Avatar %d attempted to update a field %d but doId %d was not found" % (self.avatarId, fieldId, doId))
                return

            if not doId in self.stateServer.dbObjects:
                do = self.stateServer.objects[doId]
            else:
                do = self.stateServer.dbObjects[doId]

            field = do.dclass.getFieldByIndex(fieldId)
            if not field:
                print("Avatar %d attempted to update a field but it was not found!" % (self.avatarId))
                return

            if (doId in self.__doId2ClsendOverrides and not fieldId in self.__doId2ClsendOverrides[doId]) and \
               not (field.isClsend() or (field.isOwnsend() and do.doId == self.avatarId)): # We probably should check for owner stuff too but Toontown does not implement it
                print("Avatar %d attempted to update a field but they don't have the rights!" % (self.avatarId))
                return

            # Ignore DistributedNode and DistributedSmoothNode fields for debugging
            if field.getName() not in ("setX", "setY", "setZ", "setH", "setP", "setR", "setPos", "setHpr", "setPosHpr", "setXY", "setXZ", "setXYH", "setXYZH",
                                       "setComponentL", "setComponentX", "setComponentY", "setComponentZ", "setComponentH", "setComponentP", "setComponentR", "setComponentT",
                                       "setSmStop", "setSmH", "setSmZ", "setSmXY", "setSmXZ", "setSmPos", "setSmHpr", "setSmXYZH", "setSmPosHpr", "setSmPosHprL",
                                       "clearSmoothing", "suggestResync", "returnResync"):

                print("Avatar %d updates %d (dclass %s) field %s" % (self.avatarId, do.doId, do.dclass.getName(), field.getName()))
                
            if doId in self.__doId2ClsendOverrides and fieldId in self.__doId2ClsendOverrides[doId]:
                print("Avatar %d updates %d (dclass %s) with clsend overriden field %s" % (self.avatarId, do.doId, do.dclass.getName(), field.getName()))


            if doId == self.avatarId and fieldId == self.agent.setTalkFieldId:
                # Weird case: it's broadcasting and the others can see the chat, but the client
                # does not receive it has he sent it.

                # We will change the sender to 4681 (Chat Manager) to bypass this problem
                self.messageDirector.sendMessage([doId], 4681, STATESERVER_OBJECT_UPDATE_FIELD, dg)

            else:
                # We just send the update to the StateServer.
                self.messageDirector.sendMessage([doId], self.avatarId, STATESERVER_OBJECT_UPDATE_FIELD, dg)


        elif msgType == CLIENT_OBJECT_LOCATION:
            # Client wants to move an object
            doId = di.getUint32()
            parentId = di.getUint32()
            zoneId = di.getUint32()

            # Can we move it?
            if doId != self.avatarId:
                print("Client wants to move an object it doesn't own")
                return

            # We tell the StateServer that we're moving an object.
            dg = Datagram()
            dg.addUint32(parentId)
            dg.addUint32(zoneId)
            self.messageDirector.sendMessage([doId], self.avatarId, STATESERVER_OBJECT_SET_ZONE, dg)

        elif msgType == CLIENT_REMOVE_FRIEND:
            # Friend to remove
            doId = di.getUint32()

            # Check if the target's database object exists.
            if self.databaseServer.hasDatabaseObject(doId):
                target = self.databaseServer.loadDatabaseObject(doId)

                # Make sure the friends list field exists.
                if "setFriendsList" in target.fields:
                    friendsList = target.fields["setFriendsList"][0]

                    for i in range(0, len(friendsList)):
                        if friendsList[i][0] == self.avatarId:
                            # Make sure we delete it.
                            del target.fields["setFriendsList"][0][i]
                            break
                        # If we aren't ever found. We weren't on their list to begin with.

                # Save the removal to the database.
                self.databaseServer.saveDatabaseObject(target)

            # Check if our database object exists.
            if self.databaseServer.hasDatabaseObject(self.avatarId):
                avatar = self.databaseServer.loadDatabaseObject(self.avatarId)

                # Make sure the friends list field exists.
                if "setFriendsList" in avatar.fields:
                    friendsList = avatar.fields["setFriendsList"][0]

                    for i in range(0, len(friendsList)):
                        if friendsList[i][0] == doId:
                            # Make sure we delete it.
                            del avatar.fields["setFriendsList"][0][i]
                            break
                        # If they aren't ever found. They weren't ever on our list to begin with.

                # Save the removal to the database.
                self.databaseServer.saveDatabaseObject(avatar)

        elif msgType in (CLIENT_GET_FRIEND_LIST, CLIENT_GET_FRIEND_LIST_EXTENDED):
            # We support both types of getting the friends list here.
            if msgType == CLIENT_GET_FRIEND_LIST:
                sendId = CLIENT_GET_FRIEND_LIST_RESP
            elif msgType == CLIENT_GET_FRIEND_LIST_EXTENDED:
                sendId = CLIENT_GET_FRIEND_LIST_EXTENDED_RESP

            # If we don't have a chosen response. Just don't respond at all.
            # There's no point in humoring them.
            if self.avatarId == 0:
                return

            # If our OWN database object doesn't exist... Perhaps we have bigger issues..
            if not self.databaseServer.hasDatabaseObject(self.avatarId):
                return

            fields = self.databaseServer.loadDatabaseObject(self.avatarId).fields

            if not "setFriendsList" in fields:
                dg = Datagram()
                dg.addUint8(1) # 1 - Field does not exist, Therefore they have no friends.
                self.sendMessage(sendId, dg)
                return

            friendsList = fields["setFriendsList"][0]

            count = 0
            friendData = {}
            for i in range(0, len(friendsList)):
                friendId = friendsList[i][0]

                # Make sure our friend actually has a database object!
                # If it doesn't, Skip over it and emit a warning.
                if not self.databaseServer.hasDatabaseObject(friendId):
                    print("Friend %d for Avatar %d doesn't have a database object!" % (friendId, self.avatarId))
                    continue

                # Load our fields from the friend in question.
                friendsFields = self.databaseServer.loadDatabaseObject(friendId).fields

                # We're missing a required field, And this version of getting the list doesn't sanity check these
                # individually.
                # We only run this check for the non-extended friends list type.
                if msgType == CLIENT_GET_FRIEND_LIST and (not 'setName' in friendsFields or not 'setDNAString' in friendsFields):
                    print("Friend %d for Avatar %d is missing a field in the database!" % (friendId, self.avatarId))
                    continue

                # If we don't have a name, We default to an empty string.
                name = ''
                if 'setName' in friendsFields:
                    name = friendsFields['setName'][0]

                # If we don't have a dna string, We default to an empty byte string.
                dnaString = b''
                if 'setDNAString' in friendsFields:
                    dnaString = friendsFields['setDNAString'][0]

                # It doesn't matter if there's a pet or not,
                # If the field isn't present, We default to 0.
                petId = 0
                if 'setPetId' in friendsFields:
                    petId = friendsFields['setPetId'][0]

                friendData[count] = (friendId, name, dnaString, petId)
                count += 1

            # Create our working datagram.
            dg = Datagram()

            # We've got the data already, So add the flag of success.
            dg.addUint8(0)

            # Add the amount of friends we're sending over.
            dg.addUint16(len(friendData))

            # Add all of the data in the list we collected.
            for i in friendData:
                data = friendData[i]
                dg.addUint32(data[0]) # - doId
                dg.addString(data[1]) # - name
                dg.addString(data[2].decode('utf-8')) # - dna string
                dg.addUint32(data[3]) # - pet id

            self.sendMessage(sendId, dg)

        elif msgType in (CLIENT_GET_AVATAR_DETAILS, CLIENT_GET_PET_DETAILS):
            # Client wants to get information on a object.
            # Object could either be a Pet or another Toon.
            if msgType == CLIENT_GET_AVATAR_DETAILS:
                # Details about a Toon are being requested.
                dclassName = 'DistributedToon'
                sendId = CLIENT_GET_AVATAR_DETAILS_RESP
            elif msgType == CLIENT_GET_PET_DETAILS:
                # Details about a Pet are being requested.
                dclassName = 'DistributedPet'
                sendId = CLIENT_GET_PET_DETAILS_RESP

            # The indentifier of the object.
            doId = di.getUint32()

            # Get the dclass object by name.
            dclass = self.databaseServer.dc.getClassByName(dclassName)

            # Make sure the requested object exists.
            if not self.databaseServer.hasDatabaseObject(doId):
                return

            # Grab the fields from the object via the database.
            fields = self.databaseServer.loadDatabaseObject(doId).fields

            # Pack our data to go to the client.
            packedData = self.packDetails(dclass, fields)

            # Prepare the client response.
            dg = Datagram()
            dg.addUint32(doId)
            dg.addUint8(0)
            dg.appendData(packedData)

            # Tell the client about the response.
            self.sendMessage(sendId, dg)

        elif msgType == CLIENT_GET_FRIEND_LIST:
            dg = Datagram()
            dg.addUint8(0)
            dg.addUint16(0)
            self.sendMessage(CLIENT_GET_FRIEND_LIST_RESP, dg)

        else:
            print("Received unknown message: %d" % msgType)
            self.disconnect(200) # Internal error in the client’s state machine. Contact Developers for correction.

        #else:
            #raise NotImplementedError(msgType)

        #if di.getRemainingSize():
            #raise Exception("remaining", di.getRemainingBytes())

    def packDetails(self, dclass, fields):
        # Pack required fields.
        fieldPacker = DCPacker()
        for i in range(dclass.getNumInheritedFields()):
            field = dclass.getInheritedField(i)
            if not field.isRequired() or field.asMolecularField():
                continue

            k = field.getName()
            v = fields.get(k, None)

            fieldPacker.beginPack(field)
            if not v:
                fieldPacker.packDefaultValue()
            else:
                field.packArgs(fieldPacker, v)

            fieldPacker.endPack()

        return fieldPacker.getBytes()
        
    def parse_play_token(self, playToken, tokenType):
        def get_response(returnCode, respString):
            response = {"returnCode": returnCode,
                        "respString": respString,
                        "accountName": None,
                        "accountNameApproved": 0,
                        "accountNumber": None,
                        "userName": None,
                        "swid": None,
                        "familyNumber": -1,
                        "familyAdmin": 1,
                        "openChatEnabled": 0,
                        "createFriendsWithChat": 0,
                        "chatCodeCreationRule": 0,
                        "familyMembers": None,
                        "deployment": "",
                        "whitelistChat": 1,
                        "paid": 0,
                        "hasParentAccount": 0,
                        "toontownGameKey": None,
                        "toonAccountType": 0,
                       }

            return response
        
        if tokenType == CLIENT_LOGIN_2_GREEN:
            print("CLIENT_LOGIN_2_GREEN is not yet a supported token type!")
            self.disconnect(106) # The field indicating what type of token we are processing is invalid.
            return get_response(5, "Unsupported playtoken type.")
        elif tokenType == CLIENT_LOGIN_2_BLUE:
            print("CLIENT_LOGIN_2_BLUE is not yet a supported token type!")
            self.disconnect(106) # The field indicating what type of token we are processing is invalid.
            return get_response(5, "Unsupported playtoken type.")
        # SSL Encoded Token, The main token type used for deployment and devs.
        elif tokenType == CLIENT_LOGIN_3_DISL_TOKEN or tokenType == CLIENT_LOGIN_2_PLAY_TOKEN:
            # Check if the token is encrypted, If not we only accept plain tokens on a dev enviorment.
            encrypted = False
            try:
                base64.b64decode(playToken, validate=True)
                encrypted = True
            except:
                pass
                
            if not encrypted and not __debug__:
                print("Rejecting plaintext token on non-development OTP Server.")
                self.disconnect(123) # The client agent is in a mode that disallows this type of login.
                return get_response(3, "Ill-formated playtoken.")
                
            # Pre-decrypt our play token.
            try:
                playToken = des3_cbc_decrypt(playToken, b"kvm5SAE7sAq9csdPA8UPZRe7") if encrypted else playToken
            except Exception as e:
                traceback.print_exc()
                self.disconnect(122) # Error decrypting OpenSSl token in CLIENT_LOGIN_2.
                return get_response(3, "Ill-formated playtoken.")
                
            print(playToken)
            
            # If we don't find this paramater, It's a old style token. Which are depercated. 
            if playToken.find(b"TOONTOWN_GAME_KEY") >= 0:
                return self.parse_DISL_play_token(playToken)
                
            # The token is the old style token.
            return self.parse_DISL_play_token_old(playToken)
        else:
            print("Got unknown token type '%s' for playToken!" % (str(tokenType)))
            self.disconnect(106) # The field indicating what type of token we are processing is invalid.
            return get_response(5, "Unsupported playtoken type.")

    def parse_DISL_play_token(self, playToken):
        response = {"returnCode": 0,
                    "respString": "",
                    "accountName": None,
                    "accountNameApproved": 0,
                    "accountNumber": None,
                    "userName": None,
                    "swid": None,
                    "familyNumber": -1,
                    "familyAdmin": 1,
                    "openChatEnabled": 0,
                    "createFriendsWithChat": 0,
                    "chatCodeCreationRule": 0,
                    "familyMembers": None,
                    "deployment": "",
                    "whitelistChat": 1,
                    "paid": 0,
                    "hasParentAccount": 0,
                    "toontownGameKey": None,
                    "toonAccountType": 0,
                  }
                   
        # If we can't find this parameter, The token is invalid.
        if playToken.find(b"TOONTOWN_GAME_KEY") < 0:
            print("Failed to parse play token, Format is invalid!")
            response["returnCode"] = 3
            response["respString"] = "Ill-formated playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response
            
        try:
            playToken = playToken.decode("utf-8")
        except:
            print("Failed to parse play token, Format is invalid!")
            response["returnCode"] = 3
            response["respString"] = "Ill-formated playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response
            
        # Split the token into it's variables.
        variableLines = playToken.split("&")
        
        # Parse the variables.
        variables = {}
        for varLine in variableLines:
            try:
                name, value = varLine.split('=', 1)
            except ValueError as e:
                continue

            variables[name] = value
        
        # Get our account name from the play token.
        account_name = variables.get("ACCOUNT_NAME", None)
        # If we couldn't get our account name, The token is invalid.
        if not account_name:
            print("Couldn't find required field 'ACCOUNT_NAME' for playToken!")
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["accountName"] = account_name
        
        # Get our account name apporval from the play token.
        accountNumber = variables.get("ACCOUNT_NUMBER", None)
        # If we got our account number, Set it in our response.
        if accountNumber:
            # Set the required response info.
            response["accountNumber"] = int(accountNumber)
            
        # Get our account name apporval from the play token.
        userName = variables.get("GAME_USERNAME", None)
        # If we got our username, Set it in our response.
        if userName:
            # Set the required response info.
            response["userName"] = userName
        
        # Get our SWID from the play token.
        swid = variables.get("SWID", None)
        # If we got our SWID, Set it in our response.
        if swid:
            # Set the required response info.
            response["swid"] = swid
            
        # Check if the token is valid! (I'm not sure why a valid field exists..? Is it dynamically changed originally?)
        valid = variables.get("valid", None)
        # If we couldn't get if our token is valid or not, The token is of course. Invalid.
        if not valid:
            print("Couldn't find required field 'valid' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response
            
        # Get our valid bool, If we fail to with an error. It's a automatic rejection.
        try:
            valid = bool(valid)
        except:
            print("Couldn't parse required field 'valid' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response
            
        # If the token isn't valid... Well reject login.
        if not valid:
            print("PlayToken for '%s' is invalid! Rejecting login." % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response
        
        # Get our expirey date and check if the token is already expired.
        expireTime = variables.get("expires", None)
        # If we have an expirey time, Check for if our token is expired.
        if expireTime:
            # Calcuate our local time to check the token for when it expires.
            # To do so, get our local time and convert it to UTC.
            now = datetime.now()
            now = now.astimezone(tz=pytz.UTC)
            
            # Sanity check our expire time.
            try:
                expireTime = int(expireTime)
            except:
                print("Token has invalid expire time '%s'! Rejecting the token for '%s'!" % (str(expireTime), response["accountName"]))
                response["returnCode"] = 1
                response["respString"] = "Invalid playtoken."
                self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
                return response
            
            # If our time is lower then 0. The time is invalid.
            if expireTime < 0:
                print("Token has invalid expire time '%s'! Rejecting the token for '%s'!" % (str(expireTime), response["accountName"]))
                response["returnCode"] = 1
                response["respString"] = "Invalid playtoken."
                self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
                return response
        
            # Convert the expirey string to a datetime.
            expire_now = datetime.fromtimestamp(expireTime)
            expire_now = expire_now.replace(tzinfo=pytz.UTC)
            # Make sure the token isn't expired. If it is, Reject the token.
            if expire_now <= now:
                print("Token expired on '%s'! Rejecting the token for '%s'!" % (expire_now.strftime("%a, %d %b %Y %H:%M:%S GMT"), response["accountName"]))
                response["returnCode"] = 1
                response["respString"] = "Invalid playtoken."
                self.disconnect(105) # The expiration time on this play token has passed.
                return response

            print("Token for '%s' accepted on %s, Token expires on %s." % (response["accountName"], now.strftime("%a, %d %b %Y %H:%M:%S GMT"), expire_now.strftime("%a, %d %b %Y %H:%M:%S GMT")))
        elif __debug__:
            print("Token for '%s' accepted on %s, Token doesn't ever expire." % (response["accountName"], now.strftime("%a, %d %b %Y %H:%M:%S GMT")))
        else:
            print("Couldn't find required field 'expires' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Get our account name apporval from the play token.
        account_name_apporval = variables.get("ACCOUNT_NAME_APPROVAL", None)
        # If we couldn't get our account name apporval, The token is invalid.
        if not account_name_apporval:
            print("Couldn't find required field 'ACCOUNT_NAME_APPROVAL' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["accountNameApproved"] = account_name_apporval == "YES"
            
        # Get our family number from the play token.
        familyNumber = variables.get("FAMILY_NUMBER", None)
        # If we couldn't get our family number, The token is invalid.
        if not familyNumber:
            print("Couldn't find required field 'FAMILY_NUMBER' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["familyNumber"] = int(familyNumber)
        
        # Get our family admin status from the play token.
        familyAdmin = variables.get("familyAdmin", None)
        # If we couldn't get our family admin status, The token is invalid.
        if not familyAdmin:
            print("Couldn't find required field 'familyAdmin' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["familyAdmin"] = int(familyAdmin)
        
        # Get if open chat is enabled from the play token.
        openChatEnabled = variables.get("OPEN_CHAT_ENABLED", None)
        # If we couldn't if open chat is enabled, The token is invalid.
        if not openChatEnabled:
            print("Couldn't find required field 'OPEN_CHAT_ENABLED' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["openChatEnabled"] = True if openChatEnabled == "YES" else False
        
        # Get if we can use secret codes from the play token.
        createFriendsWithChatFlags = {"NO": 0, "CODE": 1, "YES": 2} # Result to index.
        createFriendsWithChat = variables.get("CREATE_FRIENDS_WITH_CHAT", None)
        # If we couldn't find that we can use secret codes or not, The token is invalid.
        if not createFriendsWithChat:
            print("Couldn't find required field 'CREATE_FRIENDS_WITH_CHAT' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["createFriendsWithChat"] = createFriendsWithChatFlags.get(createFriendsWithChat, 0)
        
        # Get our creation rule for secret codes from the play token.
        chatCodeCreationRuleFlags = {"NO": 0, "PARENT": 1, "YES": 2} # Result to index.
        chatCodeCreationRule = variables.get("CHAT_CODE_CREATION_RULE", None)
        # If we couldn't get creation rule for secret codes, The token is invalid.
        if not chatCodeCreationRule:
            print("Couldn't find required field 'CHAT_CODE_CREATION_RULE' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["chatCodeCreationRule"] = chatCodeCreationRuleFlags.get(chatCodeCreationRule, 0)
        
        # Get if whitelist chat is enabled from the play token.
        whitelistChat = variables.get("WL_CHAT_ENABLED", None)
        # If we got our whitelist chat flag, Set it in our response.
        if whitelistChat:
            # Set the required response info.
            response["whitelistChat"] = True if whitelistChat == "YES" else False
        
        # Toontown Specfic Variables.
        
        # Get our paid content access level from the play token.
        toontown_access = variables.get("TOONTOWN_ACCESS", None)
        # If we got our paid access level, Set it in our play token.
        if toontown_access:
            # Set if our account is paid or not.
            response["paid"] = True if toontown_access == "FULL" else False
            
        # Get our game key from the play token.
        toontownGameKey = variables.get("TOONTOWN_GAME_KEY", None)
        # If we couldn't get our game key, The token is invalid.
        if not toontownGameKey:
            print("Couldn't find required field 'TOONTOWN_GAME_KEY' for playToken!")
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["toontownGameKey"] = toontownGameKey
        
        # Get our toontown account type from the play token.
        toonAccountTypeFlags = {"NO_PARENT_ACCOUNT": 0, "WITH_PARENT_ACCOUNT": 1} # Result to index.
        toonAccountType = variables.get("TOON_ACCOUNT_TYPE", None)
        # If we got a toon account type, Add it to our response.
        if not toonAccountType:
            # Set the required response info.
            response["toonAccountType"] = toonAccountTypeFlags.get(toonAccountType, 0)
        
        return response
                   
    def parse_DISL_play_token_old(self, playToken):
        response = {"returnCode": 0,
                    "respString": "",
                    "accountName": None,
                    "accountNameApproved": 0,
                    "accountNumber": None,
                    "userName": None,
                    "swid": None,
                    "familyNumber": -1,
                    "familyAdmin": 1,
                    "openChatEnabled": 0,
                    "createFriendsWithChat": 0,
                    "chatCodeCreationRule": 0,
                    "familyMembers": None,
                    "deployment": "",
                    "whitelistChat": 1,
                    "paid": 0,
                    "hasParentAccount": 0,
                    "toontownGameKey": None,
                    "toonAccountType": 0,
                   }

        # These arguments are simply always true with this token.
        # We have no way to know if they're true or not as of yet.
        response["createFriendsWithChat"] = 1
        response["chatCodeCreationRule"] = 1

        # If we can't find the header, The token is invalid.
        if playToken.find(b"PlayToken") < 0:
            print("Failed to parse old play token, Format is invalid!")
            response["returnCode"] = 3
            response["respString"] = "Ill-formated playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Remove the header.
        playToken = playToken[10:]

        # Split the token into it's variables.
        variableLines = playToken.split(b"\" ")

        # Parse the variables.
        variables = {}
        for varLine in variableLines:
            try:
                name, value = varLine.split(b'=', 1)
                value = value[1:]
            except ValueError as e:
                continue

            variables[name] = value

        # Print our variables.
        #print(variables)

        # Get our account name from the play token.
        name = variables.get(b"name", None)
        # If we couldn't get our account name, The token is invalid.
        if not name:
            print("Couldn't find required field 'name' for playToken!")
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["accountName"] = name.decode("utf-8")
        response["accountNameApproved"] = 1
        response["userName"] = name.decode("utf-8")

        # Calcuate our local time to check the token for when it expires.
        # To do so, get our local time and convert it to UTC.
        now = datetime.now()
        now = now.astimezone(tz=pytz.UTC)

        # Get our expirey date and check if the token is already expired.
        token_time = variables.get(b"expires", None)
        # If we couldn't get our expirey string, The token is invalid.
        if not token_time:
            print("Couldn't find required field 'expires' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Convert the expirey string to a datetime.
        token_now = datetime.strptime(token_time.decode("utf-8"), "%a, %d %b %Y %H:%M:%S GMT")
        token_now = token_now.replace(tzinfo=pytz.UTC)
        # Make sure the token isn't expired. If it is, Reject the token.
        if token_now <= now:
            print("Token expired on '%s'! Rejecting the token for '%s'!" % (token_now.strftime("%a, %d %b %Y %H:%M:%S GMT"), response["accountName"]))
            response["returnCode"] = 1
            response["respString"] = "Invalid playtoken."
            self.disconnect(105) # The expiration time on this play token has passed.
            return response

        print("Token accepted on %s, Token expires on %s." % (now.strftime("%a, %d %b %Y %H:%M:%S GMT"), token_now.strftime("%a, %d %b %Y %H:%M:%S GMT")))

        paid_str = variables.get(b"paid", None)
        # If we couldn't get our paid string, The token is invalid.
        if not paid_str:
            print("Couldn't find required field 'paid' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set if our account is paid or not.
        response["paid"] = bool(paid_str)

        chat_str = variables.get(b"chat", None)
        # If we couldn't get our chat string, The token is invalid.
        if not chat_str:
            print("Couldn't find required field 'chat' in playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set if our account has open chat or not.
        response["chat"] = bool(chat_str)

        # TODO: Fix this.
        '''
        # Get our deployment from the play token.
        deployment = variables.get(b"Deployment", None)
        # If we couldn't get our deployment, The token is invalid.
        if not deployment:
            print("Couldn't find required field 'Deployment' for playToken for '%s'!" % (response["accountName"]))
            response["returnCode"] = 2
            response["respString"] = "Invalid playtoken."
            self.disconnect(103) # There was an error parsing the OpenSSl token for the required fields.
            return response

        # Set the required response info.
        response["deployment"] = deployment.decode("utf-8")
        '''

        return response
        

    def writeAvatarList(self, dg):
        """
        Add client avatar list to a datagram
        """
        accountAvSet = self.account.fields["ACCOUNT_AV_SET"]

        # Avatar count
        dg.addUint16(sum(n != 0 for n in accountAvSet)) # avatarTotal

        # We send every avatar
        for pos, avId in enumerate(self.account.fields["ACCOUNT_AV_SET"]):
            if avId == 0:
                continue

            avatar = self.databaseServer.loadDatabaseObject(avId)

            dg.addUint32(avatar.doId) # avNum
            dg.addString(avatar.fields["setName"][0])
            dg.addString("")
            dg.addString("")
            dg.addString("")
            dg.addBlob(avatar.fields["setDNAString"][0])
            dg.addUint8(pos)
            dg.addUint8(0)

    def sendMessage(self, code, datagram):
        """
        Send a message
        """
        dg = Datagram()
        dg.addUint16(code)
        dg.appendData(datagram.getMessage())
        self.sendDatagram(dg)


    def sendDatagram(self, dg):
        """
        Send a datagram
        """
        try:
            self.sock.send(struct.pack("<H", dg.getLength()))
            self.sock.send(bytes(dg))
        except:
            print("Tried to send connection to client, But connection was closed!")


    def hasInterest(self, parentId, zoneId):
        """
        Check if we're interested in a zone
        """
        # Do we have this intereste cached?
        return (parentId, zoneId) in self.__interestCache


    def updateInterestCache(self):
        self.__interestCache.clear()

        for handle in self.interests:
            parentId, zones = self.interests[handle]

            for zoneId in zones:
                self.__interestCache.add((parentId, zoneId))

        return False

    def setClsendFields(self, doId, fields):
        self.__doId2ClsendOverrides[doId] = fields

    def sendObjects(self, parentId, zones):
        objects = []
        for do in self.stateServer.objects.values():
            # We're not sending our own object because
            # we already know who we are (we are the owner)
            if do.doId == self.avatarId:
                continue

            # If the object is in one of the new interest zones, we get it
            if do.parentId == parentId and do.zoneId in zones:
                objects.append(do)

        for do in self.stateServer.dbObjects.values():
            # We're not sending our own object because
            # we already know who we are (we are the owner)
            if do.doId == self.avatarId:
                continue

            # If the object is in one of the new interest zones, we get it
            if do.parentId == parentId and do.zoneId in zones:
                objects.append(do)

        # We sort them by dclass (fix some issues)
        objects.sort(key = lambda x: x.dclass.getNumber())

        # We send every object
        for do in objects:
            dg = Datagram()
            dg.addUint32(do.parentId)
            dg.addUint32(do.zoneId)
            dg.addUint16(do.dclass.getNumber())
            dg.addUint32(do.doId)
            do.packRequiredBroadcast(dg)
            do.packOther(dg)
            self.sendMessage(CLIENT_CREATE_OBJECT_REQUIRED_OTHER, dg)

    def handleSetAvatar(self, avId):
        # If avId is 0, That means it's a request to remove our avatar.
        if not avId:
            self.removeAvatar()
            return

        # If we already have a avatar, Remove it.
        if self.avatarId:
            self.removeAvatar()

        self.setAvatar(avId)

    def setAvatar(self, avId):
        """
        Choose an avatar
        """
        if not avId in self.account.fields["ACCOUNT_AV_SET"]:
            print("Client tried to pick an avatar it doesn't own.")
            return

        # We load the avatar from the database
        avatar = self.databaseServer.loadDatabaseObject(avId)
        
        # This for legavy sipport.
        if not "OwningAccount" in avatar.fields:
            avatar.update("OwningAccount", self.account.doId)

        # We ask STATESERVER to create our object
        dg = Datagram()
        dg.addUint32(0)
        dg.addUint32(0)
        dg.addUint16(avatar.dclass.getNumber())
        dg.addUint32(avatar.doId)
        avatar.packRequired(dg)
        avatar.packOther(dg)
        self.messageDirector.sendMessage([20100000], avatar.doId, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER, dg)

        # We probably should wait for an answer, but we're not threaded and everything is happening on the same script
        # (tl;dr it's blocking), so we won't.

        # We remember who we are
        self.avatarId = avatar.doId

        # We can send that we are the proud owner of a DistributedToon!
        dg = Datagram()
        dg.addUint32(avatar.doId)
        dg.addUint8(0)
        avatar.packRequired(dg)
        self.sendMessage(CLIENT_GET_AVATAR_DETAILS_RESP, dg)

        # If we have friends... We should probably let them know we're online!
        if "setFriendsList" in avatar.fields:
            friendsList = avatar.fields["setFriendsList"][0]

            # Get all of our friend ids.
            friendIds = []
            for i in range(0, len(friendsList)):
                friendIds.append(friendsList[i][0])

            for client in self.agent.clients:
                # If the id matches, It means this friend is online!
                if client.avatarId in friendIds:
                    dg = Datagram()
                    dg.addUint32(self.avatarId)
                    client.sendMessage(CLIENT_FRIEND_ONLINE, dg)

    def removeAvatar(self):
        """
        Remove an avatar
        """
        if not self.avatarId:
            print("Client tried to remove his avatar but they don't have one!")
            return

        # We load the avatar from the database
        avatar = self.databaseServer.loadDatabaseObject(self.avatarId)

        # If we have friends... We should probably let them know we're heading off.
        if "setFriendsList" in avatar.fields:
            friendsList = avatar.fields["setFriendsList"][0]

            # Get all of our friend ids.
            friendIds = []
            for i in range(0, len(friendsList)):
                friendIds.append(friendsList[i][0])

            for client in self.agent.clients:
                # If the id matches, It means this friend is now offline!
                if client.avatarId in friendIds:
                    dg = Datagram()
                    dg.addUint32(self.avatarId)
                    client.sendMessage(CLIENT_FRIEND_OFFLINE, dg)

        # We ask State Server to delete our object
        dg = Datagram()
        dg.addUint32(self.avatarId)
        self.messageDirector.sendMessage([self.avatarId], self.avatarId, STATESERVER_OBJECT_DELETE_RAM, dg)
        self.avatarId = 0
