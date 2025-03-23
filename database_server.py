from panda3d.core import ConfigVariableString, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem
from panda3d.direct import DCPacker

from otp.ai.AIMsgTypes import AIMsgName2Id
from toontown.ai.ToontownAIMsgTypes import TTAIMsgName2Id

import hashlib, os, sys, uuid, string, random
from datetime import datetime, timedelta

try:
    # Try to use simplejson if we can, Otherwise just use normal json.
    import simplejson as json
except:
    import json

from database_manager import DatabaseManager
from database_object import DatabaseObject
from distributed_object import DistributedObject

class DatabaseServer:
    def __init__(self, otp):
        # Main OTP
        self.otp = otp
        
        # DC File
        self.dc = self.otp.dc
        
        # Quick access for CA and MD 
        self.clientAgent = self.otp.clientAgent
        self.messageDirector = self.otp.messageDirector
        self.stateServer = self.otp.stateServer
        
        # Dictionaries containing info relating to all of our DC Objects with the DcObjectType field.
        self.dcObjectTypes = {}
        self.dcObjectTypeFromName = {}
        self.caculateDCObjects()
        
        # Get our Panda3D Virtual File System, And keep a reference.
        self.vfs = VirtualFileSystem.getGlobalPtr()
        
        # Create our Database Manager. 
        self.manager = DatabaseManager(self)
        
        self.rngSeed = None
        self.secretFriendCodes = {}
        
        self.databaseDirectory = os.path.normpath(os.path.expandvars(ConfigVariableString('database-directory', "database").getValue()))
        
    def caculateDCObjects(self):
        dcObjectCount = 0
        
        # Fist let's check all classes at their base and store them.
        # We don't want classes which inherited to have a different number then
        # it's base class. So we parse child classes to their parents after.
        for i in range(0, self.dc.getNumClasses()):
            dcClass = self.dc.getClass(i)
            for j in range(0, dcClass.getNumFields()):
                field = dcClass.getField(j)
                if field.getName() == "DcObjectType":
                    dcObjectCount += 1
                    self.dcObjectTypes[dcObjectCount] = dcClass
                    self.dcObjectTypeFromName[dcClass.getName()] = dcObjectCount
                    
        
        def isInheritedDcObjectClass(dcClass):
            """
            This function is will iterate the parents of a dc class
            and return if the dc class inherits a dc class in our dc object types.
            """
            isDcObject = False
            for j in range(0, dcClass.getNumParents()):
                dcClassParent = dcClass.getParent(j)
                isDcObject = dcClassParent.getName() in self.dcObjectTypeFromName
                if not isDcObject and dcClassParent.getNumParents() > 0: # Check the parent' parents for if we are one too.
                    isDcObject = isInheritedDcObjectClass(dcClassParent)
                
                if not isDcObject: # Don't even bother if we aren't one.
                    continue
                    
            return isDcObject
                   
        # Now we just iterate the dc classes for if one inherits from one 
        # of our confirmed dc classes to have a dc object type.
        for i in range(0, self.dc.getNumClasses()):
            dcClass = self.dc.getClass(i)
            isDcObject = isInheritedDcObjectClass(dcClass)
            if not isDcObject:
                continue
            
            dcObjectCount += 1
            self.dcObjectTypes[dcObjectCount] = dcClass
            self.dcObjectTypeFromName[dcClass.getName()] = dcObjectCount
            
    def handle(self, channels, sender, code, datagram):
        """
        Handle a message
        """
        for channel in channels:
            if channel == 4003:
                if code == AIMsgName2Id["DBSERVER_GET_STORED_VALUES"]:
                    self.getStoredValues(sender, datagram)
                    
                elif code == AIMsgName2Id["DBSERVER_SET_STORED_VALUES"]:
                    self.setStoredValues(sender, datagram)
                    
                elif code == AIMsgName2Id["DBSERVER_CREATE_STORED_OBJECT"]:
                    self.createStoredObject(sender, datagram)
                    
                elif code == AIMsgName2Id["DBSERVER_DELETE_STORED_OBJECT"]:
                    print("DBSERVER_DELETE_STORED_OBJECT")
                    
                elif code == TTAIMsgName2Id["DBSERVER_GET_ESTATE"]:
                    self.getEstate(sender, datagram)
                    
                elif code == AIMsgName2Id["DBSERVER_MAKE_FRIENDS"]:
                    self.makeFriends(sender, datagram)
                    
                elif code == AIMsgName2Id["DBSERVER_REQUEST_SECRET"]:
                    print("DBSERVER_REQUEST_SECRET")
                    self.requestSecret(sender, datagram)
                    
                elif code == AIMsgName2Id["DBSERVER_SUBMIT_SECRET"]:
                    print("DBSERVER_SUBMIT_SECRET")
                    self.submitSecret(sender, datagram)
                    
                else:
                    raise Exception("Unknown message on DBServer channel: %d" % code)
                    
            if channel in self.manager.cache:
                di = DatagramIterator(datagram)
                do = self.manager.cache[channel]
                
                if code == AIMsgName2Id["STATESERVER_OBJECT_UPDATE_FIELD"]:
                    # We are asked to update a field
                    doId = di.getUint32()
                    fieldId = di.getUint16()
                    
                    # Is this sent to the correct object?
                    if doId != do.doId:
                        raise Exception("Object %d does not match channel %d" % (doId, do.doId))
                    
                    # We apply the update
                    field = do.dclass.getFieldByIndex(fieldId)
                    do.receiveField(field, di)
        
    def getStoredValues(self, sender, datagram):
        """
        Get the stored field values from the object specified in the datagram.
        """
        di = DatagramIterator(datagram)
        
        # Get the context.
        context = di.getUint32()
        
        # The doId we want to get the fields from.
        doId = di.getUint32()
        
        # The number of fields we're going to search for.
        numFields = di.getUint16()
        
        # Get all of the field names we want to work with!
        fieldNames = []
        for i in range(0, numFields):
            fieldNames.append(di.getString())
            
        numFields = len(fieldNames)
        
        dg = Datagram()
        dg.addUint32(context) # Rain or shine. We want the context.
        dg.addUint32(doId) # They'll need to know what doId this was for!
        dg.addUint16(numFields) # Send back the number of fields we searched for.
        
        # Add all of our field names.
        for i in range(0, numFields):
            dg.addString(fieldNames[i])
        
        # Make sure our database object even exists first.
        if not self.manager.hasDatabaseObject(doId):
            # Failed to get our object. So we just add our response code.
            dg.addUint8(1)
            # Send out our response.
            self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_GET_STORED_VALUES_RESP"], dg)
            return
            
        dg.addUint8(0)
        
        # Load our database object.
        do = self.manager.loadDatabaseObject(doId)
        
        values = []
        found = []
        
        # Add our field values.
        for i in range(0, numFields):
            fieldName = fieldNames[i]
            if fieldName in do.fields: # Success
                values.append(do.packField(fieldName, do.fields[fieldName]).decode('ISO-8859-1'))
                found.append(True)
                continue
            # Failure, The field doesn't exist.
            #print("Couldn't find field %s for do %s!" % (fieldName, str(do.doId)))
            values.append("DEADBEEF")
            found.append(False)
            
        # Add our values.
        for i in range(0, numFields):
            value = values[i]
            dg.addString(value)
        
        # Add the list of our found field values.
        for i in range(0, numFields):
            foundField = found[i]
            dg.addUint8(foundField)

        # Send out our response.
        self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_GET_STORED_VALUES_RESP"], dg)
        
        # Generate our db object if needed!
        if do.dclass.getName() in list(self.dcObjectTypeFromName.keys()):
            if do.doId in self.stateServer.objects:
                #print("%s object %d already exists in objects!" % (do.dclass.getName(), do.doId))
                return
            if do.doId in self.stateServer.dbObjects:
                #print("%s object %d already exists in db objects!" % (do.dclass.getName(), do.doId))
                return
            #print("Creating %s db object with doId %d!" % (do.dclass.getName(), do.doId))
            self.stateServer.dbObjects[do.doId] = DistributedObject(do.doId, do.dclass, 0, 0)
        
    def setStoredValues(self, sender, datagram):
        """
        Set the values of the fields for the object specified in the datagram.
        """
        di = DatagramIterator(datagram)
        
        # The doId we want to set the fields for.
        doId = di.getUint32()
        
        # The number of fields we're going to set.
        numFields = di.getUint32()
        
        fieldNames = []
        fieldValues = []
        
        # Get all of our field names.
        for i in range(0, numFields):
            fieldNames.append(di.getString())
        
        # Get all of our field values.
        for i in range(0, numFields):
            fieldValues.append(di.getString())
            
        # Make sure our database object even exists first.
        if not self.manager.hasDatabaseObject(doId):
            return

        # Load our database object.
        do = self.manager.loadDatabaseObject(doId)
        
        # Unpack and assign the field values.
        for i in range(0, numFields):
            fieldName = fieldNames[i]
            fieldValue = fieldValues[i]
            
            if not do.dclass.getFieldByName(fieldName):
                # We can't set a field our dcclass doesn't have!
                continue
            
            unpackedValue = do.unpackField(fieldName, fieldValue)
            if unpackedValue:
                do.fields[fieldName] = unpackedValue
        
        # Save the database object to make sure we don't lose our changes.
        self.manager.saveDatabaseObject(do)
        
    def createStoredObject(self, sender, datagram):
        """
        Create a Database Object from the database object index.
        """
        di = DatagramIterator(datagram)
        
        fieldNames = []
        fieldValues = []
        
        # Get the context.
        context = di.getUint32()
        
        # We just need to do this for this unused value.
        u = di.getString()
        
        # This is our database object ID.
        dbObjectType = di.getUint16()
        
        # The amount of fields we have.
        numFields = di.getUint16()
        
        # Get all of our field names
        for i in range(0, numFields):
            fieldNames.append(di.getString())
        
        # Get all of our field values.
        for i in range(0, numFields):
            fieldValues.append(di.getString().encode('ISO-8859-1'))
        
        if not dbObjectType in self.dcObjectTypes:
            print("ERROR: Failed to create stored object with invalid db object type %d!" % (dbObjectType))
                        
            dg = Datagram()
            # Add our context.
            dg.addUint32(context)
            # We failed, So add a response code of 1.
            dg.addUint8(1)
            
            # Send out our response.
            self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_CREATE_STORED_OBJECT_RESP"], dg)
            return
        
        # Create a database object from our dc object type.
        dbObject = self.manager.createDatabaseObject(dbObjectType)
        
        # Unpack and assign the field values.
        for i in range(0, numFields):
            fieldName = fieldNames[i]
            fieldValue = fieldValues[i]

            if not dbObject.dclass.getFieldByName(fieldName):
                # We can't set a field our dcclass doesn't have!
                continue

            unpackedValue = dbObject.unpackField(fieldName, fieldValue)
            if unpackedValue:
                dbObject.fields[fieldName] = unpackedValue
                
        # Save the database object to make sure we don't lose our changes.
        self.manager.saveDatabaseObject(dbObject)
        
        dg = Datagram()
        
        # Add our context.
        dg.addUint32(context)
        
        # We successfully created and set the fields of the database object.
        dg.addUint8(0)
        
        # Add the resulting object doId.
        dg.addUint32(dbObject.doId)
        
        # Send out our response.
        self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_CREATE_STORED_OBJECT_RESP"], dg)

    def getEstate(self, sender, datagram):
        """
        Return the database values for the Estate and fields specified, 
        If some parts of the Estate aren't created. They are here.
        """
        di = DatagramIterator(datagram)
        
        # Get the context for sending back.
        context = di.getUint32()
        
        # The avatar which has the estate.
        doId = di.getUint32()
        
        dg = Datagram()
        
        # Rain or shine. We want the context.
        dg.addUint32(context)
        
        if not self.manager.hasDatabaseObject(doId):
            dg.addUint8(1) # Failed to get our avatar, So we can't get their houses either!
            self.messageDirector.sendMessage([sender], 20100000, TTAIMsgName2Id["DBSERVER_GET_ESTATE_RESP"], dg)
            return
            
        currentAvatar = self.manager.loadDatabaseObject(doId)
        
        # Somehow we don't have an account!
        if not 'setDISLid' in currentAvatar.fields:
            dg.addUint8(1) # Failed to get our avatar, So we can't get their houses either!
            self.messageDirector.sendMessage([sender], 20100000, TTAIMsgName2Id["DBSERVER_GET_ESTATE_RESP"], dg)
            return
            
        accountId = currentAvatar.fields['setDISLid'][0]
        
        # Our account doesn't exist!?
        if not self.manager.hasDatabaseObject(accountId):
            dg.addUint8(1) # Failed to get our avatar, So we can't get their houses either!
            self.messageDirector.sendMessage([sender], 20100000, TTAIMsgName2Id["DBSERVER_GET_ESTATE_RESP"], dg)
            return
            
        account = self.manager.loadDatabaseObject(accountId)
        
        # Pre-define this here.
        estate = None
        houseIds = None
        
        # We need to create an Estate!
        if not 'ESTATE_ID' in account.fields or account.fields['ESTATE_ID'] == 0:
            estate = self.manager.createDatabaseObjectFromName("DistributedEstate")
            houseIds = [0, 0, 0, 0, 0, 0]
            account.update("ESTATE_ID", estate.doId)
            account.update("HOUSE_ID_SET", houseIds)
            if not estate.doId in self.stateServer.dbObjects:
                self.stateServer.dbObjects[estate.doId] = DistributedObject(estate.doId, estate.dclass, 0, 0)
        else:
            estate = self.manager.loadDatabaseObject(account.fields['ESTATE_ID'])
            houseIds = account.fields["HOUSE_ID_SET"]
            if not estate.doId in self.stateServer.dbObjects:
                self.stateServer.dbObjects[estate.doId] = DistributedObject(estate.doId, estate.dclass, 0, 0)

        avatars = account.fields["ACCOUNT_AV_SET"]
        
        houses = []
        
        # First create all our blank houses.
        for i in range(0, len(houseIds)):
            if houseIds[i] == 0:
                house = self.manager.createDatabaseObjectFromName("DistributedHouse")
                house.update("setName", "")
                house.update("setAvatarId", 0)
                house.update("setColor", i)
                houseIds[i] = house.doId
                if not house.doId in self.stateServer.dbObjects:
                    self.stateServer.dbObjects[house.doId] = DistributedObject(house.doId, house.dclass, 0, 0)
                houses.append(house)
            else: # If the house already exists... Just generate and store it.
                house = self.manager.loadDatabaseObject(houseIds[i])
                house.update("setColor", i)
                if not house.doId in self.stateServer.dbObjects:
                    self.stateServer.dbObjects[house.doId] = DistributedObject(house.doId, house.dclass, 0, 0)
                houses.append(house)
                
        pets = []
                
        # Time to update our existing houses and pets!
        for i in range(0, len(avatars)):
            avDoId = avatars[i]
            
            # If we're missing the avatar for some reason... Skip!
            if not self.manager.hasDatabaseObject(avDoId):
                continue
                
            # Load in our avatar.
            avatar = self.manager.loadDatabaseObject(avDoId)
            
            # Load our pet for this avatar in question in.
            if "setPetId" in avatar.fields and avatar.fields["setPetId"][0] != 0:
                pet = self.manager.loadDatabaseObject(avatar.fields["setPetId"][0])
                if not pet.doId in self.stateServer.dbObjects:
                    self.stateServer.dbObjects[pet.doId] = DistributedObject(pet.doId, pet.dclass, 0, 0)
                pets.append(pet)
                
            avPositionIndex = avatar.fields["setPosIndex"][0]
            # If for some reason theres no house here... Create one!
            if houseIds[avPositionIndex] == 0:
                house = self.manager.createDatabaseObjectFromName("DistributedHouse")
                house.update("setName", avatar.fields["setName"][0])
                house.update("setAvatarId", avDoId)
                house.update("setColor", avPositionIndex)
                houseIds[avPositionIndex] = house.doId
                if not house.doId in self.stateServer.dbObjects:
                    self.stateServer.dbObjects[house.doId] = DistributedObject(house.doId, house.dclass, 0, 0)
            else: # Update our houses info just in case ours changed!
                house = self.manager.loadDatabaseObject(houseIds[avPositionIndex])
                house.update("setName", avatar.fields["setName"][0])
                house.update("setAvatarId", avDoId)
                house.update("setColor", avPositionIndex)
                if not house.doId in self.stateServer.dbObjects:
                    self.stateServer.dbObjects[house.doId] = DistributedObject(house.doId, house.dclass, 0, 0)
            
        
        # Update our ids just in case a new house was made.
        account.update("HOUSE_ID_SET", houseIds)
        
        # Make sure our account saved it's changes.
        self.manager.saveDatabaseObject(account)
        
        # We've succeeded in loading everything we need to, So we add this indicating success.
        dg.addUint8(0)
        
        # Add our estate doId
        dg.addUint32(estate.doId)
        
        # Add the amount of fields in our estate.
        dg.addUint16(len(estate.fields))
        
        # Add our field values. This in theory isn't needed at all.
        for name, value in estate.fields.items():
            try:
                dg.addString(name)
                dg.addString(estate.packField(name, value).decode('ISO-8859-1'))
                dg.addUint8(True)
            except:
                dg.addString("DEADBEEF")
                dg.addString("DEADBEEF")
                dg.addUint8(False)
                
        houseLen = len(houses)
        # Add the number of houses we have.
        dg.addUint16(houseLen)
        
        # Add all of our house doIds.
        for i in range(0, len(houses)):
            house = houses[i]
            dg.addUint32(house.doId)
        
        houseData = {}
        foundHouses = houseLen
        
        for name in list(houses[0].fields.keys()):
            houseData[name] = []
        
        # Make a our lists of field names and values. 
        for i in range(0, len(houses)):
            house = houses[i]
            for name, value in house.fields.items():
                houseData[name].append(house.packField(name, value))

        # Add the number of house keys we have.
        dg.addUint16(len(houseData))
        
        # Add our house keys.
        for name in list(houseData.keys()):
            dg.addString(name)
            
        # Add the number of house values we have.
        dg.addUint16(len(houseData))
        
        # Add our house values.
        for name, data in houseData.items():
            dg.addUint16(houseLen) # Why the fuck is this needed Disney.
            for i in range(0, len(data)):
                value = data[i]
                dg.addString(value.decode('ISO-8859-1'))

        # The amount of houses we got successfully,
        # It's not checked anymore. So it's safe to say it was scrapped.
        dg.addUint16(foundHouses)
        
        # Add in if we found a house or not, We don't really check this as of rn.
        # We've either failed earlier or gotten to this point.
        for i in range(0, len(houseData)):
            dg.addUint16(0) #hvLen, This isn't used anymore either.
            for j in range(0, houseLen):
                dg.addUint8(1)
            
        # Add the number of pets we have.
        dg.addUint16(len(pets))
        
        # Add our pet doIds.
        for i in range(0, len(pets)):
            pet = pets[i]
            dg.addUint32(pet.doId)
        
        # We can FINALLY send our message.
        self.messageDirector.sendMessage([sender], 20100000, TTAIMsgName2Id["DBSERVER_GET_ESTATE_RESP"], dg)

    def makeFriends(self, sender, datagram):
        di = DatagramIterator(datagram)
        
        # The first person who wants to make friends.
        friendIdA = di.getUint32()
        
        # The second person who wants to make to friends.
        friendIdB = di.getUint32()
        
        # The flags for this friendship.
        flags = di.getUint8()
        
        # Get the context for sending back.
        context = di.getUint32()
        
        dg = Datagram()
        
        # If one or neither of the database objects exist. They can NOT become friends.
        if not self.manager.hasDatabaseObject(friendIdA) or not self.manager.hasDatabaseObject(friendIdB):
            dg.addUint8(False)
            dg.addUint32(context)
            # Send out our response.
            self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_MAKE_FRIENDS_RESP"], dg)
            return
            
        # Load the database objects for our friends.
        friendA = self.manager.loadDatabaseObject(friendIdA)
        friendB = self.manager.loadDatabaseObject(friendIdB)
        
        # If one or either can't possibly make friends, We will respond with a failure.
        if not friendA.dclass.getFieldByName("setFriendsList") or not friendB.dclass.getFieldByName("setFriendsList"):
            dg.addUint8(False)
            dg.addUint32(context)
            # Send out our response.
            self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_MAKE_FRIENDS_RESP"], dg)
            return
            
        # Make sure we have the field already.
        if not "setFriendsList" in friendA.fields:
            friendA.fields["setFriendsList"] = ([],)
        if not "setFriendsList" in friendB.fields:
            friendB.fields["setFriendsList"] = ([],)
            
        friendAlist = friendA.fields["setFriendsList"][0]
        friendBlist = friendB.fields["setFriendsList"][0]
        
        # To know if we had the corresponding friends or not.
        HasFriendA = False
        HasFriendB = False
        
        # Check if we already have friend B in friend As list.
        # And update it if we do.
        for i in range(0, len(friendAlist)):
            friendPair = friendAlist[i]
            if friendPair[0] == friendIdB:
                # We did.  Update the code.
                friendAlist[i] = (friendIdB, flags)
                HasFriendA = True
                break
                
        if not HasFriendA:
            # We didn't already have this friend; tack it on.
            friendAlist.append((friendIdB, flags))
            
        # Check if we already have friend A in friend Bs list.
        # And update it if we do.
        for i in range(0, len(friendBlist)):
            friendPair = friendBlist[i]
            if friendPair[0] == friendIdA:
                # We did.  Update the code.
                friendBlist[i] = (friendIdA, flags)
                HasFriendB = True
                break
                
        if not HasFriendB:
            # We didn't already have this friend; tack it on.
            friendBlist.append((friendIdA, flags))
        
        # We succesfully added them as a friend!
        dg.addUint8(True)
        dg.addUint32(context)
        
        self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_MAKE_FRIENDS_RESP"], dg)
        
        # Save the database objects to make sure we don't lose our changes.
        self.manager.saveDatabaseObject(friendA)
        self.manager.saveDatabaseObject(friendB)
        
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

        self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_REQUEST_SECRET_RESP"], dg)
        
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

        self.messageDirector.sendMessage([sender], 20100000, AIMsgName2Id["DBSERVER_SUBMIT_SECRET_RESP"], dg)