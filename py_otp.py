from panda3d.core import DSearchPath, Filename, VirtualFileSystem
from panda3d.direct import DCFile

import os, socket, select

from message_director import MessageDirector, MDClient
from state_server import StateServer
from client_agent import ClientAgent
from client import Client
from database_server import DatabaseServer
from event_server import EventServer

class PyOTP:
    def __init__(self):
        # Every socket client (makes the code faster)
        self.clients = {}
        
        # DC File
        self.dc = DCFile()
        
        self.dclassesByName = {}
        self.dclassesByNumber = {}
        
        # Get our Panda3D Virtual File System.
        vfs = VirtualFileSystem.getGlobalPtr()
        
        # Look for our dc file locations and read them in.
        searchPath = DSearchPath()
        # In other environments, including the dev environment, look here:
        otpbase = os.path.expandvars('$OTP') or './otp'
        searchPath.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(otpbase+'/src/configfiles')))
        toontownbase = os.path.expandvars('$TOONTOWN') or './toontown'
        searchPath.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(toontownbase+'/src/configfiles')))
        
        # Resolve the location of our otp.dc file.
        otpDC = Filename("otp.dc")
        vfs.resolveFilename(otpDC, searchPath)
        
        # Resolve the location of our toon.dc file.
        toonDC = Filename("toon.dc")
        vfs.resolveFilename(toonDC, searchPath)
        
        # Read our DC files.
        self.readDCFile([otpDC, toonDC])
        
        # "Handlers"
        self.eventServer = EventServer(self)
        self.messageDirector = MessageDirector(self)
        self.clientAgent = ClientAgent(self)
        self.stateServer = StateServer(self)
        self.databaseServer = DatabaseServer(self)
        
        
    def handleMessage(self, channels, sender, code, datagram):
        """
        Transmit a received message from MD to SS, CA and DBSS
        """
        self.stateServer.handle(channels, sender, code, datagram)
        self.clientAgent.handle(channels, sender, code, datagram)
        self.databaseServer.handle(channels, sender, code, datagram)
        
        
    def flush(self):
        """
        Do some socket magic
        """
        # TODO: use socketserver or something different.
        # We are very limited by select here
        
        r, w, x = select.select([self.messageDirector.sock, self.clientAgent.sock, self.eventServer.sock] + list(self.clients), [], [], 0)
        for sock in r:
            if sock == self.messageDirector.sock:
                sock, addr = sock.accept()
                self.clients[sock] = MDClient(self.messageDirector, sock, addr)
                self.messageDirector.clients.append(self.clients[sock])
                
            elif sock == self.clientAgent.sock:
                sock, addr = sock.accept()
                self.clients[sock] = Client(self.clientAgent, sock, addr)
                self.clientAgent.clients.append(self.clients[sock])
                
            elif sock == self.eventServer.sock:
                data, addr = sock.recvfrom(2048)
                self.eventServer.onData(data)
                
            else:
                client = self.clients[sock]
                try:
                    data = sock.recv(2048)
                except socket.error:
                    data = None
                    
                if not data:
                    print("Dropping client %s!" % (str(self.clients[sock])))
                    del self.clients[sock]
                    
                    if type(client) == MDClient:
                        self.messageDirector.clients.remove(client)
                        
                    elif type(client) == Client:
                        self.clientAgent.clients.remove(client)
                    
                    client.onLost()
                    
                else:
                    client.onData(data)
                    
    def readDCFile(self, dcFileNames = None):
        """
        Reads in the dc files listed in dcFileNames, or if
        dcFileNames is None, reads in all of the dc files listed in
        the Config.prc file.
        """

        dcFile = self.dc
        dcFile.clear()
        self.dclassesByName = {}
        self.dclassesByNumber = {}

        if isinstance(dcFileNames, str):
            # If we were given a single string, make it a list.
            dcFileNames = [dcFileNames]

        dcImports = {}
        if dcFileNames == None:
            readResult = dcFile.readAll()
            if not readResult:
                print("Could not read dc file.")
        else:
            for dcFileName in dcFileNames:
                pathname = Filename(dcFileName)
                readResult = dcFile.read(pathname)
                if not readResult:
                    print("Could not read dc file: %s" % (pathname))

        # Now import all of the modules required by the DC file.
        for n in range(dcFile.getNumImportModules()):
            for i in range(dcFile.getNumImportSymbols(n)):
                symbolName = dcFile.getImportSymbol(n, i)

                # Maybe the symbol name is represented as "symbolName/AI".
                suffix = symbolName.split('/')
                symbolName = suffix[0]
                suffix=suffix[1:]
                for ext in suffix:
                    dclass = dcFile.getClassByName(symbolName)
                    if dclass:
                        self.dclassesByName[symbolName + ext] = dclass

        # Now get the class definition for the classes named in the DC
        # file.
        for i in range(dcFile.getNumClasses()):
            dclass = dcFile.getClass(i)
            number = dclass.getNumber()
            className = dclass.getName()

            self.dclassesByName[className] = dclass
            if number >= 0:
                self.dclassesByNumber[number] = dclass
                
        self.dc = dcFile
                    

if __name__ == "__main__":
    otp = PyOTP()
    
    while True:
        otp.flush()