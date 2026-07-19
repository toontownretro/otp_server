'''
This class is a simple class which is actually the interface additions for a server wanting to communicate with the
Message Director. The State Server, Client Agent, and Database Server all inherit this.
'''

import asyncio, functools, os, socket, struct, time

from panda3d.core import ConfigVariableInt, ConfigVariableBool, Datagram, DatagramIterator, DSearchPath, Filename, VirtualFileSystem

from connection import Client
from msgtypes import *

class ServerInterface(Client):
    def __init__(self):
        super().__init__()
        
    def load_dc(self):
        # DC File
        self.dc = DCFile()
        
        # Get our Panda3D Virtual File System.
        vfs = VirtualFileSystem.getGlobalPtr()
        
        # Look for our dc file locations and read them in.
        search_path = DSearchPath()
        # In other environments, including the dev environment, look here:
        otpbase = os.path.expandvars('$OTP') or './otp'
        search_path.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(otpbase+'/src/configfiles')))
        toontownbase = os.path.expandvars('$TOONTOWN') or './toontown'
        search_path.appendDirectory(Filename.fromOsSpecific(os.path.expandvars(toontownbase+'/src/configfiles')))
        
        # Resolve the location of our otp.dc file.
        otp_dc = Filename("otp.dc")
        vfs.resolveFilename(otp_dc, search_path)
        
        # Resolve the location of our toon.dc file.
        toon_dc = Filename("toon.dc")
        vfs.resolveFilename(toon_dc, search_path)
        
        # Read our DC files.
        self.read_dc_files([otp_dc, toon_dc])
        
    def read_dc_files(self, dc_files):
        """
        Reads in the dc files listed in dc_files, or if
        dc_files is None, reads in all of the dc files listed in
        the Config.prc file.
        """

        dc_file = self.dc
        dc_file.clear()
        self.dclasses_by_name = {}
        self.dclasses_by_number = {}

        if isinstance(dc_files, str):
            # If we were given a single string, make it a list.
            dc_files = [dc_files]

        dcImports = {}
        if dc_files == None:
            read_result = dc_file.readAll()
            if not read_result:
                print("Could not read dc file.")
        else:
            for dc_file_name in dc_files:
                pathname = Filename(dc_file_name)
                read_result = dc_file.read(pathname)
                if not read_result:
                    print("Could not read dc file: %s" % (pathname))

        # Now import all of the modules required by the DC file.
        for n in range(dc_file.getNumImportModules()):
            for i in range(dc_file.getNumImportSymbols(n)):
                symbol_name = dc_file.getImportSymbol(n, i)

                # Maybe the symbol name is represented as "symbolName/AI".
                suffix = symbol_name.split('/')
                symbol_name = suffix[0]
                suffix=suffix[1:]
                for ext in suffix:
                    dclass = dcFile.getClassByName(symbol_name)
                    if dclass:
                        self.dclasses_by_name[symbol_name + ext] = dclass

        # Now get the class definition for the classes named in the DC
        # file.
        for i in range(dc_file.getNumClasses()):
            dclass = dc_file.getClass(i)
            number = dclass.getNumber()
            class_name = dclass.getName()

            self.dclasses_by_name[class_name] = dclass
            if number >= 0:
                self.dclasses_by_number[number] = dclass
                
        self.dc = dc_file
        
    async def send_message(self, channels, sender, code, datagram):
        if self.closed:
            return
            
        # Construct our datagram which the Message Director will recieve.
        dg = Datagram()
        dg.addUint8(len(channels))
        for channel in channels:
            dg.addUint64(channel)
        dg.addUint64(sender)
        dg.addUint16(code)
        data = bytes(datagram) # Convert the Datagram to it's raw data.
        dg.appendData(data) # Append it.
        
        # Send our message.
        await self.send_datagram(dg)
        
    async def register_for_channel(self, channel):
        if self.closed:
            return

        # Construct our datagram to register a channel with the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_SET_CHANNEL)
        dg.addUint64(channel)
        
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def unregister_for_channel(self, channel):
        if self.closed:
            return

        # Construct our datagram to register a channel with the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_REMOVE_CHANNEL)
        dg.addUint64(channel)
        
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def set_connection_name(self, name):
        if self.closed:
            return

        # Construct our datagram to register our connections name with the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_SET_CON_NAME)
        dg.addString(name)
        
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def add_control_range(self, channels):
        if self.closed or len(channels) == 0:
            return

        # Construct our datagram to register our channels with the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_ADD_RANGE)
        dg.addInt16((len(channels))
        for channel in channels:
            dg.addUint64(channel)
            
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def remove_control_range(self, channels):
        if self.closed or len(channels) == 0:
            return

        # Construct our datagram to unregister our channels with the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_REMOVE_RANGE)
        dg.addInt16((len(channels))
        for channel in channels:
            dg.addUint64(channel)
            
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def set_connection_url(self, url):
        if self.closed:
            return

        # Construct our datagram to register our connections URL with the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_SET_CON_URL)
        dg.addString(url)
        
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def add_post_socket_close(self, datagram):
        if self.closed:
            return

        # Construct our datagram to add our post removal message to the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_ADD_POST_REMOVE)
        dg.addBlob(datagram.getMessage())
        
        # Send our datagram.
        await self.send_datagram(dg)
        
    async def clear_post_socket_close(self):
        if self.closed:
            return

        # Construct our datagram to add our post removal message to the Message Director.
        dg = Datagram()
        dg.addInt8(1)
        dg.addUint64(CONTROL_MESSAGE)
        dg.addUint16(CONTROL_CLEAR_POST_REMOVE)
        
        # Send our datagram.
        await self.send_datagram(dg)