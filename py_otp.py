import asyncio, os, io, logging, socket, select, traceback

from panda3d.core import ConfigVariableInt

from message_director import MessageDirector
from state_server import StateServer
from client_agent import ClientAgent
from database_server import DatabaseServer
#from event_server import EventServer
from msgtypes import *

class PyOTP:
    def __init__(self):
        #self.event_server = EventServer(self)
        self.message_director = MessageDirector()
        self.database_server = DatabaseServer(ConfigVariableInt("database-server-id", DBSERVER_ID).getValue())
        self.state_server = StateServer(ConfigVariableInt("state-server-id", 20100000).getValue())
        self.client_agent = ClientAgent(ConfigVariableInt("client-agent-id", 20200000).getValue())
        
        self.async_tasks = []
        
    async def initialize(self):
        # Start the Message Director.
        await self.message_director.start(ConfigVariableInt("msg-director-port", 6666).getValue())
        
        # Start the Client Agent.
        await self.client_agent.start(6667)
        
        # Make sure to initialize the databases for the Database Server.
        await self.database_server.manager.initialize()
        
    async def connect(self):
        # Connect the Database Server to the Message Director.
        await self.database_server.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        # Connect the State Server to the Message Director.
        await self.state_server.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        # Connect the Client Agent to the Message Director.
        await self.client_agent.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
    async def add_task(self, async_task):
        self.async_tasks.append(async_task)

if __name__ == "__main__":
    async def main():
        # Get the running loop inside an async function
        loop = asyncio.get_running_loop()
        
        otp = PyOTP()
        await otp.initialize()
        
        try:
            await otp.add_task(loop.create_task(otp.message_director.listen_routine(), name="md-listen"))
            await otp.add_task(loop.create_task(otp.client_agent.listen_routine(), name="ca-listen"))
        except Exception as e:
            traceback.print_exception(e)

        await otp.connect()
        
        try:
            await otp.add_task(loop.create_task(otp.message_director.flush_routine(), name="flush-md"))
            await otp.add_task(loop.create_task(otp.database_server.flush_routine(), name="flush-dbs"))
            await otp.add_task(loop.create_task(otp.state_server.flush_routine(), name="flush-ss"))
            await otp.add_task(loop.create_task(otp.client_agent.flush_interface_routine(), name="flush-ca-interface"))
            await otp.add_task(loop.create_task(otp.client_agent.flush_server_routine(), name="flush-ca-server"))
            await otp.add_task(loop.create_task(otp.message_director.drain_routine(), name="drain-md"))
            await otp.add_task(loop.create_task(otp.database_server.drain_routine(), name="drain-dbs"))
            await otp.add_task(loop.create_task(otp.state_server.drain_routine(), name="drain-ss"))
            await otp.add_task(loop.create_task(otp.client_agent.drain_interface_routine(), name="drain-ca-interface"))
            await otp.add_task(loop.create_task(otp.client_agent.drain_server_routine(), name="drain-ca-server"))
            await otp.add_task(loop.create_task(otp.message_director.start_message_routing(), name="md-message-routing"))
            await otp.add_task(loop.create_task(otp.message_director.start_debug_logging(), name="md-debug-log"))
        except Exception as e:
            traceback.print_exception(e)
            
        # Very slow loop. This is just here to keep the server open
        while True:
            try:
                await asyncio.sleep(240)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
            
    asyncio.run(main())