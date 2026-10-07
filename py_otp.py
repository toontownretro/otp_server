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
        self.message_director = MessageDirector("0.0.0.0", ConfigVariableInt("msg-director-port", 6666).getValue())
        self.database_server = DatabaseServer(ConfigVariableInt("database-server-id", DBSERVER_ID).getValue())
        self.state_server = StateServer(ConfigVariableInt("state-server-id", 20100000).getValue())
        self.client_agent = ClientAgent("0.0.0.0", 6667, ConfigVariableInt("client-agent-id", 20200000).getValue())
        
        self.async_tasks = []
        
    async def initialize(self):
        # Start the Message Director.
        await self.message_director.start()
        
        # Start the Client Agent.
        await self.client_agent.start()
        
    async def connect(self):
        # Connect the Database Server to the Message Director.
        await self.database_server.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        # Connect the State Server to the Message Director.
        await self.state_server.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        # Connect the Client Agent to the Message Director.
        await self.client_agent.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
    async def add_task(self, async_task):
        self.async_tasks.append(async_task)
    
    async def flush_md(self):
        # Flush the Message Director.
        while True:
            try:
                await self.message_director.flush()
                await asyncio.sleep(0)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        
    async def flush_dbs(self):
        # Flush the Database Server.
        while True:
            try:
                await self.database_server.flush()
                await asyncio.sleep(0)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        
    async def flush_ss(self):
        # Flush the State Server.
        while True:
            try:
                await self.state_server.flush()
                await asyncio.sleep(0)
                if self.state_server.is_closed(): break
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
                
    async def flush_ca_interface(self):
        # Flush the Client Agent server interface (To our Message Director)
        while True:
            try:
                await self.client_agent.flush_interface()
                await asyncio.sleep(0)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        
    async def flush_ca_server(self):
        # Flush the Client Agent server backend.
        while True:
            try:
                await self.client_agent.flush_server()
                await asyncio.sleep(0)
            except asyncio.CancelledError as e:
                break
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)

if __name__ == "__main__":
    async def main():
        # Get the running loop inside an async function
        loop = asyncio.get_running_loop()
        
        otp = PyOTP()
        await otp.initialize()
        
        try:
            await otp.add_task(loop.create_task(otp.message_director.server.serve_forever(), name="md-serve_forever"))
            await otp.add_task(loop.create_task(otp.client_agent.server.serve_forever(), name="ca-serve_forever"))
        except Exception as e:
            traceback.print_exception(e)

        await otp.connect()
        
        try:
            #await otp.add_task(loop.create_task(otp.flush_md(), name="flush-md"))
            await otp.add_task(loop.create_task(otp.flush_dbs(), name="flush-dbs"))
            await otp.add_task(loop.create_task(otp.flush_ss(), name="flush-ss"))
            await otp.add_task(loop.create_task(otp.flush_ca_interface(), name="flush-ca-interface"))
            await otp.add_task(loop.create_task(otp.flush_ca_server(), name="flush-ca-server"))
        except Exception as e:
            traceback.print_exception(e)
            
        '''
        while True:
            try:
                for task in otp.async_tasks:
                    # create a string buffer
                    buffer = io.StringIO("")
                    # print the task stack
                    task.print_stack(file=buffer)
                    # log the trace
                    print(buffer.getvalue())
                    # close the string buffer
                    buffer.close()
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
        '''
        
        await otp.flush_md()
            
    asyncio.run(main())