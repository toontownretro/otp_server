import asyncio, os, socket, select, traceback

from panda3d.core import ConfigVariableInt

from message_director import MessageDirector
from state_server import StateServer
from client_agent import ClientAgent
#from database_server import DatabaseServer
#from event_server import EventServer

class PyOTP:
    def __init__(self):
        #self.event_server = EventServer(self)
        self.message_director = MessageDirector("0.0.0.0", ConfigVariableInt("msg-director-port", 6666).getValue())
        #self.database_server = DatabaseServer(self)
        self.state_server = StateServer(ConfigVariableInt("state-server-id", 20100000).getValue())
        self.client_agent = ClientAgent("0.0.0.0", 6667, ConfigVariableInt("client-agent-id", 20200000).getValue())
        
    async def initialize(self):
        # Start the Message Director.
        await self.message_director.start()
        
        # Start the Client Agent.
        await self.client_agent.start()
        
    async def connect(self):
        # Connect the State Server to the Message Director.
        await self.state_server.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
        
        # Connect the Client Agent to the Message Director.
        await self.client_agent.connect("127.0.0.1", ConfigVariableInt("msg-director-port", 6666).getValue())
    
    async def flush(self):
        # Flush the Message Director.
        await self.message_director.flush()
        
        # Flush the State Server, But only if it's not closed.
        if not self.state_server.is_closed():
            await self.state_server.flush()
        
        # Flush the Client Agent.
        await self.client_agent.flush()

if __name__ == "__main__":
    async def main():
        # Get the running loop inside an async function
        loop = asyncio.get_running_loop()
        
        otp = PyOTP()
        await otp.initialize()
        
        try:
            loop.create_task(otp.message_director.server.serve_forever())
            loop.create_task(otp.client_agent.server.serve_forever())
        except asyncio.CancelledError:
            pass

        await otp.connect()
        
        while True:
            try:
                await otp.flush()
                await asyncio.sleep(0)
            except KeyboardInterrupt as e:
                break
            except Exception as e:
                traceback.print_exception(e)
            
    asyncio.run(main())