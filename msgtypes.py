from direct.distributed.MsgTypes import *

# OTP
from otp.ai.AIMsgTypes import *
from otp.distributed.OtpDoGlobals import *

# Toontown
from toontown.ai.ToontownAIMsgTypes import *

# Client Agents all register this channel for messages from the StateServer.
CLIENTAGENT_ID = 4002

# Database Server Object Types
DBSERVER_INVALID_OBJECT_TYPE = 0
DBSERVER_ACCOUNT_OBJECT_TYPE = 1
DBSERVER_AVATAR_OBJECT_TYPE = 2
DBSERVER_ESTATE_OBJECT_TYPE = 3
DBSERVER_HOUSE_OBJECT_TYPE = 4
DBSERVER_PET_OBJECT_TYPE = 5