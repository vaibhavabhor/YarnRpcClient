#!/usr/bin/python
 
import socket
import struct 
import uuid
import protobuf.RpcHeader_pb2 as RpcHeader_pb2
from protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto
from protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto



 
# --- Connection header ---
# Client.writeConnectionHeader()
preamble = (
    "hrpc" # Server.HEADER
    "\9" # Server.CURRENT_VERSION
    "P" # AuthMethod.SIMPLE
    "\0" # Server.IpcSerializationType.PROTOBUF
)
 
protocol = "org.apache.hadoop.mapred.JobSubmissionProtocol"
 
# --- Connection context ---
# Client.writeConnectionContext()
context = IpcConnectionContextProto()
context.userInfo.effectiveUser = "cloudera"
context.protocol = protocol
s_context = context.SerializeToString()
hello = preamble + struct.pack(">I", len(s_context)) + s_context
 
# --- RPC ---
# Client.sendParam()
print "success" 
header = RpcRequestHeaderProto()
header.rpcKind = RpcHeader_pb2.RPC_WRITABLE
header.rpcOp = RpcRequestHeaderProto.RPC_FINAL_PACKET # 
header.callId = 0
client_id = str(uuid.uuid4())
header.clientId = client_id[0:16]
header = header.SerializeToString()
#assert len(header) <= 127, repr(header)
header = chr(len(header)) + header
 
# --- Payload ---
# Because we chose RPC_WRITABLE, our payload is a WritableRpcEngine$Invocation.
writableRpcVersion = 2
payload = struct.pack(">Q", writableRpcVersion) # 8 bytes, lolz
payload += struct.pack(">H", len(protocol)) + protocol
method = "getAllJobs"
payload += struct.pack(">H", len(method)) + method
clientVersion = 28
payload += struct.pack(">Q", clientVersion) # 8 bytes again, lolz again
clientMethodsHash = 0xDEADBEEF # Unused crap
payload += struct.pack(">I", clientMethodsHash)
numParams = 0
payload += struct.pack(">I", numParams)
 
payload = header + payload
rpc = struct.pack(">I", len(payload)) + payload
 
sock = socket.socket()
sock.connect(("127.0.0.1", 8020))
sock.sendall(hello + rpc)
print repr(sock.recv(4096)) # TBD: read output properly and deserialize it
