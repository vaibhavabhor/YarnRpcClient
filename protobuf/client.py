#!/usr/bin/python

import socket
import struct
import uuid

import protobuf.RpcHeader_pb2 as RpcHeader_pb2
from protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto
from protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto 
from protobuf.ProtobufRpcEngine_pb2 import RequestHeaderProto 
from protobuf.ClientNamenodeProtocol_pb2 import ClientNamenodeProtocol as client_proto
import protobuf.ClientNamenodeProtocol_pb2 as ClientNamenodeProtocol_pb2

import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder

'''
To create a connection, send this:
 
+---------------------------------------------------------------------+
| Header, 4 bytes ("hrpc") |
+---------------------------------------------------------------------+
| Version, 1 byte (default verion 9) |
+---------------------------------------------------------------------+
| RPC service class, 1 byte (0x00) |
+---------------------------------------------------------------------+
| Auth protocol, 1 byte (Auth method None = 0) |
+---------------------------------------------------------------------+
| Length of the RpcRequestHeaderProto + length of the |
| of the IpcConnectionContextProto (4 bytes/32 bit int) |
+---------------------------------------------------------------------+
| Serialized delimited RpcRequestHeaderProto |
+---------------------------------------------------------------------+
| Serialized delimited IpcConnectionContextProto |
+---------------------------------------------------------------------+
'''

'''Create and serialize a RpcRequestHeaderProto '''
rpcrequestheader = RpcRequestHeaderProto()
rpcrequestheader.rpcKind = 2
#RpcHeader_pb2.RPC_PROTOCOL_BUFFER
rpcrequestheader.rpcOp = 0
#RpcRequestHeaderProto.RPC_FINAL_PACKET
rpcrequestheader.callId = -3 # During initial connection
# 0 otherwise
# 4 for ping i guess 
client_id = str(uuid.uuid4())
rpcrequestheader.clientId = client_id[0:16]
s_rpcrequestheader = rpcrequestheader.SerializeToString()

'''Create and serialize a IpcConnectionContextProto '''
context = IpcConnectionContextProto()
context.userInfo.effectiveUser = "hdfs"
context.protocol =  "org.apache.hadoop.hdfs.protocol.ClientProtocol" #"org.apache.hadoop.mapred.JobSubmissionProtocol"
#"org.apache.hadoop.hdfs.protocol.ClientProtocol" 
s_context = context.SerializeToString()

''' Length of the two messages '''
rpcipc_length = len(s_rpcrequestheader) + encoder._VarintSize(len(s_rpcrequestheader)) + len(s_context) + encoder._VarintSize(len(s_context))

 
''' Send to server in the order given above'''
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
sock.settimeout(10)
sock.connect(("localhost", 8020))

sock.send("hrpc") # header
sock.send(struct.pack('B', 9)) # version
sock.send(struct.pack('B', 0x00)) # RPC service class
sock.send(struct.pack('B', 0x00)) # auth none


sock.sendall(struct.pack('!I', rpcipc_length) + 
encoder._VarintBytes(len(s_rpcrequestheader)) + 
s_rpcrequestheader + 
encoder._VarintBytes(len(s_context)) + 
s_context)


'''
Create the Hadoop RPC protocol looks like this for sending requests:
When sending requests
+---------------------------------------------------------------------+
| Length of the next three parts (4 bytes/32 bit int) |
+---------------------------------------------------------------------+
| Delimited serialized RpcRequestHeaderProto (varint len + header) |
+---------------------------------------------------------------------+
| Delimited serialized RequestHeaderProto (varint len + header) |
+---------------------------------------------------------------------+
| Delimited serialized Request (varint len + request) |
+---------------------------------------------------------------------+
'''


'''
Steps:
1. create rpcrequestheader
2. create requestheader in which you can mention the name of protocol and method name you want 
3. create actual request i guess you can use it to pass parameters  

'''


''' we need a rpcrequestheaderproto for every message we send ''' 
rpcrequestheader = RpcRequestHeaderProto()
rpcrequestheader.rpcKind = 2 #RpcHeader_pb2.RPC_PROTOCOL_BUFFER
rpcrequestheader.rpcOp = 0   #RpcRequestHeaderProto.RPC_FINAL_PACKET
rpcrequestheader.callId = 0  # For all other communication other than initial, 4 for ping i guess 
client_id = str(uuid.uuid4())
rpcrequestheader.clientId = client_id[0:16]
s_rpcrequestheader = rpcrequestheader.SerializeToString()
''' ok thats our header''' 

'''lets create our requestheaderproto ''' 
requestheader = RequestHeaderProto()
requestheader.methodName =  "getServerDefaults" #"getAllJobs"
requestheader.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol" #"org.apache.hadoop.mapred.JobSubmissionProtocol" # # # org.apache.hadoop.hdfs.protocol.ClientProtocol
requestheader.clientProtocolVersion= 1  # not sure what is this 2,28 
'''serialize this ''' 
s_requestheader = requestheader.SerializeToString()


'''Now we need to write our actual request....may be lets start it here  '''  
request = ClientNamenodeProtocol_pb2.GetServerDefaultsRequestProto()
s_request = request.SerializeToString()


''' lenght of three messages ''' 
rpc_message_length = len(s_rpcrequestheader) + encoder._VarintSize(len(s_rpcrequestheader)) + \
                             len(s_requestheader) + encoder._VarintSize(len(s_requestheader)) + \
                             len(s_request) + encoder._VarintSize(len(s_request))

'''pack in the above given format and send :)  '''   
sock.sendall(struct.pack('!I', rpc_message_length) + 
encoder._VarintBytes(len(s_rpcrequestheader)) + 
s_rpcrequestheader + 
encoder._VarintBytes(len(s_requestheader))+
s_requestheader+
encoder._VarintBytes(len(s_request)) + 
s_request)

print "reading response"

'''
The RpcResponseHeaderProto contains a status field that marks SUCCESS or ERROR.
        The Hadoop RPC protocol looks like the diagram below for receiving SUCCESS requests.
        +-----------------------------------------------------------+
        |  Length of the RPC resonse (4 bytes/32 bit int)           |
        +-----------------------------------------------------------+
        |  Delimited serialized RpcResponseHeaderProto              |
        +-----------------------------------------------------------+
        |  Serialized delimited RPC response                        |
        +-----------------------------------------------------------+

        In case of an error, the header status is set to ERROR and the error fields are set.
'''


def socket_read_n(sock, n):
    """ Read exactly n bytes from the socket.
        Raise RuntimeError if the connection closed before
        n bytes were read.
    """
    buf = ''
    while n > 0:
        data = sock.recv(n)
        if data == '':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf

def get_message(sock, msgtype):
    """ Read a message from a socket. msgtype is a subclass of
        of protobuf Message.
    """
    len_buf = socket_read_n(sock, 4)
    msg_len = struct.unpack('>L', len_buf)[0]
    
    msg_buf = socket_read_n(sock, msg_len)

    msg = msgtype()
    msg.ParseFromString(msg_buf)
    return msg

print get_message(sock,ClientNamenodeProtocol_pb2.GetServerDefaultsResponseProto)








