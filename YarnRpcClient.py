import socket
import struct
import uuid
import protobuf.RpcHeader_pb2 as RpcHeader_pb2
from protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto
from protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto 
from protobuf.ProtobufRpcEngine_pb2 import RequestHeaderProto 
from protobuf.channel import RpcBufferedReader
import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder
import protobuf.yarn_service_protos_pb2 as yarn_service_protos_pb2
from protobuf.yarn_service_protos_pb2 import GetClusterMetricsRequestProto, GetClusterMetricsResponseProto,GetClusterNodesRequestProto,GetClusterNodesResponseProto, GetApplicationReportRequestProto,GetApplicationReportResponseProto, GetNewApplicationRequestProto, GetNewApplicationResponseProto , GetApplicationsResponseProto, GetApplicationsRequestProto

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
def getInfo(protocolName,methodName,requestProto,responseProto):
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
	context.userInfo.effectiveUser = "cloudera"
	
	context.protocol = protocolName
	
	#"org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB" 
	
	s_context = context.SerializeToString()

	''' Length of the two messages '''
	rpcipc_length = len(s_rpcrequestheader) + encoder._VarintSize(len(s_rpcrequestheader)) + len(s_context) + encoder._VarintSize(len(s_context))

	 
	''' Send to server in the order given above'''
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
	sock.settimeout(10)
	sock.connect(("localhost", 8032)) #8020 for name node, 8032 for yarn 

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
	rpcrequestheader.callId = 0  # For all other communication other than initial, 4 for ping  
	client_id = str(uuid.uuid4())
	rpcrequestheader.clientId = client_id[0:16]
	s_rpcrequestheader = rpcrequestheader.SerializeToString()
	''' ok thats our header''' 

	'''lets create our requestheaderproto ''' 
	requestheader = RequestHeaderProto()
	requestheader.methodName = methodName 
	#"getClusterNodes"
	
	requestheader.declaringClassProtocolName = protocolName
	#"org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
	requestheader.clientProtocolVersion= 1 
	'''serialize this ''' 
	s_requestheader = requestheader.SerializeToString()

	'''Now we need to write our actual request....may be lets start it here  ''' 
	 
	#request = protoFile.GetClusterMetricsRequestProto()
	s_request = requestProto.SerializeToString()


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
	
	#responseObject = yarn_service_protos_pb2.GetClusterMetricsResponseProto
	
	stream = recv_rpc_message(sock)
	parse_response(stream, responseProto)
	#print repr(sock.recv(4096))


def get_message(sock, requestResponse):
	len_buf = socket_read_n(sock, 4)
	msg_len = struct.unpack('>L', len_buf)[0]
	print msg_len
	msg_buf = socket_read_n(sock, msg_len)
	print msg_buf
	parse_response(msg_buf,requestResponse)
	#requestResponse.ParseFromString(msg_buf)
	msg_len = ''
	msg_buf = ''
	len_buf = ''
	return requestResponse
	
def socket_read_n(sock, n):
	buf = ''
	data = ''
	while n > 0:
		data = sock.recv(n)
		#print "received"
		if data == '':
			raise RuntimeError('unexpected connection close')
		buf += data
		n -= len(data)
	return buf

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

def parse_response(byte_stream, response_class):
    print("############## PARSING ##############")
    #Read first 4 bytes to get the total length
    len_bytes = byte_stream.read(4)
    total_length = struct.unpack("!I", len_bytes)[0]
    print "Total response length: %s" % total_length
    header = RpcResponseHeaderProto()
    (header_len, header_bytes) = get_delimited_message_bytes(byte_stream)
    header.ParseFromString(header_bytes)
    if header.status == 0:
		if header_len >= total_length:
			return
		print response_class()
		response = response_class()
		response_bytes = get_delimited_message_bytes(byte_stream, total_length - header_len)[1]
		if len(response_bytes) > 0:
			response.ParseFromString(response_bytes)
			print response
			return response
		else:
			self.handle_error(header)

def handle_error(self, header):	
	raise RequestError("\n".join([header.exceptionClassName, header.errorMsg]))

def recv_rpc_message(sock):
    print ("############## RECEIVING ##############")
    byte_stream = RpcBufferedReader(sock)
    return byte_stream

def get_delimited_message_bytes(byte_stream, nr=4):
	(length, pos) = decoder._DecodeVarint32(byte_stream.read(nr), 0)
	delimiter_bytes = nr - pos
	byte_stream.rewind(delimiter_bytes)
	message_bytes = byte_stream.read(length)
	total_len = length + pos
	return (total_len, message_bytes)
		
def main():
	print "****** Fetching cluster info ******** "
	protoName = "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
	methodName = "getClusterMetrics"
	request = yarn_service_protos_pb2.GetClusterMetricsRequestProto()
	response = yarn_service_protos_pb2.GetClusterMetricsResponseProto
	getInfo(protoName, methodName, request, response)
	
	print "****** Printing node status ******** "
	protoName = "org.apache.hadoop.yarn.api.ApplicationClientProtocolPB"
	methodName = "getClusterNodes"
	request = yarn_service_protos_pb2.GetClusterNodesRequestProto()
	response = yarn_service_protos_pb2.GetClusterNodesResponseProto
	getInfo(protoName, methodName, request, response)
	
	
main()

