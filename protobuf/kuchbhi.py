import socket
import struct
import uuid

import protobuf.RpcHeader_pb2 as RpcHeader_pb2
from protobuf.RpcHeader_pb2 import RpcRequestHeaderProto, RpcResponseHeaderProto
from protobuf.IpcConnectionContext_pb2 import IpcConnectionContextProto 
from protobuf.ProtobufRpcEngine_pb2 import RequestHeaderProto 
from protobuf.ClientNamenodeProtocol_pb2 import ClientNamenodeProtocol as client_proto
import protobuf.ClientNamenodeProtocol_pb2 as ClientNamenodeProtocol_pb2
from snakebite.channel import RpcBufferedReader
import google.protobuf.internal.encoder as encoder
import google.protobuf.internal.decoder as decoder


import MRClientProtocol_pb2 as MRClientProtocol_pb2 
import mr_service_protos_pb2 as mr_service_protos_pb2
from mr_service_protos_pb2 import GetJobReportRequestProto , GetJobReportResponseProto

import yarn_service_protos_pb2 as yarn_service_protos_pb2
from yarn_service_protos_pb2 import GetClusterMetricsRequestProto, GetClusterMetricsResponseProto,GetClusterNodesRequestProto,GetClusterNodesResponseProto, GetApplicationReportRequestProto,GetApplicationReportResponseProto, GetNewApplicationRequestProto, GetNewApplicationResponseProto , GetApplicationsResponseProto, GetApplicationsRequestProto


import application_history_client_pb2 as application_history_client_pb2
#from application_history_client_pb2 import GetApplicationReportRequestProto, GetApplicationReportResponseProto 




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
def createMsg():
	print 'I am here'
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
	context.protocol = "yarn_service_protos_pb2" #"org.apache.hadoop.yarn.api.ApplicationHistoryProtocolPB"
	#"org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB" 
	#"org.apache.hadoop.hdfs.protocol.ClientProtocol"
	#"org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB" 
	#"org.apache.hadoop.hdfs.protocol.ClientProtocol" 
	#"org.apache.hadoop.mapred.JobSubmissionProtocol"
	#"org.apache.hadoop.hdfs.protocol.ClientProtocol" 
	s_context = context.SerializeToString()

	''' Length of the two messages '''
	rpcipc_length = len(s_rpcrequestheader) + encoder._VarintSize(len(s_rpcrequestheader)) + len(s_context) + encoder._VarintSize(len(s_context))

	 
	''' Send to server in the order given above'''
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
	sock.settimeout(10)
	sock.connect(("localhost", 8032)) #8020 for name node 

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
	requestheader.methodName = "getClusterNodes"#"getJobReport" #"getContainerReport"
	#"getContainers"
	#"getDelegationToken"
	#"getQueueInfo"
	#"getClusterNodes"
	#"getClusterNodes"
	#"getQueueUserAcls" 
	#"getClusterNodes"
	#"getApplicationReport" 
	#"getClusterMetrics"#"getCounters"
	#"getJobReport"
	#"getDiagnostics" 
	#"getServerDefaults"
	 #"getAllJobs"
	requestheader.declaringClassProtocolName = "org.apache.hadoop.yarn.api.ApplicationHistoryProtocolPB"
	#"org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB" 
	#"org.apache.hadoop.hdfs.protocol.ClientProtocol"
	#"org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB" 
	#"org.apache.hadoop.yarn.protocol.MRClientProtocol" 
	#"org.apache.hadoop.hdfs.protocol.ClientProtocol" 
	#"org.apache.hadoop.mapred.JobSubmissionProtocol" 
	
	requestheader.clientProtocolVersion= 1  # not sure what is this 2,28 
	'''serialize this ''' 
	s_requestheader = requestheader.SerializeToString()

	'''Now we need to write our actual request....may be lets start it here  '''  
	request = yarn_service_protos_pb2.GetClusterNodesRequestProto()
	#request = ClientNamenodeProtocol_pb2.GetServerDefaultsRequestProto() 
	s_request = request.SerializeToString() # random shit 


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
	
	responseObject = yarn_service_protos_pb2.GetClusterNodesResponseProto()
	print responseObject
	#responseObject = ClientNamenodeProtocol_pb2.GetServerDefaultsResponseProto() 
	#get_message(sock,responseObject)
	
	#stream = recv_rpc_message(sock)
	#parse_response(stream, mr_service_protos_pb2.GetJobReportResponseProto)
	print "reading response"
	print repr(sock.recv(4096))


def main():
	print "I m in Main"
	createMsg()

main()
