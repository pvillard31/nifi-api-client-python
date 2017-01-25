#! /usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import json
import argparse
import subprocess
import datetime
import os
import re
import urllib
import time

_endpoint_pg_root = "/flow/process-groups/root"
_endpoint_pg_id = "/flow/process-groups/"
_endpoint_connection_id = "/connections/"
_endpoint_processor_id = "/processors/"
_endpoint_token = "/access/token"
_endpoint_bulletins = "/flow/bulletin-board"
_endpoint_cluster = "/controller/cluster"
_endpoint_node = "/controller/cluster/nodes/"
_endpoint_flow_status = "/flow/status"


def getToken( url ):
	endpoint = url + _endpoint_token
	data = urllib.urlencode({"username":login, "password":password})
	p = subprocess.Popen("curl -k -X POST " + endpoint + " -H 'application/x-www-form-urlencoded' --data \"" + data + "\"",
							shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if p.returncode == 0:
		return out
	else:
		print "Failed to get token"
		print err
		print out
		return None



def execRequest( url, token, type = "GET", data = None):
	if( type == "GET" ):
		curl = "curl -k " + url + " -H 'Authorization: Bearer " + token + "'"
		
	elif( type == "POST" ):
		if( data is None):
			curl = "curl -k -X POST " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "'"
		else:
			curl = "curl -k -X POST " + url + " -H 'Content-Type: application/json' -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "' --data '" + data + "'"
	
	elif( type == "PUT" ):
		if( data is None):
			curl = "curl -k -X PUT " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "'"
		else:
			curl = "curl -k -X PUT " + url + " -H 'Content-Type: application/json' -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "' --data '" + data + "'"
	
	elif( type == "DELETE" ):
		if( data is None):
			curl = "curl -k -X DELETE " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "'"
		else:
			curl = "curl -k -X DELETE " + url + " -H 'Content-Type: application/json' -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "' --data '" + data + "'"
	
	else:
		print "Type " + type + " not supported"
	
	if( debug ):
		curl = curl + " -v"
		print "CURL---------------------------"
		print curl
		print "CURL---------------------------"
	
	p = subprocess.Popen(curl, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	
	if( debug ):
		print "OUT---------------------------"
		print out
		print "OUT---------------------------"
		print "ERR---------------------------"
		print err
		print "ERR---------------------------"
	
	if p.returncode == 0:
		return out
	else:
		print "Failed to request " + url
		print err
		print out
		return None



def getIds( jData, component ):
	ids = []
	for processor in jData['processGroupFlow']['flow'][component]:
		ids.append(processor['id'])
	return ids



def listIds( url, token, component, parent = None, isRecursive = False ):
	endpoint = None
	if( parent is None ):
		endpoint = _endpoint_pg_root
	else:
		endpoint = _endpoint_pg_id + parent
	
	response = execRequest(url + endpoint, token, "GET")
	jData = json.loads(response)
	ids = getIds(jData, component)
	
	if( isRecursive ):
		pgIds = getIds(jData, "processGroups")
		for id in pgIds:
			ids.extend(listIds(url, token, component, id, True))
		return ids
	else:
		return ids



def getProcessorFromConnection( url, token, connectionId, position):
	endpoint = _endpoint_connection_id + connectionId
	response = execRequest(url + endpoint, token, "GET")
	jData = json.loads(response)
	
	if( position == "source" ):
		if ( jData['sourceType'] == "PROCESSOR" ):
			return jData['sourceId']
	else:
		if ( jData['destinationType'] == "PROCESSOR" ):
			return jData['destinationId']



def listInputProcessorId( url, token, parent = None, isRecursive = False ):
	connectionsId = listConnectionsId(url, token, parent, isRecursive)
	processorsId = listProcessorsId(url, token, parent, isRecursive)
	
	for connection in connectionsId:
		destinationId = getProcessorFromConnection( url, token, connection, "destination" )
		if(destinationId in processorsId):
			processorsId.remove(destinationId)
	return processorsId



def listProcessorsId( url, token, parent = None, isRecursive = False ):
	return listIds(url, token, "processors", parent, isRecursive)



def listConnectionsId( url, token, parent = None, isRecursive = False ):
	return listIds(url, token, "connections", parent, isRecursive)



def updateProcessor( url, token, processorId, action ):
	endpoint = _endpoint_processor_id + processorId
	response = execRequest(url + endpoint, token, "PUT", json.dumps({"revision":getProcessorRevision(url, token, processorId), "component":{"id":processorId, "state":action}}))
	print "Updated processor " + processorId + " to " + action



def updateInputProcessors( url, token, action, parent = None, isRecursive = False ):
	processorsId = listInputProcessorId(url, token, parent, isRecursive)
	for processor in processorsId:
		updateProcessor(url, token, processor, action)



def getProcessorRevision( url, token, processorId ):
	endpoint = _endpoint_processor_id + processorId
	response = execRequest(url + endpoint, token, "GET")
	jData = json.loads(response)
	return jData['revision']



def isBackpressureEnabled( url, token, connectionId ):
	endpoint = _endpoint_connection_id + connectionId
	response = execRequest(url + endpoint, token, "GET")
	jData = json.loads(response)
	return jData['status']['aggregateSnapshot']['percentUseCount'] == "100" or jData['status']['aggregateSnapshot']['percentUseBytes'] == "100"



def getPgRootId( url, token ):
	response = execRequest(url + _endpoint_pg_root, token, "GET")
	jData = json.loads(response)
	return jData['processGroupFlow']['id']



def getBulletins( url, token, processGroupId ):
	response = execRequest(url + _endpoint_pg_id + processGroupId, token, "GET")
	jData = json.loads(response)
	return jData['bulletins']



def getBulletinsBoard( url, token ):
	response = execRequest(url + _endpoint_bulletins + "?after=0", token, "GET")
	return json.loads(response)


def getNiFiStatus( url, token ):
	# check if back pressure is enabled on a connection
	connections = listConnectionsId(url, token, None, True)
	warning = False
	for connectionId in connections:
		if( isBackpressureEnabled(url, token, connectionId) ):
			print "WARNING: back pressure is enabled on connection " + connectionId
			warning = True
			
	# check if there are bulletins
	jData = getBulletinsBoard(url, token)
	nbBulletins = len(jData['bulletinBoard']['bulletins'])
	if( nbBulletins != 0 ):
		warning = True
		print "WARNING: there are bulletins, use 'bulletins' command to display bulletins"
	
	if( warning ):
		print "NiFi is NOK"
	else:
		print "NiFi is OK"



def showBulletins( url, token ):
	data = getBulletinsBoard(url, token)
	for bulletin in data['bulletinBoard']['bulletins']:
		if ( "sourceId" in bulletin.keys() ):
			print bulletin['sourceId'] + "\t" + bulletin['timestamp'] + "\t" + bulletin['bulletin']['message']
		else:
			print "\t\t\t\t" + bulletin['timestamp'] + "\t" + bulletin['bulletin']['message']



def showCluster( url, token ):
	response = execRequest(url + _endpoint_cluster, token, "GET")
	jData = json.loads(response)
	for node in jData['cluster']['nodes']:
		print node['nodeId'] + "\t" + node['address'] + "\t" + node['status'] + "\t" + node['queued']



def getNodeId( url, token, nodeAddress ):
	response = execRequest(url + _endpoint_cluster, token, "GET")
	jData = json.loads(response)
	for node in jData['cluster']['nodes']:
		if( node['address'] == nodeAddress):
			return node['nodeId']
	return None



def showNode( url, token, nodeAddress ):
	response = execRequest(url + _endpoint_node + getNodeId(url, nodeAddress), token, "GET")
	jData = json.loads(response)
	print jData
	return jData



def disconnect( url, token, nodeAddress ):
	nodeId = getNodeId(url, token, nodeAddress)
	endpoint = _endpoint_node + nodeId
	response = execRequest(url + endpoint, token, "PUT", json.dumps({"node":{"nodeId": nodeId, "status": "DISCONNECTING"}}))
	print "Disconnected node " + nodeAddress + " (" + nodeId + ")"



def connect( url, token, nodeAddress ):
	nodeId = getNodeId(url, token, nodeAddress)
	endpoint = _endpoint_node + nodeId
	response = execRequest(url + endpoint, token, "PUT", json.dumps({"node":{"nodeId": nodeId, "status": "CONNECTING"}}))
	print "Connected node " + nodeAddress + " (" + nodeId + ")"



def getQueuedFlowFiles( url, token ):
	response = execRequest(url + _endpoint_flow_status, token, "GET")
	jData = json.loads(response)
	return jData['controllerStatus']['flowFilesQueued']



def decommission( url, token, nodeAddress ):
	# check that url is not using the node we want to disconnect
	if ( nodeAddress in url ):
		print "Please do not use the node to disconnect in the URL"
		return
	
	# disconnecting node to decommission
	disconnect(url, token, nodeAddress)
	
	# get API URL of the node we disconnected
	nodeUrl = re.search('.*://(.*):.*', url)
	if ( nodeUrl ):
		node = nodeUrl.group(1)
	urlNode = url.replace(node, nodeAddress)
	
	# get token on node to disconnect
	tokenNode = getToken(urlNode)
	
	# stop input processors
	updateInputProcessors(urlNode, tokenNode, "STOPPED", None, True)
	
	# wait until not more queued flow file
	currentNb = getQueuedFlowFiles(urlNode, tokenNode)
	previousNb = 0
	idleTime = 0
	it = 1
	while (currentNb != 0):
		time.sleep(it)
		previousNb = currentNb
		currentNb = getQueuedFlowFiles(urlNode, tokenNode)
		print "Current number of queued flow files = " + str(currentNb)
		if(currentNb >= previousNb):
			idleTime = idleTime + it
		else:
			idleTime = 0
		if(idleTime > 60):
			print "WARNING - Number of queued flow files unchanged or increased after 60 seconds - process stopped"
			return
	print "Node " + nodeAddress + " successfully decommissioned"
	return
	



###################################################################################################
###################################################################################################
###################################################################################################

possibleActions = ["list-processors", "list-connections", "list-input-processors", "stop-input-processors",
						"start-input-processors", "status", "bulletins", "cluster", "node", "disconnect",
						"connect", "decommission"]

parser = argparse.ArgumentParser(description='Python client to call NiFi REST API.')

parser.add_argument('--login', help='Login to use if NiFi is secured')
parser.add_argument('--password', help='Password to use if NiFi is secured')
parser.add_argument('--debug', help='Password to use if NiFi is secured', action='store_true')

requiredNamed = parser.add_argument_group('Required arguments')
requiredNamed.add_argument('--url', help='NiFi API endpoint, Ex: http://localhost:8080/nifi-api', required=True)
requiredNamed.add_argument('--action', choices=possibleActions, help='Action to execute', required=True)

nodeArgs = parser.add_argument_group('Arguments for node related actions')
nodeArgs.add_argument('--node', help='Node address to use for the action', required=False)

args = parser.parse_args()

url = args.url
action = args.action
login = args.login
password = args.password
node = args.node
debug = args.debug

if( url.startswith('https') ):
	token = getToken(url)

if( action == "list-processors" ):
    print listProcessorsId(url, token, None, True)
elif( action == "list-connections" ):
	print listConnectionsId(url, token, None, True)
elif( action == "list-input-processors" ):
	print listInputProcessorId(url, token, None, True)
elif( action == "stop-input-processors" ):
	updateInputProcessors(url, token, "STOPPED", None, True)
elif( action == "start-input-processors" ):
	updateInputProcessors(url, token, "RUNNING", None, True)
elif( action == "status" ):
	getNiFiStatus(url, token)
elif( action == "bulletins" ):
	showBulletins(url, token)
elif( action == "cluster" ):
	showCluster(url, token)
elif( action == "node" ):
	showNode(url, token, node)
elif( action == "disconnect" ):
	disconnect(url, token, node)
elif( action == "connect" ):
	connect(url, token, node)
elif( action == "decommission" ):
	decommission(url, token, node)
else:
	print "ERROR, unknown action " + action
	





















