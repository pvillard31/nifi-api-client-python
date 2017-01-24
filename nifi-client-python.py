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

_endpoint_pg_root = "/flow/process-groups/root"
_endpoint_pg_id = "/flow/process-groups/"
_endpoint_connection_id = "/connections/"
_endpoint_processor_id = "/processors/"
_endpoint_token = "/access/token"
_endpoint_bulletins = "/flow/bulletin-board"


def getToken():
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



def execRequest( url, type = "GET", data = None):
	if( type == "GET" ):
		curl = "curl -k " + url + " -H 'Authorization: Bearer " + token + "'"
		
	elif( type == "POST" ):
		if( data is None):
			curl = "curl -k -X POST " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "'"
		else:
			curl = "curl -k -X POST " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "' --data \"" + data + "\""
	
	elif( type == "PUT" ):
		if( data is None):
			curl = "curl -k -X PUT " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "'"
		else:
			curl = "curl -k -X PUT " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "' --data \"" + data + "\""
	
	elif( type == "DELETE" ):
		if( data is None):
			curl = "curl -k -X DELETE " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "'"
		else:
			curl = "curl -k -X DELETE " + url + " -H 'Accept: application/json' -H 'Authorization: Bearer " + token + "' --data \"" + data + "\""
	
	else:
		print "Type " + type + " not supported"
		
	p = subprocess.Popen(curl, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	
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



def listIds( component, parent = None, isRecursive = False ):
	endpoint = None
	if( parent is None ):
		endpoint = _endpoint_pg_root
	else:
		endpoint = _endpoint_pg_id + parent
	
	response = execRequest(url + endpoint, "GET")
	jData = json.loads(response)
	ids = getIds(jData, component)
	
	if( isRecursive ):
		pgIds = getIds(jData, "processGroups")
		for id in pgIds:
			ids.extend(listIds(component, id, True))
		return ids
	else:
		return ids



def getProcessorFromConnection( connectionId, position):
	endpoint = _endpoint_connection_id + connectionId
	response = execRequest(url + endpoint, "GET")
	jData = json.loads(response)
	
	if( position == "source" ):
		if ( jData['sourceType'] == "PROCESSOR" ):
			return jData['sourceId']
	else:
		if ( jData['destinationType'] == "PROCESSOR" ):
			return jData['destinationId']



def listInputProcessorId( parent = None, isRecursive = False ):
	connectionsId = listConnectionsId(parent, isRecursive)
	processorsId = listProcessorsId(parent, isRecursive)
	
	for connection in connectionsId:
		destinationId = getProcessorFromConnection( connection, "destination" )
		if(destinationId in processorsId):
			processorsId.remove(destinationId)
	return processorsId



def listProcessorsId( parent = None, isRecursive = False ):
	return listIds("processors", parent, isRecursive)



def listConnectionsId( parent = None, isRecursive = False ):
	return listIds("connections", parent, isRecursive)



def updateProcessor( processorId, action ):
	endpoint = _endpoint_processor_id + processorId
	response = execRequest(url + endpoint, "PUT", json.dumps({"revision":getProcessorRevision(processorId), "component":{"id":processorId, "state":action}}))
	print "Stopped processor " + processorId



def updateInputProcessors( action, parent = None, isRecursive = False ):
	processorsId = listInputProcessorId(parent, isRecursive)
	for processor in processorsId:
		updateProcessor(processor, action)



def getProcessorRevision( processorId ):
	endpoint = _endpoint_processor_id + processorId
	response = execRequest(url + endpoint, "GET")
	jData = json.loads(response)
	return jData['revision']



def isBackpressureEnabled( connectionId ):
	endpoint = _endpoint_connection_id + connectionId
	response = execRequest(url + endpoint, "GET")
	jData = json.loads(response)
	return jData['status']['aggregateSnapshot']['percentUseCount'] == "100" or jData['status']['aggregateSnapshot']['percentUseBytes'] == "100"



def getPgRootId():
	response = execRequest(url + _endpoint_pg_root, "GET")
	jData = json.loads(response)
	return jData['processGroupFlow']['id']



def getBulletins( processGroupId ):
	response = execRequest(url + _endpoint_pg_id + processGroupId, "GET")
	jData = json.loads(response)
	return jData['bulletins']



def getBulletinsBoard():
	response = execRequest(url + _endpoint_bulletins + "?after=0", "GET")
	return json.loads(response)


def getNiFiStatus():
	# check if back pressure is enabled on a connection
	connections = listConnectionsId(None, True)
	warning = False
	for connectionId in connections:
		if( isBackpressureEnabled(connectionId) ):
			print "WARNING: back pressure is enabled on connection " + connectionId
			warning = True
			
	# check if there are bulletins
	jData = getBulletinsBoard()
	nbBulletins = len(jData['bulletinBoard']['bulletins'])
	if( nbBulletins != 0 ):
		warning = True
		print "WARNING: there are bulletins, use 'bulletins' command to display bulletins"
	
	if( warning ):
		print "NiFi is NOK"
	else:
		print "NiFi is OK"



def showBulletins():
	data = getBulletinsBoard()
	for bulletin in data['bulletinBoard']['bulletins']:
		print bulletin['sourceId'] + "\t" + bulletin['timestamp'] + "\t" + bulletin['bulletin']['message']


###################################################################################################
###################################################################################################
###################################################################################################

possibleActions = ["list-processors", "list-connections", "list-input-processors", "stop-input-processors",
						"start-input-processors", "status", "bulletins"]

parser = argparse.ArgumentParser(description='Python client to call NiFi REST API.')

parser.add_argument('--login', help='Login to use if NiFi is secured')
parser.add_argument('--password', help='Password to use if NiFi is secured')

requiredNamed = parser.add_argument_group('Required arguments')
requiredNamed.add_argument('--url', help='NiFi API endpoint, Ex: http://localhost:8080/nifi-api', required=True)
requiredNamed.add_argument('--action', choices=possibleActions, help='Action to execute', required=True)

args = parser.parse_args()

url = args.url
action = args.action
login = args.login
password = args.password

if( url.startswith('https') ):
	token = getToken()

if( action == "list-processors" ):
    print listProcessorsId(None, True)
elif( action == "list-connections" ):
	print listConnectionsId(None, True)
elif( action == "list-input-processors" ):
	print listInputProcessorId(None, True)
elif( action == "stop-input-processors" ):
	updateInputProcessors("STOPPED", None, True)
elif( action == "start-input-processors" ):
	updateInputProcessors("RUNNING", None, True)
elif( action == "status" ):
	getNiFiStatus()
elif( action == "bulletins" ):
	showBulletins()
else:
	print "ERROR, unknown action " + action
	





















