# nifi-api-client-python

**Work in progress, but this can be used**

This is a simple Python client used to make requests to the NiFi REST API (**1.x**). The goal is to ease the monitoring and management of NiFi throught scripted calls.

The client support non-secured installation and secured (with login/password) installation. Be aware that, at the moment, there is no certificate check when making requests over SSL.

Due to problems with the ``requests`` Python module when making requests over SSL, the client is performing calls using the ``curl`` command.

## Usage

```shell
$ python nifi-client-python.py -h
usage: nifi-client-python.py [-h] [--login LOGIN] [--password PASSWORD]
                             [--debug] --url URL --action
                             {list-processors,list-connections,list-input-processors,stop-input-processors,start-input-processors,status,bulletins,cluster,node,disconnect,connect,decommission,remove}
                             [--node NODE]

Python client to call NiFi REST API.

optional arguments:
  -h, --help            show this help message and exit
  --login LOGIN         Login to use if NiFi is secured
  --password PASSWORD   Password to use if NiFi is secured
  --debug               Enables debug mode

Required arguments:
  --url URL             NiFi API endpoint, Ex: http://localhost:8080/nifi-api
  --action {list-processors,list-connections,list-input-processors,stop-input-processors,start-input-processors,status,bulletins,cluster,node,disconnect,connect,decommission}
                        Action to execute

Arguments for node related actions:
  --node NODE           Node address to use for the action
```

If debug mode is enabled with ``--debug``, the commands sent to NiFi will be displayed as well as received responses.

## Actions
### List processors
This will return the list of all the processors ID on the canvas (recursively).
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action list-processors
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action list-processors
```

### List connections
This will return the list of all the connections ID on the canvas (recursively).
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action list-connections
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action list-connections
```

### List input processors
This will return the list of all the processors ID on the canvas (recursively) considered as **input** processors, meaning processors with no input relationship, in other words processors that are *probably* responsible for getting data into NiFi.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action list-input-processors
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action list-input-processors
```

### Stop input processors
This will stop all the input processors listed by the action *list-input-processors*
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action stop-input-processors
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action stop-input-processors
```

### Start input processors
This will start all the input processors listed by the action *list-input-processors*
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action start-input-processors
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action start-input-processors
```

### Status
This will display the current status of NiFi based on the following conditions: NiFi is OK if there is no back pressure enabled in any of the connections AND there is no bulletin available. Otherwise, NiFi is considered as NOK (meaning, a manual check could be a good idea).
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action status
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action status
```

### Bulletins
This will list the bulletins that can be retrieved in the bulletins board though the NiFi UI.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action bulletins
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action bulletins
```

### Cluster
This will display a summary status of the NiFi cluster.
```shell
$ python nifi-client-python.py --url http://localhost:8080/nifi-api --action cluster
$ python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action cluster
d403e2c0-44a7-4c1c-aaa4-fed2ee3d6993	node-1	CONNECTED	3,777 / 0 bytes
59f7d393-8676-4d40-bf0f-df353c761cdf	node-2	CONNECTED	13,000 / 900.68 KB
26233020-b9ea-41c5-a58a-86026cfac8e4	node-3	CONNECTED	0 / 0 bytes
```

### Node
This will display a JSON representing the status of the given node.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action node --node node-1
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action node --node node-1
```

### Disconnect
This will disconnect the given node from the cluster.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action disconnect --node node-1
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action disconnect --node node-1
```

### Connect
This will connect the given node to the cluster.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action connect --node node-1
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action connect --node node-1
```

### Decommission
The objective of this command is to disconnect a node, stop the input processors on the disconnected node alone (see above) and wait long enough to have all the queued flow files processed by this node. Once done, this would typically followed by a NiFi shutdown for administration tasks (upgrade, configuration change, custom NAR deployment, etc). If the number of queued flow files is unchanged longer than 60 seconds, the command will stop with a warning message.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action decommission --node node-1
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action decommission --node node-1
```

### Remove
This will remove the given node from the cluster. The node needs to be disconnected first.
```shell
python nifi-client-python.py --url http://localhost:8080/nifi-api --action remove --node node-1
python nifi-client-python.py --url https://localhost:9443/nifi-api --login test --password test --action remove --node node-1
```

## Contributions
I'm not used to Python and there is a lot of possible improvements. Contributions through pull requests / issues are greatly welcomed.
