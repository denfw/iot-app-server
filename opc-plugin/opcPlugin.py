#!/usr/bin/env python3
import configparser
from opcua import Client
from opcua import ua
import paho.mqtt.client as mqtt
import json
import logging
import sys
import time
from pysnmp.hlapi import *
import shelve

## TODO: Add Sphinx
## TODO: Add secure login methods
# TEST TRAVIS
# Handler class for OPC/UA events
class SubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """
    def __init__(self):
        # Singelton instance of the main controll class
        self.control = Control()
        # Dict of status nodes -> remembers the last value to decide
        self.nodes = {}

    # Check if PLC workload is running
    def checkProcess(self,node,val):
        if val == 0:
            # Process have stopped => reset/slow down polling interval 
            self.control.resetPollInterval(self)
        else:
            # Process have started => change/speed up polling interval
            self.control.changePollInterval()

    # Datachange event from the OPC/UA server
    def datachange_notification(self, node, val, data):
        #debug example: print("OPC/UA: New data change event", node, val,type(data),data)

        if node in self.nodes:
            # Check control value
            self.checkProcess(node,val)
        else:
            # Create a first node
            self.nodes[node] = val
            self.checkProcess(node,val)
            
# OpcClient class to handle all OPC/UA communication 
class OpcClient:
    def __init__(self, opc_url, variables, settings, persistency, history_length):
        # OPC/UA server url
        self.opc_url = opc_url
        # OPC/UA variables addresses
        self.variables = variables
        # OPC/UA variables config parameters
        self.settings = settings
        # subscription objects
        self.handlers = {}
        self.subscription = None
        # OPC/UA connection from client to a server
        self.client = None
        # Local registers
        self.registers = {}
        # State flag
        self.init = True
        # Persistency flag
        self.persistency = persistency
        # History length allocation
        self.history_length = int(history_length)

    # Create session to the OPC/UA server
    def login(self):
        # Init local registers
        for key, val in self.variables.items():
            self.registers[key] = {}
            self.registers[key]["min"] = None
            self.registers[key]["max"] = None
            self.registers[key]["register_timestamp"] = None

        # Create session
        try:
            self.client = Client(self.opc_url) 
            self.client.connect()
        except Exception as e:
            raise Exception("OPC/UA server is not available. Please check connectivity by cmd tools")
        logging.info("Client connected to a OPC/UA server" + str(self.opc_url))
        
    # Logout from the OPC/UA server
    def logout(self):
        try:
            self.client.disconnect()
        except Exception as e:
            raise Exception("OPC/UA server is not available for logout command. Please check connectivity by cmd tools")
        logging.info("Logout form OPC/UA server")

    # Clear value of local registers
    def clearRegister(self, name):
        self.registers[name]["min"] = None
        self.registers[name]["max"] = None
        self.registers[name]["register_timestamp"] = None

    # Store data persistently
    def storeData(self,data,key):
        pd = shelve.open(self.persist_data)
        try:
            tmp_value = data["value"]
            old_persist_value = pd[key]["value"]
            # Check lenght of stored data
            if len(old_persist_value) <= self.history_length:
                data["value"] = old_persist_value.append(tmp_value)
                pd[key] = data 
            else:
                # Remove the oldest value
                old_persist_value.pop(0)
                data["value"] = old_persist_value.append(tmp_value)
                pd[key] = data 
                
        except Exception as e:
            # Init data structure for the key
            data["value"] = [data["value"]]
            pd[key] = data

        pd.close()

    # Return stored persistent data
    def getStoredData(self, key):
        pd = shelve.open(self.persist_data)
        data = pd.get(key)
        pd.close()
        return data

    # TODO: Create support for more status variables -> right now the self.init flag is a limitation
    # Read data from OPC/UA server from predifined variables
    def pollData(self):
        data = {}
        for key, val in self.variables.items():
            node = self.client.get_node(val) 
            data[key] = {}
            data[key]["value"] = node.get_value()
            data[key]["role"] = "normal"
            data[key]["register_min"] = "n/a"
            data[key]["register_max"] = "n/a"
            data[key]["register_timestamp"] = "n/a"
            # Custom configuration parameters
            try:
                for param_key, param_val in self.settings[key].items():
                    # Add settings parameters to the data structure
                    if param_key == "register":
                        config = param_val.split(",")
                        for config_param in config:
                            if config_param == "min":
                                # Check and init the first value
                                if self.registers[key]["min"] == None:
                                    self.registers[key]["min"] = data[key]["value"]
                                    # Add timestmap for registers
                                    if self.registers[key]["register_timestamp"] == None:
                                        self.registers[key]["register_timestamp"] = time.time()*1000
                                        data[key]["register_timestamp"] = time.time()*1000

                                elif int(self.registers[key]["min"]) > int(data[key]["value"]):
                                    self.registers[key]["min"] = data[key]["value"]
                                data[key]["register_min"] = self.registers[key]["min"]
                                data[key]["register_timestamp"] = self.registers[key]["register_timestamp"]
                            elif config_param == "max":
                                # Check and init the first value
                                if self.registers[key]["max"] == None:
                                    self.registers[key]["max"] = data[key]["value"]
                                    # Add timestmap for registers
                                    if self.registers[key]["register_timestamp"] == None:
                                        self.registers[key]["register_timestamp"] = time.time()*1000
                                        data[key]["register_timestamp"] = time.time()*1000

                                elif int(self.registers[key]["max"]) < int(data[key]["value"]):
                                    self.registers[key]["max"] = data[key]["value"]
                                data[key]["register_max"] = self.registers[key]["max"]
                                data[key]["register_timestamp"] = self.registers[key]["register_timestamp"]
                            else:
                                logging.error("Invalid option for register parameter in the configuration file")
                    if param_key == "state" and self.init:
                        # Create subription
                        self.createSubscription(val)
                        self.init = False
                    if param_key == "state":
                        data[key]["role"] = "status"
            # Key for specific configuration does not exist
            except Exception as e:
                pass

            if self.persistency == "True":
                storeData(data[key],key)

        return data
         
    # Create a subscription and store the connection handle
    def createSubscription(self, address):
        try:
            handler = SubHandler()
            self.subscription = self.client.create_subscription(500, handler)
            handle = self.subscription.subscribe_data_change(self.client.get_node(address))
            self.handlers[address] = handle
        except Exception as e:
            raise Exception("Unable to create subscription to OPC/UA server address", address)

        logging.info("Subscription created for address " + address)

    # Delete subscrition 
    def unsubscribeSubscriptions(self, address=None):
        if len(self.handlers) == 0:
            return True

        # Unsubscribe defined connection handlers
        if address is not None:
            self.subscription.unsubscribe(self.handlers[address])
        # Unsubscribe all connection handlers
        else:
            for handler in self.handlers:
                self.subscription.unsubscribe(handler)
            self.subscription.delete()

        # Check handler count
        if len(self.handlers) == 0:
            # Close subscription
            self.subscription.delete()

# SNMP class to communicate with IOS-XE part 
class SnmpClient:
    def __init__(self, gw_ip, community):
        self.gw_ip = gw_ip
        self.community = community
        self.oid = {"latitude": "iso.3.6.1.4.1.9.9.661.1.4.1.1.1.4.4038",
                    "longtitude": "iso.3.6.1.4.1.9.9.661.1.4.1.1.1.5.4038",
                    "timestamp": "iso.3.6.1.4.1.9.9.661.1.4.1.1.1.6.4038"
                    }

    # Get GPS coordinates from IR1101 Cellular module
    def getCoordinates(self):
        coordinates = {"latitude":0,"longtitude":0,"timestamp":0}
        for key,val in self.oid.items():
            iterator = getCmd(SnmpEngine(),
                    CommunityData(self.community),
                    UdpTransportTarget((self.gw_ip, 161)),
                    ContextData(),
                    ObjectType(ObjectIdentity(val)))
            errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
            for varBind in varBinds:
                # Reformat timestamp vlaue to a human string
                if key == "timestamp":
                    coordinates[key] = bytes.fromhex(varBind.prettyPrint().split("=")[1].strip()[2:]).decode("utf-8")[:-1]
                else:
                    coordinates[key] = varBind.prettyPrint().split("=")[1].strip()[2:]
        return coordinates
        
# Handles all activites around MQTT 
class MqttClient:
    def __init__(self, broker,port,topic,snmp_client):
        self.broker = str(broker)
        self.topic = str(topic)
        self.port = int(port)
        self.mqtt_client = mqtt.Client(client_id="iox-app", clean_session=False)
        self.snmp_client = snmp_client
        #self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.control = None

    # Login to the MQTT broker
    def login(self):
        try:
            self.mqtt_client.connect(host=self.broker,port=int(self.port),keepalive=60)
            self.control = Control()
        except Exception as e:
            raise Exception("MQTT broker is not available. Please check connectivity by cmd tools")
        logging.info("MQTT client is connected to the broker" + self.broker)
    
    # Logout from the MQTT broker 
    def logout(self):
        self.mqtt_client.disconnect()
        logging.info("MQTT client is disconnected from the broker" + self.broker)

    # Process received message - commands
    def on_message(self,client, data, msg):
        payload_data = json.loads(str(msg.payload.decode()))
        for cmd_key, cmd_val in payload_data.items():
            if cmd_key == "poll":
                self.control.poll_interval = cmd_val
                logging.info("Received command from the server: "+cmd_key+":"+cmd_val)
            elif cmd_key == "clear":
                self.control.opc_client.clearRegister(cmd_val)
                logging.info("Received command from the server: "+cmd_key+":"+cmd_val)
            elif cmd_key == "getData":
                data = self.control.opc_client.getStoredData(cmd_val)
                logging.info("Received command from the server: "+cmd_key+":"+cmd_val)
                self.mqtt_client.publish(self.topic+cmd_val+"/storedData",payload=str(data), qos=0, retain=False)
                logging.info("Command reply sent back: ")
            else:
                logging.error("Unknown command from MQTT")

    # Send MQTT data to the broker
    def sendData(self,data):
        # Add GPS 
        gps_data = self.snmp_client.getCoordinates()
        # Prepare data records for each OPC/UA variable
        for record_key, record_val in data.items():
            # Add timestamp in ms
            # NOTE: Maybe it is better to use time from GPS
            record_val["timestamp"] = time.time()*1000
            # Latitude - check if GPS is working if not add the static value -> Charles Square, Prague, CZE
            if gps_data["latitude"][4] == "0":
                record_val["gps_lat"] = 50.0754072
            else:
                record_val["gps_lat"] = gps_data["latitude"]

            # Longtitude - check if GPS is working if not add the static value -> Charles Square, Prague, CZE
            if gps_data["longtitude"][4] == "0":
                record_val["gps_long"] = 14.4165971
            else:
                record_val["gps_long"] = gps_data["longtitude"]
            
                
            ret = self.mqtt_client.publish(self.topic+record_key,payload=str(record_val), qos=0, retain=False)

    # Subscribe to MQTT to receive commands
    def subscribe(self):
        try:
            self.mqtt_client.subscribe(self.topic+"command")
            self.mqtt_client.loop_start()
        except Exception as e:
            raise Exception("Unable to subscribe topic",self.topic+"command")

        logging.debug("MQTT topic "+self.topic+" has been subscribed")
   
 
# Class to parse configuration data        
class Config:
    def __init__(self,filename):
        self.config = configparser.ConfigParser()
        self.config.read(filename)
        
    # Get the general section
    def getGeneral(self):
        try:
            general = self.config["general"]
            # Test polling to int
            tmp = general["polling"]
            int(tmp)
            
            # Test polling to int
            tmp = general["polling_change"]
            int(tmp)

            # Simple test to ip address
            tmp = general["mqtt_broker"]
            if len(tmp.split(".")) != 4:
                raise Exception("IP adrress of MQTT broker is not formated correctly")
           
            # Simple test to port 
            tmp = general["mqtt_port"]
            int(tmp)
            
            # Simple test to opc server format
            tmp = general["opc_server"]
            if tmp.split("@")[0] != "opc.tcp://":
                raise Exception("OPC server address must start with 'opc.tcp://'")
                
            # Simple test to a mqtt format 
            tmp = general["topic_name"]
            if tmp[-1] != "/":
                raise Exception("Topic name must end with '/'")

        except Exception as e:
            logging.error("Missing mandatory General section or General parameters in     the configuration file or parameters are not formated well -> "+ str(e))
    
            
        return general
   
    # TODO: Test that strings are without quotes 
    # Get the variables section
    def getOpcVariables(self):
        variables = {}
        for key, val in self.config["variables"].items():
            variables[key] = val 
        return variables
    
    # Get custom variables settings section
    def getOpcVariablesSettings(self):
        settings = {}
        sections = self.config.sections()
        # Remote global sections
        sections.remove("general")
        sections.remove("variables")

        for section in sections:
            for key,val in self.config[section].items():
                try:    
                    settings[section][key] = val 
                # Create a first record
                except Exception as e:
                    settings[section] = {}
                    settings[section][key] = val 
        return settings

# Metaclass for singleton pattern
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# The main class to control the whole flow
class Control(metaclass=Singleton):
    def __init__(self, poll_interval=5, poll_change=1, opc_client=None, mqtt_client=None):
        self.poll_interval = int(poll_interval) 
        self.poll_change = int(poll_change)
        self.poll_normal = int(poll_interval)
        self.ready_flag = True
        self.opc_client = opc_client
        self.mqtt_client = mqtt_client

    # Change polling interval based on the configuration file
    def changePollInterval(self):
        self.poll_interval = self.poll_change

    # Reset polling interval to the default value
    def resetPollInterval(self):
        self.poll_interval = self.poll_normal

    # Start remote connections 
    def start(self):
        try:
            # Login
            self.opc_client.login()
            self.mqtt_client.login()
            self.ready_flag = True
            logging.info("MQTT and OPC connections have been established")
        except Exception as e:
            logging.error("Unable to login to a remote server -> " + str(e))
            sys.exit(1)
        try:
            self.mqtt_client.subscribe()
        except Exception as e:
            logging.error("Unable to subscribe to a remote server -> " + str(e))
            sys.exit(1)

    # Launch the main processing loop -> read and send data
    def run(self):
        data = {}
        try:
            while self.ready_flag:
                # Read OPC data
                data = self.opc_client.pollData()
                # Send them via MQTT
                self.mqtt_client.sendData(data)
                logging.debug("MQTT data have been send -> " + str(data))
                # Sleep before the next poll
                time.sleep(int(self.poll_interval))
        except Exception as e:
            logging.error("Unable to receive/send data from a remote server -> "+ str(e))
            sys.exit(1)
            
            
    # Stop all remote connections
    def stop(self):
        self.ready_flag = False
        try:
            # Logout
            self.opc_client.logout()
            self.mqtt_client.logout()
            logging.info("MQTT and OPC connection have been closed")

        except Exception as e:
            logging.error("Unable to logout from a remote server -> " + str(e))
            sys.exit(1)
            

if __name__ == "__main__":
    # Get configuration object from GUI management location
    params = Config("/data/package_config.ini")
    # General configuration parameters
    general = params.getGeneral()
    # Get OPC variables to read
    variables = params.getOpcVariables()
    # Get reading settings for variables
    settings = params.getOpcVariablesSettings()
    
    #Set logging -> used IOx file destination for logs
    debug = str(general["debug"])
    if debug == "True":
        logging.basicConfig(filename="/data/logs/"+general["log_file"],level=logging.DEBUG)
    else:
        logging.basicConfig(filename="/data/logs/"+general["log_file"],level=logging.WARNING)
    logging.debug("Configuration has been loaded")


    # Create opc, snmp mqtt client objects 
    snmp_client = SnmpClient(general["gw_ip"],general["community"])
    opc_client = OpcClient(general["opc_server"],variables,settings,general["persistency"],general["history_length"])
    mqtt_client = MqttClient(general["mqtt_broker"],general["mqtt_port"],general["topic_name"],snmp_client)
    logging.debug("OPC and MQTT objects has been created")

    # Create control object and start process
    ctl = Control(general["polling"],general["polling_change"],opc_client,mqtt_client) 
    logging.debug("Control object has been created")
    ctl.start()
    logging.debug("Control object has started")
    ctl.run()
    logging.debug("Control object is running")
        

    
