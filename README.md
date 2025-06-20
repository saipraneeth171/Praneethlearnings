# Run this into terminal first
%%pip install paho-mqtt
# Cell 1: Import required libraries
import json
import time
import threading
from datetime import datetime
import paho.mqtt.client as mqtt
import ssl
import hashlib
import hmac
import base64
from urllib.parse import quote_plus
import logging
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cell 2: Configuration
class IoTStreamingConfig:
    def __init__(self):
        # File paths
        self.TELEMATICS_DATA_FILE = r"E:\Data\IOT_Telematics\iot_telematics_data.json"
        self.DEVICE_CREDENTIALS_FILE = "device_connection_strings.json"
        
        # Azure IoT Hub settings
        self.IOT_HUB_NAME = **mention-your-hostname**
        self.MQTT_PORT = 8883
        self.MQTT_KEEPALIVE = 60
        
        # Streaming settings
        self.RECORDS_PER_MINUTE = 12  # 700 records / ~58 minutes
        self.STREAMING_DURATION_MINUTES = 60  # Total streaming time
        self.DELAY_BETWEEN_RECORDS = 60 / self.RECORDS_PER_MINUTE  # ~5 seconds
        
        print(f"Configuration:")
        print(f"- Records per minute: {self.RECORDS_PER_MINUTE}")
        print(f"- Delay between records: {self.DELAY_BETWEEN_RECORDS:.1f} seconds")
        print(f"- Total streaming duration: {self.STREAMING_DURATION_MINUTES} minutes")
config = IoTStreamingConfig()


# Cell 3: Azure IoT Hub MQTT Authentication
class AzureIoTMQTTClient:
    def __init__(self, device_id, device_key, iot_hub_name):
        self.device_id = device_id
        self.device_key = device_key
        self.iot_hub_name = iot_hub_name
        self.client = None
        self.is_connected = False
        
    def generate_sas_token(self, expiry_hours=24):
        """
        Generate SAS token for Azure IoT Hub authentication
        
        Purpose: Create secure authentication token for MQTT connection
        """
        try:
            # Create the resource URI
            resource_uri = f"{self.iot_hub_name}/devices/{self.device_id}"
            
            # Calculate expiry time
            expiry_timestamp = int(time.time() + expiry_hours * 3600)
            
            # Create the string to sign
            string_to_sign = f"{quote_plus(resource_uri)}\n{expiry_timestamp}"
            
            # Generate signature
            key = base64.b64decode(self.device_key)
            signature = base64.b64encode(
                hmac.new(key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
            ).decode('utf-8')
            
            # Create SAS token
            sas_token = f"SharedAccessSignature sr={quote_plus(resource_uri)}&sig={quote_plus(signature)}&se={expiry_timestamp}"
            
            return sas_token
            
        except Exception as e:
            logger.error(f"Failed to generate SAS token for {self.device_id}: {str(e)}")
            return None
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            self.is_connected = True
            logger.info(f"âœ“ Device {self.device_id} connected to IoT Hub")
        else:
            self.is_connected = False
            logger.error(f"âœ— Device {self.device_id} failed to connect. Code: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback"""
        self.is_connected = False
        logger.info(f"Device {self.device_id} disconnected from IoT Hub")
    
    def on_publish(self, client, userdata, mid):
        """MQTT publish callback"""
        logger.debug(f"Message {mid} published for device {self.device_id}")
    
    def connect(self):
        """
        Connect device to Azure IoT Hub using MQTT
        
        Purpose: Establish secure MQTT connection for data streaming
        """
        try:
            # Generate SAS token
            sas_token = self.generate_sas_token()
            if not sas_token:
                return False
            
            # Create MQTT client
            self.client = mqtt.Client(client_id=self.device_id, protocol=mqtt.MQTTv311)
            
            # Set authentication
            self.client.username_pw_set(
                username=f"{self.iot_hub_name}/{self.device_id}/?api-version=2021-04-12",
                password=sas_token
            )
            
            # Configure SSL/TLS
            self.client.tls_set(ca_certs=None, certfile=None, keyfile=None, 
                               cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, 
                               ciphers=None)
            
            # Set callbacks
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_publish = self.on_publish
            
            # Connect to IoT Hub
            logger.info(f"Connecting device {self.device_id} to IoT Hub...")
            self.client.connect(self.iot_hub_name, config.MQTT_PORT, config.MQTT_KEEPALIVE)
            self.client.loop_start()
            
            # Wait for connection
            timeout = 10
            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < timeout:
                time.sleep(0.5)
            
            return self.is_connected
            
        except Exception as e:
            logger.error(f"Failed to connect device {self.device_id}: {str(e)}")
            return False
    
    def send_telemetry(self, telemetry_data):
        """
        Send telemetry data to IoT Hub
        
        Purpose: Publish JSON telemetry data to Azure IoT Hub
        """
        try:
            if not self.is_connected:
                logger.error(f"Device {self.device_id} not connected")
                return False
            
            # IoT Hub telemetry topic
            topic = f"devices/{self.device_id}/messages/events/"
            
            # Convert data to JSON string
            payload = json.dumps(telemetry_data)
            
            # Publish message
            result = self.client.publish(topic, payload, qos=1)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"âœ“ Telemetry sent from {self.device_id}: {len(telemetry_data.get('events', []))} events")
                return True
            else:
                logger.error(f"âœ— Failed to send telemetry from {self.device_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending telemetry from {self.device_id}: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from IoT Hub"""
        try:
            if self.client:
                self.client.loop_stop()
                self.client.disconnect()
            logger.info(f"Device {self.device_id} disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting {self.device_id}: {str(e)}")


# Cell 4: Data Loading and Device Management
class IoTDataStreamer:
    def __init__(self):
        self.device_clients = {}
        self.telemetry_data = []
        self.device_credentials = {}
        self.streaming_stats = {
            'total_records': 0,
            'sent_records': 0,
            'failed_records': 0,
            'start_time': None,
            'devices_connected': 0
        }
    
    def load_device_credentials(self):
        """
        Load device connection strings and extract credentials
        
        Purpose: Get device keys needed for MQTT authentication
        """
        try:
            with open(config.DEVICE_CREDENTIALS_FILE, 'r') as f:
                connection_strings = json.load(f)
            
            for device_id, conn_string in connection_strings.items():
                # Parse connection string to extract device key
                parts = conn_string.split(';')
                device_key = None
                
                for part in parts:
                    if part.startswith('SharedAccessKey='):
                        device_key = part.split('=', 1)[1]
                        break
                
                if device_key:
                    self.device_credentials[device_id] = device_key
                else:
                    logger.error(f"Could not extract key for device {device_id}")
            
            logger.info(f"âœ“ Loaded credentials for {len(self.device_credentials)} devices")
            return True
            
        except FileNotFoundError:
            logger.error(f"Device credentials file not found: {config.DEVICE_CREDENTIALS_FILE}")
            logger.info("Please run the device provisioning script first!")
            return False
        except Exception as e:
            logger.error(f"Failed to load device credentials: {str(e)}")
            return False
    
    def load_telemetry_data(self):
        """
        Load telemetry data from JSON file
        
        Purpose: Read the generated IoT telematics data for streaming
        """
        try:
            with open(config.TELEMATICS_DATA_FILE, 'r') as f:
                self.telemetry_data = json.load(f)
            
            self.streaming_stats['total_records'] = len(self.telemetry_data)
            logger.info(f"âœ“ Loaded {len(self.telemetry_data)} telemetry records")
            
            # Show sample data
            if self.telemetry_data:
                sample = self.telemetry_data[0]
                logger.info(f"Sample record: Device {sample['device_id']}, Trip {sample['trip_id']}, Events: {len(sample['events'])}")
            
            return True
            
        except FileNotFoundError:
            logger.error(f"Telemetry data file not found: {config.TELEMATICS_DATA_FILE}")
            logger.info("Please run the data generation script first!")
            return False
        except Exception as e:
            logger.error(f"Failed to load telemetry data: {str(e)}")
            return False
    
    def connect_all_devices(self):
        """
        Connect all devices to IoT Hub
        
        Purpose: Establish MQTT connections for all devices before streaming
        """
        logger.info("Connecting devices to IoT Hub...")
        
        for device_id, device_key in self.device_credentials.items():
            try:
                client = AzureIoTMQTTClient(device_id, device_key, config.IOT_HUB_NAME)
                
                if client.connect():
                    self.device_clients[device_id] = client
                    self.streaming_stats['devices_connected'] += 1
                    logger.info(f"âœ“ {device_id} connected")
                else:
                    logger.error(f"âœ— Failed to connect {device_id}")
                    
            except Exception as e:
                logger.error(f"Error connecting {device_id}: {str(e)}")
        
        logger.info(f"Connected {self.streaming_stats['devices_connected']}/{len(self.device_credentials)} devices")
        return self.streaming_stats['devices_connected'] > 0
    
    def stream_data(self):
        """
        Main data streaming function
        
        Purpose: Stream telemetry data to IoT Hub at configured rate
        """
        if not self.telemetry_data:
            logger.error("No telemetry data loaded")
            return
        
        if not self.device_clients:
            logger.error("No devices connected")
            return
        
        logger.info("Starting data streaming...")
        logger.info(f"Streaming {len(self.telemetry_data)} records at {config.RECORDS_PER_MINUTE} records/minute")
        
        self.streaming_stats['start_time'] = datetime.now()
        
        for i, record in enumerate(self.telemetry_data, 1):
            try:
                device_id = record['device_id']
                
                # Check if device is connected
                if device_id not in self.device_clients:
                    logger.warning(f"Device {device_id} not connected, skipping record")
                    self.streaming_stats['failed_records'] += 1
                    continue
                
                # Send telemetry data
                client = self.device_clients[device_id]
                success = client.send_telemetry(record)
                
                if success:
                    self.streaming_stats['sent_records'] += 1
                else:
                    self.streaming_stats['failed_records'] += 1
                
                # Progress update
                if i % 10 == 0:
                    elapsed = (datetime.now() - self.streaming_stats['start_time']).total_seconds() / 60
                    logger.info(f"Progress: {i}/{len(self.telemetry_data)} records sent ({elapsed:.1f} min elapsed)")
                
                # Rate limiting - wait before next record
                if i < len(self.telemetry_data):  # Don't wait after last record
                    time.sleep(config.DELAY_BETWEEN_RECORDS)
                
            except Exception as e:
                logger.error(f"Error streaming record {i}: {str(e)}")
                self.streaming_stats['failed_records'] += 1
        
        # Final statistics
        elapsed_total = (datetime.now() - self.streaming_stats['start_time']).total_seconds() / 60
        logger.info(f"Streaming completed in {elapsed_total:.1f} minutes")
        logger.info(f"Total sent: {self.streaming_stats['sent_records']}")
        logger.info(f"Total failed: {self.streaming_stats['failed_records']}")
    
    def disconnect_all_devices(self):
        """Disconnect all devices from IoT Hub"""
        logger.info("Disconnecting all devices...")
        for device_id, client in self.device_clients.items():
            client.disconnect()
        self.device_clients.clear()

# Cell 5: Main execution function
def main():
    """
    Main execution function for IoT data streaming
    
    Purpose: Orchestrate the complete streaming workflow
    """
    streamer = IoTDataStreamer()
    
    try:
        print("=== IoT Telematics Data Streaming ===")
        print(f"Target file: {config.TELEMATICS_DATA_FILE}")
        print()
        
        # Step 1: Load device credentials
        logger.info("Step 1: Loading device credentials...")
        if not streamer.load_device_credentials():
            return False
        
        # Step 2: Load telemetry data
        logger.info("Step 2: Loading telemetry data...")
        if not streamer.load_telemetry_data():
            return False
        
        # Step 3: Connect devices to IoT Hub
        logger.info("Step 3: Connecting devices to IoT Hub...")
        if not streamer.connect_all_devices():
            logger.error("Failed to connect any devices")
            return False
        
        # Step 4: Stream data
        logger.info("Step 4: Starting data streaming...")
        streamer.stream_data()
        
        return True
        
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Streaming failed: {str(e)}")
        return False
    finally:
        # Always disconnect devices
        streamer.disconnect_all_devices()

# Cell 6: Execute streaming
if __name__ == "__main__":
    success = main()
    if success:
        print("\nâœ“ Data streaming completed successfully!")
    else:
        print("\nâœ— Data streaming failed!")
