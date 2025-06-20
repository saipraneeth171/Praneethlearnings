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
