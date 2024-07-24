import paho.mqtt.client as mqtt
import psycopg2
import json
import time
from datetime import datetime

# Configuration
MQTT_BROKER = '192.168.18.19'
MQTT_PORT = 1883
MQTT_TOPICS = ['cstr-level', 'cstr-temp', 'mtank-level', 'mtank-temp']
CONTROL_TOPICS = ['mtank/in', 'mtank/out', 'cstr/heater1', 'cstr/heater2', 'cstr/in']

DATABASE_CONFIG = {
    'dbname': 'sensordata',
    'user': 'postgres',
    'password': '399584',
    'host': 'localhost',  # Assuming the database is hosted on the Raspberry Pi
    'port': '5432'        # Default PostgreSQL port
}

# Global variables to store sensor data
cstr_temp = None
mtank_temp = None
set_cstr_temp = None
over_duration = None
temp_change = None
start_time = time.time()

# Function to get set temperatures from the database
def get_set_temperatures():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT set_temp, over_duration, temp_change FROM temp_setting ORDER BY timestamp DESC LIMIT 1")
        result = cursor.fetchone()
        conn.close()
        return result if result else (None, None, None)
    except Exception as e:
        print(f"Error fetching set temperatures: {e}")
        return (None, None, None)

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with code {rc}")
    for topic in MQTT_TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
    global cstr_temp, mtank_temp

    topic = msg.topic
    payload = json.loads(msg.payload.decode())

    if topic == 'cstr-temp':
        cstr_temp = float(payload)
    elif topic == 'mtank-temp':
        mtank_temp = float(payload)

    control_relays(client)

# Function to calculate the current setpoint temperature
def calculate_setpoint(start_time, set_temp, initial_temp, over_duration, temp_change):
    elapsed_time = time.time() - start_time
    hours_elapsed = elapsed_time / 3600
    max_temp_increase = (hours_elapsed / over_duration) * temp_change
    current_setpoint = initial_temp + max_temp_increase
    return min(current_setpoint, set_temp)

# Function to control relays based on sensor data
def control_relays(client):
    global set_cstr_temp, over_duration, temp_change, start_time

    if set_cstr_temp is None:
        set_cstr_temp, over_duration, temp_change = get_set_temperatures()
        if set_cstr_temp is None:
            print("Set temperatures not available")
            return
        start_time = time.time()
    
    initial_temp = 20  # Assume an initial starting temperature; adjust as needed
    current_setpoint = calculate_setpoint(start_time, set_cstr_temp, initial_temp, over_duration, temp_change)

    if cstr_temp is not None and mtank_temp is not None:
        if cstr_temp <= current_setpoint - 5:
            client.publish('cstr/heater1', 'on')
            client.publish('cstr/heater2', 'on')
        elif cstr_temp <= current_setpoint - 1:
            client.publish('cstr/heater1', 'on')
            client.publish('cstr/heater2', 'off')
        else:
            client.publish('cstr/heater1', 'off')
            client.publish('cstr/heater2', 'off')

        if abs(mtank_temp - cstr_temp) >= 5:
            client.publish('mtank/out', 'on')
            client.publish('cstr/in', 'off')
        elif abs(mtank_temp - cstr_temp) <= 1:
            client.publish('mtank/out', 'off')
            client.publish('cstr/in', 'on')

# Initialize MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
