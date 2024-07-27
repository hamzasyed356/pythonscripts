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
start_timestamp = None

# Global variables to store the last states of the relays
last_states = {
    'cstr/heater1': None,
    'cstr/heater2': None,
    'mtank/out': None,
    'cstr/in': None
}

# Function to get set temperatures and timestamp from the database
def get_set_temperatures_and_start_time():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT set_temp, over_duration, temp_change, timestamp FROM temp_setting ORDER BY timestamp DESC LIMIT 1")
        result = cursor.fetchone()
        conn.close()
        return result if result else (None, None, None, None)
    except Exception as e:
        print(f"Error fetching set temperatures and timestamp: {e}")
        return (None, None, None, None)

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with code {rc}")
    for topic in MQTT_TOPICS + CONTROL_TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
    global cstr_temp, mtank_temp, last_states

    topic = msg.topic
    payload = msg.payload.decode()

    if topic in MQTT_TOPICS:
        payload = json.loads(payload)

        if topic == 'cstr-temp':
            cstr_temp = float(payload)
        elif topic == 'mtank-temp':
            mtank_temp = float(payload)

        control_relays(client)
    elif topic in CONTROL_TOPICS:
        last_states[topic] = payload

# Function to calculate the current setpoint temperature
def calculate_setpoint(start_timestamp, set_temp, initial_temp, over_duration, temp_change):
    current_timestamp = time.time()
    elapsed_time = current_timestamp - start_timestamp
    hours_elapsed = elapsed_time / 3600
    max_temp_increase = (hours_elapsed / over_duration) * temp_change
    current_setpoint = initial_temp + max_temp_increase
    return min(current_setpoint, set_temp)

# Function to control relays based on sensor data
def control_relays(client):
    global set_cstr_temp, over_duration, temp_change, start_timestamp, last_states

    if set_cstr_temp is None:
        set_cstr_temp, over_duration, temp_change, db_start_time = get_set_temperatures_and_start_time()
        if set_cstr_temp is None:
            print("Set temperatures not available")
            return
        start_timestamp = time.mktime(db_start_time.timetuple())  # Convert timestamp to time in seconds since epoch
    
    initial_temp = 20  # Assume an initial starting temperature; adjust as needed
    current_setpoint = calculate_setpoint(start_timestamp, set_cstr_temp, initial_temp, over_duration, temp_change)

    if cstr_temp is not None and mtank_temp is not None:
        # Determine relay states
        heater1_state = 'off'
        heater2_state = 'off'
        mtank_out_state = 'off'
        cstr_in_state = 'on'

        if cstr_temp <= current_setpoint - 1:
            heater1_state = 'on'
        if cstr_temp <= current_setpoint - 5:
            heater2_state = 'on'

        if abs(mtank_temp - cstr_temp) >= 5:
            mtank_out_state = 'on'
            cstr_in_state = 'off'
        elif abs(mtank_temp - cstr_temp) <= 1:
            cstr_in_state = 'on'

        # Publish only if state has changed
        if last_states['cstr/heater1'] != heater1_state:
            client.publish('cstr/heater1', heater1_state)
            last_states['cstr/heater1'] = heater1_state
        if last_states['cstr/heater2'] != heater2_state:
            client.publish('cstr/heater2', heater2_state)
            last_states['cstr/heater2'] = heater2_state
        if last_states['mtank/out'] != mtank_out_state:
            client.publish('mtank/out', mtank_out_state)
            last_states['mtank/out'] = mtank_out_state
        if last_states['cstr/in'] != cstr_in_state:
            client.publish('cstr/in', cstr_in_state)
            last_states['cstr/in'] = cstr_in_state

# Initialize MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
