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
    global cstr_temp, mtank_temp

    topic = msg.topic
    payload = msg.payload.decode()

    if topic in MQTT_TOPICS:
        payload = json.loads(payload)

        if topic == 'cstr-temp':
            cstr_temp = float(payload)
        elif topic == 'mtank-temp':
            mtank_temp = float(payload)

# Function to calculate the current setpoint temperature
def calculate_setpoint(start_timestamp, set_temp, initial_temp, over_duration, temp_change):
    current_timestamp = time.time()
    elapsed_time = current_timestamp - start_timestamp
    max_temp_increase = (elapsed_time / (over_duration * 3600)) * temp_change  # Convert over_duration to seconds
    current_setpoint = initial_temp + max_temp_increase
    return min(current_setpoint, set_temp)

# Function to control CSTR relays based on sensor data
def control_cstr_relays(client):
    global set_cstr_temp, over_duration, temp_change, start_timestamp, last_states

    if set_cstr_temp is None:
        set_cstr_temp, over_duration, temp_change, db_start_time = get_set_temperatures_and_start_time()
        if set_cstr_temp is None:
            print("Set temperatures not available")
            return
        start_timestamp = time.mktime(db_start_time.timetuple())  # Convert timestamp to time in seconds since epoch
    
    initial_temp = 28  # Assume an initial starting temperature; adjust as needed
    current_setpoint = calculate_setpoint(start_timestamp, set_cstr_temp, initial_temp, over_duration, temp_change)

    if cstr_temp is not None:
        # Determine relay states
        heater1_state = 'off'
        heater2_state = 'off'
        cstr_in_state = 'on'

        if cstr_temp <= current_setpoint - 0.02:
            heater1_state = 'on'
        if cstr_temp <= current_setpoint - 1:
            heater2_state = 'on'
        if cstr_temp >= set_cstr_temp:
            heater1_state = 'off'
            heater2_state = 'off'

        # Publish only if state has changed
        publish_if_changed(client, 'cstr/heater1', heater1_state)
        publish_if_changed(client, 'cstr/heater2', heater2_state)
        publish_if_changed(client, 'cstr/in', cstr_in_state)

# Function to control MTANK relays based on sensor data
def control_mtank_relays(client):
    global last_states

    if cstr_temp is not None and mtank_temp is not None:
        # Determine relay states
        mtank_out_state = 'off'
        cstr_in_state = 'on'

        if abs(mtank_temp - cstr_temp) >= 5:
            mtank_out_state = 'on'
            cstr_in_state = 'off'
        elif abs(mtank_temp - cstr_temp) <= 1:
            cstr_in_state = 'on'

        # Publish only if state has changed
        publish_if_changed(client, 'mtank/out', mtank_out_state)
        publish_if_changed(client, 'cstr/in', cstr_in_state)

# Function to publish to a topic if the state has changed
def publish_if_changed(client, topic, state):
    if last_states[topic] != state:
        client.publish(topic, state)
        last_states[topic] = state

# Main function to initialize and start the MQTT client
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # Keep the program running and control the relays periodically
    while True:
        control_cstr_relays(client)
        control_mtank_relays(client)
        time.sleep(1)

if __name__ == '__main__':
    main()
