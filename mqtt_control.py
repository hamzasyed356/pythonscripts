import paho.mqtt.client as mqtt
import psycopg2
import time
import threading
from datetime import datetime, timedelta

# MQTT settings
broker = "192.168.18.19"
port = 1883
topics = ['cstr-temp', 'cstr-level', 'mtank-temp', 'mtank-level']

# Database settings
dbname = "sensordata"
user = "postgres"
password = "399584"
host = "localhost"
table = "temp_setting"

# Connect to PostgreSQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cursor = conn.cursor()

# MQTT client setup
client = mqtt.Client()

# Global variables for sensor values and previous states
sensor_values = {
    "cstr-temp": None,
    "cstr-level": None,
    "mtank-temp": None,
    "mtank-level": None
}
previous_states = {
    "cstr/in": None,
    "cstr/heater1": None,
    "cstr/heater2": None,
    "mtank/out": None,
    "mtank/in": None,
    "mtank-recycle": None
}
current_temp_settings = {
    "set_temp": None,
    "over_duration": None,
    "temp_change": None
}
target_temp = None
last_temp_change_time = time.time()

# Hysteresis values
hysteresis = 0.5  # Adjust as needed

# Callback when a message is received
def on_message(client, userdata, message):
    global sensor_values
    sensor_values[message.topic] = float(message.payload.decode("utf-8"))

    # Control logic
    cstr_control()
    mtank_control()

# Function to get temp settings from database
def get_temp_settings():
    cursor.execute(f"SELECT set_temp, over_duration, temp_change FROM {table} ORDER BY id DESC LIMIT 1")
    return cursor.fetchone()

# Function to update temp settings
def update_temp_settings():
    global current_temp_settings, last_temp_change_time, target_temp
    new_settings = get_temp_settings()
    if new_settings:
        current_temp_settings = {
            "set_temp": new_settings[0],
            "over_duration": new_settings[1],
            "temp_change": new_settings[2]
        }
        # Recalculate target_temp with new settings and reset last_temp_change_time
        if sensor_values["cstr-temp"] is not None:
            target_temp = sensor_values["cstr-temp"] + current_temp_settings["temp_change"]
            last_temp_change_time = time.time()

    # Ensure control logic is applied with updated settings
    cstr_control()
    mtank_control()

# Function to publish MQTT messages only on state change
def publish_state(topic, state):
    if previous_states[topic] != state:
        client.publish(topic, state)
        previous_states[topic] = state

# CSTR control logic
def cstr_control():
    global last_temp_change_time, target_temp
    set_temp = current_temp_settings["set_temp"]
    temp_change = current_temp_settings["temp_change"]
    current_time = time.time()

    if set_temp is None or temp_change is None or sensor_values["cstr-temp"] is None:
        return

    if sensor_values["cstr-level"] is not None and sensor_values["cstr-level"] >= 26.5:
        publish_state("cstr/in", "off")

    if sensor_values["cstr-temp"] is not None:
        if (current_time - last_temp_change_time) >= 3600:  # Check every hour
            target_temp = sensor_values["cstr-temp"] + temp_change
            print(target_temp)
            last_temp_change_time = current_time

        if target_temp is not None and target_temp >= set_temp:
            # Ensure the temperature does not fall below the set temperature
            if sensor_values["cstr-temp"] < set_temp:
                publish_state("cstr/heater1", "on")
                publish_state("cstr/heater2", "off")  # Use only one heater for fine control
            elif sensor_values["cstr-temp"] >= set_temp:
                publish_state("cstr/heater1", "off")
                publish_state("cstr/heater2", "off")
        elif target_temp is not None:
            # Maintain temperature at target_temp
            if sensor_values["cstr-temp"] < target_temp:
                if target_temp - sensor_values["cstr-temp"] > 1.2:  # Use both heaters if temperature difference is large
                    publish_state("cstr/heater1", "on")
                    publish_state("cstr/heater2", "on")
                else:  # Use only one heater for fine control
                    publish_state("cstr/heater1", "on")
                    publish_state("cstr/heater2", "off")
            else:
                publish_state("cstr/heater1", "off")
                publish_state("cstr/heater2", "off")

# Mtank control logic
def mtank_control():
    # Ensure mtank/in is always on
    publish_state("mtank/in", "on")
    
    if sensor_values["mtank-level"] is None:
        return
    
    if sensor_values["mtank-level"] < 8000:
        # Ignore mtank-temp and prioritize filling until level reaches 8000 ml
        publish_state("mtank-recycle", "No")
        publish_state("mtank/out", "off")
        publish_state("cstr/in", "on")
    else:
        temp_override = False

        if sensor_values["mtank-temp"] is not None and sensor_values["cstr-temp"] is not None:
            # Apply hysteresis to prevent rapid switching
            if sensor_values["mtank-temp"] <= (sensor_values["cstr-temp"] - 5 - hysteresis):
                publish_state("mtank-recycle", "Yes")
                publish_state("mtank/out", "on")
                publish_state("cstr/in", "off")
                temp_override = True
            elif sensor_values["mtank-temp"] >= (sensor_values["cstr-temp"] - 5 + hysteresis):
                publish_state("mtank-recycle", "No")
                publish_state("mtank/out", "off")
                publish_state("cstr/in", "on")
                temp_override = True

        if not temp_override and sensor_values["mtank-level"] is not None:
            if sensor_values["mtank-level"] > 8100:
                publish_state("mtank-recycle", "Yes")
                publish_state("mtank/out", "on")
                publish_state("cstr/in", "off")
            elif sensor_values["mtank-level"] < 8100:
                publish_state("mtank-recycle", "No")
                publish_state("mtank/out", "off")
                publish_state("cstr/in", "on")

# Periodic status update
def periodic_status_update():
    threading.Timer(120, periodic_status_update).start()  # Schedule the function to run every 2 minutes
    for topic in previous_states:
        if previous_states[topic] is not None:
            client.publish(topic, previous_states[topic])

# Periodic database check
def periodic_db_check():
    threading.Timer(10, periodic_db_check).start()  # Check database every 10 seconds for new settings
    update_temp_settings()

# MQTT connection setup
client.on_message = on_message
client.connect(broker, port)

# Subscribe to topics
for topic in topics:
    client.subscribe(topic)

# Start MQTT loop
client.loop_start()

# Start periodic status updates and database checks
periodic_status_update()
periodic_db_check()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting")
finally:
    client.loop_stop()
    cursor.close()
    conn.close()
