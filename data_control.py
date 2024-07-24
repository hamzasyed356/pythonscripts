import paho.mqtt.client as mqtt
import psycopg2
import json
import time
from datetime import datetime, timedelta
import requests
from supabase import create_client, Client

# Database configuration
DATABASE_CONFIG = {
    'dbname': 'sensordata',
    'user': 'postgres',
    'password': '399584',
    'host': 'localhost',  # Assuming the database is hosted on the Raspberry Pi
    'port': '5432'        # Default PostgreSQL port
}

# Supabase configuration
SUPABASE_URL = 'https://your-supabase-url'
SUPABASE_KEY = 'your-supabase-key'

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# MQTT configuration
MQTT_BROKER = '192.168.18.19'
MQTT_PORT = 1883
MQTT_TOPICS = ['cstr-level', 'cstr-temp', 'cstr-ph', 'cstr-orp', 'cstr-ec', 'cstr-tds', 'mtank-level', 'mtank-temp', 'effluent-level']
FLUX_TOPIC = 'flux'

# Global variables to store sensor data
sensor_data = {
    'cstr_temp': None,
    'cstr_level': None,
    'cstr_ph': None,
    'cstr_orp': None,
    'cstr_ec': None,
    'cstr_tds': None,
    'mtank_temp': None,
    'mtank_level': None,
    'effluent_level': None,
    'timestamp': None,
    'published': False
}

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with code {rc}")
    for topic in MQTT_TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
    global sensor_data
    topic = msg.topic
    payload = json.loads(msg.payload.decode())
    sensor_data['timestamp'] = datetime.utcnow()

    if topic == 'cstr-temp':
        sensor_data['cstr_temp'] = float(payload)
    elif topic == 'cstr-level':
        sensor_data['cstr_level'] = float(payload)
    elif topic == 'cstr-ph':
        sensor_data['cstr_ph'] = float(payload)
    elif topic == 'cstr-ec':
        sensor_data['cstr_ec'] = float(payload)
    elif topic == 'cstr-orp':
        sensor_data['cstr_orp'] = float(payload)
    elif topic == 'cstr-tds':
        sensor_data['cstr_tds'] = float(payload)
    elif topic == 'mtank-temp':
        sensor_data['mtank_temp'] = float(payload)
    elif topic == 'mtank-level':
        sensor_data['mtank_level'] = float(payload)
    elif topic == 'effluent-level':
        sensor_data['effluent_level'] = float(payload)

# Function to calculate flux
def calculate_flux(current_level, conn):
    try:
        cursor = conn.cursor()
        one_minute_ago = datetime.utcnow() - timedelta(minutes=1)
        query = '''
        SELECT effluent_level 
        FROM sensor_data 
        WHERE timestamp <= %s 
        ORDER BY timestamp DESC 
        LIMIT 1;
        '''
        cursor.execute(query, (one_minute_ago,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            previous_level = result[0]
            return current_level - previous_level
        else:
            return 0  # If no previous data is found, assume no change
    except Exception as e:
        print(f"Error calculating flux: {e}")
        return 0

# Function to save data to the PostgreSQL database
def save_to_database(data):
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        flux = calculate_flux(data['effluent_level'], conn)
        cursor = conn.cursor()
        insert_query = '''
        INSERT INTO sensor_data (timestamp, cstr_temp, cstr_level, cstr_ph, cstr_orp, cstr_ec, cstr_tds, mtank_temp, mtank_level, effluent_level, flux, published)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.execute(insert_query, (data['timestamp'], data['cstr_temp'], data['cstr_level'], data['cstr_ph'], data['cstr_orp'], data['cstr_ec'], data['cstr_tds'], data['mtank_temp'], data['mtank_level'], data['effluent_level'], flux, data['published']))
        conn.commit()
        cursor.close()
        conn.close()
        print("Data saved to database.")
        return True, flux
    except Exception as e:
        print(f"Error saving data to database: {e}")
        return False, None

# Function to check internet connectivity
def is_connected():
    try:
        requests.get('http://www.google.com', timeout=5)
        return True
    except requests.ConnectionError:
        return False

# Function to upload unpublished data to Supabase
def upload_unpublished_data():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sensor_data WHERE published = FALSE")
        data = cursor.fetchall()
        if data:
            formatted_data = []
            ids = []
            for row in data:
                ids.append(row[0])  # Assuming id is the first column
                formatted_data.append({
                    'timestamp': row[1],
                    'cstr_temp': row[2],
                    'cstr_level': row[3],
                    'cstr_ph': row[4],
                    'cstr_orp': row[5],
                    'cstr_ec': row[6],
                    'cstr_tds': row[7],
                    'mtank_temp': row[8],
                    'mtank_level': row[9],
                    'effluent_level': row[10],
                    'flux': row[11]
                })
            response = upload_data_to_supabase(formatted_data)
            if response.status_code == 201:  # Check for successful insertion
                update_published_status(ids)
                print("Data uploaded and marked as published successfully.")
            else:
                print(f"Error uploading data to Supabase: {response.status_code}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error uploading unpublished data: {e}")

# Function to update published status in PostgreSQL
def update_published_status(ids):
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        cursor.execute("UPDATE sensor_data SET published = TRUE WHERE id = ANY(%s)", (ids,))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error updating published status: {e}")

# Function to upload data to Supabase
def upload_data_to_supabase(data):
    response = supabase.table('sensor_data').insert(data).execute()
    return response

# Main loop to periodically save data
def main_loop():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    try:
        while True:
            if all(value is not None for value in sensor_data.values()):
                success, flux = save_to_database(sensor_data)
                if success:
                    # Publish flux to MQTT topic
                    client.publish(FLUX_TOPIC, json.dumps({'flux': flux}))
                    if is_connected():
                        upload_unpublished_data()
            time.sleep(30)
    except KeyboardInterrupt:
        client.loop_stop()

if __name__ == "__main__":
    main_loop()
