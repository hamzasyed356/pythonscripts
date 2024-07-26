import tkinter as tk
from tkinter import Menu, filedialog, messagebox
from tkinter.ttk import Frame, Label, Entry, Button
from tkcalendar import DateEntry  # Import DateEntry from tkcalendar
from datetime import datetime, timedelta
import psycopg2  # Using psycopg2 for PostgreSQL
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import paho.mqtt.client as mqtt
from PIL import Image, ImageTk

# MQTT Configuration
MQTT_BROKER = "192.168.18.19"
MQTT_PORT = 1883
MQTT_TOPICS = {
    "cstr_ph": "cstr-ph",
    "cstr_ec": "cstr-ec",
    "cstr_tds": "cstr-tds",
    "cstr_orp": "cstr-orp",
    "cstr_temp": "cstr-temp",
    "cstr_level": "cstr-level",
    "mtank_temp": "mtank-temp",
    "mtank_level": "mtank-level",
    "mtank_weight": "mtank-weight",
    "effluent_weight": "effluent-weight",
    "effluent_level": "effluent-level",
    "flux": "flux"
}


# Initialize MQTT values storage
mqtt_values = {topic: None for topic in MQTT_TOPICS.keys()}

# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    for topic in MQTT_TOPICS.keys():
        client.subscribe(topic)

def on_message(client, userdata, msg):
    topic = msg.topic
    value = msg.payload.decode()
    mqtt_values[topic] = value

# Initialize MQTT client and connect
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# Database configuration
DB_NAME = "sensordata"
DB_USER = "postgres"
DB_PASSWORD = "399584"
DB_HOST = "localhost"
DB_PORT = "5432"

# Function to fetch data and display time series graph
def fetch_and_display_timeseries(param, from_date, to_date, canvas, figure):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        query = f"SELECT timestamp, {param} FROM sensor_data WHERE timestamp BETWEEN '{from_date}' AND '{to_date}'"
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Plotting the data
        ax = figure.add_subplot(111)
        ax.clear()
        ax.plot(pd.to_datetime(df['timestamp']), df[param], marker='o', linestyle='-')
        ax.set_title(f'Time Series Data for {param}')
        ax.set_xlabel('Timestamp')
        ax.set_ylabel(param)
        ax.grid(True)
        canvas.draw()

    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

# Function to save the graph as an image
def save_graph_as_image(figure):
    file_path = filedialog.asksaveasfilename(defaultextension=".png", filetypes=[("PNG files", "*.png")])
    if file_path:
        figure.savefig(file_path)
        messagebox.showinfo("Success", "Image has been saved successfully.")

# Function to open the time series window
def open_timeseries_window(param):
    timeseries_window = tk.Toplevel()
    timeseries_window.title(f"Time Series Graph for {param}")
    timeseries_window.geometry("800x600")
    timeseries_window.grab_set()  # Ensure the window is modal

    input_frame = Frame(timeseries_window)
    input_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

    from_date_label = Label(input_frame, text="From Date:")
    from_date_label.pack(side=tk.LEFT, padx=5)
    from_date_input = DateEntry(input_frame, date_pattern='yyyy-mm-dd')
    from_date_input.pack(side=tk.LEFT, padx=5)

    from_time_label = Label(input_frame, text="From Time (HH:MM):")
    from_time_label.pack(side=tk.LEFT, padx=5)
    from_time_input = Entry(input_frame)
    from_time_input.pack(side=tk.LEFT, padx=5)

    to_date_label = Label(input_frame, text="To Date:")
    to_date_label.pack(side=tk.LEFT, padx=5)
    to_date_input = DateEntry(input_frame, date_pattern='yyyy-mm-dd')
    to_date_input.pack(side=tk.LEFT, padx=5)

    to_time_label = Label(input_frame, text="To Time (HH:MM):")
    to_time_label.pack(side=tk.LEFT, padx=5)
    to_time_input = Entry(input_frame)
    to_time_input.pack(side=tk.LEFT, padx=5)

    figure = plt.Figure()
    canvas = FigureCanvasTkAgg(figure, master=timeseries_window)
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

    def fetch_and_display():
        from_date_str = from_date_input.get_date().strftime('%Y-%m-%d')
        from_time_str = from_time_input.get()
        to_date_str = to_date_input.get_date().strftime('%Y-%m-%d')
        to_time_str = to_time_input.get()

        from_datetime = f"{from_date_str} {from_time_str}:00"
        to_datetime = f"{to_date_str} {to_time_str}:00"

        fetch_and_display_timeseries(param, from_datetime, to_datetime, canvas, figure)

    fetch_button = Button(input_frame, text="Show Graph", command=fetch_and_display)
    fetch_button.pack(side=tk.LEFT, padx=5)

    save_button = Button(input_frame, text="Save Image", command=lambda: save_graph_as_image(figure))
    save_button.pack(side=tk.LEFT, padx=5)

    # Default time range: past hour to now
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    fetch_and_display_timeseries(param, one_hour_ago.strftime('%Y-%m-%d %H:%M:%S'), now.strftime('%Y-%m-%d %H:%M:%S'), canvas, figure)

# Example function to bind to a parameter frame click event
def on_param_frame_click(param):
    open_timeseries_window(param)

# Function to save settings to the database
def save_settings(set_temp_input, over_duration_input, temp_change_input):
    set_temp = set_temp_input.get()
    over_duration = over_duration_input.get()
    temp_change = temp_change_input.get()
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        now = datetime.now()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO temp_setting (timestamp, set_temp, over_duration, temp_change) VALUES (%s, %s, %s, %s)",
                       (now, set_temp, over_duration, temp_change))
        conn.commit()
        cursor.close()
        conn.close()
        messagebox.showinfo("Success", "Settings have been saved successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

# Function to open the settings window
def open_settings():
    settings_window = tk.Toplevel()
    settings_window.title("Settings")
    settings_window.geometry("300x200")
    settings_window.grab_set()  # Ensure the settings window is modal
    
    set_temp_input = Entry(settings_window)
    set_temp_input.insert(0, 'Set Temp')
    over_duration_input = Entry(settings_window)
    over_duration_input.insert(0, 'Over Duration')
    temp_change_input = Entry(settings_window)
    temp_change_input.insert(0, 'Temp Change')
    
    set_temp_input.pack(pady=5)
    over_duration_input.pack(pady=5)
    temp_change_input.pack(pady=5)
    
    save_button = Button(settings_window, text="Save Settings", command=lambda: save_settings(set_temp_input, over_duration_input, temp_change_input))
    save_button.pack(pady=10)

# Function to download data as CSV
def download_data(from_date_input, to_date_input):
    from_date = from_date_input.get_date().strftime('%Y-%m-%d %H:%M:%S')
    to_date = to_date_input.get_date().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        query = f"SELECT * FROM sensor_data WHERE timestamp BETWEEN '{from_date}' AND '{to_date}'"
        df = pd.read_sql_query(query, conn)
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if file_path:
            df.to_csv(file_path, index=False)
            conn.close()
            messagebox.showinfo("Success", "Data has been downloaded successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

# Function to open the download window
def open_download():
    download_window = tk.Toplevel()
    download_window.title("Download Data")
    download_window.geometry("300x200")
    download_window.grab_set()  # Ensure the download window is modal
    
    from_date_label = Label(download_window, text="From Date:")
    from_date_label.pack(pady=5)
    from_date_input = DateEntry(download_window, date_pattern='yyyy-mm-dd')
    from_date_input.pack(pady=5)
    
    to_date_label = Label(download_window, text="To Date:")
    to_date_label.pack(pady=5)
    to_date_input = DateEntry(download_window, date_pattern='yyyy-mm-dd')
    to_date_input.pack(pady=5)
    
    download_button = Button(download_window, text="Download", command=lambda: download_data(from_date_input, to_date_input))
    download_button.pack(pady=10)

app = tk.Tk()
app.title("Aquameter Membrane Distillation")
app.geometry('1024x600')

# Create a menu
menu_bar = Menu(app)
app.config(menu=menu_bar)
settings_menu = Menu(menu_bar, tearoff=0)
menu_bar.add_cascade(label="Menu", menu=settings_menu)
settings_menu.add_command(label="Settings", command=open_settings)
settings_menu.add_command(label="Download", command=open_download)

# Load logos
left_logo_image = Image.open("nust-logo.png")  # Replace with the actual path to your left logo
right_logo_image = Image.open("resurgence_logo.png")  # Replace with the actual path to your right logo

left_logo_image = left_logo_image.resize((100, 100), Image.LANCZOS)  # Resize image if needed
right_logo_image = right_logo_image.resize((480, 75), Image.LANCZOS)  # Resize image if needed

left_logo_tk_image = ImageTk.PhotoImage(left_logo_image)
right_logo_tk_image = ImageTk.PhotoImage(right_logo_image)

# Create a transparent frame for the top title bar
title_frame = Frame(master=app)
title_frame.grid(row=0, column=0, columnspan=3, sticky="n")

# Create and place logo labels within the transparent frame
left_logo_label = Label(master=title_frame, image=left_logo_tk_image)
left_logo_label.grid(row=0, column=0, padx=20, sticky="w")

right_logo_label = Label(master=title_frame, image=right_logo_tk_image)
right_logo_label.grid(row=0, column=2, padx=20, sticky="e")

# Title within the transparent frame
title_label = Label(master=title_frame, text="Membrane Distillation", font=("Times New Roman", 44, 'bold'))
title_label.grid(row=0, column=1, pady=10)

# Section labels and frames
sections = ["Anaerobic CSTR", "Membrane Tank", "Effluent"]

# Parameters for each section
anaerobic_cstr_params = [
    ("PH", "cstr-ph", "cstr_ph", " /14"),
    ("EC", "cstr-ec", "cstr_ec", " mS/cm"),
    ("TDS", "cstr-tds", "cstr_tds", " PPM"),
    ("ORP", "cstr-orp", "cstr_orp", " mV"),
    ("Temp", "cstr-temp", "cstr_temp", " °C"),
    ("Level", "cstr-level", "cstr_level", " mL"),
]

membrane_tank_params = [
    ("Temp", "mtank-temp", "mtank_temp", " °C"),
    ("Level", "mtank-level", "mtank_level", " L"),
    ("Weight", "mtank-weight", "mtank_weight", " g")
]

effluent_params = [
    ("Weight", "effluent-weight", "effluent_weight", " g"),
    ("Level", "effluent-level", "effluent_level", " mL"),
    ("Flux", "flux", "flux", " mL/min")
]

parameters = [anaerobic_cstr_params, membrane_tank_params, effluent_params]

# Create frames and labels for each section
for i, section in enumerate(sections):
    section_frame = Frame(master=app)
    section_frame.grid(row=1, column=i, padx=20, pady=20, sticky="nsew")

    section_label = Label(master=section_frame, text=section, font=("Times New Roman", 24, 'bold'))
    section_label.grid(row=0, column=0, columnspan=2, pady=10)

    # Create subframes and labels for parameters
    if section == "Anaerobic CSTR":
        for j, (param, topic, col, unit) in enumerate(parameters[i]):
            value = mqtt_values[topic]  # Get value from MQTT
            param_frame = Frame(master=section_frame, height=100, width=200, relief=tk.RAISED, borderwidth=2)
            param_frame.grid(row=(j // 2) + 1, column=j % 2, pady=10, padx=20, sticky="nsew")

            value_label = Label(master=param_frame, text=f"{value}{unit}", font=("Times New Roman", 32, 'bold'))
            value_label.place(relx=0.5, rely=0.3, anchor="center")
            border_line = Frame(master=param_frame, height=2, width=200, bg="black")
            border_line.place(relx=0.5, rely=0.6, anchor="center")
            param_label = Label(master=param_frame, text=f"{param}", font=("Times New Roman", 20, 'bold'))
            param_label.place(relx=0.5, rely=0.65, anchor="n")
            
            # Bind click event
            param_frame.bind("<Button-1>", lambda e, param=col: on_param_frame_click(param))
            
    else:
        for j, (param, topic, col, unit) in enumerate(parameters[i]):
            value = mqtt_values[topic]  # Get value from MQTT
            param_frame = Frame(master=section_frame, height=100, width=200, relief=tk.RAISED, borderwidth=2)
            param_frame.grid(row=j + 1, column=0, pady=10, padx=20, sticky="nsew")
            
            value_label = Label(master=param_frame, text=f"{value}{unit}", font=("Times New Roman", 32, 'bold'))
            value_label.place(relx=0.5, rely=0.3, anchor="center")
            border_line = Frame(master=param_frame, height=2, width=200, bg="black")
            border_line.place(relx=0.5, rely=0.6, anchor="center")
            param_label = Label(master=param_frame, text=f"{param}", font=("Times New Roman", 20, 'bold'))
            param_label.place(relx=0.5, rely=0.65, anchor="n")
            
            # Bind click event
            param_frame.bind("<Button-1>", lambda e, param=col: on_param_frame_click(param))

# Create footer frame
footer_frame = Frame(master=app)
footer_frame.grid(row=2, column=0, columnspan=3, pady=5)
company_logo_image = Image.open("company-logo.png")  # Replace with the actual path to your company logo
company_logo_image = company_logo_image.resize((100, 50), Image.LANCZOS)  # Resize image if needed
company_logo_tk_image = ImageTk.PhotoImage(company_logo_image)
# Add company logo and copyright text to footer
company_logo_label = Label(master=footer_frame, image=company_logo_tk_image)
company_logo_label.grid(row=0, column=1, padx=20, sticky="e")

copyright_label = Label(master=footer_frame, text="All rights reserved © 2024 Pentaprism Technologies.", font=("Times New Roman", 12))
copyright_label.grid(row=0, column=0, pady=10)

# Configure grid to expand and fill the window
for i in range(3):
    app.grid_columnconfigure(i, weight=1)
app.grid_rowconfigure(1, weight=1)

app.mainloop()