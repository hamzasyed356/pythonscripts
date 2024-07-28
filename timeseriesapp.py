import customtkinter as ctk
from PIL import Image
import tkinter as tk
from tkcalendar import DateEntry
from tkinter import Canvas, Scrollbar
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from matplotlib.animation import FuncAnimation

# Function to fetch multiple parameters data from the database
def fetch_multiple_data(params, from_datetime, to_datetime):
    param_query = ", ".join(params)
    query = f"SELECT timestamp, {param_query} FROM sensor_data WHERE timestamp BETWEEN '{from_datetime}' AND '{to_datetime}' ORDER BY timestamp ASC"
    cur.execute(query)
    data = cur.fetchall()
    if data:
        columns = ['timestamp'] + params
        df = pd.DataFrame(data, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    else:
        return pd.DataFrame(columns=['timestamp'] + params)

# Function to update multi-data graphs
def animate_multi_graph(params, ax):
    def update(frame):
        now = datetime.now()
        one_day_ago = now - timedelta(days=1)
        from_datetime = one_day_ago.strftime('%Y-%m-%d %H:%M:%S')
        to_datetime = now.strftime('%Y-%m-%d %H:%M:%S')
        new_data = fetch_multiple_data(params, from_datetime, to_datetime)
        
        x = new_data['timestamp']
        y1 = new_data[params[0]]
        y2 = new_data[params[1]]
        ax.clear()
        ax.plot(x, y1, marker='o', linestyle='-', label=params[0])
        ax.plot(x, y2, marker='o', linestyle='-', label=params[1])
        ax.set_title(f'Time Series Data for {params[0]} and {params[1]}')
        ax.set_xlabel('Timestamp')
        ax.set_ylabel('Values')
        ax.legend()
        ax.grid(True)
    return update

# Multi-data graphs
multi_graphs = [
    ('cstr-orp', 'cstr-ec'),
    ('cstr-orp', 'cstr-ph'),
    ('cstr-ph', 'cstr-ec')
]

# Initialize the main application
app = ctk.CTk()
app.title("Aquameter Membrane Distillation")
app.geometry('1920x1080')

# Database connection
conn = psycopg2.connect(dbname="sensordata", user="postgres", password="399584")
cur = conn.cursor()

# Create a transparent frame for the top title bar
title_frame = ctk.CTkFrame(master=app, fg_color="transparent")
title_frame.grid(row=0, column=0, columnspan=4, sticky="n")

# Title within the transparent frame
title_label = ctk.CTkLabel(master=title_frame, text="Membrane Distillation", font=("Helvetica", 44, 'bold'))
title_label.grid(row=0, column=1, pady=5)

# Load logos for the title bar
left_logo_image = Image.open("/home/resurgencemd/pythonscripts/nust-logo.png")
right_logo_image = Image.open("/home/resurgencemd/pythonscripts/resurgence_logo.png")

left_logo_image = left_logo_image.resize((100, 100), Image.LANCZOS)
right_logo_image = right_logo_image.resize((480, 75), Image.LANCZOS)

left_logo_ctk_image = ctk.CTkImage(light_image=left_logo_image, dark_image=left_logo_image, size=(80, 80))
right_logo_ctk_image = ctk.CTkImage(light_image=right_logo_image, dark_image=right_logo_image, size=(250, 50))

left_logo_label = ctk.CTkLabel(master=title_frame, image=left_logo_ctk_image, text="")
left_logo_label.grid(row=0, column=0, padx=20, sticky="w")

right_logo_label = ctk.CTkLabel(master=title_frame, image=right_logo_ctk_image, text="")
right_logo_label.grid(row=0, column=2, padx=20, sticky="e")

# Create footer frame
footer_frame = ctk.CTkFrame(master=app, fg_color="transparent")
footer_frame.grid(row=2, column=0, columnspan=4, pady=5)

company_logo_image = Image.open("/home/resurgencemd/pythonscripts/company-logo.png")  # Replace with the actual path to your company logo
company_logo_image = company_logo_image.resize((100, 50), Image.LANCZOS)  # Resize image if needed
company_logo_ctk_image = ctk.CTkImage(light_image=company_logo_image, dark_image=company_logo_image, size=(100, 20))

# Add company logo and copyright text to footer
company_logo_label = ctk.CTkLabel(master=footer_frame, image=company_logo_ctk_image, text="")
company_logo_label.grid(row=0, column=1, padx=20, sticky="e")

copyright_label = ctk.CTkLabel(master=footer_frame, text="All rights reserved © 2024 Pentaprism Technologies.", font=("Helvetica", 12))
copyright_label.grid(row=0, column=0, pady=10)

# Create a scrollable frame for the graphs
canvas = tk.Canvas(app, bd=0, highlightthickness=0)  # Remove border and highlight thickness
scrollbar = Scrollbar(app, orient="vertical", command=canvas.yview)
scrollable_frame = ctk.CTkFrame(canvas)

scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(
        scrollregion=canvas.bbox("all")
    )
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

# Enable touch scrolling
def _on_mouse_wheel(event):
    canvas.yview_scroll(-1 * int(event.delta / 120), "units")

canvas.bind_all("<MouseWheel>", _on_mouse_wheel)
canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))

# Function to clear the main content area
def clear_main_area():
    for widget in app.winfo_children():
        if widget not in [title_frame, footer_frame, menu_bar]:
            widget.grid_forget()

# Function to display all graphs
def display_all_graphs():
    clear_main_area()
    canvas.grid(row=1, column=0, columnspan=4, sticky="nsew")
    scrollbar.grid(row=1, column=4, sticky="ns")

# Function to display individual graphs
def display_graph(param):
    clear_main_area()
    timeseries_window = ctk.CTkFrame(app)
    timeseries_window.grid(row=1, column=0, columnspan=4, sticky="nsew", padx=10, pady=10)
    
    input_frame = ctk.CTkFrame(timeseries_window)
    input_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

    row1_frame = ctk.CTkFrame(input_frame)
    row1_frame.pack(side=tk.TOP, fill=tk.X)

    from_date_label = ctk.CTkLabel(row1_frame, text="From Date:")
    from_date_label.pack(side=tk.LEFT, padx=5)
    from_date_input = DateEntry(row1_frame, date_pattern='yyyy-mm-dd')
    from_date_input.pack(side=tk.LEFT, padx=5)

    from_time_label = ctk.CTkLabel(row1_frame, text="From Time (HH:MM):")
    from_time_label.pack(side=tk.LEFT, padx=5)
    from_time_input = ctk.CTkEntry(row1_frame)
    from_time_input.pack(side=tk.LEFT, padx=5)

    row2_frame = ctk.CTkFrame(input_frame)
    row2_frame.pack(side=tk.TOP, fill=tk.X, pady=(10, 0))

    to_date_label = ctk.CTkLabel(row2_frame, text="To Date:")
    to_date_label.pack(side=tk.LEFT, padx=5)
    to_date_input = DateEntry(row2_frame, date_pattern='yyyy-mm-dd')
    to_date_input.pack(side=tk.LEFT, padx=5)

    to_time_label = ctk.CTkLabel(row2_frame, text="To Time (HH:MM):")
    to_time_label.pack(side=tk.LEFT, padx=5)
    to_time_input = ctk.CTkEntry(row2_frame)
    to_time_input.pack(side=tk.LEFT, padx=5)

    figure = plt.Figure()
    canvas = FigureCanvasTkAgg(figure, master=timeseries_window)
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)  # Using pack instead of grid

    fetch_button = ctk.CTkButton(row2_frame, text="Show Graph", command=lambda: fetch_and_display(param, from_date_input, from_time_input, to_date_input, to_time_input, canvas, figure, timeseries_window))
    fetch_button.pack(side=tk.LEFT, padx=5)

    save_button = ctk.CTkButton(row2_frame, text="Save Image", command=lambda: save_graph_as_image(figure, timeseries_window))
    save_button.pack(side=tk.LEFT, padx=5)

    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    from_date_input.set_date(one_day_ago)
    from_time_input.insert(0, one_day_ago.strftime('%H:%M'))
    to_date_input.set_date(now)
    to_time_input.insert(0, now.strftime('%H:%M'))

    fetch_and_display_timeseries(param, one_day_ago.strftime('%Y-%m-%d %H:%M:%S'), now.strftime('%Y-%m-%d %H:%M:%S'), canvas, figure, timeseries_window)

# Function to display combined graphs
def display_combined_graph(param1, param2):
    clear_main_area()
    timeseries_window = ctk.CTkFrame(app)
    timeseries_window.grid(row=1, column=0, columnspan=4, sticky="nsew", padx=10, pady=10)

    input_frame = ctk.CTkFrame(timeseries_window)
    input_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

    row1_frame = ctk.CTkFrame(input_frame)
    row1_frame.pack(side=tk.TOP, fill=tk.X)

    from_date_label = ctk.CTkLabel(row1_frame, text="From Date:")
    from_date_label.pack(side=tk.LEFT, padx=5)
    from_date_input = DateEntry(row1_frame, date_pattern='yyyy-mm-dd')
    from_date_input.pack(side=tk.LEFT, padx=5)

    from_time_label = ctk.CTkLabel(row1_frame, text="From Time (HH:MM):")
    from_time_label.pack(side=tk.LEFT, padx=5)
    from_time_input = ctk.CTkEntry(row1_frame)
    from_time_input.pack(side=tk.LEFT, padx=5)

    row2_frame = ctk.CTkFrame(input_frame)
    row2_frame.pack(side=tk.TOP, fill=tk.X, pady=(10, 0))

    to_date_label = ctk.CTkLabel(row2_frame, text="To Date:")
    to_date_label.pack(side=tk.LEFT, padx=5)
    to_date_input = DateEntry(row2_frame, date_pattern='yyyy-mm-dd')
    to_date_input.pack(side=tk.LEFT, padx=5)

    to_time_label = ctk.CTkLabel(row2_frame, text="To Time (HH:MM):")
    to_time_label.pack(side=tk.LEFT, padx=5)
    to_time_input = ctk.CTkEntry(row2_frame)
    to_time_input.pack(side=tk.LEFT, padx=5)

    figure = plt.Figure()
    canvas = FigureCanvasTkAgg(figure, master=timeseries_window)
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)  # Using pack instead of grid

    fetch_button = ctk.CTkButton(row2_frame, text="Show Graph", command=lambda: fetch_and_display_combined(param1, param2, from_date_input, from_time_input, to_date_input, to_time_input, canvas, figure, timeseries_window))
    fetch_button.pack(side=tk.LEFT, padx=5)

    save_button = ctk.CTkButton(row2_frame, text="Save Image", command=lambda: save_graph_as_image(figure, timeseries_window))
    save_button.pack(side=tk.LEFT, padx=5)

    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    from_date_input.set_date(one_day_ago)
    from_time_input.insert(0, one_day_ago.strftime('%H:%M'))
    to_date_input.set_date(now)
    to_time_input.insert(0, now.strftime('%H:%M'))

    fetch_and_display_combined_timeseries((param1, param2), one_day_ago.strftime('%Y-%m-%d %H:%M:%S'), now.strftime('%Y-%m-%d %H:%M:%S'), canvas, figure, timeseries_window)

# Create a menu bar
menu_bar = tk.Menu(app)
app.config(menu=menu_bar)

# Add buttons to the menu bar
view_all_menu = tk.Menu(menu_bar, tearoff=0)
menu_bar.add_command(label="View All", command=display_all_graphs)

# Parameters
parameters = ['cstr_ph', 'cstr_temp', 'cstr_level', 'cstr_orp', 'cstr_ec', 'cstr_tds', 'mtank_temp', 'mtank_level', 'effluent_level', 'flux']
combined_parameters = [
    ('cstr_ec', 'cstr_ph'),
    ('cstr_ec', 'cstr_orp'),
    ('cstr_ph', 'cstr_orp')
]

# Add individual graph buttons
for param in parameters:
    menu_bar.add_command(label=param.replace('_', ' ').title(), command=lambda p=param: display_graph(p))

# Add combined graph buttons
for param1, param2 in combined_parameters:
    menu_bar.add_command(label=f"{param1.replace('_', ' ').title()} & {param2.replace('_', ' ').title()}",
                         command=lambda p1=param1, p2=param2: display_combined_graph(p1, p2))

# Store the FigureCanvasTkAgg objects to avoid redrawing
graph_widgets = {}

# Create a dictionary of parameter frames
param_frames = {param: ctk.CTkFrame(scrollable_frame, width=600, height=400, fg_color="transparent") for param in parameters}
multi_param_frames = {f"{params[0]} vs {params[1]}": ctk.CTkFrame(scrollable_frame, width=600, height=400, fg_color="transparent") for params in multi_graphs}

# Arrange frames in a grid
for i, param in enumerate(param_frames.keys()):
    param_frames[param].grid(row=i // 3, column=i % 3, padx=10, pady=10)

for i, (params, frame) in enumerate(multi_param_frames.items()):
    frame.grid(row=(len(param_frames) // 3) + (i // 3), column=i % 3, padx=10, pady=10)
    param_label = ctk.CTkLabel(frame, text=params, font=("Helvetica", 20, 'bold'))
    param_label.pack(pady=10)
    multi_frame_plot = plt.Figure(figsize=(6, 4))
    ax = multi_frame_plot.add_subplot(111)
    canvas = FigureCanvasTkAgg(multi_frame_plot, master=frame)
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=1)  # Using pack instead of grid
    param_list = params.split(' vs ')
    anim = FuncAnimation(multi_frame_plot, animate_multi_graph(param_list, ax), interval=5000, save_count=100, cache_frame_data=False)
    graph_widgets[params] = canvas

# Function to fetch data from the database
def fetch_data(param, from_datetime, to_datetime):
    query = f"SELECT timestamp, {param} FROM sensor_data WHERE timestamp BETWEEN '{from_datetime}' AND '{to_datetime}' ORDER BY timestamp ASC"
    cur.execute(query)
    data = cur.fetchall()
    if data:
        df = pd.DataFrame(data, columns=['timestamp', param])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    else:
        return pd.DataFrame(columns=['timestamp', param])

# Function to update the graphs with data from the database
def update_graphs():
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    from_datetime = one_day_ago.strftime('%Y-%m-%d %H:%M:%S')
    to_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

    for param in parameters:
        data = fetch_data(param, from_datetime, to_datetime)
        if not data.empty:
            if param not in graph_widgets:
                fig, ax = plt.subplots(figsize=(6, 4))
                graph_widgets[param] = (fig, ax, FigureCanvasTkAgg(fig, master=param_frames[param]))
                graph_widgets[param][2].get_tk_widget().pack(fill=tk.BOTH, expand=1)  # Using pack instead of grid

            ax = graph_widgets[param][1]
            ax.clear()
            ax.plot(data['timestamp'], data[param], marker='o', linestyle='-', label=param)
            ax.set_title(param.replace("_", " ").title(), fontsize=16, fontname='Helvetica', fontweight='bold')
            ax.set_xlabel('Time')
            ax.set_ylabel(param)
            ax.legend()
            graph_widgets[param][2].draw()

    for params in multi_graphs:
        data = fetch_multiple_data(params, from_datetime, to_datetime)
        if not data.empty:
            frame_key = f"{params[0]} vs {params[1]}"
            if frame_key not in graph_widgets:
                fig, ax = plt.subplots(figsize=(6, 4))
                graph_widgets[frame_key] = (fig, ax, FigureCanvasTkAgg(fig, master=multi_param_frames[frame_key]))
                graph_widgets[frame_key][2].get_tk_widget().pack(fill=tk.BOTH, expand=1)  # Using pack instead of grid

            ax = graph_widgets[frame_key][1]
            ax.clear()
            ax.plot(data['timestamp'], data[params[0]], marker='o', linestyle='-', label=params[0])
            ax.plot(data['timestamp'], data[params[1]], marker='o', linestyle='-', label=params[1])
            ax.set_title(f'Time Series Data for {params[0]} and {params[1]}', fontsize=16, fontname='Helvetica', fontweight='bold')
            ax.set_xlabel('Time')
            ax.set_ylabel('Values')
            ax.legend()
            ax.grid(True)
            graph_widgets[frame_key][2].draw()

    scrollable_frame.update_idletasks()

# Function to fetch and display the time series
def fetch_and_display(param, from_date_input, from_time_input, to_date_input, to_time_input, canvas, figure, timeseries_window):
    from_date_str = from_date_input.get_date().strftime('%Y-%m-%d')
    from_time_str = from_time_input.get()
    to_date_str = to_date_input.get_date().strftime('%Y-%m-%d')
    to_time_str = to_time_input.get()

    from_datetime = f"{from_date_str} {from_time_str}:00"
    to_datetime = f"{to_date_str} {to_time_str}:00"

    fetch_and_display_timeseries(param, from_datetime, to_datetime, canvas, figure, timeseries_window)

# Function to fetch and display time series from the database
def fetch_and_display_timeseries(param, from_datetime, to_datetime, canvas, figure, timeseries_window):
    data = fetch_data(param, from_datetime, to_datetime)
    if not data.empty:
        ax = figure.add_subplot(111)
        ax.clear()
        ax.plot(data['timestamp'], data[param], marker='o', linestyle='-', label=param)
        ax.set_title(param.replace("_", " ").title(), fontsize=16, fontname='Helvetica', fontweight='bold')
        ax.set_xlabel('Time')
        ax.set_ylabel(param)
        ax.legend()
        canvas.draw()

# Function to fetch and display combined time series from the database
def fetch_and_display_combined_timeseries(params, from_datetime, to_datetime, canvas, figure, timeseries_window):
    data = fetch_multiple_data(params, from_datetime, to_datetime)
    if not data.empty:
        ax = figure.add_subplot(111)
        ax.clear()
        for param in params:
            ax.plot(data['timestamp'], data[param], marker='o', linestyle='-', label=param)
        ax.set_title(" & ".join([param.replace("_", " ").title() for param in params]), fontsize=16, fontname='Helvetica', fontweight='bold')
        ax.set_xlabel('Time')
        ax.set_ylabel(", ".join(params))
        ax.legend()
        canvas.draw()

# Function to fetch and display combined graphs
def fetch_and_display_combined(param1, param2, from_date_input, from_time_input, to_date_input, to_time_input, canvas, figure, timeseries_window):
    from_date_str = from_date_input.get_date().strftime('%Y-%m-%d')
    from_time_str = from_time_input.get()
    to_date_str = to_date_input.get_date().strftime('%Y-%m-%d')
    to_time_str = to_time_input.get()

    from_datetime = f"{from_date_str} {from_time_str}:00"
    to_datetime = f"{to_date_str} {to_time_str}:00"

    fetch_and_display_combined_timeseries((param1, param2), from_datetime, to_datetime, canvas, figure, timeseries_window)

# Function to save the graph as an image
def save_graph_as_image(figure, timeseries_window):
    file_path = tk.filedialog.asksaveasfilename(defaultextension=".png", filetypes=[("PNG files", "*.png"), ("All files", "*.*")])
    if file_path:
        figure.savefig(file_path)

# Function to periodically update graphs
def periodic_update():
    update_graphs()
    app.after(5000, periodic_update)

# Start the periodic update
app.after(5000, periodic_update)

# Initialize the main area with all graphs
display_all_graphs()

app.mainloop()

# Close the database connection on exit
conn.close()
