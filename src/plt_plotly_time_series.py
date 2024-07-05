from datetime import datetime
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
import plotly.io as pio
import math
from dash import Dash, dcc, html
import numpy as np

from sys import argv


TESLA_VIN = "LRWYGCFS6PC552861"
vin = TESLA_VIN
if len(argv) > 1:
    if argv[1] == "bmw":
        vin = "WBY71AW010FP87013"
    else:
        vin = argv[1]

df = pd.read_parquet(f"proceesed_df.parquet")
# df["date"] = df.index
# df = df.drop_duplicates("date") 
# df["rate"] = df["rate"].astype(float)

# df["raw_status"] = df["status"]
# df["status"] = df["status"].astype("string")
# df["status"] = df["status"].mask(df["status"] == "False", np.nan).ffill()

# df["is_charging"] = 0.0
# df.loc[df["status"] == "Charging", "is_charging"] = 1.0
# df.loc[df["status"] == "False", "is_charging"] = np.nan
# df.loc[(df["status"] == None) | (df["status"] == np.nan) | (df["status"] == "None"), "is_charging"] = np.nan

# print(df.loc[datetime(2024, 5, 20, 0, 44):datetime(2024, 5, 20, 0, 46), "status"].to_string(max_rows=5000))
# print(df.loc[datetime(2024, 5, 20, 0, 44):datetime(2024, 5, 20, 0, 46), "status"])
# print(df.loc[datetime(2024, 5, 20, 0, 45, 32), "status"])
# print(type(df.loc[datetime(2024, 5, 20, 0, 45, 32), "status"]))


# df["is_charging"] = df["is_charging"].ffill()
cols_to_plot = [
    'odometer',
    'inside_temp',
    'outside_temp',
    'batteryLevel',
    "soh",
    "smmoothed_soh",
    "battery_heater",
    "power"
]
# if vin == TESLA_VIN:
#     df = df[cols_to_plot]

nb_columns = len(df.columns)
rows_per_col = 6

# Calculate the number of columns needed
num_rows = math.ceil(nb_columns / rows_per_col)

# Create subplots with specified number of rows and columns
fig = make_subplots(rows=rows_per_col, cols=num_rows, shared_xaxes=True)

# Add a trace for each column
for i, col in enumerate(df.columns):
    row = (i % rows_per_col) + 1
    col_num = (i // rows_per_col) + 1
    fig.add_trace(go.Scatter(x=df.index, y=df[col], mode='lines+markers', name=col), row=row, col=col_num)

# Update layout
fig.update_layout(
    height=150*rows_per_col, 
    # width=500*num_rows,
    title='tesla variables from linkByCar processed status'
)

fig.write_html(f"{vin}.html")
fig.show()
