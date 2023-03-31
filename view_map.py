from urllib.request import urlopen
import pandas as pd
import json
import dash
from dash import html, dcc, DiskcacheManager
import dash_bootstrap_components as dbc
from dash_extensions.enrich import DashProxy
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta 
import numpy as np
from dash.dependencies import Output, Input
import plotly.express as px
import plotly.graph_objects as go
import sys
import folium
from folium import plugins
from flask import Flask
import time
import os
import os.path
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas
import diskcache
cache = diskcache.Cache("./cache")

now = datetime.now()
past_date = now + relativedelta(months=-12)
next_week = now + relativedelta(days=56)
date_format = "%m/%d/%Y"
next_week = next_week.strftime(date_format)
today = now.strftime(date_format)
today, next_week  = str(today) , str(next_week)
startDate = past_date.strftime(date_format)
endDate = datetime.today().strftime(date_format)
startDate, endDate = str(startDate), str(endDate)

num_rows = 3
query = "SELECT LATITUDE, LONGITUDE, TIMESTAMP FROM ZOD_GEO_DEFAULTZ LIMIT {num_rows}"
filename = "map.html"
moscow_lat, moscow_lon = 55.7558, 37.6173
colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred',
 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 'pink', 
 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']

import tkinter as tk
root = tk.Tk()
screen_ratio = 1
screen_width = str( int(float (root.winfo_screenwidth()) * screen_ratio ) ) 
screen_height = str( int(float (root.winfo_screenheight()) * screen_ratio) )
import random

def get_db_connector(database=os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"]):
    
    test_control_database = database
    try:
        ctx = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            # database=os.environ['SNOWFLAKE_DATABASE'],
            database=test_control_database,
            schema=os.environ['SNOWFLAKE_SCHEMA'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
            )
        return ctx
    except Exception as e:
        print(f"Error getting connector : {str(e)}")
    
def run_sql_query(query: str = "show databases like '%HOAX%'", connector = get_db_connector(), fetch_pandas=False):
    if connector is not None:
        ctx = connector
        cs = ctx.cursor()
        cs.execute(query)
        if fetch_pandas is False:
            rows = cs.fetchall()
        else:
            rows = cs.fetch_pandas_all()
        cs.close()
        ctx.close()
        return rows

def get_stats_object(database=os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"]):
    
    count_query = f"""
                    SELECT COUNT(*) AS BIG_COUNT, COUNT(DISTINCT LATITUDE,LONGITUDE) AS CC, COUNT(DISTINCT ID) AS CD
                    FROM ZOD_GEO_DEFAULTZ
                """
    selected_database = database
    count_connector = get_db_connector(database=selected_database)
    df = run_sql_query(query=count_query, connector=count_connector, fetch_pandas=True)
    DISTINCT_LATLONG_COUNT = df['CC'][0]
    UNIQUE_MAID_COUNT = df['CD'][0]
    BIG_COUNT = df['BIG_COUNT'][0]
    unique_latlong_pairs = f"{DISTINCT_LATLONG_COUNT}"
    unique_devices = f"{UNIQUE_MAID_COUNT}"
    print(f"Selected Database {selected_database}")
    print(f"Unique lat/long is {unique_latlong_pairs}")
    print(f"Unique devices is {unique_devices}")
    obj = {
        "Database Name":f"{selected_database}",
        "Total Rows" : f"{BIG_COUNT}",
        "Unique Points(Lat/Longs)": f"{DISTINCT_LATLONG_COUNT}",
        "Unique Devices" : f"{UNIQUE_MAID_COUNT}",
    }
    return obj
        
def get_stats_html_table(database=os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"]):
    row_object = get_stats_object(database=database)
        
    color_selector = html.Div(
        [
            html.Div("Select a colour theme for the Stats Table:"),
            dbc.Select(
                id="change-table-color",
                options=[
                    {"label": "primary", "value": "primary"},
                    {"label": "secondary", "value": "secondary"},
                    {"label": "success", "value": "success"},
                    {"label": "danger", "value": "danger"},
                    {"label": "warning", "value": "warning"},
                    {"label": "info", "value": "info"},
                    {"label": "light", "value": "light"},
                    {"label": "dark", "value": "dark"},
                ],
                value="light",
            ),
        ],
        className="p-3 m-2 border",
    )

    table_header = [
        html.Thead(html.Tr([html.Th("Field"), html.Th("Value")]))
    ]

    table_rows = [html.Tr([html.Td(title_key), html.Td(f"{row_object[title_key]}")]) for title_key in row_object]
    table_body = [html.Tbody(table_rows)]
    stats_table = html.Div(
        [
            color_selector,
            dbc.Table(
                # using the same table as in the above example
                table_header + table_body,
                id="table-color",
                bordered=True,
                hover=True,
                color="light",
            ),
        ]
    )
    return stats_table

    
count_query = f"""
                SELECT COUNT(*) AS BIG_COUNT, COUNT(DISTINCT LATITUDE,LONGITUDE) AS CC, COUNT(DISTINCT ID) AS CD
                FROM ZOD_GEO_DEFAULTZ
            """
selected_database = os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"]
count_connector = get_db_connector(database=selected_database)
df = run_sql_query(query=count_query, connector=count_connector, fetch_pandas=True)
DISTINCT_LATLONG_COUNT = df['CC'][0]
UNIQUE_MAID_COUNT = df['CD'][0]
BIG_COUNT = df['BIG_COUNT'][0]

MAX_QUERY_RESULTS_COUNT = 20000
unique_latlong_pairs = f"{DISTINCT_LATLONG_COUNT}"
unique_devices = f"{UNIQUE_MAID_COUNT}"

def get_query_result_map(start=0, num_rows=1000, database=os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"], csvfile="geo.csv"):
    
    all_rows = []
    field_names = None
    try:
        page_number = 1
        result_count = BIG_COUNT
        ctx = get_db_connector(database=database)
        cs = ctx.cursor()
        while result_count > ( BIG_COUNT - MAX_QUERY_RESULTS_COUNT):
            
            query = f"""
                        SELECT DISTINCT LATITUDE,LONGITUDE
                        FROM ZOD_GEO_DEFAULTZ GROUP BY LATITUDE, LONGITUDE LIMIT {MAX_QUERY_RESULTS_COUNT} offset {page_number}
                    """
            cs.execute(query)
            rows = cs.fetchall()
            
            if field_names is None or field_names == None:
                field_names = [d[0] for d in cs.description]
            all_rows = all_rows + rows
            result_count -= MAX_QUERY_RESULTS_COUNT
            page_number += 1
        df = pd.DataFrame(all_rows, columns=field_names)
        df.to_csv(csvfile)
        # don't forget to close db conn
        cs.close()
        ctx.close()
    except:
        print("error occured")
    return df
def load_map(csvfile="geo.csv", filename=filename):
        
    rows =  pd.read_csv(csvfile)

    #data = np.array(rows, dtype=[('LATITUDE','U1'),('TIMESTAMP','U1')])
    #return pd.DataFrame.from_records(data)
    #print("first five items : ",rows[:5])
    index = None
    row = None
    for i, r in rows.iterrows():    
        index=i
        row=r
        break
    if row is not None and index is not None:
        m = folium.Map(location=[rows['LATITUDE'][index], rows['LONGITUDE'][index]], zoom_start=2.8 )
        heat_data = [[rows['LATITUDE'][index], rows['LONGITUDE'][index]] for index, _ in rows.iterrows()]
        plugins.HeatMap(heat_data).add_to(m)
        m.save(filename)

    i = 0

    time.sleep(1)
    frame = html.Iframe(id='map', srcDoc=open(filename).read(), width=screen_width, height=screen_height)
    return frame

def get_app_layout():
    
    databases = run_sql_query("show databases like '%HOAX%'")
    return dbc.Container(
        children = [
            
            dbc.Container(
                children=[
                    dcc.Dropdown(
                                id="database-name",
                                options=[
                                    {"label": databases[index][1], "value": databases[index][1]}
                                    for index in range(len(databases))
                                ],
                                value=f"{selected_database}",
                                clearable=False,
                                className="dropdown",
                                
                    ),
                ],
                className="card",
            ),
            dbc.Container(
                children=[
                    html.Iframe(id='map', srcDoc=open(filename).read(), width=screen_width, height=screen_height)
                ],
                className="card",
                id="map-frame", 
            ),
            dbc.Container(
                children=[    
                    dbc.Row(
                            [
                                dbc.Col(
                                        html.Div(
                                            [
                                                dbc.Spinner(spinner_style={"width": "8rem", "height": "8rem"}, color="secondary"), " Loading"
                                            ],
                                            id="maploading",
                                            hidden=True
                                        ),
                                ),
                            ]
                    ), 
                    dbc.Row(
                            [
                                dbc.Col(
                                    get_stats_html_table()
                                ),
            
                            ]
                    ),
                ],
                className="card",
                id="stats-card", 
            )
        ],
        className="container",
        fluid=True,
    )


get_query_result_map(start=0, num_rows=1000, csvfile=f"{selected_database}.csv")

@dash.callback(
    Output("map-frame", "children"),
    Output("stats-card", "children"),
    Input("database-name", "value"),
    background=True,
    running=[
        (Output("maploading", "hidden"), False, True),
    ],
    prevent_initial_call=True,
)

def update_map(database_name):
    fname = f"{database_name}.csv"
    df = get_query_result_map(start=0, num_rows=1000,database=database_name, csvfile=fname)
    
    df.to_csv(fname)
    time.sleep(3)
    htmlfile  = f"{database_name}.html"
    f = load_map(csvfile=fname, filename=htmlfile)
    
   
    
    stats_table = get_stats_html_table(database=database_name)
    return f, stats_table


@dash.callback(
    Output("table-color", "color"), Input("change-table-color", "value")
)
def change_table_colour(color):
    return color



external_stylesheets=[dbc.themes.YETI]

server = Flask(__name__)


background_callback_manager = DiskcacheManager(cache)
app = DashProxy(
    server=server,
    external_stylesheets=external_stylesheets,
    background_callback_manager=background_callback_manager,
    transforms=[],
)
app.title = "Heat Map for PSD Data  Stores"

app.layout = get_app_layout()
if __name__ == "__main__":
    # if on production do 
    # app.run_server(host='0.0.0.0', port=9090, debug=True)
    
    app.run_server(host='0.0.0.0',port=8899, debug=True)