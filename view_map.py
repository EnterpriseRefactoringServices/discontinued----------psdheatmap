from urllib.request import urlopen
import pandas as pd
import json
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta 
import numpy as np
from dash.dependencies import Output, Input
import plotly.express as px
import plotly.graph_objects as go
import sys
from snowflakeapi import DatabaseApi
import folium
from folium import plugins
import time
import os
import snowflake
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

external_stylesheets=[dbc.themes.LUMEN]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Treasury Analytics: The Promise Land of Treasury Securities !"
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
def run_sql_query(query: str = "show databases"):
        
    test_control_database = os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"]
    ctx = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        # database=os.environ['SNOWFLAKE_DATABASE'],
        database=test_control_database,
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
        )
    cs = ctx.cursor()
    cs.execute(query)
    rows = cs.fetchall()
    return rows
count_query = f"""
                SELECT COUNT(DISTINCT LATITUDE,LONGITUDE) AS CC
                FROM ZOD_GEO_DEFAULTZ
            """
df = run_sql_query(query=count_query)
BIG_COUNT = df[0][0]
result_count = BIG_COUNT
MAX_QUERY_RESULTS_COUNT = 50000
total_pairs = f"{BIG_COUNT}"
print(f"Total lat/long is {total_pairs}")
def get_query_result_map(start=0, num_rows=1000, database=os.environ["TEST_CONTROL_SNOWFLAKE_DATABASE"]):
    
    """
        
        all_rows = []
        field_names = None
        try:
            for query in queries:
                cs.execute(query)
                rows = cs.fetchall()
                print("running query : ", query, "\n\n")
                if field_names is None or field_names == None:
                    field_names = [d[0] for d in cs.description]
                all_rows = all_rows + rows

            dataframe = pd.DataFrame(all_rows, columns=field_names)
            self.destroy_api_connection()
            return dataframe
    
    """
    
    test_control_database = database
    ctx = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        # database=os.environ['SNOWFLAKE_DATABASE'],
        database=test_control_database,
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
        )
    cs = ctx.cursor()
    
    all_rows = []
    field_names = None
    try:
        page_number = 1
        result_count = BIG_COUNT
        while result_count > ( 13477055 - MAX_QUERY_RESULTS_COUNT):
            
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
        df.to_csv("geo.csv")
    except:
        print("error occured")
        
def load_map():
        
    rows =  pd.read_csv("geo.csv")

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
    frame = dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Iframe(id='map', srcDoc=open(filename).read(), width=screen_width, height=screen_height)
                            ]
                        ),
                    ],
                    
                )
    return frame

    
get_query_result_map(start=0, num_rows=1000)

external_stylesheets=[dbc.themes.LUX]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
databases = run_sql_query("show databases like '%HOAX%'")
app.layout = dbc.Container(
    children = [
        dbc.Container(
            children=[
                dbc.Row(
                    [
                        dbc.Col(
                            [
                            html.H1(f'Heat Map of Points {total_pairs}'),
                            ]
                        ),
                    ],
                ),
                dbc.Row(
                    [
                        
                        dbc.Col(
                            dcc.Dropdown(
                                id="database-name",
                                options=[
                                    {"label": databases[index][1], "value": databases[index][1]}
                                    for index in range(len(databases))
                                ],
                                value="db",
                                clearable=False,
                                className="dropdown",
                            ),
                        ),
                    ],
                ),
            ],
            className="card",
        ),
        dbc.Container(
            children=[
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Iframe(id='map', srcDoc=open(filename).read(), width=screen_width, height=screen_height)
                            ]
                        ),
                    ],
                    
                ),    
            ],
            className="card",
            id="map-frame", 
        )
    ],
    className="container",
)

    
@dash.callback(
    Output("map-frame", "children"),
    Input("database-name", "value"),
    prevent_initial_call=True,
)

def update_map(database_name):
    
    get_query_result_map(start=0, num_rows=1000,database=database_name)
    f = dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Iframe(id='map', srcDoc=open(filename).read(), width=screen_width, height=screen_height)
                        ]
                    ),
                ],         
    )
    return f



app.title = "PSD Heat Map"
if __name__ == "__main__":
    # if on production do 
    # app.run_server(host='0.0.0.0', port=9090, debug=True)
    
    app.run_server(port=8899, debug=True)