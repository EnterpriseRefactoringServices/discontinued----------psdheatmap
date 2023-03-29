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
import time
import geopandas as gpd
import sys

external_stylesheets=[dbc.themes.LUMEN]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = " Viewing points on map !"

filename = "map.html"
moscow_lat, moscow_lon = 55.7558, 37.6173
colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred',
 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 'pink', 
 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']

import tkinter as tk
root = tk.Tk()
screen_ratio = 0.9
screen_width = str( int(float (root.winfo_screenwidth()) * screen_ratio ) ) 
screen_height = str( int(float (root.winfo_screenheight()) * screen_ratio) )
import random
import os 


def show_geojson_on_map(filename: str=""):
    

    
    data = [
            [
              34.7797723070816,
              56.60499738387864
            ],
            [
              34.16183248425128,
              54.63751270644002
            ],
            [
              37.068101194159794,
              53.882160008049595
            ],
            [
              40.856741036074254,
              55.257743883070646
            ],
            [
              39.40234530676307,
              57.54429345925061
            ],
            [
              34.7797723070816,
              56.60499738387864
            ]
        ]
   
    i = 0
    m = None
    for xy in data:
       
        lat = xy[0]
        lon = xy[1]
        randindex = random.randint(0, len(colors)-1)
        if i > 0:
            folium.Marker(
                [lat, lon], popup=f"<i> added on {str(lat)},{str(lon)}</i>", 
                tooltip=f"({lat},{lon} )", 
                icon=folium.Icon(color=colors[randindex])
            ).add_to(m)
        else:
            m = folium.Map(location=[lat, lon], zoom_start=2.8, tiles='Stamen Terrain' ) 
            
        i += 1
    
    if m is not None:
        	
        
        m.save("map.html")
        #time.sleep(1)
    
    
    



show_geojson_on_map(filename)

external_stylesheets=[dbc.themes.LUX]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = dbc.Container(
    children = [
        dbc.Container(
            children=[
                dbc.Row(
                    [
                        dbc.Col(
                            [
                            html.H1('Map of Points'),
                            ]
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
        )
    ],
    className="container",
)
app.title = "Donuts! Pizza ! Almonds !"
if __name__ == "__main__":
    # if on production do 
    # app.run_server(host='0.0.0.0', port=9090, debug=True)
    #sys.exit(0)
    app.run_server(port=8899, debug=True)