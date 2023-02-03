# Import necessary libraries 
from dash import html
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, dash_table



# Define the page layout
layout = dbc.Container([
    dbc.Row([
        html.H4('OVERLAPPING OBJECTS'),
        dbc.Col(
            dcc.Graph(id='obj_overlap_graph')
        )
    ]),
    dbc.Row([
        dbc.Col(
            id='overlapping_objects_table',
            className='tables'
        )
    ])
])