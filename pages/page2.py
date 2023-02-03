# Import necessary libraries 
from dash import html
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, dash_table



# Define the page layout
layout = dbc.Container([
    dbc.Row([
        html.H4('DATA ACCESS AUTHORIZATION FAILURES'),
        dbc.Col(
            id='auth_fail_table',
            className='tables',
            children=[
                dash_table.DataTable(
                        id='auth_dashtable')
            ]
        )
    ]),
    dbc.Row([
        dbc.Col(
            html.Div(id='cytoscape_graph')
        )
    ])
])