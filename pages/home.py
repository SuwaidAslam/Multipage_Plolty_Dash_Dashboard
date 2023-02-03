# Import necessary libraries 
from turtle import width
from dash import html
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, dash_table



# Define the page layout
layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H5('Data Access Authorization Failures', className='pie_title'),
            dcc.Graph(id="pie_chart_1", config = {'displayModeBar': False}, className='piecharts'),
        ], width=4), 
        dbc.Col([
            html.H5('Overlapping Object Policy Risk', className='pie_title'),
            dcc.Graph(id="pie_chart_2", config = {'displayModeBar': False}, className='piecharts')
        ], width=4),
        dbc.Col([
            html.H5('Least Privilege Policy Faults', className='pie_title'),
            dcc.Graph(id="pie_chart_3", config = {'displayModeBar': False}, className='piecharts'),
        ], width=4)
    ]),
    dbc.Row([
        html.H4('TOP ISSUES'),
        dbc.Col(
            id='top_issues_table',
            className='tables'
        )
    ])
])