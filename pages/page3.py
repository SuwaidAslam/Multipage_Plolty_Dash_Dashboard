# Import necessary libraries 
from dash import html
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, dash_table
import dash_datetimepicker


filter_list = ["Object.Name", "Service.Name",
               "Service.Groups", "Host.Name", "API"]

controlls = html.Div(
                children=[
                    dbc.Row([
                            dbc.Col(
                                id="select-filter-outer",
                                children=[
                                    html.Label("Filter Variable"),
                                    dcc.Dropdown(
                                        id="filter_var_select",
                                        style={'height' : '3rem'},
                                        className='feilds',
                                        options=[{"label": i, "value": i}
                                                    for i in filter_list],
                                        value=filter_list[1],
                                    ),
                                ], className='controlls_padding'
                            ),
                            dbc.Col(
                                id="input-value-outer",
                                children=[
                                    html.Label("Filter Value"),
                                    dbc.Input(
                                        className='feilds',
                                        id="filter_values_input_text",
                                        type="text",
                                        placeholder="Filter value",
                                    ),
                                ], className='controlls_padding'
                            ),
                            dbc.Col(
                                id="api-levels-div",
                                children=[
                                    html.Label("Api Levels to Show"),
                                    dbc.Input(id='select_api_levels',
                                                className='feilds',
                                                type='number',
                                                value=4,
                                                step=1,
                                                min=0,
                                                max=10,
                                                ),
                                ], className='controlls_padding'
                            ),
                    ]),
                    dbc.Row([
                            dbc.Col([
                                html.Label("Time Range", className='controlls_padding'),
                                html.Div(
                                    id="date-time-slider",
                                    className='feilds',
                                    children=[
                                        dash_datetimepicker.DashDatetimepicker(
                                            id='rangepicker'),
                                    ],
                                ),
                            ]),
                            dbc.Col(
                                id="ops-checklist",
                                children=[
                                    html.Label("Operations to show"),
                                    dbc.Checklist(
                                        style={'padding-top' : '10px'},
                                        options=[
                                            {"label": "GET",
                                                "value": 'GET'},
                                            {"label": "PUT",
                                                "value": 'PUT'},
                                            {"label": "GET_META",
                                                "value": 'GET_META'},
                                            {"label": "PUT_META",
                                                "value": 'PUT_META'},
                                        ],
                                        value=['GET', 'PUT'],
                                        inline=True,
                                        id="show_operations_list",
                                    )
                                ], className='controlls_padding'
                        ),
                    ])
                ], className="sankey_graph_controlls")

# Define the page layout
layout = dbc.Container([
    dbc.Row([
        html.H4('CONTROLLS'),
        dbc.Col(
            controlls
        )
    ]
    ),
    dbc.Row([
        html.H4('DATA FLOW'),
        dbc.Col(
            dcc.Graph(id='graph', className='sankey_graph')
        )
    ]
    )
])