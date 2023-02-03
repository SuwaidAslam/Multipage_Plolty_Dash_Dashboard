from cgitb import html
from cmath import e
import imp
import plotly.graph_objects as go
import pandas as pd
import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import callback_context, Dash, html
from .pages import home, page2, page3, page4, page5
from plotly.graph_objs import Layout
from .auth_fail_table import build_auth_fail_table
from .sankey_data_flow import build_sankey_esp
from .obj_overlap_table import build_obj_overlap_table
from dash import dash_table
import plotly.express as px
import dash_cytoscape as cyto
import re



layout = Layout(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
)
template = 'plotly_dark'




def createTable(df):
    if df is not None:
        table = dash_table.DataTable(
            columns=[{'id': c, 'name': c, "selectable": False,
                    "presentation": "markdown"} for c in df.columns],
            data=df.to_dict('records'),
            page_action='none',
            markdown_options={"html": True},
            fixed_rows={'headers': True},
            style_cell={
                'minWidth': '100px',
                            'font-family': 'arial',
                            'font-size': '12px',
                            'text-align': 'center'
            },
            style_data={
                'whiteSpace': 'normal',
                'height': 'auto',
                'backgroundColor': 'rgb(50, 50, 50)',
                'color': 'white'
            },
            style_table={
                'overflowX': 'auto',
                'overflowY': 'auto'
            },
            style_header={
                'backgroundColor': 'rgb(30, 30, 30)',
                'fontWeight': 'bold',
                'color': 'white',
            },
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{Risk} > 80',
                        'column_id': 'Risk'
                    },
                    'backgroundColor': 'red',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{Risk} > 50  && {Risk} <= 80',
                        'column_id': 'Risk'
                    },
                    'backgroundColor': 'orange',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{Risk} > 0  && {Risk} <= 50',
                        'column_id': 'Risk'
                    },
                    'backgroundColor': 'yellow',
                    'color': 'black'
                }
            ],
        )
        return table
    else:
        raise PreventUpdate
    

def create_auth_table(df):
    if df is not None:
        table = dash_table.DataTable(
            id='auth_dashtable',
            columns=[{'id': c, 'name': c, "selectable": False,
                    "presentation": "markdown"} for c in df.columns],
            row_selectable="single",
            selected_rows=[0],
            data=df.to_dict('records'),
            page_action='none',
            markdown_options={"html": True},
            fixed_rows={'headers': True},
            style_cell={
                'minWidth': '100px',
                            'font-family': 'arial',
                            'font-size': '12px',
                            'text-align': 'center'
            },
            style_data={
                'whiteSpace': 'normal',
                'height': 'auto',
                'backgroundColor': 'rgb(50, 50, 50)',
                'color': 'white'
            },
            style_table={
                'overflowX': 'auto',
                'overflowY': 'auto'
            },
            style_header={
                'backgroundColor': 'rgb(30, 30, 30)',
                'fontWeight': 'bold',
                'color': 'white',
            },
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{Risk} > 80',
                        'column_id': 'Risk'
                    },
                    'backgroundColor': 'red',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{Risk} > 50  && {Risk} <= 80',
                        'column_id': 'Risk'
                    },
                    'backgroundColor': 'orange',
                    'color': 'black'
                },
                {
                    'if': {
                        'filter_query': '{Risk} > 0  && {Risk} <= 50',
                        'column_id': 'Risk'
                    },
                    'backgroundColor': 'yellow',
                    'color': 'black'
                }
            ],
        )
        return table
    else:
        raise PreventUpdate

def createCytoscape_graph(df):
    entity = str(df['Entity']).split('<br>')[0]
    entity = re.sub("^[0-9]+", "", entity).strip()
    object = str(df['Data From Object']).split('<br>')[0]
    object = re.sub("^[0-9]+", "", object).strip()
    risk_value = int(df['Risk'])
    access_value = str(df['Access']).split('<br>')[1].strip()
    if re.search('Kb', access_value):
        access_value = re.split('Kb', access_value)[0]
        access_value = int(access_value) * 1000
    elif re.search('[0-9]+b', access_value):
        access_value = re.split('b', access_value)[0]
    access_value = int(access_value)

    edge_color = 'red'
    if risk_value > 50 and risk_value <=80:
        edge_color = 'orange'
    elif risk_value > 0 and risk_value <=50:
        edge_color = 'yellow'

    node_1 ={
            'data': {'id': 'entity', 'label': str(entity)},
            'position': {'x': 20 * 49.28, 'y': -20 * -123.12},
            'classes': 'hexagon'
        }
    node_2 ={
            'data': {'id': 'object', 'label': str(object)},
            'position': {'x': 20 *45.50, 'y': -20 * -73.57},
            'classes': 'circle'
        }
    edges = {
            'data': {'source': 'entity', 'target': 'object'}
        }

    default_stylesheet = [
        {
            'selector': 'node',
            'style': {
                'background-color': '#BFD7B5',
                'label': 'data(label)'
            }
        },
        {
            'selector': '.circle',
            'style': {
                'shape': 'circle',
                'background-color': 'blue',
            }
        },
        {
            'selector': '.hexagon',
            'style': {
                'shape': 'hexagon',
                'background-color': 'green',
            }
        },
        {
            'selector': 'edge',
            'style': {
                'line-color': edge_color,
                'width' : str(access_value/7000) + 'px'
            }
        }
    ]
    elements = [node_1, node_2, edges]
    fig = cyto.Cytoscape(
        id='cytoscape-graph_',
        elements=elements,
        stylesheet=default_stylesheet,
        style={'width': '100%', 'height': '30rem', 'background-color' : '#323232'},
        layout={
            'name': 'cose'
        }
    )
    return fig


# -----------------------------------
home_page_path = '/'
page2_path = '/page2'
page3_path = '/page3'
page4_path = '/page4'
page5_path = '/page5'

class AppCallback:
    def __init__(self, app: Dash):
        self.auth_table = None
        # Main callback to decide which page to render
        @app.callback(Output('page-content', 'children'),
                      [Input('url', 'pathname')])
        def display_page(pathname):
            if pathname == home_page_path:
                return home.layout
            if pathname == page2_path:
                return page2.layout
            if pathname == page3_path:
                return  page3.layout
            if pathname == page4_path:
                return  page4.layout
            if pathname == page5_path:
                return  page5.layout
            else:  # if redirected to unknown link
                return dash.no_update

        # callback for pie charts in Home page
        @app.callback(
            [Output('pie_chart_1', 'figure'),
             Output('pie_chart_2', 'figure'),
             Output('pie_chart_3', 'figure')],
            [Input('url', 'pathname')]
        )
        def renderPieCharts(pathname):
            if pathname == home_page_path:
                font = dict(
                    color="white",
                    size=16
                )
                df1 = pd.read_csv("Dashboard/auth_fail.csv.gz")
                if df1.empty:
                    raise PreventUpdate
                else:
                    for i, row in df1.iterrows():
                        if row['score'] > 80:
                            df1.loc[i, 'risk'] = 'high'
                        elif row['score'] > 50:
                            df1.loc[i, 'risk'] = 'medium'
                        elif row['score'] > 0:
                            df1.loc[i, 'risk'] = 'low'

                    trace = go.Pie(labels=df1['risk'], values=df1['score'], name="")
                    fig1 = go.Figure(data=trace, layout=layout)
                    # Use `hole` to create a donut-like pie chart
                    fig1.update_traces(
                        hole=.6, hoverinfo="label+percent+name", textinfo='none')
                    fig1.update_layout(
                        template=template,
                        showlegend=False,
                        annotations=[dict(text='47% Fail',
                                        font=font,
                                        x=0.5, y=0.5, showarrow=False)])

                    df2 = pd.read_csv("Dashboard/obj_overlaps.csv.gz")
                    for i, row in df2.iterrows():
                        if row['risk.score'] > 20:
                            df2.loc[i, 'risk'] = 'high'
                        elif row['risk.score'] > 4:
                            df2.loc[i, 'risk'] = 'medium'
                        elif row['risk.score'] > 0:
                            df2.loc[i, 'risk'] = 'low'

                    trace = go.Pie(labels=df2['risk'],
                                values=df2['risk.score'], name="")
                    fig2 = go.Figure(data=trace, layout=layout)
                    # Use `hole` to create a donut-like pie chart
                    fig2.update_traces(
                        hole=.6, hoverinfo="label+percent+name", textinfo='none')
                    fig2.update_layout(
                        template=template,
                        showlegend=False,
                        annotations=[dict(text='8M NMER', font=font, x=0.5, y=0.5, showarrow=False)])

                    # dumy data
                    labels = ["US", "China", "European Union", "Russian Federation", "Brazil", "India",
                            "Rest of World"]
                    trace = go.Pie(labels=labels, values=[
                                27, 11, 25, 8, 1, 3, 25], name="")
                    fig3 = go.Figure(data=trace, layout=layout)
                    # Use `hole` to create a donut-like pie chart
                    fig3.update_traces(
                        hole=.6, hoverinfo="label+percent+name", textinfo='none')
                    fig3.update_layout(
                        template=template,
                        showlegend=False,
                        annotations=[dict(text='60% Xyz', font=font, x=0.5, y=0.5, showarrow=False)])

                    return [fig1, fig2, fig3]
            else:
                raise PreventUpdate
        

        # generate top issues table in Home page
        @app.callback(
            [Output('top_issues_table', 'children')],
            [Input('url', 'pathname')]
        )
        def gen_topIssues_table(pathname):
            if pathname == home_page_path:
                data = build_auth_fail_table()
                risk_score = data['cells']['values'][0]
                entity = data['cells']['values'][1]
                access = data['cells']['values'][2]
                data_from_object = data['cells']['values'][3]
                reason = data['cells']['values'][4]
                data = {
                    'Risk': risk_score,
                    'Entity': entity,
                    'Access': access,
                    'Data From Object': data_from_object,
                    'Reason': reason
                }
                df = pd.DataFrame(data)
                if df.empty:
                    data = {
                    'Risk': [""],
                    'Entity': [""],
                    'Access': [""],
                    'Data From Object': [""],
                    'Reason': [""],
                    }
                df = pd.DataFrame(data)
                table = createTable(df)
                return [table]
            else:
                raise PreventUpdate

        # callback for sankey graph in Dataflow page
        @app.callback(
            Output("graph", "figure"),
            Input("filter_var_select", "value"),
            Input("filter_values_input_text", "value"),
            Input("show_operations_list", "value"),
            Input("select_api_levels", "value"),
            Input("rangepicker", "startDate"),
            Input("rangepicker", "endDate")
        )
        def display_sankey(filter_var, filter_val, show_ops, api_levels, start, end):
            data = build_sankey_esp(filter_var=filter_var, filter_val=filter_val, show_ops=show_ops,
                                    api_levels=api_levels, time_from=start, time_to=end)
            node = data.get('data', [])[0].get('node', {})
            link = data.get('data', [])[0].get('link', {})
            fig = go.Figure(go.Sankey(node=node, link=link))
            fig.update_layout(
                template=template,
                autosize=True,
            )
            return fig
        
        # Callback for Overlapping Objects page
        @app.callback(
        Output("obj_overlap_graph", "figure"),
        Output("overlapping_objects_table", "children"),
        Input('url', 'pathname'))
        def render_obj_overlap_page(pathname):
            if pathname == page4_path:
                df = pd.read_csv("Dashboard/obj_overlaps.csv.gz")
                if df.empty:
                    fig = go.Figure()
                else:
                    trace = px.scatter(df, x="risk.score", y="percent",
                                    size="a.nmers", color="risk.score", log_x=True, size_max=50)
                    fig = go.Figure(data=trace)
                    fig.update_layout(
                        showlegend=False,   
                        template=template,
                    )

                # table 
                data = build_obj_overlap_table()
                risk_score = data['cells']['values'][0]
                percent = data['cells']['values'][1]
                datafrom = data['cells']['values'][2]
                found_in = data['cells']['values'][3]
                data = {
                    'Risk': risk_score,
                    'Percent': percent,
                    'Data From': datafrom,
                    'Found In': found_in,
                }
                df = pd.DataFrame(data)
                if df.empty:
                    data = {
                    'Risk': ["" ],
                    'Percent': [""],
                    'Data From': [""],
                    'Found In': [""],
                    }
                    df = pd.DataFrame(data)
                table = createTable(df)
                return [fig, table]
            else:
                raise PreventUpdate

        # callback for Access Authorization Page   
        @app.callback(
        Output("auth_fail_table", "children"),
        Input('url', 'pathname'))
        def render_auth_fail_page(pathname):
            if pathname == page2_path:
                data = build_auth_fail_table()
                risk_score = data['cells']['values'][0]
                entity = data['cells']['values'][1]
                access = data['cells']['values'][2]
                data_from_object = data['cells']['values'][3]
                reason = data['cells']['values'][4]
                data = {
                    'Risk': risk_score,
                    'Entity': entity,
                    'Access': access,
                    'Data From Object': data_from_object,
                    'Reason': reason
                }
                df = pd.DataFrame(data)
                if df.empty:
                    data = {
                        'Risk': [],
                        'Entity': [],
                        'Access': [],
                        'Data From Object': [],
                        'Reason': []
                    }
                    df = pd.DataFrame(data)
                self.auth_table = df
                table = create_auth_table(df)
                return table
            else:
                raise PreventUpdate
    
        @app.callback(
        Output("cytoscape_graph", "children"),
        Input('auth_dashtable', "selected_rows"))
        def render_cytoscape_graph(selected_rows):
            if self.auth_table is None or selected_rows is None:
                raise PreventUpdate
            else:
                if len(selected_rows) !=0:
                    selected_row_df = self.auth_table.loc[selected_rows]
                    return createCytoscape_graph(selected_row_df)
                else:
                    raise PreventUpdate

        

