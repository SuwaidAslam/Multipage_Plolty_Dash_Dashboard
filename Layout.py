from dash import dcc, html, Dash
import dash_bootstrap_components as dbc

class AppLayout():
    def __init__(self, app : Dash):
        self.header = self.generateHeader(app)
        self.sidebar =  self.generateSidebarLayout()
        self.content =  self.generateContentLayout()
    # this method generates Sidebar Layout
    def generateHeader(self, app):
        header = html.Div([
                        html.A(
                            html.Img(
                                src=app.get_asset_url("logo.png"),
                                className="app__menu__img",
                            ),
                        ),
                        html.Img(
                                src=app.get_asset_url("user.png"),
                                className="user_icon",
                        ),
                        dbc.DropdownMenu(
                            label="Menu",
                            menu_variant="dark",
                            children=[
                                dbc.DropdownMenuItem("Item 1"),
                                dbc.DropdownMenuItem("Item 2"),
                                dbc.DropdownMenuItem("Item 3"),
                            ],
                            className='app_dropdown_menu',
                        ),
                ], className='header'
        )
        return header

    def generateSidebarLayout(self):
        sidebar = html.Div(
            className='sidebar',
            children=[
                    dbc.Nav([
                            dbc.NavLink("Home", href="/", id="page-1-link", active="exact"),
                            dbc.NavLink("Access Authorization", href="/page2", id="page-2-link", active="exact"),
                            dbc.NavLink("Data Flow", href="/page3", id="page-3-link", active="exact"),
                            dbc.NavLink("Overlapping Objects", href="/page4", id="page-4-link", active="exact"),
                            dbc.NavLink("Least Privilege", href="/page5", id="page-5-link", active="exact"),
                        ], vertical=True, pills=True
                    ),
            ],
        )
        return sidebar
    # this method generates content Page Layout
    def generateContentLayout(self):
        content = html.Div(id="page-content", className='content')
        return content
    
    # ------This method generates Overall App's Layout ---------
    def getAppLayout(self):
        layout = html.Div(children=[dcc.Location(id="url", refresh=False), self.header, self.sidebar, self.content])
        return layout

    # ------------------ Layout Settings End --------------------