import dash_bootstrap_components as dbc
from dash import Dash
from .Layout import AppLayout
import pandas as pd
from .AppCallback import AppCallback


class App:
    def __init__(self):
        styles = [dbc.themes.BOOTSTRAP, dbc.themes.SLATE]
        self.app = Dash(name = __name__, external_stylesheets=styles)
        self.app.config["suppress_callback_exceptions"] = True
        self.layout = AppLayout(self.app)
        self.app.layout = self.layout.getAppLayout()
        AppCallback(self.app)

    def initializeApp(self):
        return self.app

def start_and_loop(debug=False):
    app = App()
    app_instance = app.initializeApp()
    # app.run_server(host="0.0.0.0", debug=debug)
    app_instance.run_server(debug=True, port=443)