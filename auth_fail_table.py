# ##################################################################################################
#  Copyright (c) 2022.    Caber Systems, Inc.                                                      #
#  All rights reserved.                                                                            #
#                                                                                                  #
#  CABER SYSTEMS CONFIDENTIAL SOURCE CODE                                                          #
#  No license is granted to use, copy, or share this software outside of Caber Systems, Inc.       #
#                                                                                                  #
#  Filename:  auth_fail_table.py                                                                   #
#  Authors:  Rob Quiros <rob@caber.com>  rlq                                                       #
# ##################################################################################################

# Read records out of Elastic Search and build nodes for Chunksets, Objects, Systems

import numpy as np
from datetime import datetime, timezone
from .pandas_tools import columns_to_dict_series
import pandas as pd


DEBUG = True
BOT = datetime.fromtimestamp(1110000, tz=timezone.utc)
NOW = datetime.now(tz=timezone.utc)
# esp = ESP(CFG)


default_table = {"header": {
                    "values": ["A", "B", "C", "D", "E"],
                    "line_color": 'darkslategray',
                    "fill_color": 'darkslategray',
                    "align": 'center',
                    "font": {"color": 'white', "size": 11},
                    "height": 20
                    },
                 "cells": {
                    "values": ["a", "b", "c", "d", "e"],
                    "line_color": 'darkslategray',
                    "fill": {"color": ['white', 'white', 'white', 'white', 'white']},
                    # "fill_color": 'white',
                    "align": ["center", "left", "center", "left", "left"],
                    "font": {"color": 'black', "size": 10},
                    "height": 20
                    },
                 "layout": {
                    "columnorder": [1, 2, 3, 4, 5],
                    "columnwidth": [8, 40, 15, 40, 40]
                    }
                 }

# default_table = dash_table.DataTable(
#     id="procedure-stats-table",
#     columns=[{"name": i, "id": i} for i in procedure_dict.keys()],
#     data=procedure_data_df.to_dict("rows"),
#     filter_action="native",
#     sort_action="native",
#     style_cell={
#         "textOverflow": "ellipsis",
#         "background-color": "#242a3b",
#         "color": "#7b7d8d",
#     },
#     sort_mode="multi",
#     page_size=5,
#     style_as_list_view=False,
#     style_header={"background-color": "#1f2536", "padding": "2px 12px 0px 12px"},
# )

# table_html = Path(f"./csiMVP/{CFG.modhome}/auth_table.html").read_text()


# def pop_table(auth, tags):
#
#     tab = table_html.replace('%uid%', str(auth.get('uid', '')))
#     tab = tab.replace('%allow%', str(auth.get('allow', '')))
#     tab = tab.replace('%deny%', str(auth.get('deny', '')))
#     tab = tab.replace('%groups%', str(auth.get('groups', '')))
#     tab = tab.replace('%region%', str(auth.get('region', '')))
#     tab = tab.replace('%bguid%', tags.get('uid', 'bggry'))
#     tab = tab.replace('%bgallow%', tags.get('allow', 'bggry'))
#     tab = tab.replace('%bgdeny%', tags.get('deny', 'bggry'))
#     tab = tab.replace('%bggroups%', tags.get('groups', 'bggry'))
#     tab = tab.replace('%bgregion%', tags.get('region', 'bggry'))
#
#     return tab


color_map = {"bgyel": '<p style="background-color: #f8ff00">',
             "bgred": '<p style="background-color: #fd6864">',
             "bgorn": '<p style="background-color: #ffcc67">',
             "bggry": '<p style="background-color: #ecf4ff">'}

color_map2 = {"bgyel": '<font background-color="#f8ff00">',
              "bgred": '<font background-color="#fd6864">',
              "bgorn": '<font background-color="#ffcc67">',
              "bggry": '<font background-color="#ecf4ff">'}


def format_dict(auth, tags, keeps=None):
    #    keeps = ['uid', 'groups', 'allow', 'deny', 'region']
    if keeps is None:
        keeps = {'uid', 'groups', 'allow', 'deny'}
    else:
        keeps = set(keeps)

    keys = keeps.intersection(auth.keys())
    out = ""

    for k in keys:
        v = auth.get(k)
        if k in ['uid', 'name', 'owner']:
            if not v:
                v = "&lt;not set&gt;"
            else:
                v = v.replace('@', '&#64;')
        if k in ['allow'] and not v:
            v = "&#91;'all'&#93;"
        if k in ['deny'] and not v:
            v = "&#91;'none'&#93;"
        # out += f"<b>{k}:</b> {color_map.get(tags.get(k, 'bggry'))} {auth.get(k, '')} </p>  "
        # out += f"<b>{k}:</b> {color_map2.get(tags.get(k, 'bggry'))} {auth.get(k, '')} </font>  "
        out += f"<b>{k}:</b> {v}  "

    return out


def build_auth_fail_table(time_from=0, time_to=0):
    table = "auth_fail"
    print(f"Pulling authorization failure records from ES '{table}' ")

    if not time_from:
        time_from = BOT
    if isinstance(time_from, str):
        time_from = datetime.fromisoformat(time_from.replace('Z', '+00:00')).timestamp()

    if not time_to:
        time_to = NOW
    if isinstance(time_to, str):
        time_to = datetime.fromisoformat(time_to.replace('Z', '+00:00')).timestamp()

    dmdf = pd.read_csv("Dashboard/auth_fail.csv.gz")

    expected_columns = {'R.ID', 'R.TIME', 'score', 'alert.reason', 'svc_name',
                        'obj_name', 'p.method', 's.auth', 'o.auth', 'cypher'}

    dmdf = columns_to_dict_series(dmdf, 's.auth', replace_in_df=True)
    dmdf = columns_to_dict_series(dmdf, 'o.auth', replace_in_df=True)
    dmdf = columns_to_dict_series(dmdf, 'alert.tags.a', replace_in_df=True)
    dmdf = columns_to_dict_series(dmdf, 'alert.tags.b', replace_in_df=True)

    if not dmdf.empty and not expected_columns.difference(dmdf.columns):
        columns = ['<b>Risk Score</b>', '<b>Entity</b>', '<b>Access</b>', '<b>Data From Object</b>', '<b>Reason</b>']

        dmdf.sort_values('score', axis=0, ascending=False, inplace=True)
        kbytes = dmdf['bytes'].fillna(0).astype(int)
        lokb = kbytes.gt(10000)
        kbytes.loc[lokb] = (kbytes.loc[lokb] / 1024).astype(int).astype(str) + 'Kb'
        kbytes.loc[~lokb] = kbytes.loc[~lokb].astype(int).astype(str) + 'b'

        # Embedding HTML for a table in the table didn't work.
        stab = dmdf.apply(lambda C: format_dict(C['s.auth'], C['alert.tags.a'], keeps=['groups']), axis=1)
        otab = dmdf.apply(lambda C: format_dict(C['o.auth'], C['alert.tags.b'], keeps=['uid', 'allow', 'deny']), axis=1)

        data = [dmdf.loc[:, 'score'].astype(int).to_list(),
                (dmdf['svc_name'].astype(str) + '<br>' + stab.astype(str) + '</br>').to_list(),
                (dmdf['p.method'].astype(str).str.upper() + '<br>' + kbytes).to_list(),
                (dmdf['obj_name'].astype(str) + '<br>' + otab.astype(str) + '</br>').to_list(),
                (dmdf['alert.reason'].astype(str).str.replace('\n', "<br>", regex=False)).to_list(),
                ]

        lred = dmdf.loc[:, 'score'].astype(int).gt(80)
        lorn = dmdf.loc[:, 'score'].astype(int).gt(50)
        lyel = dmdf.loc[:, 'score'].astype(int).gt(20)
        dmdf.loc[lred, 'color'] = f"rgba({252},{11},{3},{.8:.1f})"
        dmdf.loc[~lred & lorn, 'color'] = f"rgba({252},{119},{3},{.8:.1f})"
        dmdf.loc[~lred & ~lorn & lyel, 'color'] = f"rgba({252},{206},{3},{.8:.1f})"
        dmdf.loc[~lred & ~lorn & ~lyel, 'color'] = f"rgba({252},{250},{217},{.8:.1f})"
        fill_color = [dmdf.loc[:, 'color'].to_list(), ['white'] * dmdf.shape[0],
                      ['white'] * dmdf.shape[0], ['white'] * dmdf.shape[0]]

        out = default_table.copy()
        out.get("header", {}).update({"values": columns})
        out.get("cells", {}).update({"values": data})
        out.get("cells", {}).get("fill", {}).update({"color": fill_color})
        
        return out

    else:
        return default_table.copy()
