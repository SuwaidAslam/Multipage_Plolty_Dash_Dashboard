# ##################################################################################################
#  Copyright (c) 2022.    Caber Systems, Inc.                                                      #
#  All rights reserved.                                                                            #
#                                                                                                  #
#  CABER SYSTEMS CONFIDENTIAL SOURCE CODE                                                          #
#  No license is granted to use, copy, or share this software outside of Caber Systems, Inc.       #
#                                                                                                  #
#  Filename:  sankey_data_flow.py                                                                         #
#  Authors:  Rob Quiros <rob@caber.com>  rlq                                                       #
# ##################################################################################################

# Read records out of Elastic Search and build nodes for Chunksets, Objects, Systems

from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from .pandas_tools import columns_to_dict_series


DEBUG = True
BOT = datetime.fromtimestamp(1110000, tz=timezone.utc)
NOW = datetime.now(tz=timezone.utc)
# esp = ESP(CFG)


default_table = {"header": {
                    "values": ["A", "B", "C", "D"],
                    "line_color": 'darkslategray',
                    "fill_color": 'darkslategray',
                    "align": 'center',
                    "font": {"color": 'white', "size": 11},
                    "height": 20
                    },
                 "cells": {
                    "values": ["a", "b", "c", "d"],
                    "line_color": 'darkslategray',
                    "fill": {"color": ['white', 'white', 'white', 'white']},
                    # "fill_color": 'white',
                    "align": ["center", "center", "left", "left"],
                    "font": {"color": 'black', "size": 10},
                    "height": 20
                    },
                 "layout": {
                    "columnorder": [1, 2, 3, 4],
                    "columnwidth": [8, 8, 45, 45]
                    }
                 }


# table_html = Path(f"./csiMVP/{CFG.modhome}/auth_table.html").read_text()  <-- Failed experiment
#
#
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


def build_obj_overlap_table(time_from=0, time_to=0):
    """
    Using a bubble chart to show object overlaps.
    :param time_from:
    :param time_to:
    :return:
    """

    table = "obj_overlaps"
    print(f"Pulling records from ES '{table}' ")

    if not time_from:
        time_from = BOT
    if isinstance(time_from, str):
        time_from = datetime.fromisoformat(time_from.replace('Z', '+00:00')).timestamp()

    if not time_to:
        time_to = NOW
    if isinstance(time_to, str):
        time_to = datetime.fromisoformat(time_to.replace('Z', '+00:00')).timestamp()

    dmdf = pd.read_csv("Dashboard/obj_overlaps.csv.gz")
    dmdf = columns_to_dict_series(dmdf, 'a.auth', replace_in_df=True)
    dmdf = columns_to_dict_series(dmdf, 'b.auth', replace_in_df=True)
    dmdf = columns_to_dict_series(dmdf, 'risk.tags.a', replace_in_df=True)
    dmdf = columns_to_dict_series(dmdf, 'risk.tags.b', replace_in_df=True)
    # short_host = get_short_hostnames()

    if not dmdf.empty and not {'risk.score', 'risk.reason', 'percent', 'a.name', 'b.name', 'a.auth', 'b.auth'}.difference(dmdf.columns):
        dmdf['a.name'] = dmdf['a.name'].str.split(':', n=1, expand=True)[1]
        dmdf['b.name'] = dmdf['b.name'].str.split(':', n=1, expand=True)[1]

        columns = ['<b>Risk Score</b>', '<b>Percent Data Overlapping</b>', '<b>Data From</b>', '<b>Found In</b>']
        dmdf.sort_values('risk.score', axis=0, ascending=False, inplace=True)
        # data = dmdf.to_dict('list')
        dmdf.loc[dmdf['percent'].gt(100), 'percent'] = 100

        # Embedding HTML for a table in the table didn't work.
        atab = dmdf.apply(lambda C: format_dict(C['a.auth'], C['risk.tags.a']), axis=1)
        btab = dmdf.apply(lambda C: format_dict(C['b.auth'], C['risk.tags.b']), axis=1)

        data = [dmdf['risk.score'].astype(int).to_list(), (dmdf['percent'].astype(int).astype(str)).to_list(),
                (dmdf['a.name'] + '<br>' + atab.astype(str) + '</br>').to_list(),
                (dmdf['b.name'] + '<br>' + btab.astype(str) + '</br>').to_list()]

        # Embedding HTML for a table in the table didn't work.
        # atab = dmdf.apply(lambda C: pop_table(C['a.auth'], C['risk.tags']['a']), axis=1)
        # btab = dmdf.apply(lambda C: pop_table(C['b.auth'], C['risk.tags']['b']), axis=1)
        #
        # data = [dmdf['risk.score'].to_list(), (dmdf['percent'].astype(str) + '%').to_list(),
        #         (dmdf['a.name'] + '<br>' + atab.astype(str) + '</br>').to_list(),
        #         (dmdf['b.name'] + '<br>' + btab.astype(str) + '</br>').to_list()]

        lred = dmdf['risk.score'].astype(int).gt(20)
        lorn = dmdf['risk.score'].astype(int).gt(4)
        lyel = dmdf['risk.score'].astype(int).gt(2)
        dmdf.loc[lred, 'color'] = f"rgba({252},{11},{3},{.8:.1f})"
        dmdf.loc[~lred & lorn, 'color'] = f"rgba({252},{119},{3},{.8:.1f})"
        dmdf.loc[~lred & ~lorn & lyel, 'color'] = f"rgba({252},{206},{3},{.8:.1f})"
        dmdf.loc[~lred & ~lorn & ~lyel, 'color'] = f"rgba({252},{250},{217},{.8:.1f})"
        fill_color = [dmdf['color'].to_list(), ['white'] * dmdf.shape[0],
                      ['white'] * dmdf.shape[0], ['white'] * dmdf.shape[0]]

        out = default_table.copy()
        out.get("header", {}).update({"values": columns})
        out.get("cells", {}).update({"values": data})
        out.get("cells", {}).get("fill", {}).update({"color": fill_color})
        
        return out

    else:
        return default_table.copy()