# ##################################################################################################
#  Copyright (c) 2022.    Caber Systems, Inc.                                                      #
#  All rights reserved.                                                                            #
#                                                                                                  #
#  CABER SYSTEMS CONFIDENTIAL SOURCE CODE                                                          #
#  No license is granted to use, copy, or share this software outside of Caber Systems, Inc.       #
#                                                                                                  #
#  Filename:  sankey_data_flow.py                                                                  #
#  Authors:  Rob Quiros <rob@caber.com>  rlq                                                       #
# ##################################################################################################

# Read records out of Elastic Search and build nodes for Chunksets, Objects, Systems

import numpy as np
import pandas as pd
from datetime import datetime, timezone
from .pandas_tools import columns_to_dict_series

import json
import random

DEBUG = True
BOT = datetime.fromtimestamp(1110000, tz=timezone.utc)
NOW = datetime.now(tz=timezone.utc)
# esp = ESP(CFG)

token = "\u22ca\u22c9"

default_sankey = {"data": [{"type": "sankey",
                            "domain": {"x": [0, 1], "y": [0, 1]},
                            "orientation": 'h',
                            "valueformat": '',
                            "valuesuffix": '',
                            "node": {"pad": 15, "thickness": 15, "line": {"color": "black", "width": 0.5},
                                     "label":  ["NO", "", "DATA"],
                                     "color":  ["red", "red", "red"],
                                     "customdata": ["A", "B"],
                                     },
                            "link": {"source": [0, 1],
                                     "target": [1, 2],
                                     "value":  [.2, .2],
                                     "customdata": [10, 20],
                                     "label":  ["NO", "DATA"],
                                     "color":  ["%A%", "%A%"]
                                     }
                            }],
                  "layout": {"title": {"text": ""},
                             "width":  1000,
                             "height": 1000,
                             "font": {"size": 8},
                             "updatemenus": [],
                             "xaxis": {
                                "rangeselector": {
                                    "buttons": [
                                        {"count": 1,
                                             "label": "min",
                                             "step": "minute",
                                             "stepmode": "backward"},
                                        {"count": 1,
                                             "label": "hrs",
                                             "step": "hour",
                                             "stepmode": "backward"},
                                        {"count": 1,
                                             "label": "day",
                                             "step": "day",
                                             "stepmode": "backward"},
                                        {"step": "all"}
                                    ]},
                                "rangeslider": {"visible": True},
                             }}
                  }


def get_short_hostnames():
    service_names = {}
    ret = pd.read_csv("Dashboard/services_map.csv.gz")
    if not ret.empty:
        xx = ret[['host', 'host_full']].to_dict(orient='records')
        service_names.update({x.get('host_full'): x.get('host') for x in xx if x.get('host') and x.get('host_full')})
    return service_names


def rand_color(x, opacity=.7):
    r = random.randint(0, 255)
    g = random.randint(0, 255)
    b = random.randint(0, 255)
    a = opacity
    return f"rgba({r},{g},{b},{a:.1f})"


def col_list_join(x, lim: int = -1):
    if isinstance(x, list):
        return '/'.join(x[:lim])
    else:
        return x


def build_sankey_esp(filter_var='', filter_val='', show_ops='', time_from=0, time_to=0, opacity: float=0.7, api_levels: int=4, obj_levels: int=3):
    table = 'flow-table'
    print(f"Pulling records from ES '{table}' ")

    tmin, tmax = get_tmax_tmin()

    if not time_from:
        time_from = tmin
    if isinstance(time_from, str):
        time_from = datetime.fromisoformat(time_from.replace('Z', '+00:00')).timestamp()

    if not time_to:
        time_to = tmax
    if isinstance(time_to, str):
        time_to = datetime.fromisoformat(time_to.replace('Z', '+00:00')).timestamp()

    dmdf = pd.read_csv("Dashboard/flow-table.csv.gz")

    short_host = get_short_hostnames()

    # Check first if we can make a Sankey diagram out of this data
    need_two = ['Service', 'API', 'Object', 'Host', 'Bucket', 'Host_A', 'Host_B', 'Host_U']
    need_all = ['e.op', 'from_name', 'from_type', 'to_name', 'to_type']

    if len(dmdf.columns.intersection(need_two)) >= 2 and not set(need_all).difference(dmdf.columns):

        # Filter out the operations that the user did not select
        if isinstance(show_ops, list) and set(show_ops).intersection(['PUT', 'GET', 'PUT_META', 'GET_META', 'DELETE']):
            dmdf = dmdf.loc[dmdf['e.op'].isin(show_ops)]

        # This section of code handles the collapsing of the name hierarchy for Services and Objects
        # based on inputs api_level and obj_level
        svcs = dmdf.get("Service").drop_duplicates()
        svcs = svcs.loc[svcs.ne('') & svcs.notna()].to_list()

        bkts = dmdf.get("Bucket").drop_duplicates()
        bkts = bkts.str.split(token, n=1, expand=True)[1]                             # Drop the CORI prefix from the bucket
        bkts = bkts.drop_duplicates()
        bkts = bkts.loc[bkts.ne('') & bkts.notna()].to_list()

        hsts = []
        for m in ['_A', '_B', '_U']:
            x = dmdf.get("Host" + m).drop_duplicates().replace(short_host)
            # x = x.str.upper()
            hsts.extend(x.loc[x.ne('') & x.notna()].to_list())
            # Create a Host column that contains the value in 'Host_A', 'Host_B', 'Host_U' depending on
            # if the value in from_type or to_type has 'Host_A', 'Host_B', 'Host_U' in it.
            which = dmdf['from_type'].eq("Host" + m)
            dmdf.loc[which, 'Host'] = dmdf.loc[which, "Host" + m].astype(str)
            dmdf['from_type'] = dmdf['from_type'].astype(str)
            dmdf.loc[which, 'from_type'] = "Host"
            which = dmdf['to_type'].eq("Host" + m)
            dmdf.loc[which, 'Host'] = dmdf.loc[which, "Host" + m].astype(str)
            dmdf['to_type'] = dmdf['to_type'].astype(str)
            dmdf.loc[which, 'to_type'] = "Host"

        bad_host = (dmdf['from_type'].eq('Host') | dmdf.loc[:, 'to_type'].eq('Host')) & dmdf.loc[:, 'Host'].isna()
        dmdf = dmdf.loc[~bad_host].fillna({"Host": ""})

        hsts = list(set(hsts))

        if obj_levels is None or obj_levels <= 0:
            objs = dmdf.get("Object").drop_duplicates()
            objs = objs.str.split('/', n=1, expand=True)[1]              # Drop the CORI prefix and bucket from the object
            objs = objs.drop_duplicates()
            objs = objs.loc[objs.ne('') & objs.notna()].to_list()
        else:
            objs = dmdf.get("Object").drop_duplicates()
            objs = objs.str.split('/', n=1, expand=True)[1]              # Drop the CORI prefix and bucket from the object
            objs = objs.str.split('/', n=obj_levels)                     # Limit the number of levels in the object prefix
            objs = objs.transform(col_list_join, lim=obj_levels)
            objs = objs.drop_duplicates()
            objs = objs.loc[objs.ne('') & objs.notna()].to_list()

        if api_levels is None or api_levels <= 0:
            apis = dmdf.get("API").drop_duplicates()
            apis = apis.str.split('/', n=1, expand=True)[1]              # Drop the host name from the API
            apis = apis.drop_duplicates()
            apis = apis.loc[apis.ne('') & apis.notna()].to_list()
        else:
            apis = dmdf.get("API").drop_duplicates()
            apis = apis.str.split('/', n=1, expand=True)[1]              # Drop the host name from the API
            apis = apis.str.split('/', n=api_levels - 1)                 # Limit levels in the API name
            apis = apis.transform(col_list_join, lim=api_levels)
            apis = apis.drop_duplicates()
            apis = apis.loc[apis.ne('') & apis.notna()].to_list()

        # Using list(svcs) to effectively copy svcs instead of referencing it since it gets modified afterwards
        s_start = 0
        s_end = len(svcs)
        o_end = s_end + len(objs)
        a_end = o_end + len(apis)
        b_end = a_end + len(bkts)
        h_end = b_end + len(hsts)
        nodes = {"Service": {"names": list(svcs), "nums": [n for n in range(0, s_end)], "color": "blue"},
                 "Object": {"names": objs, "nums": [n for n in range(s_end, o_end)], "color": "green"},
                 "API": {"names": apis, "nums": [n for n in range(o_end, a_end)], "color": "black"},
                 "Bucket": {"names": bkts, "nums": [n for n in range(a_end, b_end)], "color": "purple"},
                 "Host": {"names": hsts, "nums": [n for n in range(b_end, h_end)], "color": "orange"}}

        # Now we need to deal with the flows between the nodes. First job is to rename all the nodes based on the
        # hierarchy collapsing work we just did.

        foo = dmdf.copy()
        node_labels = []
        node_color = []
        node_type = []

        for t in ["Service", "Object", "API", "Bucket", "Host"]:
            item = nodes.get(t, {})
            for n in range(len(item.get("names", []))):
                new = item.get("names", token)[n]
                num = item.get("nums", token)[n]
                col = item.get("color", token)
                from_type = dmdf.loc[:, "from_type"].eq(t)
                to_type = dmdf.loc[:, "to_type"].eq(t)
                from_name = dmdf.loc[:, "from_name"].str.count(new).gt(0)
                to_name = dmdf.loc[:, "to_name"].str.count(new).gt(0)
                loco = dmdf.loc[:, t].str.count(new).gt(0)
                dmdf.loc[from_type & from_name, "from_name_new"] = new if t != 'Host' else new.upper()
                dmdf.loc[from_type & from_name, "from_num"] = num
                dmdf.loc[to_type & to_name, "to_name_new"] = new if t != 'Host' else new.upper()
                dmdf.loc[to_type & to_name, "to_num"] = num
                dmdf.loc[loco, f"{t}.num"] = num
                node_labels.append(new if t != 'Host' else new.upper())
                node_color.append(col)
                node_type.append(t if t != 'Service' else 'Entity')

        # We want to aggregate the flows based on some criteria such as a Tag placed on a data flow, or data flows
        # from a certain object or bucket.  We do this by using Pandas groupby:

        if filter_val and filter_var:
            dmdf["selector"] = dmdf.loc[:, filter_var].str.count(filter_val)
        else:
            dmdf["selector"] = dmdf.loc[:, "Object"].str.count('kldsflksdjlkfjsdl;ajf;lasdjkf;lk')

        gets_color = f"rgba(150, 42, 87, {opacity})"  # Dark magenta or reddish purple
        puts_color = f"rgba(72, 42, 150, {opacity})"  # Dark bluish purple
        gets_meta_color = f"rgba(150, 42, 87, {opacity/2})"  # Dark magenta or reddish purple
        puts_meta_color = f"rgba(72, 42, 150, {opacity/2})"
        select_get_color = f"rgba(5, 168, 21, {.9})"  # Orange
        select_put_color = f"rgba(5, 168, 152, {.9})"  # Blue
        select_get_meta_color = f"rgba(5, 168, 21, {.6})"  # Orange
        select_put_meta_color = f"rgba(5, 168, 152, {.6})"  # Blue

        func_b = {'bytes': sum, 'R.TIME': max}
        dmdf = dmdf.groupby(['e.op', 'from_num', 'to_num', 'selector'], as_index=False).agg(func_b)

        sel = dmdf['selector'].eq(1)
        put = dmdf['e.op'].eq('PUT')
        get = dmdf['e.op'].eq('GET')
        putm = dmdf['e.op'].eq('PUT_META')
        getm = dmdf['e.op'].eq('GET_META')

        dmdf.loc[sel & put, 'color'] = select_put_color
        dmdf.loc[sel & putm, 'color'] = select_put_meta_color
        dmdf.loc[sel & get, 'color'] = select_get_color
        dmdf.loc[sel & getm, 'color'] = select_get_meta_color
        dmdf.loc[~sel & put, 'color'] = puts_color
        dmdf.loc[~sel & putm, 'color'] = puts_meta_color
        dmdf.loc[~sel & get, 'color'] = gets_color
        dmdf.loc[~sel & getm, 'color'] = gets_meta_color

        values = []
        xx = 8
        dmdf['bytes'] = dmdf.loc[:, 'bytes'].astype(int)
        foo, bins = pd.qcut(dmdf['bytes'], q=xx, retbins=True, duplicates='drop')

        for n in range(1, len(bins)):
            dmdf.loc[dmdf['bytes'].gt(bins[n-1]) & dmdf['bytes'].le(bins[n]), 'val'] = xx**2.5
            xx -= 1

        # bytes_skew = dmdf.loc[:, 'bytes'].skew()
        # bytes_krts = dmdf.loc[:, 'bytes'].kurtosis()
        # bytes_stdv = dmdf.loc[:, 'bytes'].std()
        # bytes_mean = dmdf.loc[:, 'bytes'].mean()
        # if bytes_skew > 4:
        #     values = (dmdf.loc[:, 'bytes'].transform(np.log10) ** bytes_skew).to_list()
        # elif bytes_skew > 2:
        #     values = (dmdf.loc[:, 'bytes'].transform(np.log) ** bytes_skew).to_list()
        # else:
        #     values = (dmdf.loc[:, 'bytes'].transform(np.log)).to_list()

        # Now we have all the data for the Sankey diagram!  Fill in the values and return them.
        out = default_sankey.copy()
        out.get("data", {})[0].get("node", {}).update({"customdata": node_type})
        out.get("data", {})[0].get("node", {}).update({"label": node_labels})
        out.get("data", {})[0].get("node", {}).update({"color": node_color})
        out.get("data", {})[0].get("link", {}).update({"source": dmdf.loc[:, 'from_num'].astype(int).to_list()})
        out.get("data", {})[0].get("link", {}).update({"target": dmdf.loc[:, 'to_num'].astype(int).to_list()})
        out.get("data", {})[0].get("link", {}).update({"customdata": dmdf.loc[:, 'bytes'].astype(int).to_list()})
        out.get("data", {})[0].get("link", {}).update({"value": dmdf.loc[:, 'val'].to_list()})
        out.get("data", {})[0].get("link", {}).update({"color": dmdf.loc[:, 'color'].to_list()})
        out.get("data", {})[0].get("link", {}).update({"label": dmdf.loc[:, 'selector'].astype(str).to_list()})
        return out

    else:
        # If we can't make a Sankey diagram out of the data, return a default diagram
        out = json.dumps(default_sankey)
        out = out.replace("%A%", f"rgba(245, 195, 66, {opacity})")
        out = out.replace("%B%", f"rgba(130, 130, 255, {opacity})")
        return json.loads(out)


def get_tmax_tmin():
    return NOW.isoformat(), BOT.isoformat()

    # table = CFG.G["dbTables"].get('dataFlowTable', 'data-flows')
    # query = {"size": 0, "aggs": {"tmax": {"max": {"field": "R.TIME"}}, "tmin": {"min": {"field": "R.TIME"}}}}
    # ret = esp.esp_search(table, query)
    # ret = ret.get('aggregations', {})
    # if ret:
    #     tmax = ret['tmax'].get('value_as_string', BOT.isoformat())
    #     tmin = ret['tmin'].get('value_as_string', NOW.isoformat())
    #     return tmax, tmin
    # else:
    #     return 'Z', 'Z'
