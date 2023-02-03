# ##################################################################################################
#  Copyright (c) 2022.    Caber Systems, Inc.                                                      #
#  All rights reserved.                                                                            #
#                                                                                                  #
#  CABER SYSTEMS CONFIDENTIAL SOURCE CODE                                                          #
#  No license is granted to use, copy, or share this software outside of Caber Systems, Inc.       #
#                                                                                                  #
#  Filename:  pandas_tools.py                                                                      #
#  Authors:  Rob Quiros <rob@caber.com>  rlq                                                       #
# ##################################################################################################

import base64
import json
from json import JSONDecodeError
from datetime import datetime, timezone

from collections.abc import Mapping, Iterable

import pandas as pd
import numpy as np
# from functools import lru_cache
# from urllib.parse import quote_plus, unquote_plus

# from ..Common.init import CFG, beginning_of_time, end_of_time

anyone = ['any', 'anyone', 'everyone', 'anybody', 'everybody', 'all', 'allow',
          'allow_all', 'allow_any', 'allow-all', 'allow-any']
no_one = ['none', 'no-one', 'no_one', 'nobody', 'deny', 'deny-all', 'deny_all']
noauth = ['unknown', '<unknown>', 'none', '<none>', '']


def _check_na(x):
    y = pd.notna(x)
    if isinstance(y, np.ndarray):
        return y.any()
    else:
        return y


def _remove_empty_items(d, **kwargs):
    if isinstance(d, Mapping):
        upd = {k: v for k, v in d.items() if _check_na(v)}
    elif isinstance(d, Iterable):
        upd = {k: v for k, v in d if _check_na(v)}
    else:
        upd = {}
    if kwargs:
        upd.update({k: v for k, v in kwargs.items() if pd.notna(v)})
    return upd


class NoNanDict(dict):
    """
    Subclass of dictionary that doesn't allow any insertion of items where
    bool(value) is false.  This includes empty strings, empty dicts, None, NaN,
    etc.  This is to solve the problem where OpenSearch barfs on dict values that
    contain NaNs.
    """
    def __init__(self, mapping=None, /, **kwargs):
        mapping = _remove_empty_items(mapping, **kwargs)
        super().__init__(mapping)

    def __setitem__(self, key, value):
        if value:
            super().__setitem__(key, value)

    def update(self, d, **kwargs):
        upd = _remove_empty_items(d, **kwargs)
        if upd:
            super().update(upd)


def fill_na_with_empty_list(indf, cols):
    mna = indf[cols[0]].isna()
    ml = indf.loc[mna].shape[0]
    match = {c: [[]] * ml for c in cols}
    tmp = pd.DataFrame(match, columns=cols, index=indf.loc[mna].index)
    indf.loc[mna, cols] = tmp
    return indf


def fill_na_with_empty_dict(indf, cols):
    mna = indf[cols[0]].isna()
    ml = indf.loc[mna].shape[0]
    match = {c: [{}] * ml for c in cols}
    tmp = pd.DataFrame(match, columns=cols, index=indf.loc[mna].index)
    indf.loc[mna, cols] = tmp
    return indf


def update_with_any(indf, cols=None, any_value=None, **kwargs):
    """
    A more generalized version of fill_na_with_empty_dict.  Uses update instead of fillna
    to allow updating values across a DataFrame with the value of arg 'any'.  Any can
    be an empty dict, an empty list, or anything else.

    Becasue pd.DataFrame.update kwarg 'overwrite' defaults to false, update_with_any(indf, ['A'], {})
    is equivalent to fill_na_with_empty_dict(indf, ['A'])

    Returns updated indf

    :param indf:
    :param cols:
    :param any_value:
    :param kwargs:
    :return:
    """
    overwrite = kwargs.pop("overwrite", False)
    ukwargs = {k: kwargs.pop(k) for k in kwargs.keys() if k in ["filter_func", "errors"]}
    if not cols:
        cols = indf.columns
    elif isinstance(cols, str):
        cols = [cols]

    # cols = indf.columns.intersection(cols)
    # if indf.empty or cols.empty:
    #     return indf

    mi, _ = indf.shape
    match = {c: [any_value] * mi for c in cols}
    tmp = pd.DataFrame(match, columns=cols, index=indf.index)
    indf.update(tmp, overwrite=overwrite, **ukwargs)
    return indf


def fill_na_with_dict(indf, dct):
    match = {c: [dct] * indf.shape[0] for c in indf.columns}
    tmp = pd.DataFrame(match, index=indf.index)
    return indf.fillna(tmp)


def add_empty_list_cols(indf, cols):
    ml = indf.shape[0]
    match = {c: [[]] * ml for c in cols}
    tmp = pd.DataFrame(match, columns=cols, index=indf.index)
    indf = pd.concat([indf, tmp], axis='columns')
    return indf


def set_x(z, list_of_one=True, empty_list=True):
    """
    Returns the non-nan/empty values in a pd.Series as a set
    :param z:
    :param list_of_one:
    :param empty_list:
    :return:
    """
    u = set(z.loc[z.notna()].explode().to_list())
    u.discard('')
    u.discard(np.nan)
    u.discard(None)
    if len(u) == 0:
        if empty_list:
            return list(u)
        else:
            return ''
    elif len(u) == 1:
        if list_of_one:
            return list(u)
        else:
            return u.pop()
    else:
        return list(u)


# def uid_x(z):
#     """
#     Returns the UID for elements in a series
#     :param z:
#     :return:
#     """
#     u = set(z.loc[z.notna()].explode().to_list())
#     u.discard('')
#     u.discard(np.nan)
#     u.discard(None)
#     if len(u) > 1:
#         u.discard(CFG.G.get("authZoptions", {}).get("defaultSvcUID", ""))
#         u.discard(CFG.G.get("authZoptions", {}).get("defaultDatUID", ""))
#     if len(u) > 1:
#         print(f"[DEBUG] uid_x more than one uid left in set {u}")
#         return u.pop()
#     if len(u) == 1:
#         return u.pop()
#     return ''


def bld_x(z):
    """
    Returns -1 or the largest number if -1 is not present
    :param z: input series
    :return:
    """
    u = set(z.explode().to_list())
    u.discard('')
    u.discard(np.nan)
    u.discard(None)
    if min(u) == -1:
        return -1
    else:
        return max(u)


def first_x(z):
    """
    Returns the first non-nan value in a pd.Series
    :param z: input series
    :return:
    """
    z = z.dropna().drop(labels=[''], errors='ignore')
    if z.empty:
        return ''
    return z.iloc[0]


def top_x(z):
    """
    Returns the most often occurring non-nan value in a pd.Series
    :param z: input series
    :return:
    """
    z = z.drop(labels=[''], errors='ignore').value_counts()
    if z.empty:
        return ''
    return z.index[0]


def json_x(z):
    """
    Returns the json.dumps of the most often occurring dict value in a pd.Series
    :param z: input series
    :return:
    """
    z = z.drop(labels=[''], errors='ignore').transform(json.dumps).value_counts()
    if z.empty:
        return ''
    return z.index[0]


def top_l(z, tp=str):
    """
    Returns the most often occurring value of type 'tp' in a list
    :param z: input list
    :param tp:
    :return: list
    """
    u = [(z.count(s), s) for s in set(z) if isinstance(s, tp)]
    u.sort(reverse=True, key=lambda x: x[0])
    return u[0][1]


# utkn = CFG.token
# qtkn = quote_plus(CFG.token)


# @lru_cache(10000)
# def bksq_x(x, k=''):
#     """
#     Takes a CORIs, name or SHA256s and converts each item to a dict
#     of Bucket, Key, remote object CORI, and quoted CORI
#     :param k: if set to B, K, S, or Q then return only the single value not the dict
#     :param x: string
#     :return: dict or value
#     """
#
#     try:
#         from ..Toolbox.filenames import FileNames
#     except ImportError:
#         raise ImportError("Fix to pass FileNames?")
#     else:
#         if utkn in x:
#             ret = FileNames(x, names_only=True).bksq
#         elif qtkn in x:  # and utkn not in x
#             ret = FileNames(unquote_plus(x), names_only=True).bksq
#         else:
#             ret = FileNames(utkn + x, names_only=True).bksq
#
#         if k in ['B', 'K', 'S', 'Q']:
#             return ret[k]
#         else:
#             return ret


def timesort(item, column='R.TIME', old_to_new=True):
    x = item[column]
    return (datetime.now(timezone.utc) - x).seconds


def clean_allow_list(x):
    y = set(x).difference(set(anyone))
    z = set(x).intersection(set(anyone))
    if y:
        return list(y)
    elif z:
        return ['all']
    else:
        return []


def clean_deny_list(x):
    y = set(x).difference(set(no_one))
    z = set(x).intersection(set(no_one))
    if y:
        return list(y)
    else:
        return []


def pop_nans(list_of_dicts):
    for dct in list_of_dicts:
        pops = []
        for k in dct.keys():
            try:
                # np.nan != np.nan and pd.NaT != pd.NaT are always true
                # pd.NA != pd.NA throws 'TypeError: boolean value of NA is ambiguous'
                if dct[k] is None or dct[k] != dct[k]:
                    pops.append(k)
            except TypeError:
                pops.append(k)
        for p in pops:
            dct.pop(p)
    return list_of_dicts


def id_to_sha256(hashin):
    if hashin is None:
        return None
    elif hashin == '':
        return None
    else:
        return base64.b85decode(hashin.encode('utf-8')).hex()


def sha256_to_id(hashin):
    if hashin is None:
        return None
    elif hashin == '':
        return None
    else:
        return base64.b85encode(bytes.fromhex(hashin)).decode('utf-8')

#
# def lookup_user(apikey):
#     for rec in CFG.D['accounts']:
#         if rec['APIkey'] == apikey:
#             return {'name': rec['name'], 'groups': rec['groups']}
#     return {'name': 'unknown', 'groups': ['']}


def update_then_concat(to_df, from_df, **kwargs):
    """
    Avoids the problem of creating 'ColName_x', 'ColName_y' when concatenating two DataFrames
    that both have a column named 'ColName'.  First does a pd.update on the common columns, then
    does a concat of the remaining columns.

    :param to_df: The dataframe to be modified and returned.
    :param from_df: The input dataframe used to modify to_df.

    :keyword delete_from: True if the from_df should be deleted when done

    Keyword params passed to DataFrame.update()
    :keyword overwrite: As DataFrame.update() except default is False
    :keyword filter_func: Same as DataFrame.update()
    :keyword errors: Same as DataFrame.update()

    Keyword params passed to DataFrame.concat()
    :keyword axis: Defaults to 1
    :keyword join:
    :keyword ignore_index:
    :keyword keys:
    :keyword levels:
    :keyword names:
    :keyword verify_integrity:
    :keyword sort:
    :keyword copy:

    :return: Modified pandas DataFrame 'to_df'
    """

    axis = kwargs.pop("axis", 1)
    overwrite = kwargs.pop("overwrite", False)
    delete_from = kwargs.pop("delete_from", False)
    skip_dindex_empty = kwargs.pop("skip_dindex_empty", True)

    ukwargs = {k: kwargs.pop(k) for k in kwargs.keys() if k in ["filter_func", "errors"]}

    xc = to_df.columns.intersection(from_df.columns)
    to_df.update(from_df[xc].copy(), overwrite=overwrite, **ukwargs)

    from_df = from_df[from_df.columns.difference(xc)]
    to_df = pd.concat([to_df, from_df.copy()], axis=axis, **kwargs)

    if skip_dindex_empty and 'D.Index' in to_df.columns:
        to_df = to_df.loc[to_df.get('D.Index').ne('')]

    if delete_from:
        del from_df
    return to_df


def update_then_merge(to_df, from_df, **kwargs):
    """
    Avoids the problem of creating 'ColName_x', 'ColName_y' when merging two DataFrames
    that both have a column named 'ColName'.  First does a pd.update on the common columns, then
    does a concat of the remaining columns.

    :param to_df: The dataframe to be modified and returned.
    :param from_df: The input dataframe used to modify to_df.

    :keyword delete_from: True if the from_df should be deleted when done

    Keyword params passed to DataFrame.update()
    :keyword overwrite: As DataFrame.update() except default is False
    :keyword filter_func: Same as DataFrame.update()
    :keyword errors: Same as DataFrame.update()

    All other keyword params passed to DataFrame.merge()

    :return: Modified pandas DataFrame 'to_df'
    """
    overwrite = kwargs.pop("overwrite", False)
    delete_from = kwargs.pop("delete_from", False)

    ukwargs = {k: kwargs.pop(k) for k in kwargs.keys() if k in ["filter_func", "errors"]}

    # Don't update the merge key of to_df
    right_on_col = kwargs.get("right_on", None)
    xc = to_df.columns.intersection(from_df.columns.difference([right_on_col]))

    to_df.update(from_df[xc], overwrite=overwrite, **ukwargs)
    to_df = to_df.merge(from_df.drop(columns=xc).copy(), **kwargs)

    if delete_from:
        del from_df
    return to_df

#
# def series_to_bksq(ser: pd.Series, k=''):
#     su = ser.unique()
#     su = pd.Series(su, name='result', index=su)
#     if k:
#         return pd.merge(ser, su.transform(bksq_x, k=k), how='left', left_on=ser.name, right_index=True) \
#             .drop(columns=[ser.name]).rename(columns={'result': ser.name})
#     else:
#         sr = pd.DataFrame(su.apply(bksq_x).to_list(), index=su.index)
#         return pd.merge(ser, sr, how='left', left_on=ser.name, right_index=True).drop(columns=[ser.name])


def columns_from_dict_column(dict_df, column=None):
    # # OPTION 1: Apply pd.Series to each dict to convert it to separate columns.
    # setup = ""
    # statement = "jloc = dict_df.loc[:, col].apply(lambda x: isinstance(x, dict)); "\
    #             "expdf = dict_df.loc[jloc, col].apply(pd.Series, name=col)"
    # c = timeit.timeit(stmt=statement, setup=setup, number=100,
    #                   globals={'pd': pd, 'dict_df': dict_df, 'col': col})
    #
    # # OPTION 2: Use to_list then build DataFrame
    # #           the string accurately in this case.  d = 0.8118365840055048
    # setup = ""
    # statement = "jloc = dict_df.loc[:, col].apply(lambda x: isinstance(x, dict)); "\
    #             "expdf = pd.DataFrame(dict_df.loc[jloc, col].to_list(), index=dict_df.loc[jloc].index)"
    # d = timeit.timeit(stmt=statement, setup=setup, number=100,
    #                   globals={'pd': pd, 'dict_df': dict_df, 'col': col})
    # c = 0.8396367920213379
    # d = 0.41012450004927814     <------Option 2 is twice as fast

    col = column
    if isinstance(dict_df, pd.Series):
        col = dict_df.name
        dict_df = pd.DataFrame(dict_df, columns=[dict_df.name])

    if not col:
        col = dict_df.columns[0]

    jloc = dict_df[col].apply(lambda x: isinstance(x, dict))
    if not jloc.all():
        try:
            dict_df.loc[~jloc, col] = dict_df.loc[~jloc, col].apply(json.loads)
        except JSONDecodeError:
            pass
        else:
            jloc.loc[~jloc] = True

    if jloc.any():
        # unpack the dicts and look for matching column names from target_columns
        expdf = pd.DataFrame(dict_df.loc[jloc, col].to_list(), index=dict_df.loc[jloc].index)

        # Rename the columns in the new DataFrame with the column from the original data frame they came from
        if column is not None:
            xcols = {x: f"{column}.{x}" for x in expdf.columns.to_list()}
            expdf.rename(columns=xcols, inplace=True)

        return expdf

    else:
        return pd.DataFrame()


def columns_to_dict_series(dict_df, base='', replace_in_df=False):
    """
    Convert a number of dataframe columns to a single column with each item a dict of the original column items.

    :param dict_df: Input dataframe.
    :param base: If base is not set will take all columns in dict_df and output an unnamed series.  If base is set,
                 only columns in dict_df that start with base will be aggregated into a series named base.
    :param replace_in_df: If base is set, and replace_in_df is True, the output will be dict_df with all columns
                 starting with base removed and a new column named 'base' inserted.
    :return:
    """
    if base:
        cols = {c: c.split(base)[-1].strip('.') for c in dict_df.columns if c.startswith(base) and c != base}
        if not cols:
            return dict_df
        foo = dict_df[cols.keys()].rename(columns=cols)
        foo = foo.to_dict(orient='records', into=NoNanDict)
        bar = pd.Series(foo, index=dict_df.index, name=base, dtype='object')
        if replace_in_df:
            return pd.concat([dict_df.drop(columns=cols.keys()), bar], axis=1)
    else:
        foo = dict_df.to_dict(orient='records', into=NoNanDict)
        bar = pd.Series(foo, index=dict_df.index, name='out', dtype='object')

    return bar


def hashable_column_from_unhashable(indf, unhashable_column='', out_column='merge_key', set_index=False):
    """
    Since we can't merge on a column containing dicts, we have to transform the dicts to a something hashable
    if set_index is True then the index of the resulting DataFrame will be set to the hashable values.
    Otherwise, the hashable values will be returned in a separate column with the column name given by
    out_col (default is 'merge_key')
    """

    # # Testing which variation on getting a column of dicts to a hashable type is fastest.  A little surprising
    # # to find out OPTION 3 was ~3x faster than OPTION 1.  However, OPTION 3 cannot handle mixed columns (with
    # # some values being dictionaries, and others scalars.  That means Option 2 is the best approach.
    # #
    # #    a = 1.2020659589907154    b = 0.5948062079609372     c = 0.4423773339949548   d = 0.8118365840055048
    #
    # import timeit
    #
    # # OPTION 1: Use hash of frozendict  a = 1.1997133340337314
    # setup = "foo = merged.copy()"
    # statement = "bar = foo.loc[locd, locd.name].transform(frozendict.FrozenOrderedDict); " \
    #             "foo['merge_key'] = pd.util.hash_pandas_object(bar)"
    # a = timeit.timeit(stmt=statement, setup=setup, number=100,
    #                   globals={'merged': merged, 'pd': pd, 'frozendict': frozendict, 'locd': locd})
    #
    # # OPTION 2: Individually transform each item to json string.  b = 0.6948062079609372
    # setup = "foo = merged.copy()"
    # statement = "foo['merge_key'] = foo.loc[locd, locd.name].transform(json.dumps); "
    # b = timeit.timeit(stmt=statement, setup=setup, number=100,
    #                   globals={'merged': merged, 'pd': pd, 'frozendict': frozendict, 'locd': locd, 'json': json})
    #
    # # OPTION 3: Convert entire series to one json string, split it, then recreate dataframe c = 0.4423773339949548
    # setup = "foo = merged.copy()"
    # statement = "aa = foo.loc[locd, locd.name].to_json(orient='values').strip('[]').replace('},{', '}###{').split('###'); foo['merge_key'] = pd.DataFrame(aa, index=foo.index, columns=[locd.name])"
    # c = timeit.timeit(stmt=statement, setup=setup, number=100,
    #                   globals={'merged': merged, 'pd': pd, 'frozendict': frozendict, 'locd': locd, 'json': json})
    #
    # # OPTION 4: Same as Option 3 but with checking to exclude any non-dict or non-list items since we can't split
    # #           the string accurately in this case.  d = 0.8118365840055048
    # setup = "foo = merged.copy()"
    # statement = "locd = foo.loc[:, 'c.Identity'].apply(lambda x: isinstance(x, (list, dict))); " \
    #             "aa = foo.loc[locd, 'c.Identity'].to_json(orient='values').strip('[]').replace('},{', '}###{').split('###'); " \
    #             "foo['merge_key'] = pd.DataFrame(aa, index=foo.loc[locd].index, columns=['c.Identity'])"
    # d = timeit.timeit(stmt=statement, setup=setup, number=100,
    #                   globals={'merged': merged, 'pd': pd})

    if isinstance(indf, pd.Series):
        unhashable_column = indf.name
        indf = pd.DataFrame(indf, columns=[unhashable_column])

    unhashable_column = indf.columns[0] if not unhashable_column else unhashable_column

    if not set_index:
        indf[out_column] = indf[unhashable_column].transform(json.dumps)
        return indf
    else:
        return indf.set_index(indf[unhashable_column].transform(json.dumps))


def nans_in_dict_columns(indf):
    dict_cols = indf.apply(lambda C: [isinstance(c, dict) for c in C], axis=0).any()
    test = indf.loc[:, dict_cols].apply(lambda D: any(1 for k, v in D.items() if pd.isna(v)))
    return test.any()
