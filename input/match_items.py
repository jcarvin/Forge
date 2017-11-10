from emailer import send_email_file_loaded
from matching import compare_dfs
import pandas as pd
from pandas import ExcelWriter
import geopandas as gp
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import json
from fuzzywuzzy import fuzz
from math import pi,sqrt,sin,cos,atan2,isnan
import time
pd.options.mode.chained_assignment = None  # default='warn'
global_con_string = 'postgres://postgres:temp@192.168.168.174:5432/postgres'


def upc10(upc):
    if upc:
        if len(upc) == 14:
            return upc[3:13]
        if len(upc) == 13:
            return upc[2:12]
        if len(upc) == 12:
            return upc[1:11]
        if len(upc) == 11:
            return upc[1:11]
        else:
            return upc
    else:
        return "0"

global_engine = create_engine(global_con_string, poolclass=NullPool)
global_conn = global_engine.connect()
try:
    matched_xrefs = pd.read_sql_query('SELECT * FROM global.import_item where master_item_id is not null', global_conn)
    xrefs = pd.read_sql_query('SELECT * FROM global.import_item where master_item_id is null', global_conn)
except:
    print('no')
global_conn.close()
global_engine.dispose()


xrefs['id'] = xrefs['import_item_id']
xrefs['upc10'] = xrefs['reported_upc'].apply(lambda z: upc10(z))
matched_xrefs['id'] = matched_xrefs['master_item_id']
matched_xrefs['upc10'] = matched_xrefs['reported_upc'].apply(lambda z: upc10(z))

df_1_cols_to_parse = {
}


df_2_cols_to_parse = {
}

df_1_cols = {
    "att1": {
        "label": "manufacturer",
        "weight": 1
    },
    "att2": {
        "label": "sku",
        "weight": 1
    },
    "att3": {
        "label": "description",
        "weight": 1
    },
    "att4": {
        "label": "measure",
        "weight": 1
    },
    "att5": {
        "label": "pack",
        "weight": 1
    },
    "att6": {
        "label": "reported_upc",
        "weight": 1
    },
    "att7": {
        "label": "warehouse_id",
        "weight": 1
    },
    "att8": {
        "label": "item_category",
        "weight": 1
    },
    "att9": {
        "label": "item_sub_category",
        "weight": 1
    },
    "att10": {
        "label": "item_fine_line_category",
        "weight": 1
    }
}

df_2_cols = {
    "att1": {
        "label": "manufacturer",
        "weight": 1
    },
    "att2": {
        "label": "sku",
        "weight": 1
    },
    "att3": {
        "label": "description",
        "weight": 1
    },
    "att4": {
        "label": "measure",
        "weight": 1
    },
    "att5": {
        "label": "pack",
        "weight": 1
    },
    "att6": {
        "label": "reported_upc",
        "weight": 1
    },
    "att7": {
        "label": "warehouse_id",
        "weight": 1
    },
    "att8": {
        "label": "item_category",
        "weight": 1
    },
    "att9": {
        "label": "item_sub_category",
        "weight": 1
    },
    "att10": {
        "label": "item_fine_line_category",
        "weight": 1
    }
}


numzips = None
filename = '/home/scientist/host/output/10.05.17_' + str(numzips) + '_zips.csv'
maxfilename = '/home/scientist/host/output/max_10.05.17_' + str(numzips) + '_zips.csv'
try:
    x = compare_dfs(
        matched_xrefs,
        df_1_cols,
        df_1_cols_to_parse,
        "upc10",
        xrefs,
        df_2_cols,
        df_2_cols_to_parse,
        "upc10",
        None,
        None,
        20
    )

    x['df_list'].to_csv(filename)
    x['max_df_list'].to_csv(maxfilename)
    send_email_file_loaded(filename, x['message'])
except Exception as e:
    if hasattr(e, 'message'):
        text = e.message
    else:
        text = e
    send_email_file_loaded('Error', str(text))
