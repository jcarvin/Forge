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

# TODO if multiple names exist at one address, weight the name higher

# Utility Functions-----------------------------------------------------------------------------------------------------


def standardize_zpcd(zpcd):
    zpcd = str(zpcd)
    if len(zpcd) <= 4:
        zpcd = str(zpcd)[:4].rjust(5, '0')
    elif len(zpcd) <= 8 and len(zpcd) > 5:
        zpcd = str(zpcd)[:4].rjust(5, '0')
    else:
        zpcd = str(zpcd)[:5]
    return zpcd[:5]


def prep_addresses_for_parser(df, cols):

    df[cols['zip']] = df[cols['zip']].apply(lambda z: str(z)[:5])
    json_to_parse = []
    for idx, value in df.iterrows():
        json_to_parse.append({
            "address": str(value[cols['street']] + ', ' + str(value[cols['city']]) + ' ' +
                           str(value[cols['state']]) + ' ' + str(value[cols['zip']])),
            'id': str(value[cols['id']])
        })
    return parse_address_list(json_to_parse, True)


def parse_address_list(jsons, pgac=False):

    global_engine = create_engine(global_con_string, poolclass=NullPool)
    global_conn = global_engine.connect()

    sqltxt = ''''''

    if pgac:
        sqltxt = '''
        SET max_parallel_workers_per_gather = 24;
        SET work_mem = '8GB';
        SELECT id
         ,name
         ,origin
         ,(x.ga).address
         ,(x.ga).predirabbrev
         ,(x.ga).streetname
         ,(x.ga).streettypeabbrev
         ,(x.ga).postdirabbrev
         ,(x.ga).internal
         ,(x.ga).location
         ,(x.ga).stateabbrev
         ,(x.ga).zip as parsed_zip
         ,(x.ga).parsed
        FROM (
         SELECT pagc_normalize_address("address") AS ga, address AS origin, name, id, lat, lon
         FROM json_to_recordset(:addresses) AS x("address" TEXT, "lat" NUMERIC, "lon" NUMERIC, "name" TEXT, "id" TEXT)
         ) x
        '''
    else:
        sqltxt = '''
        SET max_parallel_workers_per_gather = 24;
        SET work_mem = '8GB';
        SELECT id
         ,name
         ,origin
         ,(x.ga).address
         ,(x.ga).predirabbrev
         ,(x.ga).streetname
         ,(x.ga).streettypeabbrev
         ,(x.ga).postdirabbrev
         ,(x.ga).internal
         ,(x.ga).location
         ,(x.ga).stateabbrev
         ,(x.ga).zip as parsed_zip
         ,(x.ga).parsed
        FROM (
         SELECT normalize_address("address") AS ga, address AS origin, name, id, lat, lon
         FROM json_to_recordset(:addresses) AS x("address" TEXT, "lat" NUMERIC, "lon" NUMERIC, "name" TEXT, "id" TEXT)
         ) x
        '''

    add_json = []
    for obj in jsons:
        add_json.append({"address": str(obj["address"]), "id": str(obj['id'])})

    x = pd.read_sql_query(
            sa.text(sqltxt),
            con=global_conn,
            params={'addresses': json.dumps(add_json)}
        )

    x['id'] = x['id'].astype('int')

    global_conn.close()
    global_engine.dispose()
    return x


global_engine = create_engine(global_con_string, poolclass=NullPool)
global_conn = global_engine.connect()
try:
    matched_xrefs = pd.read_sql_query('SELECT * FROM global.master_customer', global_conn)
    matched_xrefs['standardized_zip'] = matched_xrefs['zip'].apply(lambda z: standardize_zpcd(z))
    matched_xrefs['output_id'] = matched_xrefs['master_customer_id']
except:
    print('no')
global_conn.close()
global_engine.dispose()

global_engine = create_engine(global_con_string, poolclass=NullPool)
global_conn = global_engine.connect()
try:
    xrefs = pd.read_sql_query('SELECT * FROM global.import_customer where master_customer_id is null', global_conn)
    xrefs['standardized_zip'] = xrefs['zip'].apply(lambda z: standardize_zpcd(z))
except:
    print('no')
global_conn.close()
global_engine.dispose()

df_1_cols_to_parse = {
    "id": "master_customer_id",
    "zip": "zip",
    "street": "address_1",
    "city": "city",
    "state": "state",
    "right_on": 'id'
}


df_2_cols_to_parse = {
    "id": "import_customer_id",
    "zip": "zip",
    "street": "address_1",
    "city": "city",
    "state": "state",
    "right_on": 'id'
}

df_1_cols = {
    "att1": {
        "label": "customer_name",
        "weight": 5
    },
    "att2": {
        "label": "origin",
        "weight": 1
    },
    "att3": {
        "label": "streetname",
        "weight": 5
    },
    "att4": {
        "label": "address", # street num
        "weight": 4
    },
    "att5": {
        "label": "parsed_zip",
        "weight": 0
    },
    "att6": {
        "label": "predirabbrev", # pre direction abbreviation 456 N HWY
        "weight": 2
    },
    "att7": {
        "label": "streettypeabbrev", #highway HWY Street Ste
        "weight": 1
    },
    "att8": {
        "label": "postdirabbrev",
        "weight": 2
    },
    "att9": {
        "label": "internal", #suite
        "weight": 2
    },
    "att10": {
        "label": "location",
        "weight": 3
    },
    "att11": {
        "label": "stateabbrev",
        "weight": 5
    }
}

df_2_cols = {
    "att1": {
        "label": "customer_name",
        "weight": 5
    },
    "att2": {
        "label": "origin",
        "weight": 1
    },
    "att3": {
        "label": "streetname",
        "weight": 5
    },
    "att4": {
        "label": "address", # street num
        "weight": 4
    },
    "att5": {
        "label": "parsed_zip",
        "weight": 0
    },
    "att6": {
        "label": "predirabbrev", # pre direction abbreviation 456 N HWY
        "weight": 2
    },
    "att7": {
        "label": "streettypeabbrev", #highway HWY Street Ste
        "weight": 1
    },
    "att8": {
        "label": "postdirabbrev",
        "weight": 2
    },
    "att9": {
        "label": "internal", #suite
        "weight": 2
    },
    "att10": {
        "label": "location",
        "weight": 3
    },
    "att11": {
        "label": "stateabbrev",
        "weight": 5
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
        "standardized_zip",
        xrefs,
        df_2_cols,
        df_2_cols_to_parse,
        "standardized_zip",
        prep_addresses_for_parser,
        "att1",
        numzips
    )

    x['df_list'].to_csv(filename)
    x['max_df_list'].to_csv(maxfilename)
    send_email_file_loaded(filename, x['message'])
except Exception as e:
    if hasattr(e, 'message'):
        text = e.message
    else:
        text = e
    # send_email_file_loaded('Error', str(text))


