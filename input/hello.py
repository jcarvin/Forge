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

# Utility Functions-----------------------------------------------------------------------------------------------------

def standardize_zpcd(zpcd):
    zpcd = str(zpcd)
    if len(zpcd) <= 4:
        zpcd = str(zpcd)[:4].rjust(5, '0')
    elif len(zpcd) > 5:
        zpcd = str(zpcd)[:-4].rjust(5, '0')
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


def haversine(pos1, pos2):
    lat1 = float(pos1['lat'])
    long1 = float(pos1['long'])
    lat2 = float(pos2['lat'])
    long2 = float(pos2['long'])

    degree_to_rad = float(pi / 180.0)

    d_lat = (lat2 - lat1) * degree_to_rad
    d_long = (long2 - long1) * degree_to_rad

    a = pow(sin(d_lat / 2), 2) + cos(lat1 * degree_to_rad) * cos(lat2 * degree_to_rad) * pow(sin(d_long / 2), 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    km = 6367 * c
    mi = 3956 * c

    return {"km":km, "miles":mi}


def similar(a, b):
    """
    :param a: A string to be compared to 'b'
    :param b: A string to be compared to 'a'
    :return: The ratio in pct of how similar string 'a' is to string 'b'
    """
    return fuzz.token_set_ratio(str(a), str(b))


def rank_it(score):
    if score >= 1050:
        return 1
    elif score >= 1000:
        return 2
    elif score >= 950:
        return 3
    elif score >= 900:
        return 4
    else:
        return 5


# Real shit ------------------------------------------------------------------------------------------------------------
def compare_dfs(
    df_1,
    df_1_cols,
    df_1_cols_to_parse,
    df_1_batch_col,   # partition column
    df_2,
    df_2_cols,
    df_2_cols_to_parse,
    df_2_batch_col,
    parser=prep_addresses_for_parser,
    primary_attribute=None,
    num_of_batches=None
):

    """
    :param df_1: The data set that you are comparing the unmatched data to. The 'constant'. As a pd.DataFrame

    :param df_1_cols: Object containing the attribute columns to be compared that DON'T need parsed (in the same index
        positioning as the columns you are comparing them to in df_2_cols) always starting with the unique id followed
        by the output id. Be sure to include cols that will be added by your parser :
            {
                "id": "import_customer_id",
                "output_id": "master_customer_id",
                "attr_1": "customer_name",
                "attr_2": "phone"
                "attr_3": "column name",
                ect...
            }

    :param df_1_cols_to_parse: Object containing the attribute columns that need to be sent to your parser before being
        compared. Same format as df_1_cols minus the output_id:
            {
                "id": "import_customer_id",
                "p_attr_1": "address_1",
                "p_attr_2": "city",
                "p_attr_3": "state",
                "p_attr_4": "zip",
            }

    :param df_1_batch_col: The name of the column that will determine how to batch the data.: "zip"
        Note: this will be compared directly with df_2_batch_col. ONLY RECORDS THAT MATCH ON THIS COLUMN EXACTLY WILL
              BE COMPARED.

    :param df_2: The unmatched data. As a pd.DataFrame

    :param df_2_cols: Same as df_1_cols minus output_id.

    :param df_2_cols_to_parse: See df_1_cols_to_parse.

    :param df_2_batch_col: See df_1_batch_col.

    :param parser: This is a function that will take (df_1_single_batch, df_1_cols_to_parse) and
        (df_2_single_batch, df_2_cols_to_parse) and return a dataframe with parsed and normalized data.

    :param primary_attribute: Defaults to 'None'. If defined, The program won't evaluate two records who's primary
        attributes aren't at least a little bit similar in this

    :param num_of_batches: Defaults to 'None'. accepts an integer. Should only be used for testing purposes.

    :returns: Joined dataframe
    """

    # Time the whole thing for comparisons later
    start_time = time.time()

    # Allow for errors and handle them
    # try:
    # Keep a count of all the different matches for comparison later
    counts = {
        'rank_1_match_count': 0,
        'rank_2_match_count': 0,
        'rank_3_match_count': 0,
        'rank_4_match_count': 0,
        'rank_5_match_count': 0,
        'rank_6_match_count': 0,
        'matchless_count': 0,
        'zip_with_no_msr': 0
    }

    # This will be the list of dataframes that will be put together and returned in the end
    df_list = []
    max_df_list = []

    # Create a list of Zip codes from the given xrefs so that you don't iterate over any unnecessary zips
#     ziptest = df_2[df_2_cols['zip']].apply(lambda z: int(str(z)[:5])).tolist()    # Only the first 5 digits are needed
    batch = df_2[df_2_batch_col].tolist()

    # numofzips can be set when the function is called. This will just be
    # an intiger representing the first x amt of zips to be looked at.

    if type(num_of_batches) == int or num_of_batches is None:
        batch = list(set(batch))[:num_of_batches]
    elif type(num_of_batches) == list:
        batch = num_of_batches
    # batch = [1005]

    # Iterate over each zip. The idx is only used for the messages
    for idx, item in enumerate(batch):

        # Create the message to be shown at the beginning of each item
        msg = (str(item) + ' ' + str(idx + 1) + '/' + str(len(batch)) + ' ')
        msg = msg.ljust(28, '-')
        msg += ('> ' + str(int(((idx + 1)/len(batch)) * 100)) + '% ')
        msg = msg.ljust(38, '-')
        msg += ('>' + str(counts['rank_1_match_count']) + " total first rank matches")
        print(msg)

        # Filter the dfs down to a single zip----------------------------------------------------------------------
        df_1_single_batch = df_1[(df_1[df_1_batch_col] == item)]
        # Here we have to create a list of dicts to sent to our sqlalchemy function.
        # e.g. = [
        #     {
        #         "address": "123 S Turd Street, Pittsburgh PA 15207",
        #         "lon": 70.65463,
        #         "lat": 40.68543,
        #         "name": "Turd St. Smoke Shop",
        #         "id": 246546
        #     }
        # ]
        if len(df_1_cols_to_parse) > 0:



            # Send it off to our parse_address_list Function. It will return a table, essentially.
            df_1_parsed_single_batch = parser(df_1_single_batch, df_1_cols_to_parse)
            df_1_single_batch = df_1_single_batch.merge(
                df_1_parsed_single_batch,
                left_on=df_1_cols_to_parse['id'],
                right_on=df_1_cols_to_parse['right_on']
            )


        # Do all that same stuff now with the xrefs
        df_2_single_batch = df_2[(df_2[df_2_batch_col] == item)]

        if len(df_2_cols_to_parse) > 0:
            df_2_parsed_single_batch = parser(df_2_single_batch, df_2_cols_to_parse)
            df_2_single_batch = df_2_single_batch.merge(
                df_2_parsed_single_batch,
                left_on=df_2_cols_to_parse['id'],
                right_on=df_2_cols_to_parse['right_on']
            )


        # These columns need to be created so that later in the program df_2_single_batch.columns names can be
        # referenced and the shapes of the dataframes will match up.

        # If we add any columns later they need to first be created here.
        # (looking at you, df_2_single_batch['distance_score']...)

        # Each attr will get a score col whether it's weighted or not (some will just be 0)
        col_list = []
        for key, value in df_2_cols.items():
            new_score_col = str(value["label"] + "_score")
            matched_col = str("matched_" + value["label"])
            col_list.append(new_score_col)
            col_list.append(matched_col)
            df_2_single_batch[new_score_col] = None
            df_2_single_batch[matched_col] = None

        df_2_single_batch['matched_id'] = None
        df_2_single_batch['rank'] = None
        df_2_single_batch['score'] = None
        df_2_single_batch['distance_score'] = None
#         print(col_list)

        # as long as there are master records for the current item...
        if len(df_2_single_batch) != 0:

            # ...and as long as there are xref records for the current zip code...
            if len(df_1_single_batch) != 0:

                # ...make them into dataframes.
                df_1_single_batch = gp.GeoDataFrame(df_1_single_batch)
                df_2_single_batch = gp.GeoDataFrame(df_2_single_batch)

                # iterate over each xref and create a unique list of locations from masters that might match
                for df_2_idx, df_2_value in df_2_single_batch.iterrows():


                    # Each match will be a key, value pair where the key is the ms_id and the value is another dict
                    # of attributes relating to that ms_id. Each one of these will be added to the columns that we
                    # created earlier on in the code

                    # e.g. {
                    #    246546: {
                    #        'score': 95,
                    #        'str_num_score': 72,
                    #        'str_name_score': 20,
                    #        'name_score': 3,
                    #        'master_name': "Turd St. Smoke Shop",
                    #        'master_addr': "123 S Turd Street, Pittsburgh PA 15207"
                    #    }
                    # }
                    possible_matches = {}

                    # a list of all the 'score' attr in possible_matches.
                    # This is used later for determining max scores among a set of possible matches.
                    scores = []

                    # For each master in the current batch
                    for df_1_idx, df_1_value in df_1_single_batch.iterrows():

                        # Compare the address of the xref to the address of the master
                        primary_attr_comparison = None
                        if primary_attribute:
                            primary_attr_comparison = similar(str(df_1_value[df_1_cols[primary_attribute]]).lower(),
                                                       str(df_2_value[df_2_cols[primary_attribute]]).lower()) / 100
                        else:
                            primary_attr_comparison = 1

                        # if the address are remotely similar...
                        # I may increase this from .5 to like .7 later
                        if primary_attr_comparison > .5:
                            score = 0              # Set the individual score...
                            distance_score = 5     # ...the lower the better. If I were to make the default here '0'
                                                   # it would naturally favor records with no lat lon...

                            # if the record as location data (lat and lon)
                            if 'lat' in df_1_value:
                                # and if the location data isn't just set at the default (0)
                                if df_1_value['lat'] != 0:

                                    # determine the distance between the master record and the xref record.
                                    # I suspect that this distance will be a huge deciding factor on the overall
                                    # score.
                                    # Once I know what an average reasonable distance score looks like, I intend to
                                    # multiply it by some amount and subtract it from the overall score.
                                    # The greater the distance score, the less likely it is to match the xref.
                                    distance = haversine({
                                            "lat": df_1_value['lat'],
                                            "long": df_1_value['lon']
                                            },
                                        {
                                            "lat": df_2_value['lat'],
                                            "long": df_2_value['lon']
                                        })['miles']
                                    distance_score = distance
                                    print(distance)  # just print it for now.
                                    # print(df_1_value['lat'], df_1_value['lon'])
                                    # print(df_2_value['lat'], df_2_value['lon'])
                                    # score -= distance



                            # add the msr to the possible_matches dict with theses attributes.
                            possible_matches[df_1_value['id']] = {
                                            'distance_score': distance_score
                                        }

                            for key, value in df_2_cols.items():
                                if key in df_1_cols:
                                    new_score = similar(df_2_value[value["label"]], df_1_value[df_1_cols[key]["label"]]) * value['weight']
                                    score += new_score
                                    possible_matches[df_1_value['id']][str(value["label"] + "_score")] = new_score
                                    possible_matches[df_1_value['id']][str("matched_" + value["label"])] = df_1_value[df_1_cols[key]["label"]]
                                    possible_matches[df_1_value['id']]['score'] = score

                            # add the score to the list of scores.
                            scores.append(score)
                        else:
                            print('not similar at all')



        # ------------------------------------------------ Okay, now scoring is done.-------------------------------

                    # Now we'll take the scores and assign each possible match with a rank.

                    # If there are any possible matches:
                    if len(possible_matches) > 0:

                        # Loop over them and add their attributes to the appropriate columns of the current xref.
                        # Also assign them to a rank.
                        for key, value in possible_matches.items():

                            # If the match is the highest rated one for this xref, and if it is also a rank 1:
                            # Add it's attributes to the appropriate columns and give it the appropriate rank.
                            # If an xref has more than one rank 1 match with exactly the same score;
                            # both will be added as a rank one match.
                            # Otherwise only the match will the highest score will be added.
                            for v_k, v_v in value.items():
                                df_2_value[v_k] = v_v
                            df_2_value['matched_id'] = key
                            df_2_value['rank'] = rank_it(float(value['score']))

                            # Adding the pandas series to the df_list. Each MATCH will have its own series which
                            # will eventually be turned into a row in the final dataframe.
                            # This means that each xref will have x amount of rows, where x is the amount of
                            # possible matches that it has.
                            # Also, each msr can be represented on multiple rows as a possible match to multiple
                            # different xrefs.
                            df_list.append(df_2_value.values.tolist())
                            if value['score'] == max(scores): #  and rank_it(float(max(scores))) == 1
                                max_df_list.append(df_2_value.values.tolist())

                            # Keep track of how many matches of each rank exist.
                            # This makes for easy, surface layer, comparisons and analysis.
                            counts['rank_%s_match_count' % rank_it(float(value['score']))] += 1
                    else:
                        df_2_value['rank'] = 5
                        df_list.append(df_2_value.values.tolist())
                        max_df_list.append(df_2_value.values.tolist())
                        counts['matchless_count'] += 1
            else:
                print(str(item) + " had no df_1 records, but does have df_2 record")
            for df_2_idx, df_2_value in df_2_single_batch.iterrows():
                df_list.append(df_2_value.values.tolist())
                max_df_list.append(df_2_value.values.tolist())
                counts['matchless_count'] += 1
            counts['zip_with_no_msr'] += 1
        else:
            print(str(item) + " Had no df_2, but does have df_1 records. Something is VERY wrong.")

    # Make a dataframe from the list of lists
    max_df_list = pd.DataFrame(max_df_list, columns=df_2_single_batch.columns)
    df_list = pd.DataFrame(df_list, columns=df_2_single_batch.columns)
    message = (("--- %s seconds ---" % (time.time() - start_time)) + '<br />\n' +
               str(counts['rank_1_match_count']) + " rank 1 matches<br />\n" +
               str(counts['rank_2_match_count']) + " rank 2 matches<br />\n" +
               str(counts['rank_3_match_count']) + " rank 3 matches<br />\n" +
               str(counts['rank_4_match_count']) + " rank 4 matches<br />\n" +
               str(counts['matchless_count']) + " matchless locations<br />\n" +
               str(counts['zip_with_no_msr']) + " zip codes with mo master stores<br />\n" +
               str(num_of_batches) + ' zips checked')
    return {"df_list": df_list, "max_df_list": max_df_list, 'message': message}
    # except Exception as e:
    #     message = ("--- %s seconds ---" % (time.time() - start_time)) + '<br />\n' + str(e)
    #     return {"message": message}
# ----------------------------------------------------------------------------------------------------------------------


global_engine = create_engine(global_con_string, poolclass=NullPool)
global_conn = global_engine.connect()
try:
    matched_xrefs = pd.read_sql_query('SELECT * FROM global.master_customer', global_conn)
    matched_xrefs['standardized_zip'] = matched_xrefs['zip'].apply(lambda z: standardize_zpcd(z))
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
        "weight": 1
    },
    "att2": {
        "label": "origin",
        "weight": 1
    },
    "att3": {
        "label": "streetname",
        "weight": 1
    },
    "att4": {
        "label": "address",
        "weight": 1
    },
    "att5": {
        "label": "parsed_zip",
        "weight": 1
    },
    "att6": {
        "label": "predirabbrev",
        "weight": 1
    },
    "att7": {
        "label": "streettypeabbrev",
        "weight": 1
    },
    "att8": {
        "label": "postdirabbrev",
        "weight": 1
    },
    "att9": {
        "label": "internal",
        "weight": 1
    },
    "att10": {
        "label": "location",
        "weight": 1
    },
    "att11": {
        "label": "stateabbrev",
        "weight": 1
    }
}

df_2_cols = {
    "att1": {
        "label": "customer_name",
        "weight": 1
    },
    "att2": {
        "label": "origin",
        "weight": 1
    },
    "att3": {
        "label": "streetname",
        "weight": 1
    },
    "att4": {
        "label": "address",
        "weight": 1
    },
    "att5": {
        "label": "parsed_zip",
        "weight": 1
    },
    "att6": {
        "label": "predirabbrev",
        "weight": 1
    },
    "att7": {
        "label": "streettypeabbrev",
        "weight": 1
    },
    "att8": {
        "label": "postdirabbrev",
        "weight": 1
    },
    "att9": {
        "label": "internal",
        "weight": 1
    },
    "att10": {
        "label": "location",
        "weight": 1
    },
    "att11": {
        "label": "stateabbrev",
        "weight": 1
    }
}


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
        None
    )

filename = '/home/scientist/output/10.03.17_full_even_weights.csv'

x['df_list'].to_csv(filename)