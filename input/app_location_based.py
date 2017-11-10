import pandas as pd
import geopandas as gp
from shapely.geometry import Point
from shapely.geometry.polygon import LinearRing, Polygon
from shapely.ops import nearest_points
from difflib import SequenceMatcher
import usaddress

# Load Files-----------------------------------------------------------------------------------------------------------

msr_df = pd.read_csv('/home/scientist/input/mars_tdlinx_files/msrs_with_geom.csv')
msr_df = msr_df[msr_df[' lat'].notnull()]

tdlinx_df = pd.read_csv('/home/scientist/input/mars_tdlinx_files/tdlinx.CSV', header=None)

zip_df = pd.read_csv('/home/scientist/input/mars_tdlinx_files/zips.csv')

# Utility Functions-----------------------------------------------------------------------------------------------------


def similar(a, b):
    """
    :param a: A string to be compared to 'b'
    :param b: A string to be compared to 'a'
    :return: The ratio in pct of how similar string 'a' is to string 'b'
    """
    return SequenceMatcher(None, str(a), str(b)).ratio()

# print(similar('penn ave', 'Ollie st'))

# Real shit ------------------------------------------------------------------------------------------------------------
def compare_dfs(df1, df2):
    """
    :param df1: Our MSR Dataframe with Geodata
    :param df2: TDLINX Dataframe
    :return: Joined dataframe
    """
    match_count = 0
    df_list = []
    # zipset = [92801, 15212, 16301, 48650]
    ziptest = df2[10].apply(lambda z: int(str(z)[:5])).tolist()
    ziptest = [int(item) for item in ziptest]
    zipset = list(set(ziptest))


    for idx, zip in enumerate(zipset):
        msg = ((str(zip) + ' ' + str(idx + 1) + '/' + str(len(zipset)) + ' '))
        msg = msg.ljust(28, '-')
        msg += ('> ' + str(int(((idx + 1)/len(zipset)) * 100))+ '% ')
        msg = msg.ljust(38, '-')
        msg += ('>' + str(match_count) + " total possible matches")
        print(msg)

        # Filter the dfs down to a single state and add point geom column -------------
        df1_single_state = df1[(df1[' zip'] == zip)]
        df1_single_state['possible_tdlinx_match'] = None
        df1_single_state['tdlinx_name'] = None
        df1_single_state['tdlinx_address'] = None
        df1_single_state['geometry'] = None

        df2[10] = df2[10].apply(lambda z: int(str(z)[:5]))
        df2_single_state = df2[(df2[10] == zip)]

        if len(df1_single_state) != 0:
            if len(df2_single_state) != 0:

                df1_single_state['geometry'] = df1_single_state.apply(lambda z: Point(z[' lon'], z[' lat']), axis=1)
                df1_single_state['possible_tdlinx_match'] = None
                df1_single_state = gp.GeoDataFrame(df1_single_state)

                df2_single_state['geometry'] = df2_single_state.apply(lambda z: Point(z[21], z[20]), axis=1)
                df2_single_state['polygon'] = df2_single_state['geometry'].apply(lambda z: z.buffer(.1))
                df2_single_state = gp.GeoDataFrame(df2_single_state)

                # iterate over each polygon from df2 and create a unique list of points from df1 that exist within that polygon
                for idx2, df2_value in df2_single_state.iterrows():
                    intersect_points = []
                    for idx1, df1_value in df1_single_state.iterrows():
                        if df2_value['polygon'].intersects(df1_value['geometry']):
                            intersect_points.append(df1_value)

                    # pull out the street number of df2 that is currently being worked with
                    df2_addr = usaddress.parse(df2_value[7])
                    df2_street_num = ''
                    for i in range(len(df2_addr)):
                            if df2_addr[i][1] == 'AddressNumber':
                                df2_street_num = df2_addr[i][0]

                    # For each location that exists in the current polygon, compare their address to determine match likelihood
                    for pt in intersect_points:
                        match_list = None
                        x = usaddress.parse(pt[' global_address'])
                        for i in range(len(x)):
                            if x[i][1] == 'AddressNumber':
                                if len(x[i][0]) != 0 and len(df2_street_num) != 0:
                                    if similar(x[i][0], df2_street_num) == 1:
                                        # if similar(pt[' global_address'][:len(df2_value[7])].lower(), df2_value[7].lower()) > .6:
                                        match_list = int(round(df2_value[0]))
                                else:
                                    if similar(pt[' global_address'][:len(df2_value[7])].lower(), df2_value[7].lower()) > .6:
                                        match_list = int(round(df2_value[0]))
                        pt['possible_tdlinx_match'] = match_list
                        if match_list:
                            match_count += 1
                            pt['tdlinx_name'] = str(df2_value[5])
                            pt['tdlinx_address'] = str(df2_value[7])
                        df_list.append(pt.values.tolist())
            else:
                print(str(zip) + " Had no tdlinx records, but does have msr records")

    # Make a dataframe from the list of lists
    df_list = pd.DataFrame(df_list, columns=df1_single_state.columns)
    print(str(match_count) + " matches")
    return df_list


def rank_matches(df, msid_list):

    df['prefered'] = None

    for idx, msid in enumerate(msid_list):
        msg = ('Setting Preferences for ' + str(msid) + ' ' + str(idx) + '/' + str(len(msid_list)))
        msg = msg.ljust(50, '-')
        msg += ('> ' + str(int((idx/len(msid_list)) * 100)) + '%')
        print(msg)
        potential_matches = {}
        prefered = ''
        single_msid_df = df[(df[' ms_id'] == msid)]
        for idx, row in single_msid_df.iterrows():
            if row['tdlinx_name']:
                potential_matches[row['tdlinx_name']] = similar(str(row[' store_name']).lower(), str(row['tdlinx_name']).lower())
        if len(list(potential_matches.values())) > 0:
            prefered = [key for key, value in potential_matches.items() if value == max(list(potential_matches.values()))][0]
        df.loc[((df[' ms_id'] == msid) & (df['tdlinx_name'] == prefered)), 'prefered'] = 'x'
    return df
# ----------------------------------------------------------------------------------------------------------------------
pd.options.mode.chained_assignment = None  # default='warn'
x = compare_dfs(msr_df, tdlinx_df)
x.to_csv('/home/scientist/output/testing.csv')

new_df = rank_matches(x, x[' ms_id'].unique().tolist())
new_df.to_csv('/home/scientist/output/testing_w_prefered.csv')

