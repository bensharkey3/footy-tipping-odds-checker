import requests
import os
import pandas as pd
import numpy as np


API_KEY = os.getenv("api_key")


SPORT = 'aussierules_afl' # use the sport_key from the /sports endpoint below, or use 'upcoming' to see the next 8 games across all sports
REGIONS = 'au' # uk | us | eu | au. Multiple can be specified if comma delimited
MARKETS = 'h2h' # h2h | spreads | totals. Multiple can be specified if comma delimited
ODDS_FORMAT = 'decimal' # decimal | american
DATE_FORMAT = 'iso' # iso | unix

FILENAME = r'filename.csv'


def create_odds_df(API_KEY, SPORT, REGIONS, MARKETS, ODDS_FORMAT, DATE_FORMAT):
    odds_response = requests.get(
    f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds',
        params={
            'api_key': API_KEY,
            'regions': REGIONS,
            'markets': MARKETS,
            'oddsFormat': ODDS_FORMAT,
            'dateFormat': DATE_FORMAT,
        }
    )
     
    if odds_response.status_code != 200:
        print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')
     
    else:
        odds_json = odds_response.json()
        print('Number of events:', len(odds_json))
        # print(odds_json)
     
        # Check the usage quota
        print('Remaining requests', odds_response.headers['x-requests-remaining'])
        print('Used requests', odds_response.headers['x-requests-used'])
    
    df1 = pd.json_normalize(odds_json)
    
    dftemp = pd.json_normalize(odds_json, record_path=['bookmakers'])
    dftemp = dftemp[dftemp['key'] == 'sportsbet']
    dftemp.reset_index(drop=True, inplace=True)
    
    df = pd.concat([df1.drop(['bookmakers'], axis = 1), dftemp], axis = 1)
    
    lst = []
    for i in df['markets']:
        print(i)
        lst.append(i)
        print('------------------')
        print(lst)
        print('xxxxxxxxxxxxx')
    
    lst2 = []
    for i in lst:
        print(i[0]['outcomes'])
        lst2.append(i[0]['outcomes'])
        print('----')
    
    dftemp = pd.json_normalize(lst2)
    df_1 = dftemp[0].apply(lambda x: pd.Series(x.values()))
    df_2 = dftemp[1].apply(lambda x: pd.Series(x.values()))
    df_out = pd.concat([df_1, df_2])
    df_out.columns =['team', 'odds']
    
    df_main = df1[['id', 'commence_time', 'home_team', 'away_team']]
    df_main = df_main.merge(df_out, how='left', left_on='home_team', right_on='team')
    df_main = df_main.merge(df_out, how='left', left_on='away_team', right_on='team')
    df_main.drop(['team_x', 'team_y'], axis='columns', inplace=True)
    df_main.rename(columns={'odds_x':'home_odds', 'odds_y':'away_odds'}, inplace=True)
    
    df_main['commence_time'] = pd.to_datetime(df_main['commence_time'])
    df_main['commence_time'] = df_main['commence_time'].dt.floor('Min').dt.tz_convert(tz='Australia/Melbourne').dt.tz_localize(None)
    df_main['snapshot_time'] = pd.Timestamp.utcnow()
    df_main['snapshot_time'] = df_main['snapshot_time'].dt.floor('Min').dt.tz_convert(tz='Australia/Melbourne').dt.tz_localize(None)
    return df_main


def read_file_from_csv():
    afl_odds = pd.read_csv(FILENAME, index_col=False)
    afl_odds['commence_time'] = pd.to_datetime(afl_odds['commence_time'], format='%Y-%m-%d %H:%M')
    afl_odds['snapshot_time'] = pd.to_datetime(afl_odds['snapshot_time'], format='%Y-%m-%d %H:%M')
    return afl_odds


def create_file():
    afl_odds = pd.DataFrame(columns=['id', 'commence_time', 'home_team', 'away_team', 'home_odds', 'away_odds', 'snapshot_time'])
    # afl_odds['commence_time'] = convert_df_datestamp_col(afl_odds['commence_time'])
    # afl_odds['snapshot_time'] = convert_df_datestamp_col(afl_odds['snapshot_time'])
    return afl_odds


def incremental_load(afl_odds, df_main):
    odds_out = pd.concat([afl_odds, df_main])
    odds_out = odds_out.groupby(['id', 'commence_time', 'home_team', 'away_team', 'home_odds', 'away_odds'])[['snapshot_time']].max().sort_values(by=['snapshot_time', 'id'], ascending=False).reset_index()
    return odds_out

def favourite_has_changed(odds_out):
    #get upcoming games with successfull odds updates
    temp1 = odds_out.copy()
    temp1['snapshot_time'] = temp1['snapshot_time'].dt.floor('h')
    temp2 = pd.DataFrame(temp1, columns=['id', 'snapshot_time', 'current_hour'])
    temp2['current_hour'] = pd.to_datetime('today').floor('h').tz_localize(None)
    temp2['match'] = np.where(temp2['snapshot_time'] == temp2['current_hour'], temp2['id'], None)
    temp2 = temp2['match'].dropna().drop_duplicates()
    
    #get matches where the favourite has just changed
    temp3 = odds_out[odds_out['id'].isin(temp2)]
    temp3['favourite'] = np.where(temp3['home_odds'] <= temp3['away_odds'], 'home', 'away')
    temp3['rank'] = temp3.groupby('id')['snapshot_time'].rank(method='max', ascending=False)
    temp3 = temp3[temp3['rank'] <= 2]
    
    temp3['prev_favourite'] = temp3.groupby(['id'])['favourite'].shift(-1)
    temp3 = temp3[temp3['prev_favourite'].notna()]
    temp3['favourite_changed'] = np.where(temp3['favourite'] == temp3['prev_favourite'], False, True)
    
    #create alert message
    count = 0
    message = None
    for i in temp3['favourite_changed']:
        if i == True:
            count+=1
        else:
            pass
    if count > 0:
        message = f'favourite has changed for {count} upcoming game(s)'
    print(message)
    return message


def write_to_csv_file(odds_out):
    odds_out = odds_out.sort_values(by=['snapshot_time', 'id'], ascending=False)
    odds_out.to_csv(FILENAME, index=False, date_format='%Y-%m-%d %H:%M')
    return odds_out


def main():
    print('starting...')
    df_main = create_odds_df(API_KEY, SPORT, REGIONS, MARKETS, ODDS_FORMAT, DATE_FORMAT)
    try:
        afl_odds = read_file_from_csv()
        print('succesfully read from csv file')
    except:
        afl_odds = create_file()
        print('failed to read from csv, created new file instead')
    print('merging files...')
    odds_out = incremental_load(afl_odds, df_main)
    print('sending message notification...')
    message = favourite_has_changed(odds_out)
    print('writing output to csv...')
    odds_out = write_to_csv_file(odds_out)
    print('completed')
    return odds_out, df_main


if __name__ == '__main__':
    odds_out, df_main = main()
