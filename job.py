import requests
import os
import pandas as pd

API_KEY = os.getenv("api_key")

SPORT = 'aussierules_afl' # use the sport_key from the /sports endpoint below, or use 'upcoming' to see the next 8 games across all sports
REGIONS = 'au' # uk | us | eu | au. Multiple can be specified if comma delimited
MARKETS = 'h2h' # h2h | spreads | totals. Multiple can be specified if comma delimited
ODDS_FORMAT = 'decimal' # decimal | american
DATE_FORMAT = 'iso' # iso | unix
 
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
df_out

df_main = df1[['id', 'commence_time', 'home_team', 'away_team']]
df_main
df_main = df_main.merge(df_out, how='left', left_on='home_team', right_on='team')
df_main = df_main.merge(df_out, how='left', left_on='away_team', right_on='team')
df_main.drop(['team_x', 'team_y'], axis='columns', inplace=True)
df_main.rename(columns={'odds_x':'home_odds', 'odds_y':'away_odds'}, inplace=True)
df_main
