import pandas as pd
import numpy as np
import warnings
import sqlite3
from sqlite3 import Error
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder

def season_string(season):
    return str(season) + '-' + str(season+1)[-2:]

class transform:

    def __init__(self,conn,start_season,end_season):
        self.conn = conn
        self.start_season = start_season
        self.end_season = end_season

    def load_team_data(self):
        """Loads basic, advanced, and scoring boxscores 
        from sqlite database and merges them into one dataframe"""

        basic = pd.read_sql("SELECT * FROM team_basic_boxscores", self.conn)
        adv = pd.read_sql("SELECT * FROM team_advanced_boxscores", self.conn)
        scoring = pd.read_sql("SELECT * FROM team_scoring_boxscores", self.conn)


        temp = pd.merge(basic, adv, how='left', on=[
                        'GAME_ID', 'TEAM_ID'], suffixes=['', '_y'])
        df = pd.merge(temp, scoring, how='left', on=[
                      'GAME_ID', 'TEAM_ID'], suffixes=['', '_y'])


        df = df.loc[df['SEASON'].between(season_string(self.start_season), season_string(self.end_season))]

    
        return df
    
    def create_matchups(self, df):
        """This function makes each row a matchup between 
        team and opp"""
        df2 = df.copy()

        matchups = pd.merge(df, df2, on=['GAME_ID'], suffixes=['_home', '_away'], copy=False, how="left")
        matchups = matchups.loc[matchups['TEAM_ABBREVIATION_home'] != matchups['TEAM_ABBREVIATION_away']]

        return matchups
    
    def clean_team_data(self, df):
        """This function cleans the team_data
        1) Changes W/L to 1/0 
        2) Changes franchise abbreviations to their most 
        recent abbreviation for consistency
        3) Converts GAME_DATE to datetime object
        4) Creates a binary column 'HOME_GAME'
        5) Removes 3 games where advanced stats were not collected
        """
        df = df.copy()
        df['WL'] = (df['WL'] == 'W').astype(int)

        abbr_mapping = {'NJN': 'BKN',
                        'CHH': 'CHA',
                        'VAN': 'MEM',
                        'NOH': 'NOP',
                        'NOK': 'NOP',
                        'SEA': 'OKC'}

        df['TEAM_ABBREVIATION'] = df['TEAM_ABBREVIATION'].replace(abbr_mapping)
        df['MATCHUP'] = df['MATCHUP'].str.replace('NJN', 'BKN')
        df['MATCHUP'] = df['MATCHUP'].str.replace('CHH', 'CHA')
        df['MATCHUP'] = df['MATCHUP'].str.replace('VAN', 'MEM')
        df['MATCHUP'] = df['MATCHUP'].str.replace('NOH', 'NOP')
        df['MATCHUP'] = df['MATCHUP'].str.replace('NOK', 'NOP')
        df['MATCHUP'] = df['MATCHUP'].str.replace('SEA', 'OKC')

        df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

        df['HOME_GAME'] = df['MATCHUP'].str.contains('vs').astype(int)

        df.dropna(inplace=True)

        return df
    
    def convert_pcts(self,df):
        """This function...
        1) Removes categories that are percentages,
        as we will be averaging them and do not want to average 
        percentages. 
        2) Converts shooting percentage stats into raw values"""
        df = df.copy()

        df = df.drop(columns=['FT_PCT', 'FG_PCT', 'FG3_PCT', 'DREB_PCT',
                              'OREB_PCT', 'REB_PCT', 'AST_PCT', 'AST_TOV',
                              'AST_RATIO', 'E_TM_TOV_PCT', 'TM_TOV_PCT',
                              'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT',
                              'PACE', 'PACE_PER40', 'MIN'])

        df['FG2M'] = df['FGM'] - df['FG3M']
        df['FG2A'] = df['FGA'] - df['FG3A']
        df['PTS_2PT_MR'] = (df['PTS'] * df['PCT_PTS_2PT_MR']).astype('int8')
        df['PTS_FB'] = (df['PTS'] * df['PCT_PTS_FB']).astype('int8')
        df['PTS_OFF_TOV'] = (df['PTS'] * df['PCT_PTS_OFF_TOV']).astype('int8')
        df['PTS_PAINT'] = (df['PTS'] * df['PCT_PTS_PAINT']).astype('int8')
        df['AST_2PM'] = (df['FG2M'] * df['PCT_AST_2PM']).astype('int8')
        df['AST_3PM'] = (df['FG3M'] * df['PCT_AST_3PM']).astype('int8')
        df['UAST_2PM'] = (df['FG2M'] * df['PCT_UAST_2PM']).astype('int8')
        df['UAST_3PM'] = (df['FG3M'] * df['PCT_UAST_3PM']).astype('int8')


        df = df[['SEASON', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID',
                 'GAME_DATE', 'MATCHUP', 'HOME_GAME', 'WL', 'FG2M', 'FG2A', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB',
                 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS',
                 'E_OFF_RATING', 'OFF_RATING', 'E_DEF_RATING', 'DEF_RATING',
                 'E_NET_RATING', 'NET_RATING', 'POSS', 'PIE', 'PTS_2PT_MR',
                 'PTS_FB', 'PTS_OFF_TOV', 'PTS_PAINT', 'AST_2PM', 'AST_3PM',
                 'UAST_2PM', 'UAST_3PM']]
#
        return df
    
    def transform(self,type='team_boxscores'):
        '''
        agg player data is the mean of by team and game idss

        '''
        types = ['team_boxscores','player_boxscores','agg_boxscores','matchup_boxscores']        
        
        team_boxscores = self.load_team_data()
        team_boxscores = self.clean_team_data(team_boxscores)
        team_boxscores = team_boxscores.dropna(subset='PCT_PTS_2PT')
        team_boxscores = self.convert_pcts(team_boxscores)
        matchups = self.create_matchups(team_boxscores).reset_index()
        matchups = matchups.drop('index',axis=1)

        team_boxscores['TEAM_ID'] = team_boxscores['TEAM_ID'].astype('string')
        team_boxscores['GAME_ID'] = team_boxscores['GAME_ID'].astype('string')

        player_boxscores = pd.read_sql("SELECT * FROM player_game_logs", self.conn)
        player_boxscores = self.clean_team_data(player_boxscores)
        player_boxscores = player_boxscores.loc[:,~player_boxscores.columns.str.contains('_RANK')].drop(['WNBA_FANTASY_PTS','AVAILABLE_FLAG'],axis=1)
        
        player_boxscores['MIN'] = player_boxscores['MIN'].round(0)
        player_boxscores['TEAM_ID'] = player_boxscores['TEAM_ID'].astype('string')
        player_boxscores['GAME_ID'] = player_boxscores['GAME_ID'].astype('string')
        player_boxscores['PLAYER_ID'] = player_boxscores['PLAYER_ID'].astype('string')

        transformed_players = player_boxscores.groupby(["TEAM_ID",'GAME_ID']).mean(numeric_only=True).reset_index().drop(['WL','HOME_GAME'],axis=1)

        merged_data = pd.merge(left=team_boxscores,right=transformed_players,on=['GAME_ID','TEAM_ID'],suffixes=['_game','_players'])
        #'TEAM_NAME_y','TEAM_ABBREVIATION_y','TEAM_CITY_y','MIN_y','TEAM_CITY'
        merged_data = merged_data.drop(['TEAM_ID','GAME_ID','TEAM_NAME','GAME_DATE','TD3'],axis=1)
    
        if type == types[0]:
            df = team_boxscores
        elif type == types[1]:
            df = player_boxscores
        elif type == types[2]:
            df = merged_data
        elif type == types[3]:
            df = matchups

        return df

    def standard_scalar(self,df):
        scaler = StandardScaler()
        filter = [a for i,a in enumerate(df.dtypes.keys()) if (((df.dtypes[a] in [np.dtype('int8'),np.dtype('int16'),np.dtype('int32'),np.dtype('int64')]) or (df.dtypes[a] in [np.dtype('float16'),np.dtype('float32'),np.dtype('float64')])) and (a not in ['WL','HOME_GAME','SEASON','TEAM_ABBREVIATION','SEASON_home','HOME_GAME_away','WL_home','WL_away','HOME_GAME_home','TEAM_ABBREVIATION_home','TEAM_ABBREVIATION_away','TEAM_ID_home']))]
        df[filter] = scaler.fit_transform(df[filter])
        return df

    def label_encoder(self,df,columns):
        for column in columns:
            encoder = LabelEncoder()
            encoder.fit(df[column])
            df[column] = encoder.transform(df[column])

        return df
    
    def create_windows(self,df,window_size,type='single',matchup=False):
        X_list = []
        y_list = []

        matchup_sub = ''

        if matchup == True:
            matchup_sub = '_home'

        if type == 'multi':
            for i in range(len(df["TEAM_ABBREVIATION{0}".format(matchup_sub)].unique())):
                team = df.loc[df["TEAM_ABBREVIATION{0}".format(matchup_sub)] == i]
                team_label = team["WL{0}".format(matchup_sub)].copy()
                team = team.drop("WL{0}".format(matchup_sub),axis=1)

                team_np = team.to_numpy()
                team_label = team_label.to_numpy()

                #temp_X = []
                #temp_y = []

                for i in range(len(team_np)-window_size):        
                  row = [a for a in team_np[i:i+window_size]]
                  X_list.append(row)

                  label = team_label[i+window_size]
                  y_list.append(label)

            X_list = np.array(X_list)
            y_list = np.array(y_list)

            return X_list,y_list
        if type == 'single':
            team = df.drop("WL{0}".format(matchup_sub),axis=1)
            team_label = df["WL{0}".format(matchup_sub)].copy()

            team_np = team.to_numpy()
            team_label = team_label.to_numpy()

            for i in range(len(team_np)-window_size):        
                  row = [a for a in team_np[i:i+window_size]]
                  X_list.append(row)

                  label = team_label[i+window_size]
                  y_list.append(label)

            X_list = np.array(X_list)
            y_list = np.array(y_list)

            return X_list,y_list
        
    def convert_pct_change(self,df,type='single'):
        filter = [a for i,a in enumerate(df.dtypes.keys()) if (((df.dtypes[a] in [np.dtype('int8'),np.dtype('int16'),np.dtype('int32'),np.dtype('int64')]) or (df.dtypes[a] in [np.dtype('float16'),np.dtype('float32'),np.dtype('float64')])) and (a not in ['WL','HOME_GAME','SEASON','TEAM_ABBREVIATION']))]
        if type == 'single':
            df[filter] = df[filter].pct_change(axis='index',periods=1)
            df[filter] = df[filter].replace([np.inf,-np.inf], 0)
            df.reset_index(inplace=True)

            return df
        elif type == 'multi':
            df_list = []
            for i in range(len(df['TEAM_ABBREVIATION'].unique())):
                team = df.loc[df['TEAM_ABBREVIATION'] == i]
                team[filter] = team[filter].pct_change(axis='index',periods=-1)
                team[filter] = team[filter].replace([np.inf,-np.inf], 0)
                team.reset_index(inplace=True)
                df_list.append(team)
            df = pd.concat(df_list).reset_index()
            df = df.loc[2:,:].drop(columns=['TEAM_ID','TEAM_NAME','GAME_ID','GAME_DATE','MATCHUP','UAST_3PM','PTS_2PT_MR','level_0','index'])
            return df


            



        
  