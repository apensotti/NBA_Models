import sqlite3
import pandas as pd
import numpy as np
import warnings

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

        matchups = pd.merge(df, df2, on=['GAME_ID'], suffixes=['_team', '_opp'], copy=False, how="left")
        matchups = matchups.loc[matchups['TEAM_ABBREVIATION_team'] != matchups['TEAM_ABBREVIATION_opp']]

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

        df['POINT_DIFF'] = df['PLUS_MINUS']
        df['RECORD'] = df['WL']
        df['TEAM_SCORE'] = df['PTS']

        df = df[['SEASON', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID',
                 'GAME_DATE', 'MATCHUP', 'HOME_GAME', 'TEAM_SCORE', 'POINT_DIFF', 'WL',
                 'RECORD', 'FG2M', 'FG2A', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB',
                 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS',
                 'E_OFF_RATING', 'OFF_RATING', 'E_DEF_RATING', 'DEF_RATING',
                 'E_NET_RATING', 'NET_RATING', 'POSS', 'PIE', 'PTS_2PT_MR',
                 'PTS_FB', 'PTS_OFF_TOV', 'PTS_PAINT', 'AST_2PM', 'AST_3PM',
                 'UAST_2PM', 'UAST_3PM']]

        return df

