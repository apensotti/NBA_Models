import pandas as pd
import numpy as np
import warnings
import sqlite3
from sqlite3 import Error

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
        matchups = self.create_matchups(team_boxscores)

        team_boxscores['TEAM_ID'] = team_boxscores['TEAM_ID'].astype('string')
        team_boxscores['GAME_ID'] = team_boxscores['GAME_ID'].astype('string')

        player_boxscores = pd.read_sql("SELECT * FROM player_game_logs", self.conn)
        player_boxscores = self.clean_team_data(player_boxscores)

        player_boxscores['TEAM_ID'] = player_boxscores['TEAM_ID'].astype('string')
        player_boxscores['GAME_ID'] = player_boxscores['GAME_ID'].astype('string')

        transformed_players = player_boxscores.groupby(["TEAM_ID",'GAME_ID']).mean(numeric_only=True).reset_index()
        transformed_players = transformed_players.drop(columns=['WNBA_FANTASY_PTS_RANK','AVAILABLE_FLAG','WNBA_FANTASY_PTS'])

        merged_data = pd.merge(left=team_boxscores,right=transformed_players,on=['GAME_ID','TEAM_ID'],suffixes=['_game','_players'])
    
        if type == types[0]:
            df = team_boxscores
        elif type == types[1]:
            df = player_boxscores
        elif type == types[2]:
            df = merged_data
        elif type == types[3]:
            df = matchups

        return df




