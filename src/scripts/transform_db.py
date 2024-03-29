import sqlite3
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

class load_clean:
    def __init__(self,db_file):
        self.db_file = db_file
        self.conn, self.cur = self.create_connection()

    def create_connection(self):
        """ create a database connection to the SQLite database
        specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(self.db_file)
        except Error as e:
            print(e)
        
        cur = conn.cursor()

        return conn, cur
    
    def basic_boxscores(self):
        self.cur.execute("SELECT * FROM team_basic_boxscores")
        rows = self.cur.fetchall()

        db_list = []

        for row in rows:
            db_list.append(row)

        basic_boxscores = pd.DataFrame(data=db_list)

        basic_boxscores.columns = ['SEASON', 'TEAM_ID', 'TEAM_ABBREVIATION', 
                    'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'FGM', 'FGA', 
                    'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
                    'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 
                    'PLUS_MINUS']
        
        return basic_boxscores
    
    def advanced_boxscores(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM team_advanced_boxscores")
        rows = cur.fetchall()

        db_advanced_list = []

        for row in rows:
            db_advanced_list.append(row)

        advanced_boxscores = pd.DataFrame(data=db_advanced_list)

        advanced_boxscores.columns = ['GAME_ID', 'TEAM_ID', 'TEAM_NAME', 
                    'TEAM_ABBREVIATION', 'TEAM_CITY', 'MIN', 'E_OFF_RATING', 'OFF_RATING', 'E_DEF_RATING', 
                    'DEF_RATING', 'E_NET_RATING', 'NET_RATING', 'AST_PCT', 'AST_TOV', 
                    'AST_RATIO', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'E_TM_TOV_PCT', 
                    'TM_TOV_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT', 'E_PACE', 
                    'PACE', 'PACE_PER40', 'POSS', 'PIE']
        
        return advanced_boxscores
    
    def scoring_boxscores(self):
        self.cur.execute("SELECT * FROM team_scoring_boxscores")
        rows = self.cur.fetchall()

        db_scoring_list = []

        for row in rows:
            db_scoring_list.append(row)

        scoring_boxscores = pd.DataFrame(data=db_scoring_list)

        scoring_boxscores.columns = ['GAME_ID', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CITY',
               'MIN', 'PCT_FGA_2PT', 'PCT_FGA_3PT', 'PCT_PTS_2PT', 'PCT_PTS_2PT_MR',
               'PCT_PTS_3PT', 'PCT_PTS_FB', 'PCT_PTS_FT', 'PCT_PTS_OFF_TOV',
               'PCT_PTS_PAINT', 'PCT_AST_2PM', 'PCT_UAST_2PM', 'PCT_AST_3PM',
               'PCT_UAST_3PM', 'PCT_AST_FGM', 'PCT_UAST_FGM']

        return scoring_boxscores
    
    def boxscore_matchups(self):
        self.cur.execute("SELECT * FROM boxscores")
        rows3 = self.cur.fetchall()

        boxscores_list = []

        columns = ['SEASON_home', 'TEAM_ID_home', 'TEAM_ABBREVIATION_home',
                'TEAM_NAME_home', 'GAME_ID', 'GAME_DATE_home', 'MATCHUP_home',
                'HOME_GAME_home', 'WL_home',
                'FG2M_home', 'FG2A_home', 'FG3M_home', 'FG3A_home',
                'FTM_home', 'FTA_home', 'OREB_home', 'DREB_home', 'REB_home',
                'AST_home', 'STL_home', 'BLK_home', 'TOV_home', 'PF_home', 'PTS_home',
                'PLUS_MINUS_home', 'E_OFF_RATING_home', 'OFF_RATING_home',
                'E_DEF_RATING_home', 'DEF_RATING_home', 'E_NET_RATING_home',
                'NET_RATING_home', 'POSS_home', 'PIE_home', 'PTS_2PT_MR_home',
                'PTS_FB_home', 'PTS_OFF_TOV_home', 'PTS_PAINT_home', 'AST_2PM_home',
                'AST_3PM_home', 'UAST_2PM_home', 'UAST_3PM_home', 'SEASON_away',
                'TEAM_ID_away', 'TEAM_ABBREVIATION_away', 'TEAM_NAME_away', 'GAME_DATE_away', 
                'MATCHUP_away', 'HOME_GAME_away','WL_away', 
                'FG2M_away', 'FG2A_away',
                'FG3M_away', 'FG3A_away', 'FTM_away', 'FTA_away', 'OREB_away', 'DREB_away', 
                'REB_away', 'AST_away', 'STL_away', 'BLK_away', 
                'TOV_away', 'PF_away', 'PTS_away', 'PLUS_MINUS_away', 'E_OFF_RATING_away', 
                'OFF_RATING_away',
                'E_DEF_RATING_away', 'DEF_RATING_away', 'E_NET_RATING_away', 
                'NET_RATING_away', 'POSS_away', 'PIE_away', 'PTS_2PT_MR_away', 'PTS_FB_away',
                'PTS_OFF_TOV_away', 'PTS_PAINT_away', 'AST_2PM_away', 'AST_3PM_away', 
                'UAST_2PM_away', 'UAST_3PM_away']

        for row in rows3:
            boxscores_list.append(row)

        boxscores = pd.DataFrame(data=boxscores_list)

        boxscores.columns = columns

        boxscores_clean = boxscores[['GAME_ID','TEAM_ID_home','TEAM_ID_away','SEASON_home','GAME_DATE_home',
                'TEAM_NAME_home','TEAM_NAME_away', 'MATCHUP_home','TEAM_ABBREVIATION_home',
                'HOME_GAME_home', 'WL_home',
                'FG2M_home', 'FG2A_home', 'FG3M_home', 'FG3A_home',
                'FTM_home', 'FTA_home', 'OREB_home', 'DREB_home', 'REB_home',
                'AST_home', 'STL_home', 'BLK_home', 'TOV_home', 'PF_home', 'PTS_home',
                'PLUS_MINUS_home', 'E_OFF_RATING_home', 'OFF_RATING_home',
                'E_DEF_RATING_home', 'DEF_RATING_home', 'E_NET_RATING_home',
                'NET_RATING_home', 'POSS_home', 'PIE_home', 'PTS_2PT_MR_home',
                'PTS_FB_home', 'PTS_OFF_TOV_home', 'PTS_PAINT_home', 'AST_2PM_home',
                'AST_3PM_home', 'UAST_2PM_home', 'UAST_3PM_home',
                'TEAM_ABBREVIATION_away', 'HOME_GAME_away','WL_away', 
                'FG2M_away', 'FG2A_away',
                'FG3M_away', 'FG3A_away', 'FTM_away', 'FTA_away', 'OREB_away', 'DREB_away', 
                'REB_away', 'AST_away', 'STL_away', 'BLK_away', 
                'TOV_away', 'PF_away', 'PTS_away', 'PLUS_MINUS_away', 'E_OFF_RATING_away', 
                'OFF_RATING_away',
                'E_DEF_RATING_away', 'DEF_RATING_away', 'E_NET_RATING_away', 
                'NET_RATING_away', 'POSS_away', 'PIE_away', 'PTS_2PT_MR_away', 'PTS_FB_away',
                'PTS_OFF_TOV_away', 'PTS_PAINT_away', 'AST_2PM_away', 'AST_3PM_away', 
                'UAST_2PM_away', 'UAST_3PM_away']]

        return pd.DataFrame(boxscores_clean)

    def players(self):

        self.cur.execute("SELECT * FROM player_game_logs")
        rows3 = self.cur.fetchall()

        db_scoring_list = []

        for row in rows3:
            db_scoring_list.append(row)

        player_index = pd.DataFrame(data=db_scoring_list)

        player_index.columns = ["SEASON_YEAR","PLAYER_ID","PLAYER_NAME","NICKNAME",
                    "TEAM_ID","TEAM_ABBREVIATION","TEAM_NAME","GAME_ID","GAME_DATE","MATCHUP","WL","MIN","FGM","FGA",
                    "FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT","OREB","DREB","REB","AST",
                    "TOV","STL","BLK","BLKA","PF","PFD","PTS","PLUS_MINUS","NBA_FANTASY_PTS","DD2",
                    "TD3","GP_RANK","W_RANK","L_RANK","W_PCT_RANK","MIN_RANK","FGM_RANK","FGA_RANK",
                    "FG_PCT_RANK","FG3M_RANK","FG3A_RANK","FG3_PCT_RANK","FTM_RANK","FTA_RANK","FT_PCT_RANK",
                    "OREB_RANK","DREB_RANK","REB_RANK","AST_RANK","TOV_RANK","STL_RANK","BLK_RANK",
                    "BLKA_RANK","PF_RANK","PFD_RANK","PTS_RANK","PLUS_MINUS_RANK","NBA_FANTASY_PTS_RANK",
                    "DD2_RANK","TD3_RANK",'WNBA_FANTASY_PTS','WNBA_FANTASY_PTS_RANK','AVAILABLE_FLAG']

        player_index['TEAM_ID'] = player_index['TEAM_ID'].astype(str, copy=True)
        player_index['GAME_ID'] = player_index['GAME_ID'].astype(str, copy=True)
        player_index['PLAYER_ID'] = player_index['PLAYER_ID'].astype(str, copy=True)

        return pd.DataFrame(player_index)
    
    def agg_boxscores_raw(self):
        self.cur.execute("SELECT * FROM team_basic_boxscores as bb JOIN team_advanced_boxscores as ab on bb.GAME_ID = ab.GAME_ID AND bb.TEAM_ID = ab.TEAM_ID JOIN team_scoring_boxscores as sb ON bb.GAME_ID = sb.GAME_ID and bb.TEAM_ID = sb.TEAM_ID")
        rows3 = self.cur.fetchall()

        db_scoring_list = []

        for row in rows3:
            db_scoring_list.append(row)

        player_index = pd.DataFrame(data=db_scoring_list)

        player_index.columns = ['SEASON', 'TEAM_ID', 'TEAM_ABBREVIATION', 
                    'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'FGM', 'FGA', 
                    'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
                    'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 
                    'PTS_DIFF', 'GAME_ID_drop', 'TEAM_ID_drop', 'TEAM_NAME_drop', 
                    'TEAM_ABBREVIATION_drop', 'TEAM_CITY_drop', 'MIN_drop', 'E_OFF_RATING', 'OFF_RATING', 'E_DEF_RATING', 
                    'DEF_RATING', 'E_NET_RATING', 'NET_RATING', 'AST_PCT', 'AST_TOV', 
                    'AST_RATIO', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'E_TM_TOV_PCT', 
                    'TM_TOV_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'E_USG_PCT', 'E_PACE', 
                    'PACE', 'PACE_PER40', 'POSS', 'PIE', 'GAME_ID_drop', 'TEAM_ID_drop', 'TEAM_NAME_drop', 'TEAM_ABBREVIATION_drop', 'TEAM_CITY_drop',
                    'MIN_drop', 'PCT_FGA_2PT', 'PCT_FGA_3PT', 'PCT_PTS_2PT', 'PCT_PTS_2PT_MR',
                    'PCT_PTS_3PT', 'PCT_PTS_FB', 'PCT_PTS_FT', 'PCT_PTS_OFF_TOV',
                    'PCT_PTS_PAINT', 'PCT_AST_2PM', 'PCT_UAST_2PM', 'PCT_AST_3PM',
                    'PCT_UAST_3PM', 'PCT_AST_FGM', 'PCT_UAST_FGM']
        
        remove_column_lst = ['TEAM_ABBREVIATION_drop','TEAM_ID_drop', 'TEAM_CITY_drop', 'TEAM_NAME_drop', 'GAME_ID_drop', 'MIN_drop']

        tdf1 = player_index[player_index.columns[~player_index.columns.isin(remove_column_lst)]]
        
        return tdf1