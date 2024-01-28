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
    
    def boxscores(self):
        self.cur.execute("SELECT * FROM boxscores")
        rows3 = self.cur.fetchall()

        boxscores_list = []

        columns = ['SEASON_team', 'TEAM_ID_team', 'TEAM_ABBREVIATION_team',
                'TEAM_NAME_team', 'GAME_ID', 'GAME_DATE_team', 'MATCHUP_team',
                'HOME_GAME_team', 'TEAM_SCORE_team', 'POINT_DIFF_team', 'WL_team',
                'RECORD_team', 'FG2M_team', 'FG2A_team', 'FG3M_team', 'FG3A_team',
                'FTM_team', 'FTA_team', 'OREB_team', 'DREB_team', 'REB_team',
                'AST_team', 'STL_team', 'BLK_team', 'TOV_team', 'PF_team', 'PTS_team',
                'PLUS_MINUS_team', 'E_OFF_RATING_team', 'OFF_RATING_team',
                'E_DEF_RATING_team', 'DEF_RATING_team', 'E_NET_RATING_team',
                'NET_RATING_team', 'POSS_team', 'PIE_team', 'PTS_2PT_MR_team',
                'PTS_FB_team', 'PTS_OFF_TOV_team', 'PTS_PAINT_team', 'AST_2PM_team','AST_3PM_team', 'UAST_2PM_team', 'UAST_3PM_team', 'SEASON_opp',
                'TEAM_ID_opp', 'TEAM_ABBREVIATION_opp', 'TEAM_NAME_opp', 'GAME_DATE_opp', 'MATCHUP_opp', 'HOME_GAME_opp', 
                'TEAM_SCORE_opp', 'POINT_DIFF_opp', 'WL_opp', 'RECORD_opp', 'FG2M_opp', 'FG2A_opp',
                'FG3M_opp', 'FG3A_opp', 'FTM_opp', 'FTA_opp', 'OREB_opp', 'DREB_opp', 'REB_opp', 'AST_opp', 'STL_opp', 'BLK_opp', 'TOV_opp', 'PF_opp', 'PTS_opp', 'PLUS_MINUS_opp', 'E_OFF_RATING_opp', 'OFF_RATING_opp',
                'E_DEF_RATING_opp', 'DEF_RATING_opp', 'E_NET_RATING_opp', 'NET_RATING_opp', 'POSS_opp', 'PIE_opp', 'PTS_2PT_MR_opp', 'PTS_FB_opp',
                'PTS_OFF_TOV_opp', 'PTS_PAINT_opp', 'AST_2PM_opp', 'AST_3PM_opp', 'UAST_2PM_opp', 'UAST_3PM_opp']

        for row in rows3:
            boxscores_list.append(row)

        boxscores = pd.DataFrame(data=boxscores_list)

        boxscores.columns = columns

        return pd.DataFrame(boxscores)

    def players(self):
        import numpy as np

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