import sqlite3
from sqlite3 import Error

from nba_api.stats.static import players, teams
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import boxscoreadvancedv2
from nba_api.stats.endpoints import boxscorescoringv2

from IPython.core.display import clear_output

from tqdm import tqdm

import pandas as pd
import time 

def season_string(season):
        return str(season) + '-' + str(season+1)[-2:]

class db:
    def __init__(self, conn):
        self.conn = conn
        self.season_boxscores = []
        self.season_df = None

    def create_connection(self, db_file):
        """ create a database connection to a SQLite database """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except Error as e:
            print(e)
        finally:
            if self.conn:
                self.conn.close()
    
    def add_basic_boxscores(self, start_season, end_season, if_exists='append'):
    
        table_name = 'team_basic_boxscores'

        if if_exists == 'replace':
            self.conn.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.execute('VACUUM')

        self.conn.execute("""CREATE TABLE IF NOT EXISTS {} (SEASON TEXT, TEAM_ID INTEGER, TEAM_ABBREVIATION TEXT, 
            TEAM_NAME TEXT, GAME_ID TEXT, GAME_DATE DATE, MATCHUP TEXT, WL TEXT, MIN INTEGER, FGM INTEGER, FGA INTEGER, 
            FG_PCT FLOAT, FG3M INTEGER, FG3A  INTEGER, FG3_PCT FLOAT, FTM INTEGER, FTA INTEGER, FT_PCT FLOAT, OREB INTEGER,
            DREB INTEGER, REB INTEGER, AST INTEGER, STL INTEGER, BLK INTEGER, TOV INTEGER, PF INTEGER, PTS INTEGER, 
            PLUS_MINUS INTEGER)""".format(table_name))    

        for season in range(start_season, end_season+1):
            season_str = season_string(season)

            for season_type in ['Regular Season', 'Playoffs']:
                boxscores = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
                self.season_boxscores.append(boxscores)
                time.sleep(2)
            self.season_df = pd.concat(self.season_boxscores)
            self.season_df['SEASON'] = season_str
            self.season_df.drop(columns = ['SEASON_ID', 'VIDEO_AVAILABLE'], inplace=True)

            self.season_df.to_sql(table_name, self.conn, if_exists='append', index=False)

            time.sleep(3)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return None
    
    def add_advanced_boxscores(self, start_season, end_season, if_exists='append'):
        """
        This function pulls advanced team boxscores from the NBA_API package 
        and appends (or creates a new table if not exists) it to the table team_advanced_boxscores in the sqlite db

        Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
        If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
        """

        table_name = 'team_advanced_boxscores'
        game_ids_not_added = []

        if if_exists == 'replace':
            self.conn.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.execute('VACUUM')

        self.conn.execute('''CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, TEAM_ID INTEGER, TEAM_NAME TEXT, 
            TEAM_ABBREVIATION TEXT, TEAM_CITY TEXT, MIN TEXT, E_OFF_RATING FLOAT, OFF_RATING FLOAT, E_DEF_RATING FLOAT, 
            DEF_RATING FLOAT, E_NET_RATING FLOAT, NET_RATING FLOAT, AST_PCT FLOAT, AST_TOV FLOAT, 
            AST_RATIO FLOAT, OREB_PCT FLOAT, DREB_PCT FLOAT, REB_PCT FLOAT, E_TM_TOV_PCT FLOAT, 
            TM_TOV_PCT FLOAT, EFG_PCT FLOAT, TS_PCT FLOAT, USG_PCT FLOAT, E_USG_PCT FLOAT, E_PACE FLOAT, 
            PACE FLOAT, PACE_PER40 FLOAT, POSS FLOAT, PIE FLOAT)'''.format(table_name))


        for season in range(start_season, end_season+1):
            season_str = season_string(season)
            season_team_boxscores = []

            for season_type in ['Regular Season', 'Playoffs']:
                logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
                game_ids = logs['GAME_ID'].unique()

                print('games {}'.format(len(game_ids)))
                for game_id in tqdm(game_ids, desc='progress'):
                    try:
                        team_boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()[1]                    
                        team_boxscores.to_sql(table_name, self.conn, if_exists='append', index=False)
                    except:
                        game_ids_not_added.append(game_id)
                    time.sleep(1)
                clear_output(wait=True)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return None
    
    def add_scoring_boxscores(self, start_season, end_season, if_exists='append'):
        """
        This function pulls scoring team boxscores from the NBA_API package 
        and appends (or creates a new table if not exists) it to the table team_scoring_boxscores in the sqlite db.

        Note: Because of timeout errors and that we have to pull each game's individually, each season takes 1-2 hours.
        If some games were not pulled in certain seasons, you can use the update functions to gather those individual games.
        """

        table_name = 'team_scoring_boxscores'
        game_ids_not_added = []

        if if_exists == 'replace':
            self.conn.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.execute('VACUUM')

        conn.execute('''CREATE TABLE IF NOT EXISTS {} (GAME_ID TEXT, TEAM_ID INTEGER, TEAM_NAME TEXT, TEAM_ABBREVIATION TEXT, TEAM_CITY TEXT,
           MIN TEXT, PCT_FGA_2PT FLOAT, PCT_FGA_3PT FLOAT, PCT_PTS_2PT FLOAT, PCT_PTS_2PT_MR FLOAT,
           PCT_PTS_3PT FLOAT, PCT_PTS_FB FLOAT, PCT_PTS_FT FLOAT, PCT_PTS_OFF_TOV FLOAT,
           PCT_PTS_PAINT FLOAT, PCT_AST_2PM FLOAT, PCT_UAST_2PM FLOAT, PCT_AST_3PM FLOAT,
           PCT_UAST_3PM FLOAT, PCT_AST_FGM FLOAT, PCT_UAST_FGM FLOAT)'''.format(table_name))


        for season in range(start_season, end_season+1):
            season_str = season_string(season)
            season_team_boxscores = []

            for season_type in ['Regular Season', 'Playoffs']:
                logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
                game_ids = logs['GAME_ID'].unique()

                print('{} games {} in {}'.format(season_type ,len(game_ids), season))
                for game_id in tqdm(game_ids, desc='progress'):
                    try:
                        scoring_boxscores = boxscorescoringv2.BoxScoreScoringV2(game_id).get_data_frames()[1]
                        scoring_boxscores.to_sql(table_name, self.conn, if_exists='append', index=False)
                    except:
                        game_ids_not_added.append(game_id)
                    time.sleep(.5)
                clear_output(wait=True)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return game_ids_not_added
    
    def update_team_basic_boxscores(self, season):
        table_name = 'team_basic_boxscores'
        season_str = season_string(season)

        dfs = []
        for season_type in ['Regular Season', 'Playoffs']:
            team_gamelogs = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
            dfs.append(team_gamelogs)

        team_gamelogs_updated = pd.concat(dfs)
        team_gamelogs_updated['SEASON'] = season_str
        team_gamelogs_updated.drop(columns = ['SEASON_ID', 'VIDEO_AVAILABLE'], inplace=True)

        team_gamelogs_updated.to_sql(table_name, self.conn, if_exists='append', index=False)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return None
    
    def update_team_advanced_boxscores(self, season, dates):
        table_name = 'team_advanced_boxscores'

        season_str = season_string(season)

        game_ids_not_added = []

        # Pull the GAME_IDs from my data
        game_ids_in_db = pd.read_sql('''SELECT DISTINCT team_basic_boxscores.GAME_ID FROM team_basic_boxscores
                    INNER JOIN team_advanced_boxscores 
                    ON team_basic_boxscores.GAME_ID = team_advanced_boxscores.GAME_ID
                    AND team_basic_boxscores.TEAM_ID = team_advanced_boxscores.TEAM_ID
                    WHERE SEASON = "{}" '''.format(season_str), self.conn)

        game_ids_in_db = game_ids_in_db['GAME_ID'].tolist()

        missing_game_ids = []
        if len(dates) != 0:
            for date in dates:
                gamelogs = leaguegamelog.LeagueGameLog(
                    season=season_str, date_from_nullable=date, date_to_nullable=date).get_data_frames()[0]
                missing_game_ids.extend(gamelogs['GAME_ID'].unique())

        else:        
            # get up to date GAME_IDs
            to_date_game_ids = []
            for season_type in ['Regular Season', 'Playoffs']:
                to_date_gamelogs = leaguegamelog.LeagueGameLog(season=season_str, season_type_all_star=season_type).get_data_frames()[0]
                to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())

            # See which game_ids are missing
            missing_game_ids = set(to_date_game_ids) - set(game_ids_in_db)

        num_games_updated = len(missing_game_ids)
        print("num_games_updated:", num_games_updated)

        if num_games_updated == 0:
            print("All team advanced boxscores up to date in season {}".format(season_str))
            return None

        for game_id in tqdm(missing_game_ids, desc='progress'):
            try:
                boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()[1]
                boxscores.to_sql(table_name, self.conn, if_exists='append', index=False)
                time.sleep(1)
            except:
                game_ids_not_added.append(game_id)  

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return game_ids_not_added
    
    def update_team_scoring_boxscores(self, season, dates):
        table_name = 'team_scoring_boxscores'

        season_str = season_string(season)

        game_ids_not_added = []

        # Pull the GAME_IDs from my data
        game_ids_in_db = pd.read_sql(f'''SELECT DISTINCT team_scoring_boxscores.GAME_ID FROM team_basic_boxscores
                    INNER JOIN team_scoring_boxscores 
                    ON team_basic_boxscores.GAME_ID = team_scoring_boxscores.GAME_ID
                    AND team_basic_boxscores.TEAM_ID = team_scoring_boxscores.TEAM_ID
                    WHERE SEASON = "{season_str}" ''', self.conn)

        game_ids_in_db = game_ids_in_db['GAME_ID'].tolist()

        missing_game_ids = []
        if len(dates) != 0:
            for date in dates:
                gamelogs = leaguegamelog.LeagueGameLog(
                    season=season_str, date_from_nullable=date, date_to_nullable=date).get_data_frames()[0]
                missing_game_ids.extend(gamelogs['GAME_ID'].unique())

        else:
            # get up to date GAME_IDs
            to_date_game_ids = []
            for season_type in ['Regular Season', 'Playoffs']:
                to_date_gamelogs = leaguegamelog.LeagueGameLog(
                    season=season_str, season_type_all_star=season_type).get_data_frames()[0]
                to_date_game_ids.extend(to_date_gamelogs['GAME_ID'].unique())

            # See which game_ids are missing
            missing_game_ids = set(to_date_game_ids) - set(game_ids_in_db)

        num_games_updated = len(missing_game_ids)
        print("num_games_updated:", num_games_updated)

        if num_games_updated == 0:
            print("All team advanced boxscores up to date in season {}".format(season_str))
            return None

        for game_id in tqdm(missing_game_ids, desc='progress'):
            try:
                boxscores = boxscorescoringv2.BoxScoreScoringV2(
                    game_id).get_data_frames()[1]
                boxscores.to_sql(table_name, self.conn,
                                 if_exists='append', index=False)
                time.sleep(2)
            except:
                game_ids_not_added.append(game_id)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT max(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(
            table_name, table_name))
        self.conn.commit()

        return game_ids_not_added


def update_all_data(conn, season):
    """Combines all the update functions above into one function that updates all my data"""
    obj = db(conn=conn)
    print("updating basic team boxscores")
    obj.update_team_basic_boxscores(season=season)
    print("updating advanced team/player boxscores")
    obj.update_team_advanced_boxscores(season=season)
    print("updating scoring boxscores")
    obj.update_team_scoring_boxscores(season=season)
    
if __name__ == '__main__':
    conn = sqlite3.connect("C:\\Users\\alexp\\src\\NBA_Models\\sqlite\\db\\nba_data.db")
    obj = db(conn=conn)
    #obj.add_basic_boxscores(2013,2023)
    #obj.add_advanced_boxscores(2013,2023)
    obj.add_scoring_boxscores(2013,2023)
    print(obj.season_df)