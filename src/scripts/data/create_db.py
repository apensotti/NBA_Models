import sqlite3
from sqlite3 import Error

from nba_api.stats.static import players, teams
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import boxscoreadvancedv2
from nba_api.stats.endpoints import boxscorescoringv2
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.endpoints import playerindex

from IPython.core.display import clear_output

from tqdm import tqdm

import pandas as pd
import time 

from .transform_db import transform

def season_string(season):
        return str(season) + '-' + str(season+1)[-2:]

class db:
    def __init__(self, conn):
        self.conn = conn
        self.season_boxscores = []
        self.season_df = None
        self.players = []
        self.player_df = None

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
    
    def add_basic_boxscores(self, start_season, end_season, if_exists='replace'):
    
        table_name = 'team_basic_boxscores'

        if if_exists == 'replace':
            self.conn.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.execute('VACUUM')

        self.conn.execute("""CREATE TABLE IF NOT EXISTS {} (SEASON, TEAM_ID, TEAM_ABBREVIATION, 
            TEAM_NAME, GAME_ID, GAME_DATE DATE, MATCHUP, WL, MIN, FGM, FGA, 
            FG_PCT, FG3M, FG3A , FG3_PCT, FTM, FTA, FT_PCT, OREB,
            DREB, REB, AST, STL, BLK, TOV, PF, PTS, 
            PLUS_MINUS)""".format(table_name))    

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
    
    def add_advanced_boxscores(self, start_season, end_season, if_exists='replace'):
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

        self.conn.execute('''CREATE TABLE IF NOT EXISTS {} (GAME_ID, TEAM_ID, TEAM_NAME, 
            TEAM_ABBREVIATION, TEAM_CITY, MIN, E_OFF_RATING, OFF_RATING, E_DEF_RATING, 
            DEF_RATING, E_NET_RATING, NET_RATING, AST_PCT, AST_TOV, 
            AST_RATIO, OREB_PCT, DREB_PCT, REB_PCT, E_TM_TOV_PCT, 
            TM_TOV_PCT, EFG_PCT, TS_PCT, USG_PCT, E_USG_PCT, E_PACE, 
            PACE, PACE_PER40, POSS, PIE)'''.format(table_name))


        for season in range(start_season, end_season+1):
            season_str = season_string(season)
            season_team_boxscores = []

            for season_type in ['Regular Season', 'Playoffs']:
                logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
                game_ids = logs['GAME_ID'].unique()

                print('{} games {}'.format(season,len(game_ids)))
                for game_id in tqdm(game_ids, desc='progress'):
                    try:
                        team_boxscores = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id).get_data_frames()[1]                    
                        team_boxscores.to_sql(table_name, self.conn, if_exists='append', index=False)
                    except:
                        game_ids_not_added.append(game_id)
                clear_output(wait=True)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return None
    
    def add_scoring_boxscores(self, start_season, end_season, if_exists='replace'):
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

        conn.execute('''CREATE TABLE IF NOT EXISTS {} (GAME_ID, TEAM_ID, TEAM_NAME, TEAM_ABBREVIATION, TEAM_CITY,
           MIN, PCT_FGA_2PT, PCT_FGA_3PT, PCT_PTS_2PT, PCT_PTS_2PT_MR,
           PCT_PTS_3PT, PCT_PTS_FB, PCT_PTS_FT, PCT_PTS_OFF_TOV,
           PCT_PTS_PAINT, PCT_AST_2PM, PCT_UAST_2PM, PCT_AST_3PM,
           PCT_UAST_3PM, PCT_AST_FGM, PCT_UAST_FGM)'''.format(table_name))


        for season in range(start_season, end_season+1):
            season_str = season_string(season)
            season_team_boxscores = []

            for season_type in ['Regular Season', 'Playoffs']:
                logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
                game_ids = logs['GAME_ID'].unique()

                print('{} games {} in {}'.format(season_type ,len(game_ids), season))
                for game_id in tqdm(game_ids, desc='progress'):
                    try:
                        player_logs = boxscorescoringv2.BoxScoreScoringV2(game_id).get_data_frames()[1]
                        player_logs.to_sql(table_name, self.conn, if_exists='append', index=False)
                    except:
                        game_ids_not_added.append(game_id)
                    time.sleep(.5)
                clear_output(wait=True)

        cur = self.conn.cursor()
        cur.execute('DELETE FROM {} WHERE rowid NOT IN (SELECT min(rowid) FROM {} GROUP BY TEAM_ID, GAME_ID)'.format(table_name, table_name))
        self.conn.commit()

        return game_ids_not_added
    
    def add_player_game_logs(self, start_season, end_season, if_exists='replace'):
        table_name = 'player_game_logs'
        game_ids_not_added = []

        if if_exists == 'replace':
            self.conn.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.execute('VACUUM')

        conn.execute('''CREATE TABLE IF NOT EXISTS {} (SEASON_YEAR, PLAYER_ID, PLAYER_NAME, NICKNAME, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME,
            GAME_ID, GAME_DATE, MATCHUP, WL, MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM,FTA, FT_PCT, OREB,
            DREB, REB, AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, PLUS_MINUS, NBA_FANTASY_PTS, DD2, TD3, GP_RANK,
            W_RANK, L_RANK, W_PCT_RANK, MIN_RANK, FGM_RANK, FGA_RANK, FG_PCT_RANK, FG3M_RANK, FG3A_RANK, FG3_PCT_RANK,
            FTM_RANK, FTA_RANK, FT_PCT_RANK, OREB_RANK, DREB_RANK, REB_RANK, AST_RANK, TOV_RANK, STL_RANK, BLK_RANK,
            BLKA_RANK, PF_RANK, PFD_RANK, PTS_RANK, PLUS_MINUS_RANK, NBA_FANTASY_PTS_RANK, DD2_RANK, TD3_RANK, WNBA_FANTASY_PTS,WNBA_FANTASY_PTS_RANK,AVAILABLE_FLAG)'''.format(table_name))
        
        for season in range(start_season, end_season+1):
            season_str = season_string(season)
            season_team_boxscores = []

            scoring_boxscores = playergamelogs.PlayerGameLogs(season_nullable=season_str,league_id_nullable='00').get_data_frames()[0]
            scoring_boxscores.to_sql(table_name, self.conn, if_exists='append', index=False)
            
            time.sleep(.5)

        cur = self.conn.cursor()
        self.conn.commit()

        return game_ids_not_added
    
    def add_matchups(self, if_exists='replace'):
        table_name = 'boxscore'

        if if_exists == 'replace':
            self.conn.execute('DROP TABLE IF EXISTS ' + table_name)
            self.conn.execute('VACUUM')

        conn.execute('''CREATE TABLE IF NOT EXISTS {} (SEASON_home, TEAM_ID_home, TEAM_ABBREVIATION_home,
        TEAM_NAME_home, GAME_ID, GAME_DATE_home, MATCHUP_home,
        HOME_GAME_home, WL_home, FG2M_home, FG2A_home, FG3M_home, FG3A_home,
        FTM_home, FTA_home, OREB_home, DREB_home, REB_home,
        AST_home, STL_home, BLK_home, TOV_home, PF_home, PTS_home,
        PLUS_MINUS_home, E_OFF_RATING_home, OFF_RATING_home,
        E_DEF_RATING_home, DEF_RATING_home, E_NET_RATING_home,
        NET_RATING_home, POSS_home, PIE_home, PTS_2PT_MR_home,
        PTS_FB_home, PTS_OFF_TOV_home, PTS_PAINT_home, AST_2PM_home,AST_3PM_home, UAST_2PM_home, UAST_3PM_home, SEASON_away,
        TEAM_ID_away, TEAM_ABBREVIATION_away, TEAM_NAME_away, GAME_DATE_away, MATCHUP_away, HOME_GAME_away, WL_away, FG2M_away, FG2A_away,
        FG3M_away, FG3A_away, FTM_away, FTA_away, OREB_away, DREB_away, REB_away, AST_away, STL_away, BLK_away, TOV_away, PF_away, PTS_away, PLUS_MINUS_away, E_OFF_RATING_away, OFF_RATING_away,
        E_DEF_RATING_away, DEF_RATING_away, E_NET_RATING_away, NET_RATING_away, POSS_away, PIE_away, PTS_2PT_MR_away, PTS_FB_away,
        PTS_OFF_TOV_away, PTS_PAINT_away, AST_2PM_away, AST_3PM_away, UAST_2PM_away, UAST_3PM_away)'''.format(table_name))

        obj = transform(conn=conn,start_season=2013,end_season=2023)
        data = obj.load_team_data()
        cleaned = obj.clean_team_data(data)
        cleaned = cleaned.dropna(subset='PCT_PTS_2PT')
        convert_pct = obj.convert_pcts(cleaned)
        matchups = obj.create_matchups(convert_pct)
        df = matchups

        df = df[df['HOME_GAME_home'] == 1]

        df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        self.conn.commit
    
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

    

def update_all_data(conn, season, dates):
    """Combines all the update functions above into one function that updates all my data"""
    obj = db(conn=conn)
    print("updating basic team boxscores")
    obj.update_team_basic_boxscores(season=season)
    print("updating advanced team/player boxscores")
    obj.update_team_advanced_boxscores(season=season,dates=dates)
    print("updating scoring boxscores")
    obj.update_team_scoring_boxscores(season=season,dates=dates)
    
    
if __name__ == '__main__':
    conn = sqlite3.connect("C:\\Users\\alexp\\src\\NBA_Models\\sqlite\\db\\nba_data.db")
    obj = db(conn=conn)
    #obj.add_agg_boxscores()
    #obj.add_basic_boxscores(2013,2023)
    #obj.add_advanced_boxscores(2013,2023)
    #obj.add_player_game_logs(2013,2023,if_exists='replace')
    #update_all_data(conn=conn, season=2023,dates=[])
    #print(obj.season_df)