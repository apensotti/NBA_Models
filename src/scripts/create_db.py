import sqlite3
from sqlite3 import Error

from nba_api.stats.static import players, teams
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import boxscoreadvancedv2
from nba_api.stats.endpoints import boxscorescoringv2

import pandas as pd
import time 

def season_string(season):
        return str(season) + '-' + str(season+1)[-2:]

class db:
    def __init__(self, conn):
        self.conn = conn
        self.season_boxscores = []
        self.season_df = None

    def create_connection(self,db_file):
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

if __name__ == '__main__':
    conn = sqlite3.connect("C:\\Users\\alexp\\src\\NBA_Models\\sqlite\\db\\nba_data.db")
    obj = db(conn=conn)
    obj.add_basic_boxscores(2013,2023)
    print(obj.season_df)