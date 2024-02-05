import keras
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import InputLayer, LSTM, Dense
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras.losses import MeanSquaredError
from tensorflow.keras.metrics import RootMeanSquaredError, Accuracy
from tensorflow.keras.optimizers import Adam
from sklearn.model_selection import train_test_split
import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np
import re
import sqlite3
from scripts.DataETL.transform_db import load_clean
from scripts.DataETL.transform_db import transform

db_path = "/home/alexp/NBA_Models/sqlite/db/nba_data.db"

class ModelConfig:
    version_number = 1
    global_model_name = ''
    def __init__(self, X, y, train_test_val_split):
        self.version_str = ''
        self.X = X
        self.y = y
        self.train_test_val_split = train_test_val_split
        self.data_shape = self.X.shape

        self.model_path = None
        self.config_path = None

        self.epochs = None
        self.model = None

        self.X_train = None
        self.y_train = None
        self.X_val = None
        self.y_val = None
        self.X_test = None
        self.y_test = None

    def train_test_val(self):
        train_ratio = self.train_test_val_split[0]
        test_ratio = self.train_test_val_split[1]
        val_ratio = self.train_test_val_split[2]

        self.X_train, X_test_temp, self.y_train, y_test_temp = train_test_split(self.X, self.y, test_size=1 - train_ratio)
        self.X_val, self.X_test, self.y_val, self.y_test = train_test_split(X_test_temp, y_test_temp, test_size=test_ratio/(test_ratio + val_ratio))

        return self.X_train,self.y_train,self.X_test,self.y_test,self.X_val,self.y_val

    def create_sequential(self):
        self.model = Sequential()

        input_shape = self.data_shape[1:]

        self.model.add(InputLayer(input_shape))

        return self.model
    
    def version(self,model_name):
        if ModelConfig.global_model_name != model_name:
            ModelConfig.version_number = 1
            ModelConfig.global_model_name = model_name
            self.version_str = f'{ModelConfig.global_model_name}v{ModelConfig.version_number}'
        else:
            ModelConfig.version_number += 1
            ModelConfig.global_model_name = model_name
            self.version_str = f'{ModelConfig.global_model_name}v{ModelConfig.version_number}'

        return self.version_str

    def create_config(self,model_name,config_path):
        self.config_path = config_path
        self.version_str = self.version(model_name)
        path_string = config_path+self.version_str+'.json'

        layers = []
        optimizer = self.model.optimizer.get_config()
        loss = self.model.loss.name
        metrics = [metric.name for metric in self.model.metrics]

        optimizer['epsilon'] = ("%.15f" % optimizer['epsilon']).rstrip('0')
        optimizer['learning_rate'] = ("%.15f" % optimizer['learning_rate']).rstrip('0')

        for layer in self.model.layers:
            layers.append(layer.get_config())

        config = {
            
            "version_name":self.version_str,
            "version":self.version_number,
            "build_info":tf.sysconfig.get_build_info(),
            "input_shape":self.data_shape[1:],
            "output_classes":layers[-1]['units'],
            "layers":layers,
            "compile":{
                "loss":loss,
                "optimizer":optimizer,
                "metrics":metrics
            }
            }
        
        with open(path_string, 'w') as f:
            json.dump(config, f, indent=4)

        return f"Config Created: {self.config_path}{self.version_str}.json"

    
    def create_model_from_config(self,config):
        pass

class InputData:
    def __init__(self,df):
        self.conn = sqlite3.connect(db_path)
        self.bas_boxscores = load_clean.basic_boxscores()
        self.adv_boxscores = load_clean.basic_boxscores()
        self.scr_boxscores = load_clean.basic_boxscores()
        self.player_boxscores = load_clean.basic_boxscores()
        self.basic_boxscores = load_clean.basic_boxscores()
        self.basic_boxscores = load_clean.basic_boxscores()

    def raw_data_df

    def clean_df(self):
        # raw data
        load = load_clean(db_path)
        agg_boxscores = load.agg_boxscores_raw()
        players = load.players()

        #merged data
        conn = sqlite3.connect(db_path)

        clean = transform(conn,2013,2023)
        game_data = clean.load_team_data()
        game_data = clean.clean_team_data(game_data)
        game_data = game_data.dropna(subset='PCT_PTS_2PT')
        game_data = clean.convert_pcts(game_data)

        game_data['TEAM_ID'] = game_data['TEAM_ID'].astype('string')
        game_data['GAME_ID'] = game_data['GAME_ID'].astype('string')

        transformed_players = players.groupby(["TEAM_ID",'GAME_ID']).mean(numeric_only=True).reset_index()

        transformed_players['TEAM_ID'] = transformed_players['TEAM_ID'].astype('string')
        transformed_players['GAME_ID'] = transformed_players['GAME_ID'].astype('string')

        transformed_players = transformed_players.drop(columns=['WNBA_FANTASY_PTS_RANK','AVAILABLE_FLAG','WNBA_FANTASY_PTS'])

        merged_data = pd.merge(left=game_data,right=transformed_players,on=['GAME_ID','TEAM_ID'],suffixes=['_game','_players'])
        merged_data = merged_data.drop(columns=['TEAM_ID','GAME_ID','TEAM_NAME','GAME_DATE','MATCHUP','TD3','TD3_RANK'])
        