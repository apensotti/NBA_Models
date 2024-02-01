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
import numpy as np
import re
import sqlite3

version_number = 1

class ModelConfig:
    def __init__(self, X, y, train_test_val_split):
        self.X = X
        self.y = y
        self.train_test_val_split = train_test_val_split
        self.data_shape = self.X.shape
        self.epochs = None
        self.model_path = None

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


    def create_sequential(self):
        self.model = Sequential()

        input_shape = self.data_shape[1:]

        self.model.add(InputLayer(input_shape))

        return self.model
    
    def save_version():
        version_str = f'v'

    def compile(self):
        cp = ModelCheckpoint(self.model_path, save_best_only=True)
        self.model.compile(loss=MeanSquaredError(), optimizer=Adam(learning_rate=.00001), metrics=[RootMeanSquaredError()])

    def create_config(self):
        pass

    def load_config(self):
        pass
