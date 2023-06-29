import json
from random import random

from fastapi import APIRouter, Depends, Response
from fastapi.websockets import WebSocket
from depends import get_action_repository
from ML.repositories.actions import ActionRepository
from ML.models.action import Action, Chosen_Asset
import os
import zipfile
import io
from config import DATA_STORAGE_PATH
from ML.models.action import DownloadDataSelected
from os.path import basename
from authorization.core.security import JWTBearer

from statsmodels.regression.linear_model import OLS
from statsmodels.tsa.stattools import adfuller
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta
import pandas as pd
import sys
from ML.models.action import SimpleArray,SimpleDict,SimpleAny
import sys
import os
import pandas as pd
import datetime as dt
import numpy as np
root_for_data = 'C:\OSTC\Spreads_Data'
sys.path.insert(1, root_for_data)
from config_products import tick_price

router = APIRouter()

@router.post("/get_settlement")
async def get_settlement(dict_input: SimpleDict):
    print('Begin of get_spread_time_period_data')
    front_inp = dict_input.array
    product = front_inp['First']['Product']
    spread = front_inp['First']['Spread']
    last_n_days = front_inp['First']['Last_N_Days']

    data = get_data_from_files(product, spread, last_n_days)
    data = base_preprocessing_data(data)
    result = settlement_analysis(data, '14:00:29.182', '14:05:29.182')
    print('End of get_spread_time_period_data')
    print(result)

    return result


def get_data_from_files(product_, spread_, last_n_days=100000):
    print('Begin of get_data_from_files')
    path_ = f'{root_for_data}/Tick/{product_}/{spread_}'
    files = os.listdir(path_)
    result = pd.DataFrame()

    for file in files[-last_n_days:]:
        temp = pd.read_csv(f'{path_}/{file}')
        temp = temp.iloc[::-1]
        result = pd.concat([result, temp])

    result.reset_index(drop=True, inplace=True)
    print('End of get_data_from_files')
    return result


def base_preprocessing_data(df):
    print('Begin of base_preprocessing_data')
    df_ = df.copy()

    chosen_columns = ['Datetime', 'Last Price', 'Last Size', 'Bid', 'Ask']
    df_ = df_[chosen_columns]
    df_['Side'] = ((df_['Last Price'] == df_['Bid']) + 2 * (df_['Last Price'] == df_['Ask'])).map(
        {0: 'Gap', 1: 'Sell', 2: 'Buy', 3: 'No Gap'})
    df_['Datetime'] = pd.to_datetime(df_['Datetime'])
    print('End of base_preprocessing_data')
    return df_[['Datetime', 'Last Price', 'Side', 'Last Size']]


def settlement_analysis(df, settlement_begin, settlement_end, minute_window=5):
    df_ = df.copy()

    df_['Time'] = pd.to_datetime(df_['Datetime']).dt.time
    start_diap = (pd.to_datetime(settlement_begin) - pd.Timedelta(minutes=minute_window)).time()
    end_diap = (pd.to_datetime(settlement_end) + pd.Timedelta(minutes=minute_window)).time()
    reduced_df = df_[(df_['Time'] >= start_diap) & (df_['Time'] <= end_diap)]
    reduced_df['TimeUnix'] = reduced_df['Datetime'].values.astype(np.int64) // 10 ** 9

    result = []
    for side in ['Buy', 'Sell']:

        dict_day = {}
        dict_day['data'] = list(zip(reduced_df[reduced_df['Side'] == side]['TimeUnix'],
                                    reduced_df[reduced_df['Side'] == side]['Last Price'],
                                    reduced_df[reduced_df['Side'] == side]['Last Size']))
        dict_day['data'] = [{'x': i[0], 'y': i[1]} for i in dict_day['data']]
        result.append(dict_day)

    print(result)
    return result
