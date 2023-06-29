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

@router.post("/get_day_seasonal")
async def get_spread_time_period_data(dict_input: SimpleDict):
    print('Begin of get_spread_time_period_data')
    front_inp = dict_input.array
    product = front_inp['First']['Product']
    spread = front_inp['First']['Spread']
    timeframe = front_inp['First']['Timeframe']
    last_n_days = front_inp['First']['Last_N_Days']
    difference = front_inp['First']['Diff']

    data = get_data_from_files(product, spread, last_n_days)
    data = base_preprocessing_data(data)
    data = n_minutes_frames(data, timeframe)
    result = day_seasonality(data, difference)
    print('End of get_spread_time_period_data')
    # print(result)

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

def candlestick_combining(df, timeframe, time_anchor= '2023-04-21'):
    print('Begin of candlestick_combining')
    current_group = 0
    current_time = pd.to_datetime(time_anchor)
    number_of_groups = round(
        (pd.Timestamp.today() + dt.timedelta(days=1) - current_time) / dt.timedelta(minutes=timeframe) + 1)

    df_ = df.copy()
    df_['Datetime'] = pd.to_datetime(df_['Datetime'])
    df_.sort_values(by='Datetime', inplace=True)

    group_table = pd.DataFrame()
    group_table['Datetime'] = [current_time + dt.timedelta(minutes=timeframe * i) for i in range(number_of_groups)]
    group_table['Timegroup'] = [i for i in range(number_of_groups)]
    table_processed = pd.merge_asof(df_, group_table, on='Datetime', direction='forward')
    table_processed = pd.merge(table_processed, group_table, on="Timegroup")
    table_processed["Datetime"] = table_processed["Datetime_y"]
    table_processed.drop(columns=['Datetime_x', 'Datetime_y'], inplace=True)

    table_processed['Buy Last Size'] = (table_processed['Side'] == 'Buy') * table_processed['Last Size']
    table_processed['Sell Last Size'] = (table_processed['Side'] == 'Sell') * table_processed['Last Size']
    table_processed['Neutral Last Size'] = ((table_processed['Side'] == 'Gap') + (
                table_processed['Side'] == 'No Gap')) * table_processed['Last Size']

    def table_group(df):
        grouped_table = pd.DataFrame()
        grouped_table['Timegroup'] = np.unique(df['Timegroup'])
        grouped_table['Datetime'] = np.unique(df['Datetime'])

        grouped_table['Low'] = df.groupby(by="Timegroup", dropna=False).min()['Last Price'].values
        grouped_table['High'] = df.groupby(by="Timegroup", dropna=False).max()['Last Price'].values
        grouped_table['Open'] = df.groupby(by="Timegroup", dropna=False).first()['Last Price'].values
        grouped_table['Close'] = df.groupby(by="Timegroup", dropna=False).last()['Last Price'].values
        grouped_table['Volume'] = df.groupby(by="Timegroup", dropna=False).sum()['Last Size'].values
        grouped_table['Buy Volume'] = df.groupby(by="Timegroup", dropna=False).sum()['Buy Last Size'].values
        grouped_table['Sell Volume'] = df.groupby(by="Timegroup", dropna=False).sum()['Sell Last Size'].values
        grouped_table['Neutral Volume'] = df.groupby(by="Timegroup", dropna=False).sum()['Neutral Last Size'].values
        grouped_table.set_index('Datetime', inplace=True)

        return grouped_table

    result = table_group(table_processed)
    print('End of candlestick_combining')
    return result


def n_minutes_frames(df, timeframe):
    df_ = df.copy()
    df_.sort_values(by='Datetime', inplace=True)
    result = pd.DataFrame()

    time_anchor = df_.loc[0, 'Datetime']
    current_group = 0
    # current_time = pd.to_datetime(str(time_anchor).split(' ')[0])
    current_time = pd.to_datetime(pd.to_datetime(time_anchor).date())
    number_of_groups = round(
        (pd.Timestamp.today() + dt.timedelta(days=1) - current_time) / dt.timedelta(minutes=timeframe) + 1)
    result['Datetime'] = [current_time + dt.timedelta(minutes=timeframe * i) for i in range(number_of_groups)]
    result['Slices'] = result['Datetime']
    result = pd.merge_asof(df_, result, on='Datetime', direction='forward')
    result.drop_duplicates(subset=['Slices'], keep='last', inplace=True)
    result['Datetime'] = result['Slices']
    result['Close'] = result['Last Price']
    result.set_index('Datetime', inplace=True)
    return result[['Close']]


def day_seasonality(df, diff=False):
    df_ = df.copy()

    df_['Day'] = df_.index.date
    df_['Time'] = df_.index.time
    result = []
    set_hour = sorted(np.unique(df_['Time']))
    unique_days = sorted(np.unique(df_['Day']))
    day_seasonality_table = pd.DataFrame()
    day_seasonality_table.index = set_hour
    for u_d in unique_days:
        day_seasonality_table[u_d] = None

    for i, row in df_.iterrows():
        day_seasonality_table.loc[row['Time'], row['Day']] = row['Close']

    if diff:
        for day in unique_days:
            first_not_na_price = day_seasonality_table[day].loc[~day_seasonality_table[day].isnull()].iloc[0]
            day_seasonality_table[day].loc[~day_seasonality_table[day].isnull()] -= first_not_na_price
            day_seasonality_table[day] = day_seasonality_table[day].astype(float).round(4)

    # day_seasonality_table['Mean'] = day_seasonality_table.mean(axis = 1)
    # unique_days.append('Mean')
    # day_seasonality_table = day_seasonality_table * 100
    day_seasonality_table = day_seasonality_table.astype(object).replace(np.nan, None)
    for day in unique_days:
        dict_day = {}
        dict_day['name'] = day
        dict_day['data'] = list(zip(day_seasonality_table.index, day_seasonality_table[day]))

        dict_day['data'] = [{'x': i[0], 'y': i[1]} for i in dict_day['data']]
        result.append(dict_day)

    return result