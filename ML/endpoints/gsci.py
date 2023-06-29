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
from ML.models.action import SimpleArray, SimpleDict, SimpleAny
import sys
import os
import pandas as pd
import datetime as dt
import numpy as np
from ML.endpoints.files_downloader import get_products_and_spreads_names_not_async

root_for_data = 'C:\OSTC\Spreads_Data\Day'
sys.path.insert(1, root_for_data)

import scipy.stats as st

basic_roll_shift_dict = {'CORN': 0,
                         'SOYBEANS': 0,
                         'SOYBEAN MEAL': 0, #MAYBE
                         'SOYBEAN OIL': 0, #MAYBE
                         'COFFEE': 0,
                         'SUGAR': 1,
                         'COCOA': 0,
                         'COTTON': 0,
                         'LEAN HOGS': 0,
                         'LIVE CATTLE': 0,
                         'FEEDER CATTLE': 0,
                         'WTI CRUDE OIL': 1,
                         'HEATING OIL': 1,
                         'RBOB GASOLINE': 1,
                         'BRENT CRUDE OIL': 2,
                         'NATURAL GAS': 1,
                         'COPPER': 0,
                         'VIX': 0
                         }

router = APIRouter()


@router.post("/get_gsci")
async def get_gsci(dict_input: SimpleDict):
    print('Begin of get_gsci')
    print(dict_input)
    front_inp = dict_input.array
    product = front_inp['data']['Product']
    front = front_inp['data']['Front']
    business_day = front_inp['data']['Business_day']
    last_n_days = front_inp['data']['Last_N_Days']
    print(product, front, business_day, last_n_days)
    result = {}
    if product != "":
        result = preparing_for_return(product, front, business_day, last_n_days)
    print('End of get_gsci')
    return result


@router.get("/get_gsci_2")
async def get_gsci_2():
    print('Begin of get_gsci')
    front_inp = {'data': {'Product': 'BRENT CRUDE OIL', 'Front': 6, 'Business_day': 9, 'Last_N_Days': 15}}
    product = front_inp['data']['Product']
    front = front_inp['data']['Front']
    business_day = front_inp['data']['Business_day']
    last_n_days = front_inp['data']['Last_N_Days']
    print(product, front, business_day, last_n_days)
    result = {}
    if product != "":
        result = preparing_for_return(product, front, business_day, last_n_days)
    print('End of get_gsci')
    return result

def one_spread_roll_for_month(product_, spreads_name_, roll_shift_, b_day, n_last_days_, roll_month_res):
    x = pd.read_csv(f'{root_for_data}\\{product_}\\{spreads_name_}')
    spread_month = int(spreads_name_.split('-')[0][-2:])
    spread_year = int(spreads_name_.split('-')[0][-4:-2])

    if spread_month < roll_month_res + roll_shift_:
        spread_year -= 1

    x['date'] = pd.to_datetime(x['date'])
    x['day_of_the_week'] = x['date'].dt.dayofweek
    x = x[x['day_of_the_week'] <= 4]
    x.reset_index(drop=True, inplace=True)
    x['day_of_month'] = x['date'].dt.day
    x['month'] = x['date'].dt.month
    x['year'] = x['date'].dt.year
    x['bus_day'] = None
    last_day = 0
    bus_day = 0
    for i, row in x.iterrows():
        if row['day_of_month'] > last_day:
            bus_day += 1
            last_day = row['day_of_month']
            x.loc[i, 'bus_day'] = bus_day
        else:
            bus_day = 1
            last_day = row['day_of_month']
            x.loc[i, 'bus_day'] = bus_day

    last_roll_day = x[(x['month'] == roll_month_res) & (x['year'] == spread_year + 2000) & (x['bus_day'] == b_day)]
    x_15 = None
    try:
        x_15 = x.loc[int(last_roll_day.index[0]) - n_last_days_ + 1:int(last_roll_day.index[0]), ]

    except:
        x_15 = None
        print(f'{spreads_name_} has no enough data in function')

    return x_15


def gsci_for_product(product_, front, bus_day, n_last_days):
    listed_spreads = os.listdir(f'{root_for_data}\{product_}')
    basic_roll_shift = basic_roll_shift_dict[product_]
    results = pd.DataFrame()
    results.index = listed_spreads
    for i in np.arange(-n_last_days + 5, 5, 1):
        results[i] = None
    results = results.astype(float)

    print(f'Roll shift: {basic_roll_shift}, n_last_days: {n_last_days}, bus_day: {bus_day}')
    for spread_name in listed_spreads:
        try:
            df = one_spread_roll_for_month(product_, spread_name, basic_roll_shift, bus_day, n_last_days, front)
            if df is not None:
                if len(df) == n_last_days:
                    results.loc[spread_name:spread_name, :] = df['close_p'].values
                else:
                    print(f'{spread_name} has no enough data less than n_last_days')
            else:
                print(f'{spread_name} has no enough data is None')
        except:
            print(f'Exception with spread {spread_name}')
            break

    results['date'] = results.index
    results['date'] = results['date'].str.split("-", expand=True)[0].str.slice(start=-4)
    results['month'] = results['date'].str.slice(start=-2)
    results = results.dropna()

    return results


def profit_calculation(df):
    df_ = df.copy()
    #(df_[0] + df_[1] + df_[2] + df_[3] + df_[4])
    print(df_)
    df_['price_change'] = (df_.iloc[:, -3] - df_.iloc[:, 0])
    return df_['price_change']


def t_confidence_low_bound(data, bound_):
    return float(st.t.interval(alpha=bound_ / 100, df=len(data) - 1, loc=np.mean(data), scale=st.sem(data))[0])


def t_confidence_up_bound(data, bound_):
    return float(st.t.interval(alpha=bound_ / 100, df=len(data) - 1, loc=np.mean(data), scale=st.sem(data))[1])


def calculate_statistics(df):
    bound = 80
    df_ = df.copy()
    df_['price_change'] = profit_calculation(df_)
    functions = {'median': np.median,
                 'mean': np.mean,
                 'std': np.std,
                 'max_fall_in_price': min,
                 'max_growth_in_price': max,
                 'count': len}
    complex_functions = {
        f'low_{bound}%_bound': t_confidence_low_bound,
        f'up_{bound}%_bound': t_confidence_up_bound,
    }
    month_statistics = pd.DataFrame()
    # for col in functions:
    #     month_statistics[col] = None
    for un_m in df_['month']:
        month_statistics.loc[un_m, 'month'] = un_m

    for f in functions:
        for un_m in df_['month']:
            month_statistics.loc[un_m, f] = np.round(
                float(functions[f](df_.loc[df_['month'] == un_m, 'price_change'].values)),
                5)
    for f in complex_functions:
        for un_m in df_['month']:
            month_statistics.loc[un_m, f] = np.round(
                complex_functions[f](df_.loc[df_['month'] == un_m, 'price_change'].values, bound),
                5)
    month_statistics['significant'] = 1 * ((month_statistics[f'low_{bound}%_bound'] * month_statistics[
        f'up_{bound}%_bound']) > 0)

    print(month_statistics.dtypes)

    return month_statistics


def plot_month(n_month, df):
    print(n_month)
    df_ = df[df['month'] == str(n_month).zfill(2)].copy()
    df_.drop(['date', 'month'], inplace=True, axis=1)
    df_.transpose().iplot(kind='scatter', title=f'{n_month} month spread before rolls')


def preparing_for_return(product, front, bus_day, n_last_days):
    x = gsci_for_product(product, front, bus_day, n_last_days)
    output = {}
    dx = calculate_statistics(x)
    output['columns'] = [{'field': col} for col in list(dx.columns)]
    output['statistics'] = []
    for i, row in dx.iterrows():
        d = {}
        for col in dx.columns:
            d[col] = row[col]

        d['charts'] = []
        d['charts_diff'] = []
        month_charts = x[x['month'] == d['month']]
        for j, rowj in month_charts.iterrows():
            d['charts'].append({'name': j, 'data': [float(np.round(m, 6)) for m in list(rowj.values[:-2])]})
            d['charts_diff'].append(
                {'name': j, 'data': [float(np.round(m - rowj.values[0], 6)) for m in list(rowj.values[:-2])]})
        output['statistics'].append(d)
    return output
