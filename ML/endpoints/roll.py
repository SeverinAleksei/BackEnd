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
from ML.endpoints.files_downloader import get_products_and_spreads_names_not_async
root_for_data = 'C:\OSTC\Spreads_Data'
sys.path.insert(1, root_for_data)
from config_products import tick_price

router = APIRouter()

@router.post("/get_roll_data")
async def get_roll_data(dict_input: SimpleDict):
    print('Begin of get_roll_data')
    front_inp = dict_input.array
    print(front_inp)
    product = front_inp['data']['Product']
    last_n_days = front_inp['data']['Last_N_Days']
    result = f(product, last_n_days)
    print('End of get_roll_data')

    return result

def get_data_from_files(product_, spread_, last_n_days=2):
    path_ = f'{root_for_data}/Tick/{product_}/{spread_}'
    files = os.listdir(path_)
    result = pd.DataFrame()

    for file in files[-last_n_days:]:
        temp = pd.read_csv(f'{path_}/{file}')
        temp = temp.iloc[::-1]
        result = pd.concat([result, temp])

    result['Datetime'] = pd.to_datetime(result['Datetime'])
    result['Datetime'] += pd.Timedelta(8, "h")
    result.reset_index(drop=True, inplace=True)
    return result


def base_preprocessing_data(df):
    df_ = df.copy()

    chosen_columns = ['Datetime', 'Last Price', 'Last Size', 'Bid', 'Ask']
    df_ = df_[chosen_columns]
    df_['Side'] = ((df_['Last Price'] == df_['Bid']) + 2 * (df_['Last Price'] == df_['Ask'])).map(
        {0: 'Gap', 1: 'Sell', 2: 'Buy', 3: 'No Gap'})
    df_['Datetime'] = pd.to_datetime(df_['Datetime'])
    df_['Sided Size'] = (df_['Side'] == 'Buy') * df_['Last Size'] - (df_['Side'] == 'Sell') * df_['Last Size']
    # display(df_[df_['Side'] == 'Gap'])
    return df_[['Datetime', 'Last Price', 'Side', 'Last Size', 'Sided Size']]


# def roll_Vitaly_one_asset(df, start_settlement, end_settlement):
#
#     print('roll_Vitaly_one_asset')
#     df_ = df.copy()
#     df_['Time'] = df_['Datetime'].dt.time
#     df_['Day'] = df_['Datetime'].dt.date.apply(str)
#
#     result_one = None
#
#     result_one = df_.groupby(['Day']).sum()
#     output_dictionary = {}
#     output_dictionary['product'] = None
#     output_dictionary['asset'] = None
#     for res_ind in result_one.index:
#         output_dictionary[f'VT {res_ind}'] = int(result_one.loc[res_ind, 'Last Size'])
#
#     for res_ind in result_one.index:
#         output_dictionary[f'VD {res_ind}'] = int(result_one.loc[res_ind, 'Sided Size'])
#
#     for res_ind in result_one.index:
#         output_dictionary[f'PC {res_ind}'] = np.round(df_.loc[df_['Day'] == res_ind, 'Last Price'].values[-1] -
#                                                   df_.loc[df_['Day'] == res_ind, 'Last Price'].values[0],4)
#     return output_dictionary
def roll_Vitaly_one_asset(df, start_settlement, end_settlement):
    # print('Roll Vit', start_settlement, end_settlement)
    df_ = df.copy()
    df_['Time'] = df_['Datetime'].dt.time
    df_['Hour'] = df_['Datetime'].dt.hour
    df_['Day'] = df_['Datetime'].dt.date.apply(str)


    settlement_df = df_[
        (df_['Time'] > pd.to_datetime(start_settlement).time()) & (df_['Time'] < pd.to_datetime(end_settlement).time())]

    result_one = df_.groupby(['Day']).sum()
    output_dictionary = {}
    output_dictionary['product'] = None
    output_dictionary['asset'] = None

    output_dictionary[f'PC'] = float(np.round(df_.loc[:, 'Last Price'].values[-1] - df_.loc[:, 'Last Price'].values[0], 3))
    output_dictionary[f'TV'] = float(df_['Last Size'].sum())
    output_dictionary[f'DV'] = float(df_['Sided Size'].sum())
    if output_dictionary[f'TV']!= 0:
        output_dictionary[f'Ratio DV/TV'] = float(np.round(output_dictionary[f'DV'] / output_dictionary[f'TV'], 2))
    else:
        output_dictionary[f'Ratio DV/TV'] = 0
    division_step = 3

    hour_table = pd.DataFrame()
    hour_table.index = np.unique(df_['Day'])
    for i in range(0, 24, division_step):
        hour_table[f'{str(i).zfill(2)}/{min(24, i + division_step)}_O'] = 0
        hour_table[f'{str(i).zfill(2)}/{min(24, i + division_step)}_C'] = 0

    for day in hour_table.index:
        for hour in range(0, 24, division_step):
            dx = df_.loc[
                (df_['Hour'] >= hour) & (df_['Hour'] < hour + division_step) & (df_['Day'] == day), 'Last Price'].values

            if len(dx) == 0:
                hour_table.loc[day, f'{str(hour).zfill(2)}/{min(24, hour + division_step)}_O'] = 0
                hour_table.loc[day, f'{str(hour).zfill(2)}/{min(24, hour + division_step)}_C'] = 0
            else:
                hour_table.loc[day, f'{str(hour).zfill(2)}/{min(24, hour + division_step)}_O'] = dx[0]
                hour_table.loc[day, f'{str(hour).zfill(2)}/{min(24, hour + division_step)}_C'] = dx[-1]



    print(hour_table)
    for i in range(0, 24, division_step):
        output_dictionary[f'PC {str(i).zfill(2)}/{i + division_step}'] = float(np.round(
            (hour_table[f'{str(i).zfill(2)}/{i + division_step}_C'] - hour_table[f'{str(i).zfill(2)}/{i + division_step}_O']).sum(), 4))
    #     display(df_.loc[(df_['Hour'] >= i)&(df_['Hour'] < i+division_step),:])
    #     for res_ind in result_one.index:
    #         output_dictionary[f'{res_ind} Total Volume'] = int(result_one.loc[res_ind, 'Last Size'])

    #     for res_ind in result_one.index:
    #         output_dictionary[f'{res_ind} Signed Volume'] = int(result_one.loc[res_ind, 'Sided Size'])

    #     for res_ind in result_one.index:
    #         output_dictionary[f'{res_ind} PC'] = np.round(df_.loc[df_['Day'] == res_ind, 'Last Price'].values[-1] -
    #                                                       df_.loc[df_['Day'] == res_ind, 'Last Price'].values[0],3)
    return output_dictionary

def roll_Vitaly_all_spreads(product, spreads, last_n_days):
    print('roll_Vitaly_all_assets')
    result = []
    columns = []
    for spread in spreads:
        x = get_data_from_files(product, spread, last_n_days)
        x = base_preprocessing_data(x)
        res_dict = roll_Vitaly_one_asset(x, '19:28', '19:30')
        res_dict['asset'] = spread
        res_dict['product'] = product
        for key in res_dict.keys():
            columns.append(key)

        result.append(res_dict)

    print('After roll-one_asset')
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]
    columns = sorted(f7(columns))
    columns_dict = [{'field': 'product'}, {'field': 'asset'}]
    for c in columns:
        if (c != 'product') and (c != 'asset'):
            columns_dict.append({'field': c})

    print('Columns', columns_dict)
    print('Result', result)
    print(result)
    return {'columns_name': columns_dict, 'table_values': result}



def f(product, last_n_days):
    x = get_products_and_spreads_names_not_async()

    spreads = []
    for spread_a in x['Spreads'][product]:
        spreads.append(spread_a['Name'])
    result = roll_Vitaly_all_spreads(product, spreads, last_n_days)
    return result