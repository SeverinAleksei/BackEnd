import json
from random import random
from tqdm import tqdm
from fastapi import APIRouter, Depends, Response

from ML.models.action import SimpleArray, SimpleDict, SimpleAny
import sys
import os
import pandas as pd

import numpy as np
import random

config_data = 'C:\OSTC\Spreads_Data'
os.listdir(config_data)
sys.path.insert(1, config_data)
from config_products import letters, tick_price, product_future_letters_map, \
    product_abb_to_normal, product_normal_to_abb,futures_number_to_letter_dict, \
    futures_letter_to_number_dict, products_spreads_dict

root_for_data = 'C:\OSTC\Spreads_Data\Hour'
sys.path.insert(1, root_for_data)



router = APIRouter()


def denote_download_spreads(input_spreads):
    download_spreads = []
    for product in input_spreads:
        full_name_of_product = product['Product']
        abb_product = product_normal_to_abb[full_name_of_product]
        for spread in product['Month']:
            for year in range(10, 25):
                first_part_year_addition = int(spread.split('-')[0][0])
                second_part_year_addition = int(spread.split('-')[1][0])
                first_part_month = spread.split('-')[0][1:]
                second_part_month = spread.split('-')[1][1:]
                full_name = f'{abb_product}{year + first_part_year_addition}{first_part_month}-{abb_product}{year + second_part_year_addition}{second_part_month}.csv'
                full_path = f'{full_name_of_product}/{full_name}'
                download_spreads.append(full_path)
    return download_spreads


def obtain_general_table(download_spreads):
    df = pd.DataFrame()
    df.index = [pd.to_datetime('2013-03-12') + pd.Timedelta(i, unit='H') for i in range(100000)]

    for download_spread in download_spreads:
        try:
            product = download_spread.split('/')[0]
            dx = pd.read_csv(f'{root_for_data}/{download_spread}')
            if len(dx) != 0:
                dx['date'] = pd.to_datetime(dx['date'])
                dx['time'] = pd.to_timedelta(dx['time'])
                dx['full_time'] = dx['date'] + dx['time']
                dx = dx.set_index(dx['full_time'])
                dx = dx[['close_p']] / tick_price[product]['tick_size'] * tick_price[product]['tick_price']
                dx.columns = [download_spread.split('/')[1]]
                last_date = dx.iloc[[-1], :].index.values[0]
                one_year_long = last_date - pd.Timedelta(365, unit='D')
                dx = dx.loc[one_year_long:, :]

                df = df.join(dx)
        except:
            pass

    df.dropna(how='all', inplace=True)
    df['hour'] = df.index.hour
    df['day'] = df.index.day
    df['month'] = df.index.month
    df['year'] = df.index.year

    df['month_day_hour'] = df['month'].astype(str).str.zfill(2) + '-' + df['day'].astype(str).str.zfill(2) + '-' + df[
        'hour'].astype(str).str.zfill(2)
    return df


def function_for_name(tr_name, year):
    name = tr_name.split('.')[0]
    first_part_name = name.split('-')[0]
    second_part_name = name.split('-')[1]

    first_part_name = first_part_name[:-4] + str(int(first_part_name[-4:-2]) + 2000 - year) + first_part_name[-2:]
    second_part_name = second_part_name[:-4] + str(int(second_part_name[-4:-2]) + 2000 - year) + second_part_name[-2:]

    result = f'{first_part_name}-{second_part_name}'
    return result


def obtain_tables_by_years(general_table, dates):
    start = f'{str(pd.to_datetime(dates["0"]).month).zfill(2)}-{str(pd.to_datetime(dates["0"]).day).zfill(2)}-00'

    end = f'{str(pd.to_datetime(dates["1"]).month).zfill(2)}-{str(pd.to_datetime(dates["1"]).day).zfill(2)}-23'

    df_diff_years = {}
    dr = general_table.copy()
    dr = dr[(dr['month_day_hour'] >= start) & (dr['month_day_hour'] <= end)]
    for year in tqdm(sorted(set(dr['year']))[1:]):

        d_one_year = dr[dr['year'] == year]
        d_one_year.dropna(axis=1, how='all', inplace=True)
        d_one_year.fillna(method='ffill', inplace=True)
        d_one_year.drop(columns=['month_day_hour', 'hour', 'day', 'month', 'year'], inplace=True)
        d_one_year.fillna(method='bfill', inplace=True)
        # d_one_year.dropna(axis = 0,inplace = True)
        new_columns_names = []
        for col in d_one_year.columns:
            new_columns_names.append(function_for_name(col, year))
        d_one_year.columns = new_columns_names
        df_diff_years[year] = d_one_year
    return df_diff_years


def check_intersection_columns(df_diff_years):
    intersection_of_columns_by_years = set()
    for df_year in df_diff_years.values():
        columns_names = set(df_year.columns)
        if len(intersection_of_columns_by_years) == 0:
            intersection_of_columns_by_years = columns_names
        else:
            intersection_of_columns_by_years = set(intersection_of_columns_by_years).intersection(columns_names)

    needed_columns = sorted(list(intersection_of_columns_by_years))
    return needed_columns


def trend_estimator(price):
    return {'max': max(price), 'min': min(price), 'std': np.std(price), 'price': price}


def aggregate_function(combinations_):
    diff_years_trend = []
    for year_combination in combinations_[:-1]:
        diff_years_trend.append(trend_estimator(year_combination))

    max_min_range = sum([i['max'] - i['min'] for i in diff_years_trend])
    std_range = np.mean([i['std'] for i in diff_years_trend])
    max_bound_vol = np.std([i['max'] for i in diff_years_trend])
    min_bound_vol = np.std([i['min'] for i in diff_years_trend])
    change = np.mean([i['price'][-1] - i['price'][0] for i in diff_years_trend])
    change_std = np.mean([np.std(i['price']) for i in diff_years_trend])

    max_bound_max = np.max([i['max'] for i in diff_years_trend])
    min_bound_min = np.min([i['min'] for i in diff_years_trend])

    if (combinations_[-1][-1] > (max_bound_max * 0.75 + min_bound_min * 0.25)):
        if (combinations_[-1][-1] > (max(combinations_[-1]) * 0.75 + min(combinations_[-1]) * 0.25)):
            return change_std / (min_bound_vol + max_bound_vol)
        else:
            return -10 ** 9
    elif (combinations_[-1][-1] < (max_bound_max * 0.25 + min_bound_min * 0.75)):
        if (combinations_[-1][-1] < (max(combinations_[-1]) * 0.25 + min(combinations_[-1]) * 0.75)):
            return change_std / (min_bound_vol + max_bound_vol)
        else:
            return -10 ** 9
    else:

        return -10 ** 9


def change_weights(array, possible_actions_, left_border=-9, right_border=9):
    array_ = array.copy()
    position = random.randint(0, len(array_) - 1)
    action = possible_actions_[random.randint(0, len(possible_actions_) - 1)]

    array_[position] += action
    if array_[position] > right_border:
        array_[position] = right_border

    elif array_[position] < left_border:
        array_[position] = left_border

    return array_


def optimization_step(df_diff_years, needed_columns, max_weight):
    start_w = -max_weight
    end_w = +max_weight
    possible_actions = [-10, -5, -2, -1, 1, 2, 5, 10]

    state = np.array([random.randint(start_w, end_w) for i in range(len(needed_columns))])

    score = -10 ** 9
    last_n_no_changes = 0
    max_no_changes = 1000
    last_combinations = [[-1 for j in range(len(df_diff_years[key]))] for key in df_diff_years]
    counter = 0
    new_state = []
    while True:

        counter += 1

        combinations = []
        # new_state = state
        if last_n_no_changes < 40:
            new_state = change_weights(state, possible_actions, start_w, end_w)
        for key in df_diff_years:
            combination = np.dot(np.array(df_diff_years[key].loc[:, needed_columns].values), new_state)
            combinations.append(list(combination))

        new_score = aggregate_function(combinations)

        if new_score > score:
            score = new_score
            state = new_state
            last_n_no_changes = 0
            last_combinations = list(combinations)
            print('Update')
        else:
            last_n_no_changes += 1
            if last_n_no_changes > 40:
                new_state = np.array([random.randint(start_w, end_w) for i in range(len(needed_columns))])
            if last_n_no_changes > max_no_changes:
                break

        if counter % 100 == 0:
            print(f'The {counter}-th iteration')

    # print({'combinations': combinations, 'state': list(state)})

    return {'combinations': last_combinations, 'state': list(state)}


def get_times(df_diff_years):
    times = []
    for year in df_diff_years:
        df_year_table = df_diff_years[year]
        new_time = pd.DataFrame()
        new_time['times'] = df_year_table.index
        new_time['times'] = new_time['times'].apply(str)
        new_time['times'] = new_time['times'].str[5:]
        print(new_time['times'])
        print(new_time['times'].values)
        times.append(new_time['times'].values.tolist())
    return times


def input_spreads_verification(input_spreads_, needed_spreads):
    verification_spreads = []
    for prod in input_spreads_:
        for spr in prod['Month']:
            verification_spreads.append(
                f'{product_normal_to_abb[prod["Product"]]}{spr.split("-")[0]}-{product_normal_to_abb[prod["Product"]]}{spr.split("-")[1]}')

    print(verification_spreads)
    print(needed_spreads)
    verification_spreads = sorted(list(set(verification_spreads).intersection(needed_spreads)))
    print(verification_spreads)
    return verification_spreads


def processing_intrahedge(dates, max_weight, input_spreads):
    download_spreads = denote_download_spreads(input_spreads)
    general_table = obtain_general_table(download_spreads)
    table_by_years = obtain_tables_by_years(general_table, dates)
    needed_columns = check_intersection_columns(table_by_years)
    needed_columns = input_spreads_verification(input_spreads, needed_columns)

    print(input_spreads)
    res = optimization_step(table_by_years, needed_columns, max_weight)
    res['times'] = get_times(table_by_years)
    res['needed_columns'] = needed_columns
    return res


def preparing_for_return(preresult):
    output_function = {}

    output_function['state'] = [int(j) for j in preresult['state']]
    output_function['combinations'] = [[np.round(float(hour_point), 2) for hour_point in year_combo] for year_combo in
                                       preresult['combinations']]
    output_function['times'] = preresult['times']
    output_function['title'] = ''

    for i in range(len(output_function['state'])):
        if output_function['state'][i] > 0:
            break
        elif output_function['state'][i] < 0:
            output_function['state'] = [-j for j in output_function['state']]
            output_function['combinations'] = [[-j for j in comb] for comb in output_function['combinations']]
            break

    for i in range(len(preresult['state'])):
        if output_function["state"][i] > 0:
            output_function['title'] += f'+{output_function["state"][i]}({preresult["needed_columns"][i]})'
        elif output_function["state"][i] < 0:
            output_function['title'] += f'{output_function["state"][i]}({preresult["needed_columns"][i]})'

    output_function['series'] = [
        {'name': i, 'data': [[output_function['times'][i][j], output_function['combinations'][i][j]]
                             for j in range(len(output_function['combinations'][i]))]}
        for i in range(len(output_function['combinations']))]

    for i in range(len(output_function['combinations'])):
        if len(output_function['combinations']) != len(output_function['times']):
            raise

    output_function['times'] = sorted(list(set(i for j in output_function['times']  for i in j)))
    return output_function


@router.post("/get_intrahedge")
async def get_gsci(dict_input: SimpleDict):
    print('Begin of get_intrahedge')
    front_inp = dict_input.array['data']
    dates = front_inp['Dates']
    max_weight = int(front_inp['MaxWeight'])
    input_spreads = front_inp['Spreads']
    input_spreads = list(input_spreads.values())
    print(dates)
    print(max_weight)
    print(input_spreads)
    preresult = processing_intrahedge(dates, max_weight,input_spreads)
    result = preparing_for_return(preresult)
    print(result)
    print(front_inp)
    print('End of get_intrahedge')
    return result



@router.get("/get_products_and_possible_spreads")
async def get_products_and_possible_spreads():
    result = {}
    result['products'] = list(products_spreads_dict.keys())
    result['possible_spreads'] = products_spreads_dict
    return result
