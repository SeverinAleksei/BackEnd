import json
from random import random

from fastapi import APIRouter, Depends, Response

from ML.models.action import SimpleArray, SimpleDict, SimpleAny
import sys
from confluent_kafka import Consumer, Producer
import json
from random import random
import pandas as pd
from time import time

root_for_data = 'C:\OSTC\Spreads_Data\Day'
sys.path.insert(1, root_for_data)

import scipy.stats as st

columns = ['Symbol', 'Most Recent Trade', 'Most Recent Trade Size', 'Most Recent Trade Time', 'Most Recent Trade Market Center',
           'Total Volume', 'Bid', 'Bid Size', 'Ask', 'Ask Size', 'Open', 'High', 'Low', 'Close', 'Message Contents',
           'Most Recent Trade Conditions', 'local_time']
router = APIRouter()


@router.post("/get_delta")
async def get_delta(dict_input: SimpleDict):
    print('Begin of get_detla')
    front_inp = dict_input.array
    product = front_inp['data']['Product']
    print(product)
    result = {}

    result = preparing_for_return(delta_table())
    print(result)
    print('End of get_delta')
    return result


def preparing_for_return(results):
    output = {}

    columns = []
    if len(results) != 0:
        first_row = results[0]
        columns = [{'field': key} for key in list(first_row.keys())]

    output['columns'] = columns
    output['body'] = results
    return output


def delta_table():
    name = random()
    conf = {'bootstrap.servers': "localhost:9092",
            'group.id': f"{name}",
            'auto.offset.reset': 'earliest'}

    consumer = Consumer(conf)
    current_time = pd.to_datetime(time(), unit='s')
    subscribed_channels = ['QNGN23', 'QNGN23-QNGQ23']
    windows = [1, 5, 15, 60, 120, 720]

    # Obtain raw Messages
    r_m = raw_messages_function(consumer, subscribed_channels)
    formed_table = form_table(r_m, current_time, windows)
    table_results = gain_results(formed_table, windows)

    return table_results

def raw_messages_function(c, channels):

    c.subscribe(channels)
    raw_messages = []
    i = 0
    c.poll(5.0)
    while True:
        print(i)
        i += 1
        msg = c.poll(0.0)
        if msg is None:
            break

        if msg.error():
            print('Error')
            continue

        raw_messages.append(json.loads(msg.value()))

    return raw_messages

def form_table(raw_messages, current_time, windows):
    # Obtain Table
    frame = pd.DataFrame(raw_messages)
    frame.columns = columns
    frame['local_time'] = pd.to_datetime(frame['local_time'], unit='ms')

    frame['Side'] = (frame['Most Recent Trade'] == frame['Bid']) * -1 + (frame['Most Recent Trade'] == frame['Ask']) * 1
    for window in windows:
        frame[f'in_window_{window}'] = (current_time - frame['local_time'] < pd.Timedelta(window, unit='m'))
        frame[f'trade_in_window_{window}'] = frame[f'in_window_{window}'] * frame[f'Most Recent Trade Size'] * frame[
            'Side']

    return frame

def gain_results(frame, windows):
    #Obtain results
    results = []
    for channel in set(frame['Symbol']):
        dx = frame[frame['Symbol'] == channel]
        result = {}
        result['Symbol'] = channel
        for window in windows:
            result[f'VD in {window}'] = int(dx[f'trade_in_window_{window}'].sum())
        results.append(result)

    return results


