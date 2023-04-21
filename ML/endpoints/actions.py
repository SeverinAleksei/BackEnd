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
root_for_data = 'C:\OSTC\Spreads_Data'
sys.path.insert(1, root_for_data)
from config_products import tick_price

router = APIRouter(
)


# @router.put("/", response_model= str)
# async def check_token(
#     current_user: User = Depends(get_current_user)):
#     return current_user.email

# @router.put("/", response_model= bool)
# async def write_action(
#         action: Action,
#         actions: ActionRepository = Depends(get_action_repository)):
#     actions.check(action)
#     return True



@router.put("/", response_model=Action)
async def write_action(
        action: Action,
        actions: ActionRepository = Depends(get_action_repository)):
    print(actions)

    return await actions.create(a=action)


@router.get("/check", response_model=bool)
async def write_action():
    return True


def zipfiles(dir, filenames):
    zip_filename = "archive.zip"

    s = io.BytesIO()
    zf = zipfile.ZipFile(s, "w")
    for folderName, subfolders, filenames_ in os.walk(dir):
        print(f'folderName {folderName}, subfolders {subfolders}, filenames {filenames_}')

        for filename in filenames_:
            filePath = os.path.join(folderName, filename).replace('\\', '/')
            # Add file to zip
            inFolderPath = filePath.replace(dir, '')
            zf.write(filePath, inFolderPath)

    zf.close()
    # Grab ZIP file from in-memory, make response with correct MIME-type
    resp = Response(s.getvalue(), media_type="application/x-zip-compressed", headers={
        'Content-Disposition': f'attachment;filename={zip_filename}'
    })
    return resp


@router.get("/getfile")
async def get_file():
    entries = os.listdir(DATA_STORAGE_PATH)
    list_files = [DATA_STORAGE_PATH + i for i in entries]
    return zipfiles(DATA_STORAGE_PATH, list_files)


@router.put("/downloaddataapi")
async def download_data_api(nodes: DownloadDataSelected):
    entries = os.listdir(DATA_STORAGE_PATH)
    print(nodes)
    # list_files = [DATA_STORAGE_PATH + i for i in entries]
    #return zipfiles(DATA_STORAGE_PATH, list_files)


# Function to get instruments, its category and available dates for download
@router.get("/getassets")
async def get_assets():
    def list_files(startpath):
        print('Get Assets')
        core_assets = []
        spec_assets = {}
        dates = {}

        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            root_array = root.split('\\')
            if level == 1:
                core_assets.append(os.path.basename(root))
            if level == 2:
                spec_assets[root_array[-2]] = spec_assets.setdefault(root_array[-2], []) + [os.path.basename(root)]
            if level == 3:
                dates[root_array[-2]] = dates.setdefault(root_array[-2], []) + [os.path.basename(root)]

        nodes = []
        for core_asset in core_assets:
            core_node = {"key": core_asset, "data": {"name": core_asset}, "children": []}
            for spec_asset in spec_assets[core_asset]:
                spec_node = {"key": spec_asset, "data": {}}
                spec_node['data']['name'] = spec_asset
                spec_node['data']['min_data'] = min(dates[spec_asset])
                spec_node['data']['max_data'] = max(dates[spec_asset])
                core_node['children'].append(spec_node)
            nodes.append(core_node)

        return nodes

    return list_files(DATA_STORAGE_PATH)


##Function for only general Name
# Function to get instruments, its category and available dates for download
@router.get("/getAssetsNames")
async def get_assets_names():
    def list_files(startpath):
        core_assets = []
        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            if level == 1:
                core_assets.append(os.path.basename(root))

        return core_assets

    return list_files(DATA_STORAGE_PATH)


##Function for certain spread of asset
# Function to get instruments, its category and available dates for download
@router.post("/getSpreadsNames")
async def get_spreads_names(ch_asset: Chosen_Asset):
    # chosen_asset = chosen_asset[chosen_asset]
    print(ch_asset.chosen_asset)

    def list_files(startpath):
        core_assets = []
        spec_assets = {}
        dates = {}

        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            root_array = root.split('\\')
            if level == 1:
                core_assets.append(os.path.basename(root))
            if level == 2:
                spec_assets[root_array[-2]] = spec_assets.setdefault(root_array[-2], []) + [os.path.basename(root)]
            if level == 3:
                dates[root_array[-2]] = dates.setdefault(root_array[-2], []) + [os.path.basename(root)]

        nodes = []
        for core_asset in core_assets:
            core_node = {"key": core_asset, "data": {"name": core_asset}, "children": []}
            for spec_asset in spec_assets[core_asset]:
                spec_node = {"key": spec_asset, "data": {}}
                spec_node['data']['name'] = spec_asset
                spec_node['data']['min_data'] = min(dates[spec_asset])
                spec_node['data']['max_data'] = max(dates[spec_asset])
                core_node['children'].append(spec_node)
            nodes.append(core_node)

        print(spec_assets[ch_asset.chosen_asset])
        print(type(spec_assets[ch_asset.chosen_asset]))
        return spec_assets[ch_asset.chosen_asset]

    return list_files(DATA_STORAGE_PATH)


# Function to get instruments, its category and available dates for download
@router.get("/getAssetsSpreadsDates")
async def get_assets_spreads_dates():
    def list_files(startpath):
        core_assets = []
        spec_assets = {}
        dates = {}

        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            root_array = root.split('\\')
            if level == 1:
                core_assets.append(os.path.basename(root))
            if level == 2:
                spec_assets[root_array[-2]] = spec_assets.setdefault(root_array[-2], []) + [os.path.basename(root)]
            if level == 3:
                dates[root_array[-2]] = dates.setdefault(root_array[-2], []) + [os.path.basename(root)]

        nodes = []
        for core_asset in core_assets:
            core_node = {"key": core_asset, "data": {"name": core_asset}, "children": []}
            for spec_asset in spec_assets[core_asset]:
                spec_node = {"key": spec_asset, "data": {}}
                spec_node['data']['name'] = spec_asset
                spec_node['data']['min_data'] = min(dates[spec_asset])
                spec_node['data']['max_data'] = max(dates[spec_asset])
                core_node['children'].append(spec_node)
            nodes.append(core_node)

        print(core_assets, spec_assets, dates)
        return {"assets": core_assets, "spreads": spec_assets, "dates": dates}

    return list_files(DATA_STORAGE_PATH)

@router.get("/getDataPlot")
async def getDataPlot():
    first = {'Product': 'BRENT', 'Spread': 'BRENT_23_05-BRENT_23_06'}
    second = {'Product': 'HEATING OIL', 'Spread': 'HEATING OIL_23_04-HEATING OIL_23_05'}

    def reading_files(path):
        files = os.listdir(path)
        concated_data = []
        for file in files:
            concated_data.append(pd.read_csv(f'{path}\{file}')[::-1])

        concated_data = pd.concat(concated_data).reset_index(drop=True)
        return concated_data

    reading_files(f"{root_for_data}\{first['Product']}\{first['Spread']}")

    first_frame = reading_files(f"{root_for_data}\{first['Product']}\{first['Spread']}")
    second_frame = reading_files(f"{root_for_data}\{second['Product']}\{second['Spread']}")


    first_ = first_frame[['datetime', 'open_p']]
    first_['open_p'] *= tick_price[first['Product']]

    second_ = second_frame[['datetime', 'open_p']]
    second_['open_p'] *= tick_price[second['Product']]

    first_.rename(columns={'open_p': f'open_first'}, inplace=True)
    second_.rename(columns={'open_p': f'open_second'}, inplace=True)

    first_['datetime'] = pd.to_datetime(first_['datetime'])
    second_['datetime'] = pd.to_datetime(second_['datetime'])

    first_.set_index(['datetime'], inplace=True)
    second_.set_index(['datetime'], inplace=True)

    merged = pd.concat([first_, second_], axis=1, join='outer')
    merged.fillna(method='ffill', inplace=True)


    today = datetime.now()
    last_n_days = 14
    slice_day = today - timedelta(days=last_n_days)
    print(slice_day)

    merged_reduced = merged.copy()

    merged_reduced = merged_reduced.loc[merged_reduced.index > slice_day]
    reg = LinearRegression().fit(merged_reduced.open_second.values.reshape(-1, 1), merged_reduced.open_first)
    return {'First': list(merged_reduced.open_first.values[-400:]),
            'Second':list(merged_reduced.open_second.values[-400:]),
            'Hedge': list(merged_reduced.open_first.values - merged_reduced.open_second.values * reg.coef_[0])[-400:],
            "Time":  list(merged_reduced.index.values.astype(str))[-400:]}
