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
from datetime import datetime, timedelta
import pandas as pd
import sys
from ML.models.action import SimpleArray,SimpleDict,SimpleAny

root_for_data = 'C:\OSTC\Spreads_Data\Minute'
sys.path.insert(1, root_for_data)

router = APIRouter()


def zipfiles(dir, filenames):
    zip_filename = "archive.zip"

    print('zipfiles')
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


def zipfiles_modify(selected_files):
    zip_filename = "archive.zip"
    s = io.BytesIO()
    zf = zipfile.ZipFile(s, "w")
    for folderName, subfolders, filenames_ in os.walk(DATA_STORAGE_PATH):
        # print(f'folderName {folderName}, subfolders {subfolders}, filenames {filenames_}')
        last_part = folderName.split("\\")[-1]
        if last_part in list(selected_files.keys()):
            for filename in filenames_:
                filePath = os.path.join(folderName, filename).replace('\\', '/')
                # Add file to zip
                inFolderPath = filePath.replace(DATA_STORAGE_PATH, '')
                zf.write(filePath, inFolderPath)

    zf.close()
    # Grab ZIP file from in-memory, make response with correct MIME-type
    resp = Response(s.getvalue(), media_type="application/x-zip-compressed", headers={
        'Content-Disposition': f'attachment;filename={zip_filename}'
    })
    return resp


@router.get("/get_all_files")
async def get_file():
    print('getfile files_downloader')
    entries = os.listdir(DATA_STORAGE_PATH)
    list_files = [DATA_STORAGE_PATH + i for i in entries]
    return zipfiles(DATA_STORAGE_PATH, [])


@router.post("/get_chosen_files")
async def get_chosen_files(nodes: SimpleDict):
    print('get chosen files')
    chosen_nodes = nodes.array
    print(chosen_nodes)
    return zipfiles_modify(chosen_nodes)


# Function to get instruments, its category and available dates for download
def get_full_info_about_products():
    result = {}

    def recursive_writing(result_recursive, folder_path):
        for folder in os.listdir(folder_path):
            if ('.csv' not in folder) and ('config_products' not in folder) and ('__pycache__' not in folder):
                result_recursive[folder] = {}
                recursive_writing(result_recursive[folder], f'{folder_path}\{folder}')
            else:
                if '.csv' in folder:
                    if "Days" not in result_recursive:
                        result_recursive["Days"] = []
                    result_recursive["Days"].append(folder)

    recursive_writing(result, DATA_STORAGE_PATH)
    return result


@router.get("/get_full_info_about_products_2")
async def get_full_info_about_products_2():
    structure = get_full_info_about_products()
    result = []
    for type_data_index, type_data in enumerate(structure.keys()):
        result.append({'key': type_data, 'data': {'name': type_data}, 'children': []})
        print(type_data_index, type_data)
        for product_index, product in enumerate(structure[type_data]):
            result[type_data_index]['children'].append({'key': product, 'data': {'name': product}, 'children': []})
            for spread_index, spread in enumerate(structure[type_data][product]):
                print(type_data, product, spread)
                if len(structure[type_data][product][spread]) > 0:
                    result[type_data_index]['children'][product_index]['children'].append(
                        {'key': spread, 'data': {'name': spread}})
                    print('yes')
                else:
                    print('no')

    return result
    # print(structure)
