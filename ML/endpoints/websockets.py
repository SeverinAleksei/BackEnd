import random
import time
import csv
import pandas as pd
from fastapi import APIRouter,WebSocket, WebSocketDisconnect
import asyncio

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()

router = APIRouter()


@router.websocket_route("/ws")
async def hello(websocket):
    await websocket.accept()
    print("Websocket Accepted!")
    counter = 0


    try:
        while True:
            await asyncio.sleep(1)
            counter += 1
            # await websocket.send_text(f"Router Hello! {counter}")
            # data[0]['counter'] =  counter
            file = open('example_df.csv')
            reader = csv.DictReader(file)
            my_list = list()
            for dictionary in reader:
                dictionary['Change'] = random.random()
                my_list.append(dictionary)
            await websocket.send_json(my_list[0:10])
            #response = await websocket.receive_text()
            #print(response)
    except:
        await websocket.close()
        print("Router Closed")

@router.get("/get")
async def hello_2():
    counter = 0
    data = [{'hi': 'hi'}, {'bye': 'bye'}, {'counter': 100}]
    return data

@router.get("/get2")
async def hello_2():
    counter = 0
    data = [{'hi': 'hi'}, {'bye': 'bye'}, {'counter': 1000}]
    return data
    # finally:
    #     await websocket.close()
    #     print("Router Closed")