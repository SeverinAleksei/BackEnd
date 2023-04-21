from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import sys

sys.path.append('../')
from base_db import database
from authorization.endpoints import users, auth
from ML.endpoints import actions, websockets, files_downloader

app = FastAPI(title='Trading Helper')


@app.websocket("/ws")
async def send_data(websocket: WebSocket):
    print('CONNECTING...')
    await websocket.accept()
    while True:
        try:
            await websocket.receive_text()
            resp = {
                "message": "message from websocket"
            }
            await websocket.send_json(resp)
        except Exception as e:
            print(e)
            break
    print("CONNECTION DEAD...")


app.include_router(users.router, prefix='/users', tags=['users'])
app.include_router(auth.router, prefix='/auth', tags=['auth'])
app.include_router(actions.router, prefix='/actions', tags=['actions'])
app.include_router(files_downloader.router, prefix='/files_downloader', tags=['files_downloader'])
app.include_router(websockets.router, prefix='/websockets', tags=['websockets'])
# app.include_router(actions.router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event('startup')
async def startup():
    await database.connect()


@app.on_event('shutdown')
async def shutdown():
    await database.disconnect()
