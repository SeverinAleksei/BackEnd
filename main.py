import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import sys

sys.path.append('../')
from base_db import database
from authorization.endpoints import users, auth
from ML.endpoints import actions, websockets, files_downloader, settlement, roll, gsci, delta, intrahedge

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
app.include_router(settlement.router, prefix='/settlement', tags=['settlement'])
app.include_router(roll.router, prefix='/roll', tags=['roll'])
app.include_router(files_downloader.router, prefix='/files_downloader', tags=['files_downloader'])
app.include_router(gsci.router, prefix='/gsci', tags=['gsci'])
# app.include_router(delta.router, prefix='/delta', tags=['delta'])
app.include_router(intrahedge.router, prefix='/intrahedge', tags=['intrahedge'])
# app.include_router(websockets.router, prefix='/websockets', tags=['websockets'])
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

if __name__ == '__main__':
    uvicorn.run(
        "main:app",
        port=8000,
        host='10.100.110.13',
        reload = True,
        ssl_keyfile="key.pem",
        ssl_certfile="cert.pem")