import datetime
from typing import Optional, Dict, Union, Any, List
from pydantic import BaseModel, EmailStr

class Action(BaseModel):
    id: Optional[str] = None
    email: EmailStr
    action: str
    action_time: datetime.datetime

class Chosen_Asset(BaseModel):
    chosen_asset: str

class SimpleArray(BaseModel):
    array: List

class SimpleDict(BaseModel):
    array: Dict

class SimpleAny(BaseModel):
    array: Any


class DownloadDataAsset(BaseModel):
    checked: bool
    partialChecked: bool


class DownloadDataSelected(BaseModel):
    assets: Dict[str, DownloadDataAsset]

class DownloadDataPossible(BaseModel):
    body: dict