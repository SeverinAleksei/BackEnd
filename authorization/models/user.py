import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, validator, constr

class User(BaseModel):
    id: Optional[str] = None
    name: str
    email: EmailStr
    hashed_password: str
    created_at: datetime.datetime
    last_visit: datetime.datetime

class UserRegistration(BaseModel):
    name: str
    email: EmailStr
    password: str
    password2: str

    # @validator("password2")
    # def password_match(cls, v, values, **kwargs):
    #     if 'password' in values and v != values["password"]:
    #         raise ValueError("passwords don't match")
    #     return v