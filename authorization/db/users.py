import sqlalchemy
from base_db import metadata
import datetime

users = sqlalchemy.Table(
    'users',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=True, unique=True),
    sqlalchemy.Column('email', sqlalchemy.String, primary_key=True, unique=True),
    sqlalchemy.Column('name', sqlalchemy.String, primary_key=True, unique=True),
    sqlalchemy.Column('hashed_password', sqlalchemy.String, default = ''),
    sqlalchemy.Column('created_at', sqlalchemy.DateTime, default=datetime.datetime.utcnow()),
    sqlalchemy.Column('last_visit', sqlalchemy.DateTime, default=datetime.datetime.utcnow())

)
