import sqlalchemy
from base_db import metadata
import datetime

actions = sqlalchemy.Table(
    'actions',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, autoincrement=True, unique=True),
    sqlalchemy.Column('email', sqlalchemy.String, primary_key=True, unique=True),
    sqlalchemy.Column('action', sqlalchemy.String, default = ''),
    sqlalchemy.Column('action_time', sqlalchemy.DateTime, default=datetime.datetime.utcnow()),
)
