from ML.db.actions import actions
from base_db import metadata, engine

metadata.create_all(bind=engine)