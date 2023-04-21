from authorization.db.users import users
from base_db import metadata, engine

metadata.create_all(bind=engine)