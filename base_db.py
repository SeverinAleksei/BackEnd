from databases import Database
from sqlalchemy import create_engine, MetaData

way = 'postgresql://postgres:pass@localhost:5432/postgres'
database = Database(way)
metadata = MetaData()
engine = create_engine(
    way,
)
