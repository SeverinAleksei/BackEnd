import datetime
from authorization.db.users import users
from authorization.models.user import User, UserRegistration
from base_repository import BaseRepository


class UserRepository(BaseRepository):

    async def create(self, u: UserRegistration) -> User:
        user = User(
            name=u.name,
            email=u.email,
            hashed_password=u.password,
            created_at=datetime.datetime.utcnow(),
            last_visit=datetime.datetime.utcnow()
        )
        values = {**user.dict()}
        values.pop('id', None)
        query = users.insert().values(**values)
        user.id = await self.database.execute(query)
        return user


    async def update(self, id: str, u: UserRegistration) -> User:

        user = await self.get_by_email(u.email)
        values = {**user.dict()}
        values['hashed_password'] = u.password
        values['name'] = u.name
        values['id'] = int(values['id'])
        print('values', values)
        query = users.update().where(users.c.id==id).values(values)
        user.id = await self.database.execute(query)

        return user

    async def get_by_id(self, id: str):
        query = users.select().where(users.c.id==id)
        user = await self.database.fetch_one(query)
        if user is None:
            return None
        return User.parse_obj(user)

    async def get_by_email(self, email: str) -> User:
        query = users.select().where(users.c.email==email)
        user = await self.database.fetch_one(query)
        if user is None:
            return None
        return User.parse_obj(user)

