import datetime
from ML.db.actions import actions
from ML.models.action import Action
from base_repository import BaseRepository


def action_auto_logging(func):

    def wrapper(*args, **kwargs):

        print("Something is happening before the function is called.")
        func(*args, **kwargs)
        print("Something is happening after the function is called.")

    return wrapper


class ActionRepository(BaseRepository):

    async def create(self, a:Action) -> Action:
        action = Action(
            email=a.email,
            action=a.action,
            action_time=datetime.datetime.utcnow()
        )
        values = {**action.dict()}
        values.pop('id', None)
        query = actions.insert().values(**values)
        action.id = await self.database.execute(query)
        return action


    @action_auto_logging
    def check(self, a: Action) -> bool:

        print('Hi')
        return True

    @action_auto_logging
    async def write(self, a: Action) -> Action:

        values = {**a.dict()}
        values.pop('id', None)
        query = actions.insert().values(**values)
        a.id = await self.database.execute(query)
        return a

