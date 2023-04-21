from ML.repositories.actions import ActionRepository
from authorization.repositories.users import UserRepository
from base_db import database


def get_action_repository() -> ActionRepository:

    return ActionRepository(database)

def get_user_repository() -> UserRepository:

    return UserRepository(database)

