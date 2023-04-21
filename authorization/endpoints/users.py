from fastapi import APIRouter, Depends, HTTPException, status
from authorization.repositories.users import UserRepository
from authorization.models.user import User, UserRegistration
from depends import get_user_repository
from .depends import get_current_user


router = APIRouter()

@router.post("/signup", response_model=User)
async def create_user(
    user: UserRegistration,
    users: UserRepository = Depends(get_user_repository)):
    print('Hi')
    print(user)
    return await users.create(u=user)

@router.put("/", response_model=User)
async def update_user(
    id: int,
    user: UserRegistration,
    users: UserRepository = Depends(get_user_repository),
    current_user: User = Depends(get_current_user)):
    old_user = await users.get_by_id(id=id)
    print('old', old_user)
    print('current', current_user)
    if old_user is None or old_user.email != current_user.email:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found user")
    return await users.update(id=id, u=user)