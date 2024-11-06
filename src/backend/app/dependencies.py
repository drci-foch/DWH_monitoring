from typing import AsyncGenerator
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import engine
from app.crud import DatabaseQualityChecker

async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency that provides an async database session.
    Ensures proper handling of session lifecycle and connection pooling.
    """
    session = AsyncSession(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        await session.close()

def get_db_checker(session: AsyncSession = Depends(get_db_session)) -> DatabaseQualityChecker:
    """
    Dependency that provides a DatabaseQualityChecker instance.
    Uses connection pooling through the shared engine.
    """
    return DatabaseQualityChecker(engine=engine)