from fastapi import APIRouter, Depends
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker

router = APIRouter()

@router.get("/api/archive_status")
async def get_archive_status(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    return await db_checker.get_archive_status()