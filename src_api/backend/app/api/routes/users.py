
from fastapi import APIRouter
from app.core.db import engine
from app.crud import DatabaseQualityChecker


router = APIRouter()
db_checker = DatabaseQualityChecker(engine=engine)

@router.get("/api/top_users")
async def get_top_users():
    all_stats = db_checker.get_all_statistics()
    return all_stats['top_users']

@router.get("/api/top_users_current_year")
async def get_top_users_current_year():
    all_stats = db_checker.get_all_statistics()
    return all_stats['top_users_current_year']