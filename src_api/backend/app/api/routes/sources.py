from fastapi import APIRouter

from app.core.db import engine
from app.crud import DatabaseQualityChecker


router = APIRouter()
db_checker = DatabaseQualityChecker(engine=engine)


@router.get("/api/document_counts_by_year/{origin_code}")
async def get_document_counts_by_year(origin_code: str):
    all_stats = db_checker.get_all_statistics()
    return all_stats['document_counts_by_year'].get(origin_code, [])

@router.get("/api/recent_document_counts_by_month/{origin_code}")
async def get_recent_document_counts_by_month(origin_code: str):
    all_stats = db_checker.get_all_statistics()
    return all_stats['recent_document_counts_by_month'].get(origin_code, [])
