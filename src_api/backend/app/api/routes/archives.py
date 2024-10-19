from fastapi import APIRouter
from app.core.db import engine
from app.crud import DatabaseQualityChecker


router = APIRouter()
db_checker = DatabaseQualityChecker(engine=engine)

@router.get("/api/archive_status")
async def get_archive_status():
    all_stats = db_checker.get_all_statistics()
    return {
        "archive_period": all_stats['archive_period'],
        "total_documents_to_suppress": all_stats['total_documents_to_suppress'][0][0],
        "documents_to_suppress": all_stats['documents_to_suppress']
    }