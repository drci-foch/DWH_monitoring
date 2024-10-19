
from fastapi import APIRouter
from app.core.db import engine
from app.crud import DatabaseQualityChecker


router = APIRouter()
db_checker = DatabaseQualityChecker(engine=engine)


@router.get("/api/document_metrics")
async def get_document_metrics():
    all_stats = db_checker.get_all_statistics()
    stats = all_stats['stats_and_delays'][0]
    return {
        "min_delay": stats[0],
        "q1": stats[1],
        "median": stats[2],
        "q3": stats[3],
        "max_delay": stats[4],
        "avg_delay": stats[5]
    }

@router.get("/api/document_counts")
async def get_document_counts():
    all_stats = db_checker.get_all_statistics()
    return all_stats['document_counts']

@router.get("/api/recent_document_counts")
async def get_recent_document_counts():
    all_stats = db_checker.get_all_statistics()
    return all_stats['recent_document_counts']