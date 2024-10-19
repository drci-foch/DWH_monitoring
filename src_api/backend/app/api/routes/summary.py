

from fastapi import APIRouter

from app.core.db import engine
from app.crud import DatabaseQualityChecker


router = APIRouter()
db_checker = DatabaseQualityChecker(engine=engine)

@router.get("/api/summary")
async def get_summary():
    all_stats = db_checker.get_all_statistics()
    return {
        "patient_count": all_stats['patient_count'][0][0],
        "total_documents": sum(count for _, count in all_stats['document_counts']),
        "document_origins_count": len(all_stats['document_counts']),
        "recent_documents": sum(count for _, count in all_stats['recent_document_counts']),
        "recent_document_sources": len(all_stats['recent_document_counts'])
    }
