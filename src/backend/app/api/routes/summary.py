from fastapi import APIRouter, Depends, HTTPException
from typing import Dict
import asyncio
import logging

from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker


logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/summary")
async def get_summary(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> Dict[str, int]:
    """Get summary statistics using existing functions"""
    try:
        # Execute only the needed queries concurrently
        patient_counts, doc_counts, recent_counts = await asyncio.gather(
            db_checker.get_patient_counts(),
            db_checker.get_document_counts(),
            db_checker.get_recent_document_counts(),
        )

        # Calculate totals using the existing format
        total_documents = sum(
            item["unique_document_count"]
            for item in doc_counts
            if isinstance(item, dict) and "unique_document_count" in item
        )

        recent_documents = sum(
            item["unique_document_count"]
            for item in recent_counts
            if isinstance(item, dict) and "unique_document_count" in item
        )

        return {
            "patient_count": patient_counts.get("patient_count", 0),
            "test_patient_count": patient_counts.get("test_patient_count", 0),
            "research_patient_count": patient_counts.get("research_patient_count", 0),
            "celebrity_patient_count": patient_counts.get("celebrity_patient_count", 0),
            "total_documents": total_documents,
            "recent_documents": recent_documents,
        }

    except Exception as e:
        logger.error(f"Error in get_summary: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching summary statistics",
        )
