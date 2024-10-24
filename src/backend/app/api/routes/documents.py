from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["documents"])


@router.get("/document_metrics", response_model=Dict[str, float])
async def get_document_metrics(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
):
    """Get document metrics"""
    try:
        logger.debug("Fetching document metrics")
        metrics = await db_checker.get_document_metrics()

        if not metrics:
            logger.warning("No document metrics found")
            raise HTTPException(status_code=404, detail="Document metrics not found")

        return metrics

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching document metrics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while fetching document metrics"
        )


@router.get("/document_counts", response_model=List[Dict[str, Any]])
async def get_document_counts(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
):
    """Get document counts"""
    try:
        logger.debug("Fetching document counts")
        counts = await db_checker.get_document_counts()
        return counts

    except Exception as e:
        logger.error(f"Error fetching document counts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while fetching document counts"
        )


@router.get("/recent_document_counts", response_model=List[Dict[str, Any]])
async def get_recent_document_counts(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
):
    """Get recent document counts"""
    try:
        logger.debug("Fetching recent document counts")
        counts = await db_checker.get_recent_document_counts()
        return counts

    except Exception as e:
        logger.error(f"Error fetching recent document counts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching recent document counts",
        )
