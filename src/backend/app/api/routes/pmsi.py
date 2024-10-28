from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["pmsi"])


@router.get("/pmsi", response_model=Dict[str, float])
async def get_pmsi(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
):
    """Get PMSI metrics"""
    try:
        logger.debug("Fetching document metrics")
        metrics = await db_checker.get_pmsi()

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
        