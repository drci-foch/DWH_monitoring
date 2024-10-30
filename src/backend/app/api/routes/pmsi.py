from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import Dict, List, Optional
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["pmsi"])

class PMSIResponse(BaseModel):
    last_upload: Optional[str]
    time_period: Dict[str, Optional[str | int]]
    monthly_counts: List[Dict[str, str | int]]
    gaps: List[str]
    stats: Dict[str, int | float]

@router.get("/pmsi", response_model=PMSIResponse)
async def get_pmsi(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
):
    """Get PMSI metrics"""
    try:
        logger.debug("Fetching PMSI metrics")
        metrics = await db_checker.get_pmsi()
        if not metrics:
            logger.warning("No PMSI metrics found")
            raise HTTPException(status_code=404, detail="PMSI metrics not found")
        return metrics
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching PMSI metrics: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while fetching PMSI metrics"
        )