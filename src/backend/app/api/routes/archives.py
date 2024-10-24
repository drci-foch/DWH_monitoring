from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import Dict, Any, List
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)


class ArchiveStatus(BaseModel):
    archive_period: float
    total_documents_to_suppress: int
    documents_to_suppress: List[tuple]  # List of (origin_code, count) tuples


router = APIRouter(prefix="/api", tags=["archives"])


@router.get(
    "/archive_status",
    response_model=ArchiveStatus,
    summary="Get archive status and documents to suppress",
    description="Returns archive period information and details about documents that should be suppressed",
)
async def get_archive_status(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> Dict[str, Any]:
    """
    Get archive status including:
    - Archive period in years
    - Total number of documents to suppress
    - Breakdown of documents to suppress by origin
    """
    try:
        logger.debug("Fetching archive status")

        archive_status = await db_checker.get_archive_status()

        if not archive_status:
            logger.warning("No archive status data found")
            raise HTTPException(
                status_code=404, detail="No archive status data available"
            )

        # Validate returned data structure
        if not all(
            key in archive_status
            for key in [
                "archive_period",
                "total_documents_to_suppress",
                "documents_to_suppress",
            ]
        ):
            logger.error("Invalid archive status data structure")
            raise HTTPException(
                status_code=500, detail="Invalid archive status data structure"
            )

        logger.debug(f"Archive status retrieved successfully: {archive_status}")
        return archive_status

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching archive status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while fetching archive status"
        )
