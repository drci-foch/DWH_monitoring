from fastapi import APIRouter, Depends, HTTPException, Query
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["documents"])


async def validate_origin_codes(
    origin_codes: str, db_checker: DatabaseQualityChecker
) -> List[str]:
    """Validate origin codes against available origins in database"""
    if not origin_codes:
        raise HTTPException(
            status_code=400, detail="Origin codes parameter is required"
        )

    # Get available origins from database
    available_origins = set(await db_checker.get_document_origins())

    # Process and validate requested origins
    requested_origins = [
        code.strip() for code in origin_codes.split(",") if code.strip()
    ]

    # Check if all requested origins are valid
    invalid_origins = set(requested_origins) - available_origins
    if invalid_origins:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid origin codes: {', '.join(invalid_origins)}",
        )

    return requested_origins


@router.get("/document_counts_by_year")
async def get_document_counts_by_year(
    origin_codes: str = Query(..., description="Comma-separated list of origin codes"),
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> List[Dict[str, Any]]:
    """Get yearly document counts for specified origin codes"""
    try:
        logger.debug(f"Processing request for origin codes: {origin_codes}")

        # Validate origin codes against database
        validated_origins = await validate_origin_codes(origin_codes, db_checker)

        # Get batch results and extract yearly data
        batch_results = await db_checker.get_document_counts_batch(validated_origins)
        yearly_data = batch_results.get("yearly", [])

        if not yearly_data:
            logger.warning(f"No yearly data found for origin codes: {origin_codes}")
            raise HTTPException(
                status_code=404,
                detail=f"No data found for origin codes: {origin_codes}",
            )

        return yearly_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing yearly counts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching yearly document counts",
        )


@router.get("/recent_document_counts_by_month")
async def get_recent_document_counts_by_month(
    origin_codes: str = Query(..., description="Comma-separated list of origin codes"),
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> List[Dict[str, Any]]:
    """Get monthly document counts for specified origin codes"""
    try:
        logger.debug(f"Processing request for origin codes: {origin_codes}")

        # Validate origin codes against database
        validated_origins = await validate_origin_codes(origin_codes, db_checker)

        # Get batch results and extract monthly data
        batch_results = await db_checker.get_document_counts_batch(validated_origins)
        monthly_data = batch_results.get("monthly", [])

        if not monthly_data:
            logger.warning(f"No monthly data found for origin codes: {origin_codes}")
            raise HTTPException(
                status_code=404,
                detail=f"No data found for origin codes: {origin_codes}",
            )

        return monthly_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing monthly counts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching monthly document counts",
        )
