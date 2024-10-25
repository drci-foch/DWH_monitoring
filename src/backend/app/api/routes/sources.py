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

async def get_batch_data(
    origin_codes: List[str],
    db_checker: DatabaseQualityChecker,
    data_type: str
) -> List[Dict[str, Any]]:
    """Get batch data with consistent error handling"""
    batch_results = await db_checker.get_document_counts_batch(origin_codes)
    
    if not batch_results:
        logger.warning(f"No data found for origin codes: {', '.join(origin_codes)}")
        return []
        
    data = batch_results.get(data_type, [])
    
    if not data:
        logger.info(
            f"No {data_type} data found for origin codes: {', '.join(origin_codes)}, "
            f"full results: {batch_results}"
        )
        return []
        
    return data

@router.get("/document_counts_by_year")
async def get_document_counts_by_year(
    origin_codes: str = Query(..., description="Comma-separated list of origin codes"),
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> List[Dict[str, Any]]:
    """Get yearly document counts for specified origin codes"""
    try:
        logger.debug(f"Processing yearly request for origin codes: {origin_codes}")
        validated_origins = await validate_origin_codes(origin_codes, db_checker)
        
        # Get yearly data
        yearly_data = await get_batch_data(validated_origins, db_checker, "yearly")
        
        # Return empty list instead of 404 if no data found
        if not yearly_data:
            logger.info(f"No yearly data found for valid origin codes: {origin_codes}")
            return []
            
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
        logger.debug(f"Processing monthly request for origin codes: {origin_codes}")
        validated_origins = await validate_origin_codes(origin_codes, db_checker)
        
        # Get monthly data
        monthly_data = await get_batch_data(validated_origins, db_checker, "monthly")
        
        # Return empty list instead of 404 if no data found
        if not monthly_data:
            logger.info(f"No monthly data found for valid origin codes: {origin_codes}")
            return []
            
        return monthly_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing monthly counts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching monthly document counts",
        )