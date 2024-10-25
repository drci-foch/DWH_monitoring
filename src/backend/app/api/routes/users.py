from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["users"])


@router.get("/top_users", response_model=List[Dict[str, Any]])
async def get_top_users(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    """Get top users for all time"""
    try:
        logger.debug("Fetching top users")
        top_users = await db_checker.get_top_users()
        logger.debug(f"Retrieved top users: {top_users}")

        return [
            {
                "firstname": user["firstname"],
                "lastname": user["lastname"],
                "query_count": user["query_count"],
            }
            for user in top_users
        ]
    except Exception as e:
        logger.error(f"Error in get_top_users: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while fetching top users"
        )


@router.get("/top_users_current_year", response_model=List[Dict[str, Any]])
async def get_top_users_current_year(
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
):
    """Get top users for current year only"""
    try:
        logger.debug("Fetching top users for current year")
        top_users = await db_checker.get_top_users(current_year=True)
        logger.debug(f"Retrieved top users for current year: {top_users}")

        return [
            {
                "firstname": user["firstname"],
                "lastname": user["lastname"],
                "query_count": user["query_count"],
            }
            for user in top_users
        ]
    except Exception as e:
        logger.error(f"Error in get_top_users_current_year: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while fetching top users for current year",
        )
