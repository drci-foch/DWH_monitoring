from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/api/top_users", response_model=List[Dict[str, Any]])
async def get_top_users(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        all_stats = await db_checker.get_all_statistics()
        logger.debug(f"Retrieved all_stats: {all_stats}")
        top_users = all_stats.get('top_users', [])
        logger.debug(f"Top users: {top_users}")
        return [
            {
                "firstname": user["firstname"],
                "lastname": user["lastname"],
                "query_count": user["query_count"]
            }
            for user in top_users
        ]
    except Exception as e:
        logger.error(f"Error in get_top_users: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/api/top_users_current_year", response_model=List[Dict[str, Any]])
async def get_top_users_current_year(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        all_stats = await db_checker.get_all_statistics()
        logger.debug(f"Retrieved all_stats: {all_stats}")
        top_users_current_year = all_stats.get('top_users_current_year', [])
        logger.debug(f"Top users current year: {top_users_current_year}")
        return [
            {
                "firstname": user["firstname"],
                "lastname": user["lastname"],
                "query_count": user["query_count"]
            }
            for user in top_users_current_year
        ]
    except Exception as e:
        logger.error(f"Error in get_top_users_current_year: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")