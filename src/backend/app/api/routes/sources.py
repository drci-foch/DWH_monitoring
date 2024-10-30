from fastapi import APIRouter, Depends, HTTPException, Query
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any, Optional, Tuple
import logging
from functools import lru_cache
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["sources"])


class BatchDataCache:
    def __init__(self, ttl_seconds: int = 3600):  # 1 hour default TTL
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.timestamps: Dict[str, datetime] = {}
        self.ttl = timedelta(seconds=ttl_seconds)

    def get_cache_key(self, origin_codes: List[str]) -> str:
        return ",".join(sorted(origin_codes))

    def get(self, origin_codes: List[str]) -> Optional[Dict[str, Any]]:
        cache_key = self.get_cache_key(origin_codes)
        if cache_key not in self.cache:
            return None

        if datetime.now() - self.timestamps[cache_key] > self.ttl:
            del self.cache[cache_key]
            del self.timestamps[cache_key]
            return None

        return self.cache[cache_key]

    def set(self, origin_codes: List[str], data: Dict[str, Any]):
        cache_key = self.get_cache_key(origin_codes)
        self.cache[cache_key] = data
        self.timestamps[cache_key] = datetime.now()


batch_data_cache = BatchDataCache()


async def validate_origin_codes(
    origin_codes: str, db_checker: DatabaseQualityChecker
) -> List[str]:
    """Validate origin codes against available origins in database"""
    if not origin_codes:
        raise HTTPException(
            status_code=400, detail="Origin codes parameter is required"
        )

    available_origins = set(await db_checker.get_document_origins())

    requested_origins = [
        code.strip() for code in origin_codes.split(",") if code.strip()
    ]

    invalid_origins = set(requested_origins) - available_origins
    if invalid_origins:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid origin codes: {', '.join(invalid_origins)}",
        )

    return requested_origins


async def get_or_fetch_batch_data(
    origin_codes: List[str],
    db_checker: DatabaseQualityChecker,
) -> Dict[str, Any]:
    """Get batch data from cache or fetch from database if not available"""
    cached_data = batch_data_cache.get(origin_codes)
    if cached_data is not None:
        logger.debug(f"Cache hit for origin codes: {', '.join(origin_codes)}")
        return cached_data

    logger.debug(f"Cache miss for origin codes: {', '.join(origin_codes)}")
    batch_results = await db_checker.get_document_counts_batch(origin_codes)

    if batch_results:
        batch_data_cache.set(origin_codes, batch_results)
        return batch_results

    logger.warning(f"No data found for origin codes: {', '.join(origin_codes)}")
    return {}


async def process_request(
    origin_codes: str, db_checker: DatabaseQualityChecker, data_type: str
) -> List[Dict[str, Any]]:
    """Process request for either yearly or monthly data"""
    try:
        validated_origins = await validate_origin_codes(origin_codes, db_checker)

        batch_results = await get_or_fetch_batch_data(validated_origins, db_checker)

        data = batch_results.get(data_type, [])

        if not data:
            logger.info(
                f"No {data_type} data found for valid origin codes: {origin_codes}"
            )
            return []

        return data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing {data_type} counts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching {data_type} document counts",
        )


@router.get("/document_counts_by_year")
async def get_document_counts_by_year(
    origin_codes: str = Query(..., description="Comma-separated list of origin codes"),
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> List[Dict[str, Any]]:
    """Get yearly document counts for specified origin codes"""
    logger.debug(f"Processing yearly request for origin codes: {origin_codes}")
    return await process_request(origin_codes, db_checker, "yearly")


@router.get("/recent_document_counts_by_month")
async def get_recent_document_counts_by_month(
    origin_codes: str = Query(..., description="Comma-separated list of origin codes"),
    db_checker: DatabaseQualityChecker = Depends(get_db_checker),
) -> List[Dict[str, Any]]:
    """Get monthly document counts for specified origin codes"""
    logger.debug(f"Processing monthly request for origin codes: {origin_codes}")
    return await process_request(origin_codes, db_checker, "monthly")
