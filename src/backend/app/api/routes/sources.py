from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List

router = APIRouter()

@router.get("/document_counts_by_year")
async def get_document_counts_by_year(origin_codes: str, db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    origin_codes_list = origin_codes.split(',')
    result = await db_checker.get_document_counts_by_year(origin_codes_list)
    if not result:
        raise HTTPException(status_code=404, detail=f"No data found for origin codes: {origin_codes}")
    return result

@router.get("/recent_document_counts_by_month")
async def get_recent_document_counts_by_month(origin_codes: str, db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    origin_codes_list = origin_codes.split(',')
    result = await db_checker.get_recent_document_counts_by_month(origin_codes_list)
    if not result:
        raise HTTPException(status_code=404, detail=f"No data found for origin codes: {origin_codes}")
    return result