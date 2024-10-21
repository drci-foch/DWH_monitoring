from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any

router = APIRouter()

@router.get("/document_counts")
async def get_document_counts(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        result = await db_checker.get_document_counts()
        if not result:
            raise HTTPException(status_code=404, detail="No document counts available")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/document_counts_by_year/{origin_code}")
async def get_document_counts_by_year(origin_code: str, db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        result = await db_checker.get_document_counts_by_year(origin_code)
        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for origin code: {origin_code}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/recent_document_counts_by_month/{origin_code}")
async def get_recent_document_counts_by_month(origin_code: str, db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        result = await db_checker.get_recent_document_counts_by_month(origin_code)
        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for origin code: {origin_code}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")