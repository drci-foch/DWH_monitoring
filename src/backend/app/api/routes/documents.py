from fastapi import APIRouter, Depends, HTTPException
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker
from typing import List, Dict, Any

router = APIRouter()


@router.get("/api/document_metrics", response_model=Dict[str, float])
async def get_document_metrics(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        all_stats = await db_checker.get_all_statistics()
        document_metrics = all_stats.get('document_metrics', {})
        if not document_metrics:
            raise HTTPException(status_code=404, detail="Document metrics not found")
        return document_metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/api/document_counts", response_model=List[Dict[str, Any]])
async def get_document_counts(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        all_stats = await db_checker.get_all_statistics()
        document_counts = all_stats.get('document_counts', [])
        return document_counts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/api/recent_document_counts", response_model=List[Dict[str, Any]])
async def get_recent_document_counts(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        all_stats = await db_checker.get_all_statistics()
        recent_document_counts = all_stats.get('recent_document_counts', [])
        
        return recent_document_counts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")