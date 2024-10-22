from fastapi import APIRouter, Depends, HTTPException
from app.core.db import engine
from app.crud import DatabaseQualityChecker
from app.dependencies import get_db_checker

router = APIRouter()
@router.get("/api/summary")
async def get_summary(db_checker: DatabaseQualityChecker = Depends(get_db_checker)):
    try:
        all_stats = await db_checker.get_all_statistics()
        
        def sum_counts(data):
            print("Raw data received:")
            print(data)
            print("Type of data:", type(data))
            
            total = sum(item['unique_document_count'] for item in data if isinstance(item, dict) and 'unique_document_count' in item)
            print(f"Final sum: {total}")
            return total
        
        total_documents = sum_counts(all_stats.get('document_counts', []))
        recent_documents = sum_counts(all_stats.get('recent_document_counts', []))
        
        return {
            "patient_count": all_stats.get('patient_count', 0),
            "test_patient_count": all_stats.get('test_patient_count', 0),
            "research_patient_count": all_stats.get('research_patient_count', 0),
            "celebrity_patient_count": all_stats.get('celebrity_patient_count', 0),
            "total_documents": total_documents,
            "recent_documents": recent_documents
        }
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

