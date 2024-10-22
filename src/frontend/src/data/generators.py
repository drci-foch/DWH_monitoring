from datetime import datetime, timedelta
import random
import math
from typing import Dict, List, Any

def generate_document_counts() -> List[Dict[str, Any]]:
    """
    Generate all document counts data.
    
    Returns:
        List[Dict[str, Any]]: List of document counts by origin code
    """
    origin_codes = [
        "EMR", "LAB", "RAD", "SCAN", "NOTE", "PATH", "PROC", 
        "REPORT", "CONSULT", "ADMIN", "IMAGE", "TEST"
    ]
    
    return [
        {
            "document_origin_code": code,
            "unique_document_count": random.randint(1000, 10000)
        }
        for code in origin_codes
    ]
    
def generate_document_counts_by_year(origin_codes: List[str]) -> List[Dict[str, Any]]:
    """Generate yearly document counts for specified origin codes"""
    current_date = datetime.now()
    years = [(current_date - timedelta(days=365 * i)).year for i in range(5, -1, -1)]  # Last 5 years

    data = []
    for code in origin_codes:
        base_count = random.randint(1000, 5000)
        for year in years:
            # Create an upward trend with some randomness
            yearly_count = int(base_count * (1 + (year - min(years)) * 0.1 * random.uniform(0.8, 1.2)))
            data.append({
                "document_origin_code": code,
                "year": year,  # Keep as integer
                "count": yearly_count
            })
    
    return data
def generate_recent_document_counts_by_month(origin_codes: List[str]) -> List[Dict[str, Any]]:
    """
    Generate monthly document counts for specified origin codes.
    
    Args:
        origin_codes (List[str]): List of document origin codes to generate data for
        
    Returns:
        List[Dict[str, Any]]: List of monthly document counts by origin code
    """
    current_date = datetime.now()
    # Generate dates for the last 12 months
    months = [
        (current_date - timedelta(days=30 * i)).replace(day=1)
        for i in range(11, -1, -1)  # Last 12 months
    ]
    
    data = []
    for code in origin_codes:
        base_count = random.randint(100, 500)
        for month in months:
            # Create a realistic trend with seasonal variation
            seasonal_factor = 1 + 0.2 * math.sin(month.month * math.pi / 6)  # Seasonal variation
            monthly_count = int(base_count * seasonal_factor * random.uniform(0.8, 1.2))
            data.append({
                "document_origin_code": code,
                "month": month.strftime("%Y-%m-%d"),
                "count": monthly_count
            })
    
    return data

def generate_top_users(current_year: bool = False) -> List[Dict[str, Any]]:
    """
    Generate simulated top users data.
    
    Args:
        current_year (bool): If True, generates data for current year only
        
    Returns:
        List[Dict[str, Any]]: List of top users with query counts
    """
    users = [
        ("CODOC", "CODOC", random.randint(800, 1000)),  # CODOC team with highest usage
        ("John", "Smith", random.randint(500, 800)),
        ("Emma", "Johnson", random.randint(400, 700)),
        ("Michael", "Brown", random.randint(300, 600)),
        ("Sarah", "Davis", random.randint(200, 500)),
        ("David", "Wilson", random.randint(100, 400)),
        ("Lisa", "Taylor", random.randint(50, 300)),
        ("James", "Anderson", random.randint(25, 200)),
        ("Emily", "Thomas", random.randint(10, 150)),
        ("Robert", "Moore", random.randint(5, 100))
    ]
    
    # Reduce query counts for current year to make it more realistic
    if current_year:
        users = [(f, l, count // 2) for f, l, count in users]
    
    return [
        {
            "firstname": first,
            "lastname": last,
            "query_count": count
        }
        for first, last, count in users
    ]

def generate_archive_sample_data() -> Dict[str, Any]:
    """
    Generate sample archive data matching the CRUD structure.
    
    Returns:
        Dict[str, Any]: Archive data including period and documents to suppress
    """
    current_date = datetime.now()
    oldest_date = current_date - timedelta(days=random.randint(2000, 3000))  # 5.5-8.2 years
    archive_period = (current_date - oldest_date).days / 365.25
    
    # Generate documents to suppress based on realistic origin codes
    origin_codes = ["EMR", "LAB", "RAD", "SCAN", "NOTE", "PATH", "PROC"]
    documents_to_suppress = [
        (code, random.randint(100, 5000))
        for code in origin_codes
    ]
    
    total_to_suppress = sum(count for _, count in documents_to_suppress)
    
    return {
        "archive_period": archive_period,
        "total_documents_to_suppress": total_to_suppress,
        "documents_to_suppress": documents_to_suppress
    }

def generate_sample_data() -> Dict[str, Any]:
    """
    Generate complete sample dataset.
    
    Returns:
        Dict[str, Any]: Complete sample data including summary and top users
    """
    return {
        "summary": {
            "patient_count": random.randint(50000, 100000),
            "test_patient_count": random.randint(1000, 5000),
            "research_patient_count": random.randint(500, 2000),
            "celebrity_patient_count": random.randint(10, 100),
            "total_documents": random.randint(200000, 500000),
            "recent_documents": random.randint(1000, 5000)
        },
        "top_users": [
            {
                "firstname": first,
                "lastname": last,
                "query_count": random.randint(100, 1000)
            }
            for first, last in [
                ("John", "Smith"), ("Emma", "Johnson"), ("Michael", "Brown"),
                ("Sarah", "Davis"), ("David", "Wilson"), ("Lisa", "Taylor"),
                ("James", "Anderson"), ("Emily", "Thomas"), ("Robert", "Moore"),
                ("Jessica", "Martin")
            ]
        ]
    }