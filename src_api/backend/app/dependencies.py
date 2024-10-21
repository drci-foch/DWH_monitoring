from app.core.db import engine
from app.crud import DatabaseQualityChecker

def get_db_checker():
    return DatabaseQualityChecker(engine=engine)

