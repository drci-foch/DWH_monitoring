from fastapi import APIRouter

from app.api.routes import summary, users, archives, documents, sources

api_router = APIRouter()
api_router.include_router(summary.router, prefix="/summary", tags=["summary"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(documents.router, prefix="/documents", tags=["documents"])
api_router.include_router(sources.router, prefix="/sources", tags=["sources"])
api_router.include_router(archives.router, prefix="/archives", tags=["archives"])