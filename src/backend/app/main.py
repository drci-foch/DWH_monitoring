from app.core.config import settings
from app.api.main import api_router
from app.core.db import engine
from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="Monitoring of the DWH database")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles database connection pool and other resource management.
    """
    logger.info("🚀 Application startup...")
    try:
        logger.info("✅  Database connection pool initialized")
        yield
    except Exception as e:
        logger.error(f"🛑 Error during startup: {e}")
        raise
    finally:
        # Shutdown
        logger.info("🌐 Application shutdown...")
        try:
            await engine.dispose()
            logger.info("❌ Database connection pool closed")
        except Exception as e:
            logger.error(f"🛑 Error during shutdown: {e}")
            
app.include_router(api_router, prefix=settings.API_V1_STR, lifespan=lifespan)
