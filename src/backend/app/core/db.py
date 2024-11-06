from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import QueuePool
import multiprocessing as mp
from app.core.config import settings

# Calculate optimal pool size based on CPU count
DEFAULT_POOL_SIZE = mp.cpu_count() * 2
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_POOL_TIMEOUT = 30

# Create pooled engine
engine = create_async_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    poolclass=QueuePool,
    pool_size=DEFAULT_POOL_SIZE,
    max_overflow=DEFAULT_MAX_OVERFLOW,
    pool_timeout=DEFAULT_POOL_TIMEOUT,
    pool_pre_ping=True,  # Enable connection health checks
    pool_recycle=3600,  # Recycle connections after 1 hour
    echo=settings.DB_ECHO,  # SQL query logging
)