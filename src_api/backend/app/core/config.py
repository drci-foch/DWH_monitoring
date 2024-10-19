import secrets
from typing import Annotated, Any, Literal

from pydantic import (
    AnyUrl,
    BeforeValidator,
    PostgresDsn,
    computed_field,
 
)
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict



def parse_cors(v: Any) -> list[str] | str:
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Use top level .env file (one level above ./backend/)
        env_file="../.env",
        env_ignore_empty=True,
        extra="ignore",
    )
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    FRONTEND_HOST: str = "http://localhost:5173"
    ENVIRONMENT: Literal["local", "staging", "production"] = "local"

    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def all_cors_origins(self) -> list[str]:
        return [str(origin).rstrip("/") for origin in self.BACKEND_CORS_ORIGINS] + [
            self.FRONTEND_HOST
        ]
        
    
    PROJECT_NAME: str
    DWH_PORT: int = 5432
    DWH_USERNAME: str
    DWH_PASSWORD: str = ""
    DWH_DATABASE: str = ""
    DWH_HOSTNAME: str = ""
    DWH_SERVICE_NAME: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> PostgresDsn:
        return MultiHostUrl.build(
            scheme="oracle+oracledb",
            username=self.DWH_USERNAME,
            password=self.DWH_PASSWORD,
            host=self.DWH_HOSTNAME,
            port=self.DWH_PORT,
            path=self.DWH_DATABASE,
        )
        # return f"oracle+oracledb://{self.DWH_USERNAME}:{self.DWH_PASSWORD}@{self.DWH_HOSTNAME}:{self.DWH_PORT}/?service_name={self.DWH_SERVICE_NAME}"


    
settings = Settings()