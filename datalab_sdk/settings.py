from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    # Paths
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    LOGLEVEL: str = "DEBUG"

    # Base settings
    DATALAB_API_KEY: str | None = None
    DATALAB_HOST: str = "https://www.datalab.to"


settings = Settings()
