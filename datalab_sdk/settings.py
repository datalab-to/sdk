from importlib.metadata import PackageNotFoundError, version as _package_version
import os

from pydantic_settings import BaseSettings


def _sdk_version() -> str:
    """Return the installed package version, so it cannot drift from pyproject."""
    try:
        return _package_version("datalab-python-sdk")
    except PackageNotFoundError:
        return "0.0.0+unknown"


class Settings(BaseSettings):
    # Paths
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    LOGLEVEL: str = "DEBUG"
    VERSION: str = _sdk_version()

    # Base settings
    DATALAB_API_KEY: str | None = None
    DATALAB_HOST: str = "https://www.datalab.to"


settings = Settings()
