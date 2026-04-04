from pydantic_settings import BaseSettings, SettingsConfigDict


class ResourceAPIkey(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    bls_api_key: str | None = None
    fred_api_key: str | None = None
    bea_api_key: str | None = None
    postgres_dsn: str | None = None
