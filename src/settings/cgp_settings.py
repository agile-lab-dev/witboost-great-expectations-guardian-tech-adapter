from pydantic_settings import BaseSettings, SettingsConfigDict


class CGPSettings(BaseSettings):
    base_url: str

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="cgp_", extra="ignore"
    )
