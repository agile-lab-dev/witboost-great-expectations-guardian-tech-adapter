from pydantic_settings import BaseSettings, SettingsConfigDict


class AirflowSettings(BaseSettings):
    connection_id: str

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="airflow_", extra="ignore"
    )
