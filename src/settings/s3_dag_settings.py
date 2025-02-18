from pydantic_settings import BaseSettings, SettingsConfigDict


class S3DagSettings(BaseSettings):
    bucket_name: str
    folder: str

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="s3_dag_", extra="ignore"
    )
