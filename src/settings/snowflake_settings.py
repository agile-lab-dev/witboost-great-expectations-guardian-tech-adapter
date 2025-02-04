from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    username: str
    password: str
    account_identifier: str
    database_name: str
    schema_name: str
    warehouse_name: str
    role_name: str

    model_config = SettingsConfigDict(env_file=".env", env_prefix="snowflake_")
