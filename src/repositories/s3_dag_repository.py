import re

import boto3

from src.repositories.dag_repository import DagRepositoryError
from src.settings.s3_dag_settings import S3DagSettings
from src.utility.logger import get_logger


class S3DagRepository:
    def __init__(self, s3_dag_settings: S3DagSettings):
        self.s3_dag_settings = s3_dag_settings
        self.logger = get_logger(__name__)

    def create_or_update_dag(
        self, data_contract_id: str, content: str, environment: str
    ) -> None | DagRepositoryError:
        try:
            data_contract_id_sanitized = self._sanitize_string(data_contract_id)
            s3_client = boto3.client("s3")
            key = f"{self._ensure_trailing_slash(self.s3_dag_settings.folder)}dag_{data_contract_id_sanitized}_{environment}.py"  # noqa: E501
            s3_client.put_object(
                Body=content, Bucket=self.s3_dag_settings.bucket_name, Key=key
            )
            return None
        except Exception as e:
            error_msg = f"An error occurred while publishing the DAG related to {data_contract_id}. Please try again later. Details: {str(e)}"  # noqa: E501
            self.logger.exception(error_msg)
            return DagRepositoryError(error_msg=error_msg)

    def delete_dags(
        self, data_contract_ids: list[str], environment: str
    ) -> None | DagRepositoryError:
        try:
            s3_client = boto3.client("s3")
            for data_contract_id in data_contract_ids:
                data_contract_id_sanitized = self._sanitize_string(data_contract_id)
                key = f"{self._ensure_trailing_slash(self.s3_dag_settings.folder)}dag_{data_contract_id_sanitized}_{environment}.py"  # noqa: E501
                s3_client.delete_object(
                    Bucket=self.s3_dag_settings.bucket_name, Key=key
                )
            return None
        except Exception as e:
            error_msg = f"An error occurred while deleting the DAGs. Please try again later. Details: {str(e)}"
            self.logger.exception(error_msg)
            return DagRepositoryError(error_msg=error_msg)

    def _ensure_trailing_slash(self, input: str) -> str:
        return input if input.endswith("/") else input + "/"

    def _sanitize_string(self, input_string: str) -> str:
        allowed_pattern = r"[^\w]"
        return re.sub(allowed_pattern, "_", input_string)
