from unittest import mock
from unittest.mock import call

from src.repositories.dag_repository import DagRepositoryError
from src.repositories.s3_dag_repository import S3DagRepository, S3DagSettings

s3_dag_settings = S3DagSettings(bucket_name="bucket_name", folder="dags")
s3_dag_repository = S3DagRepository(s3_dag_settings)
data_contract_id = (
    "urn:dmb:cmp:marketing:system-with-data-contract:0:consumable-data-contract"
)
data_contract_ids = [
    "urn:dmb:cmp:marketing:system-with-data-contract:0:consumable-data-contract-1",
    "urn:dmb:cmp:marketing:system-with-data-contract:0:consumable-data-contract-2",
]
content = "content"
environment = "environment"


@mock.patch("src.repositories.s3_dag_repository.boto3.client")
def test_create_or_update_dag_ok(mock_client):
    mock_client.return_value.put_object.return_value = None

    res = s3_dag_repository.create_or_update_dag(data_contract_id, content, environment)

    assert res is None
    mock_client.return_value.put_object.assert_called_once_with(
        Body=content,
        Bucket="bucket_name",
        Key="dags/dag_urn_dmb_cmp_marketing_system_with_data_contract_0_consumable_data_contract_environment.py",
    )


@mock.patch("src.repositories.s3_dag_repository.boto3.client")
def test_create_or_update_dag_error(mock_client):
    mock_client.return_value.put_object.side_effect = ValueError("error")

    res = s3_dag_repository.create_or_update_dag(data_contract_id, content, environment)

    assert isinstance(res, DagRepositoryError)


@mock.patch("src.repositories.s3_dag_repository.boto3.client")
def test_delete_dags_ok(mock_client):
    mock_client.return_value.delete_object.return_value = None
    calls = [
        call(
            Bucket="bucket_name",
            Key="dags/dag_urn_dmb_cmp_marketing_system_with_data_contract_0_consumable_data_contract_1_environment.py",
        ),
        call(
            Bucket="bucket_name",
            Key="dags/dag_urn_dmb_cmp_marketing_system_with_data_contract_0_consumable_data_contract_2_environment.py",
        ),
    ]

    res = s3_dag_repository.delete_dags(data_contract_ids, environment)

    assert res is None
    mock_client.return_value.delete_object.assert_has_calls(calls, any_order=True)


@mock.patch("src.repositories.s3_dag_repository.boto3.client")
def test_delete_dags_error(mock_client):
    mock_client.return_value.delete_object.side_effect = ValueError("error")

    res = s3_dag_repository.delete_dags(data_contract_ids, environment)

    assert isinstance(res, DagRepositoryError)
