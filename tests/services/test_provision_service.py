from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from src.models.api_models import ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.repositories.gx_task_repository import GxTaskBase, GxTaskRepositoryError
from src.services.provision_service import ProvisionService
from src.services.validation_service import validate_components
from src.utility.parsing_pydantic_models import parse_yaml_with_model


@pytest.fixture(name="descriptor_str")
def descriptor_str_fixture():
    return Path("tests/descriptors/descriptor_valid.yaml").read_text()


@pytest.fixture(name="unpacked_request")
def unpacked_request_fixture(descriptor_str):
    request = yaml.safe_load(descriptor_str)
    data_product = parse_yaml_with_model(request.get("dataProduct"), DataProduct)
    component_to_provision = request.get("componentIdToProvision")
    return validate_components((data_product, component_to_provision))


def test_provision_ok(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    repository = Mock()
    task = GxTaskBase(component_id="", passive_policy_id="", full_descriptor="")
    repository.upsert_task.return_value = task
    provisioner = ProvisionService(repository=repository)

    provisioning_status = provisioner.provision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


def test_provision_ko(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    repository = Mock()
    error_msg = "error"
    error = GxTaskRepositoryError(error_msg=error_msg)
    repository.upsert_task.return_value = error
    provisioner = ProvisionService(repository=repository)

    system_err = provisioner.provision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(system_err, SystemErr)
    assert system_err.error == error_msg


def test_unprovision_ok(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    repository = Mock()
    repository.delete_tasks_by_component_ids.return_value = None
    provisioner = ProvisionService(repository=repository)

    provisioning_status = provisioner.unprovision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


def test_unprovision_ko(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    repository = Mock()
    error_msg = "error"
    error = GxTaskRepositoryError(error_msg=error_msg)
    repository.delete_tasks_by_component_ids.return_value = error
    provisioner = ProvisionService(repository=repository)

    system_err = provisioner.unprovision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(system_err, SystemErr)
    assert system_err.error == error_msg
