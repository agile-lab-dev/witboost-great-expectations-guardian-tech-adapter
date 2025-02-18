from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from src.models.api_models import ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.repositories.dag_repository import DagRepositoryError
from src.services.provision_service import ProvisionService
from src.services.template_service import TemplateService, TemplateServiceError
from src.services.validation_service import validate_components
from src.settings.airflow_settings import AirflowSettings
from src.settings.cgp_settings import CGPSettings
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
    dag_repository = Mock()
    cgp_settings = CGPSettings(base_url="http://localhost:8088")
    template_service = TemplateService()
    dag_repository.create_or_update_dag.return_value = None
    airflow_settings = AirflowSettings(connection_id="connection_id")
    provisioner = ProvisionService(
        dag_repository, template_service, cgp_settings, airflow_settings
    )

    provisioning_status = provisioner.provision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


def test_provision_ko_create_dag(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    dag_repository = Mock()
    error_msg = "error"
    error = DagRepositoryError(error_msg=error_msg)
    dag_repository.create_or_update_dag.return_value = error
    cgp_settings = CGPSettings(base_url="http://localhost:8088")
    template_service = TemplateService()
    airflow_settings = AirflowSettings(connection_id="connection_id")
    provisioner = ProvisionService(
        dag_repository, template_service, cgp_settings, airflow_settings
    )

    system_err = provisioner.provision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(system_err, SystemErr)
    assert system_err.error == error_msg


def test_provision_ko_render_template(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    dag_repository = Mock()
    dag_repository.create_or_update_dag.return_value = None
    cgp_settings = CGPSettings(base_url="http://localhost:8088")
    error_msg = "error"
    error = TemplateServiceError(error_msg=error_msg)
    template_service = Mock()
    template_service.render_template.return_value = error
    airflow_settings = AirflowSettings(connection_id="connection_id")
    provisioner = ProvisionService(
        dag_repository, template_service, cgp_settings, airflow_settings
    )

    system_err = provisioner.provision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(system_err, SystemErr)
    assert system_err.error == error_msg


def test_unprovision_ok(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    dag_repository = Mock()
    dag_repository.delete_dags.return_value = None
    cgp_settings = CGPSettings(base_url="http://localhost:8088")
    template_service = TemplateService()
    airflow_settings = AirflowSettings(connection_id="connection_id")
    provisioner = ProvisionService(
        dag_repository, template_service, cgp_settings, airflow_settings
    )

    provisioning_status = provisioner.unprovision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


def test_unprovision_ko(unpacked_request, descriptor_str):
    data_product, workload, data_contracts = unpacked_request
    dag_repository = Mock()
    error_msg = "error"
    error = DagRepositoryError(error_msg=error_msg)
    dag_repository.delete_dags.return_value = error
    cgp_settings = CGPSettings(base_url="http://localhost:8088")
    template_service = TemplateService()
    airflow_settings = AirflowSettings(connection_id="connection_id")
    provisioner = ProvisionService(
        dag_repository, template_service, cgp_settings, airflow_settings
    )

    system_err = provisioner.unprovision(
        data_product, workload, data_contracts, descriptor_str
    )

    assert isinstance(system_err, SystemErr)
    assert system_err.error == error_msg
