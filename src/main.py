from __future__ import annotations

import uuid

from fastapi import Request
from starlette.background import BackgroundTask
from starlette.responses import Response

from src.app_config import app
from src.check_return_type import check_response
from src.dependencies import (
    ProvisionServiceDep,
    UnpackedUpdateAclRequestDep,
)
from src.models.api_models import (
    ProvisioningRequest,
    ProvisioningStatus,
    SystemErr,
    ValidationError,
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
)
from src.services.validation_service import ValidateComponentsDep
from src.utility.logger import get_logger

logger = get_logger()


def log_info(req_body, res_body):
    id = str(uuid.uuid4())
    logger.info("[%s] REQUEST: %s", id, req_body.decode("utf-8"))
    logger.info("[%s] RESPONSE: %s", id, res_body.decode("utf-8"))


@app.middleware("http")
async def log_request_response_middleware(request: Request, call_next):
    req_body = await request.body()
    response = await call_next(request)
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk)
    res_body = b"".join(chunks)
    task = BackgroundTask(log_info, req_body, res_body)
    return Response(
        content=res_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
        background=task,
    )


@app.post(
    "/v1/provision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def provision(
    request: ValidateComponentsDep,
    provision_service: ProvisionServiceDep,
    provisioning_request: ProvisioningRequest,
) -> Response:
    """
    Deploy a data product or a single component starting from a provisioning descriptor
    """

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, workload, data_contracts = request

    resp = provision_service.provision(
        data_product, workload, data_contracts, provisioning_request.descriptor
    )

    return check_response(out_response=resp)


@app.get(
    "/v1/provision/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_status(token: str) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/unprovision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def unprovision(
    request: ValidateComponentsDep,
    provision_service: ProvisionServiceDep,
    provisioning_request: ProvisioningRequest,
) -> Response:
    """
    Undeploy a data product or a single component
    given the provisioning descriptor relative to the latest complete provisioning request
    """  # noqa: E501

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, workload, data_contracts = request

    resp = provision_service.unprovision(
        data_product, workload, data_contracts, provisioning_request.descriptor
    )

    return check_response(out_response=resp)


@app.post(
    "/v1/updateacl",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def updateacl(request: UnpackedUpdateAclRequestDep) -> Response:
    """
    Request the access to a specific provisioner component
    """

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, component_id, witboost_users = request

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/validate",
    response_model=None,
    responses={"200": {"model": ValidationResult}, "500": {"model": SystemErr}},
    tags=["SpecificProvisioner"],
)
def validate(request: ValidateComponentsDep) -> Response:
    """
    Validate a provisioning request
    """

    if isinstance(request, ValidationError):
        return check_response(ValidationResult(valid=False, error=request))

    return check_response(out_response=ValidationResult(valid=True))


@app.post(
    "/v2/validate",
    response_model=None,
    responses={
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def async_validate(
    body: ValidationRequest,
) -> Response:
    """
    Validate a deployment request
    """

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.get(
    "/v2/validate/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ValidationStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_validation_status(
    token: str,
) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)
