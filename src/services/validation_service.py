from typing import Annotated, Tuple

import pydantic
from fastapi import Depends

from src.dependencies import UnpackedProvisioningRequestDep
from src.models.api_models import ValidationError
from src.models.data_product_descriptor import DataProduct
from src.models.gx_models import GXComponent, GXGuardianWorkload, SnowflakeOutputPort
from src.utility.logger import get_logger

logger = get_logger(__name__)


def validate_components(
    request: UnpackedProvisioningRequestDep,
) -> Tuple[DataProduct, GXGuardianWorkload, list[GXComponent]] | ValidationError:
    if isinstance(request, ValidationError):
        return request

    data_product, component_id = request

    try:
        component_to_provision: GXGuardianWorkload = (
            data_product.get_typed_component_by_id(component_id, GXGuardianWorkload)
        )
    except pydantic.ValidationError as ve:
        error_msg = (
            f"Failed to parse the component {component_id} as a GXGuardianWorkload:"
        )
        logger.exception(error_msg)
        combined = [error_msg]
        combined.extend(
            map(
                str,
                ve.errors(
                    include_url=False, include_context=False, include_input=False
                ),
            )
        )
        return ValidationError(errors=combined)

    if component_to_provision is None:
        error_msg = f"Component with ID {component_id} not found in descriptor"
        logger.error(error_msg)
        return ValidationError(errors=[error_msg])

    data_contract_components_to_guard = [
        cmp
        for cmp in data_product.get_data_contract_components()
        if cmp.id
        in map(
            lambda x: x.dataContractId,
            component_to_provision.dataContractGuardianSpec.guards,
        )
    ]

    len_components_to_guard = len(
        component_to_provision.dataContractGuardianSpec.guards
    )
    len_dcs_to_guard = len(data_contract_components_to_guard)
    if len_dcs_to_guard != len_components_to_guard:
        error_msg = f"Not all components to guard ({len_components_to_guard}) are defined as Data Contracts in descriptor ({len_dcs_to_guard})"  # noqa: E501
        logger.error(error_msg)
        return ValidationError(errors=[error_msg])

    try:
        # at the moment only snowflake ops are supported
        snowflake_output_ports: list[GXComponent] = [
            SnowflakeOutputPort.model_validate(dc_cmp.model_dump(by_alias=True))
            for dc_cmp in data_contract_components_to_guard
        ]
    except pydantic.ValidationError as ve:
        logger.exception(
            "The Data Contract component is not a valid Snowflake Ouput Port"
        )
        combined = [
            "One or more components to guard are not supported by this Tech Adapter:"
        ]
        combined.extend(
            map(
                str,
                ve.errors(
                    include_url=False, include_context=False, include_input=False
                ),
            )
        )
        return ValidationError(errors=combined)

    return data_product, component_to_provision, snowflake_output_ports


ValidateComponentsDep = Annotated[
    Tuple[DataProduct, GXGuardianWorkload, list[GXComponent]] | ValidationError,
    Depends(validate_components),
]
