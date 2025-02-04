from pathlib import Path

from src.models.api_models import ValidationError
from src.models.data_product_descriptor import DataProduct
from src.services.validation_service import validate_components
from src.utility.parsing_pydantic_models import parse_yaml_with_model


def test_validate_components_valid_guardian():
    descriptor_str = Path(
        "tests/descriptors/data_product_with_guardian_valid.yaml"
    ).read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)

    actual_res = validate_components(
        (result, "urn:dmb:cmp:marketing:system-with-data-contract:0:guardian")
    )

    assert not isinstance(actual_res, ValidationError)
    _, _, ops = actual_res
    assert len(ops) == 1


def test_validate_components_is_validation_error():
    result = ValidationError(errors=["error"])

    actual_res = validate_components(result)

    assert isinstance(actual_res, ValidationError)
    assert actual_res == result


def test_validate_components_not_valid_guardian():
    descriptor_str = Path(
        "tests/descriptors/data_product_with_guardian_not_valid.yaml"
    ).read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)

    actual_res = validate_components(
        (result, "urn:dmb:cmp:marketing:system-with-data-contract:0:guardian")
    )

    assert isinstance(actual_res, ValidationError)
    assert len(actual_res.errors) == 2
    assert (
        "Failed to parse the component urn:dmb:cmp:marketing:system-with-data-contract:0:guardian as a GXGuardianWorkload:"  # noqa: E501
        == actual_res.errors[0]
    )


def test_validate_components_no_guardian():
    descriptor_str = Path("tests/descriptors/data_product_valid.yaml").read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)
    component_id = "urn:dmb:cmp:marketing:system-with-data-contract:0:guardian"

    actual_res = validate_components((result, component_id))

    assert isinstance(actual_res, ValidationError)
    assert len(actual_res.errors) == 1
    assert (
        f"Component with ID {component_id} not found in descriptor"
        == actual_res.errors[0]
    )


def test_validate_components_no_data_contracts():
    descriptor_str = Path(
        "tests/descriptors/data_product_with_guardian_incomplete.yaml"
    ).read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)
    component_id = "urn:dmb:cmp:marketing:system-with-data-contract:0:guardian"

    actual_res = validate_components((result, component_id))

    assert isinstance(actual_res, ValidationError)
    assert len(actual_res.errors) == 1
    assert (
        "Not all components to guard (1) are defined as Data Contracts in descriptor (0)"
        == actual_res.errors[0]
    )


def test_validate_components_unsupported_data_contracts():
    descriptor_str = Path(
        "tests/descriptors/data_product_with_guardian_unsupported_op.yaml"
    ).read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)
    component_id = "urn:dmb:cmp:marketing:system-with-data-contract:0:guardian"

    actual_res = validate_components((result, component_id))

    assert isinstance(actual_res, ValidationError)
    assert len(actual_res.errors) == 6
    assert (
        "One or more components to guard are not supported by this Tech Adapter:"
        == actual_res.errors[0]
    )
