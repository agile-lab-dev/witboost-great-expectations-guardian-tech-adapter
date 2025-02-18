import json

import yaml
from fastapi.encoders import jsonable_encoder

from src.models.api_models import ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.models.gx_models import GXComponent, GXGuardianWorkload
from src.repositories.dag_repository import DagRepository, DagRepositoryError
from src.services.template_service import TemplateService, TemplateServiceError
from src.settings.airflow_settings import AirflowSettings
from src.settings.cgp_settings import CGPSettings
from src.utility.logger import get_logger


class ProvisionService:
    def __init__(
        self,
        dag_repository: DagRepository,
        template_service: TemplateService,
        cgp_settings: CGPSettings,
        airflow_settings: AirflowSettings,
    ):
        self.dag_repository = dag_repository
        self.template_service = template_service
        self.cgp_settings = cgp_settings
        self.airflow_settings = airflow_settings
        self.logger = get_logger(__name__)

    def provision(
        self,
        data_product: DataProduct,
        workload: GXGuardianWorkload,
        data_contracts: list[GXComponent],
        full_descriptor: str,
    ) -> ProvisioningStatus | SystemErr:
        descriptor_json_str = json.dumps(
            jsonable_encoder(yaml.safe_load(full_descriptor))
        )
        for data_contract in data_contracts:
            self.logger.info(
                "Rendering DAG Template for data contract with ID: %s",
                data_contract.get_id(),
            )
            params = {
                "data_contract_id": data_contract.get_id(),
                "environment": data_product.environment,
                "descriptor": descriptor_json_str,
                "passive_policy_id": workload.info.privateInfo.dataContractGuardian.policyId,
                "cgp_base_url": self.cgp_settings.base_url,
                "airflow_connection_id": self.airflow_settings.connection_id,
            }
            rendered_dag = self.template_service.render_template(
                data_contract.get_technology(), params
            )
            if isinstance(rendered_dag, TemplateServiceError):
                return SystemErr(error=rendered_dag.error_msg)
            self.logger.info(
                "Publishing DAG for data contract with ID: %s", data_contract.get_id()
            )
            res = self.dag_repository.create_or_update_dag(
                data_contract.get_id(), rendered_dag, data_product.environment
            )
            if isinstance(res, DagRepositoryError):
                return SystemErr(error=res.error_msg)
        return ProvisioningStatus(status=Status1.COMPLETED, result="")

    def unprovision(
        self,
        data_product: DataProduct,
        workload: GXGuardianWorkload,
        data_contracts: list[GXComponent],
        full_descriptor: str,
    ) -> ProvisioningStatus | SystemErr:
        self.logger.info(
            "Deleting DAGs for data contracts: %s",
            ",".join(list(map(lambda dc: dc.get_id(), data_contracts))),
        )
        res = self.dag_repository.delete_dags(
            list(map(lambda dc: dc.get_id(), data_contracts)), data_product.environment
        )
        if isinstance(res, DagRepositoryError):
            return SystemErr(error=res.error_msg)
        return ProvisioningStatus(status=Status1.COMPLETED, result="")
