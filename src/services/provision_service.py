from src.models.api_models import ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.models.gx_models import GXComponent, GXGuardianWorkload
from src.repositories.gx_task_repository import (
    GxTaskBase,
    GxTaskRepository,
    GxTaskRepositoryError,
)
from src.utility.logger import get_logger


class ProvisionService:
    def __init__(self, repository: GxTaskRepository):
        self.repository = repository
        self.logger = get_logger(__name__)

    def provision(
        self,
        data_product: DataProduct,
        workload: GXGuardianWorkload,
        data_contracts: list[GXComponent],
        full_descriptor: str,
    ) -> ProvisioningStatus | SystemErr:
        for data_contract in data_contracts:
            task = GxTaskBase(
                component_id=data_contract.get_id(),
                passive_policy_id=workload.info.privateInfo.dataContractGuardian.policyId,
                full_descriptor=full_descriptor,
            )
            res = self.repository.upsert_task(task)
            if isinstance(res, GxTaskRepositoryError):
                return SystemErr(error=res.error_msg)
        return ProvisioningStatus(status=Status1.COMPLETED, result="")

    def unprovision(
        self,
        data_product: DataProduct,
        workload: GXGuardianWorkload,
        data_contracts: list[GXComponent],
        full_descriptor: str,
    ) -> ProvisioningStatus | SystemErr:
        res = self.repository.delete_tasks_by_component_ids(
            list(map(lambda dc: dc.get_id(), data_contracts))
        )
        if isinstance(res, GxTaskRepositoryError):
            return SystemErr(error=res.error_msg)
        return ProvisioningStatus(status=Status1.COMPLETED, result="")
