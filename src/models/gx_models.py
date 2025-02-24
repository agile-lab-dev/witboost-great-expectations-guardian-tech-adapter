from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field

from src.models.data_product_descriptor import DataContract, OutputPort, Workload


class GXWorkloadSpecific(BaseModel):
    pass


class Guard(BaseModel):
    dataContractId: str
    monitoringResultScheduling: dict | None = None


class DataContractGuardianSpec(BaseModel):
    guards: list[Guard]


class DataContractGuardian(BaseModel):
    policyId: str


class GuardianPrivateInfo(BaseModel):
    dataContractGuardian: DataContractGuardian = Field(alias="__dataContractGuardian")


class GuardianInfo(BaseModel):
    privateInfo: GuardianPrivateInfo


class GXGuardianWorkload(Workload):
    dataContractGuardianSpec: DataContractGuardianSpec = Field(
        alias="__dataContractGuardianSpec"
    )
    specific: GXWorkloadSpecific
    info: GuardianInfo | None = None


class GXImplementation(BaseModel):
    type: str
    args: dict[str, Any]


class GXExpectation(BaseModel):
    type: Literal["custom"]
    engine: Literal["greatExpectations"]
    implementation: GXImplementation


class GxDataContract(DataContract):
    quality: list[GXExpectation]


class SnowflakeSpecific(BaseModel):
    database: str
    schema_: str = Field(..., alias="schema")
    tableName: str
    viewName: str


class SnowflakeOutputPort(OutputPort):
    technology: Literal["Snowflake"]
    dataContract: GxDataContract
    specific: SnowflakeSpecific

    def get_id(self) -> str:
        return self.id

    def get_contract(self) -> GxDataContract:
        return self.dataContract

    def get_technology(self) -> str:
        return self.technology

    def get_database(self) -> str:
        return self.specific.database

    def get_schema(self) -> str:
        return self.specific.schema_

    def get_table(self) -> str:
        return self.specific.tableName


class GXComponent(Protocol):
    def get_id(self) -> str:
        pass

    def get_contract(self) -> GxDataContract:
        pass

    def get_technology(self) -> str:
        pass

    def get_database(self) -> str:
        pass

    def get_schema(self) -> str:
        pass

    def get_table(self) -> str:
        pass
