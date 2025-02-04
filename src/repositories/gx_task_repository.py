import datetime
from typing import Protocol, Sequence

from sqlmodel import Field, SQLModel


class GxTaskBase(SQLModel):
    component_id: str = Field(..., primary_key=True)
    passive_policy_id: str
    full_descriptor: str


class GxTask(GxTaskBase, table=True):
    created_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    updated_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )


class GxTaskRepositoryError:
    def __init__(self, error_msg: str):
        self.error_msg = error_msg


class GxTaskRepository(Protocol):
    def upsert_task(self, task: GxTaskBase) -> GxTaskBase | GxTaskRepositoryError:
        pass

    def delete_tasks_by_component_ids(
        self, component_ids: list[str]
    ) -> None | GxTaskRepositoryError:
        pass

    def get_all_tasks(self) -> Sequence[GxTaskBase] | GxTaskRepositoryError:
        pass
