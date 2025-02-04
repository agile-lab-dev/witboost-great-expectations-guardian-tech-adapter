import datetime
from typing import Sequence

from sqlalchemy import Engine
from sqlmodel import Session, delete, select

from src.repositories.gx_task_repository import (
    GxTask,
    GxTaskBase,
    GxTaskRepositoryError,
)
from src.utility.logger import get_logger


class SnowflakeGxTaskRepository:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.logger = get_logger(__name__)

    def upsert_task(self, task: GxTaskBase) -> GxTaskBase | GxTaskRepositoryError:
        try:
            with Session(self.engine) as session:
                statement = select(GxTask).where(
                    GxTask.component_id == task.component_id
                )
                result = session.exec(statement).first()
                if result is None:
                    result = GxTask.model_validate(task)
                else:
                    result.updated_at = datetime.datetime.now(datetime.timezone.utc)
                for key, value in task.model_dump(exclude_unset=True).items():
                    setattr(result, key, value)
                session.add(result)
                session.commit()
                session.refresh(result)
                return result
        except Exception as e:
            self.logger.exception("Exception in upsert_task")
            error_msg = f"An unexpected error occurred. Please try again or contact the platform team. Details: {str(e)}"  # noqa: E501
            return GxTaskRepositoryError(error_msg=error_msg)

    def delete_tasks_by_component_ids(
        self, component_ids: list[str]
    ) -> None | GxTaskRepositoryError:
        try:
            with Session(self.engine) as session:
                stmt = delete(GxTask).where(GxTask.component_id.in_(component_ids))  # type: ignore[attr-defined]
                session.exec(stmt)  # type: ignore[call-overload]
                session.commit()
                return None
        except Exception as e:
            self.logger.exception("Exception in delete_tasks_by_component_ids")
            error_msg = f"An unexpected error occurred. Please try again or contact the platform team. Details: {str(e)}"  # noqa: E501
            return GxTaskRepositoryError(error_msg=error_msg)

    def get_all_tasks(self) -> Sequence[GxTaskBase] | GxTaskRepositoryError:
        try:
            with Session(self.engine) as session:
                return session.exec(select(GxTask)).all()
        except Exception as e:
            self.logger.exception("Exception in get_all_tasks")
            error_msg = f"An unexpected error occurred. Please try again or contact the platform team. Details: {str(e)}"  # noqa: E501
            return GxTaskRepositoryError(error_msg=error_msg)
