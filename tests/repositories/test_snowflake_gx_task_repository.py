import pytest
from sqlalchemy import Engine, StaticPool, create_engine
from sqlmodel import Session, SQLModel

from src.repositories.gx_task_repository import GxTask, GxTaskBase, GxTaskRepository
from src.repositories.snowflake_gx_task_repository import SnowflakeGxTaskRepository


@pytest.fixture(name="engine")
def engine_fixture():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    yield engine
    engine.dispose()


def test_upsert_task_is_insert(engine: Engine):
    repository: GxTaskRepository = SnowflakeGxTaskRepository(engine=engine)
    task = GxTaskBase(
        component_id="component1",
        passive_policy_id="policy1",
        full_descriptor="descriptor1",
    )

    created_task = repository.upsert_task(task)

    assert isinstance(created_task, GxTaskBase)
    assert created_task.component_id == "component1"
    assert created_task.passive_policy_id == "policy1"
    assert created_task.full_descriptor == "descriptor1"
    all_tasks = repository.get_all_tasks()
    assert isinstance(all_tasks, list)
    assert len(all_tasks) == 1


def test_upsert_task_is_update(engine: Engine):
    repository: GxTaskRepository = SnowflakeGxTaskRepository(engine=engine)
    with Session(engine) as session:
        record_1 = GxTask(
            component_id="component1",
            passive_policy_id="policy1",
            full_descriptor="descriptor1",
        )
        record_2 = GxTask(
            component_id="component2",
            passive_policy_id="policy2",
            full_descriptor="descriptor2",
        )
        session.add(record_1)
        session.add(record_2)
        session.commit()
    task = GxTaskBase(
        component_id="component1",
        passive_policy_id="policy1_updated",
        full_descriptor="descriptor1_updated",
    )

    updated_task = repository.upsert_task(task)

    assert isinstance(updated_task, GxTaskBase)
    assert updated_task.component_id == "component1"
    assert updated_task.passive_policy_id == "policy1_updated"
    assert updated_task.full_descriptor == "descriptor1_updated"
    all_tasks = repository.get_all_tasks()
    assert isinstance(all_tasks, list)
    assert len(all_tasks) == 2


def test_delete_tasks_by_component_ids(engine: Engine):
    repository: GxTaskRepository = SnowflakeGxTaskRepository(engine=engine)
    with Session(engine) as session:
        record_1 = GxTask(
            component_id="component1",
            passive_policy_id="policy1",
            full_descriptor="descriptor1",
        )
        record_2 = GxTask(
            component_id="component2",
            passive_policy_id="policy2",
            full_descriptor="descriptor2",
        )
        session.add(record_1)
        session.add(record_2)
        session.commit()
    components_ids_to_delete = ["component1", "component3"]

    res = repository.delete_tasks_by_component_ids(
        component_ids=components_ids_to_delete
    )

    assert res is None
    all_tasks = repository.get_all_tasks()
    assert isinstance(all_tasks, list)
    assert len(all_tasks) == 1


def test_get_all_tasks_no_records(engine: Engine):
    repository: GxTaskRepository = SnowflakeGxTaskRepository(engine=engine)

    tasks = repository.get_all_tasks()

    assert isinstance(tasks, list)
    assert len(tasks) == 0


def test_get_all_tasks_with_records(engine: Engine):
    repository: GxTaskRepository = SnowflakeGxTaskRepository(engine=engine)
    with Session(engine) as session:
        record_1 = GxTask(
            component_id="component1",
            passive_policy_id="policy1",
            full_descriptor="descriptor1",
        )
        record_2 = GxTask(
            component_id="component2",
            passive_policy_id="policy2",
            full_descriptor="descriptor2",
        )
        session.add(record_1)
        session.add(record_2)
        session.commit()
    tasks = repository.get_all_tasks()

    assert isinstance(tasks, list)
    assert len(tasks) == 2
