from typing import Protocol


class DagRepositoryError:
    def __init__(self, error_msg: str):
        self.error_msg = error_msg


class DagRepository(Protocol):
    def create_or_update_dag(
        self, data_contract_id: str, content: str, environment: str
    ) -> None | DagRepositoryError:
        pass

    def delete_dags(
        self, data_contract_ids: list[str], environment: str
    ) -> None | DagRepositoryError:
        pass
