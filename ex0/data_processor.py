from typing import Any
from abc import ABC, abstractmethod

class DataProcessor(ABC):
    """
    description goes here
    """
    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        ...


class NumericProcessor(DataProcessor):
    """
    description goes here
    """

    def __init__ (self) -> None:
        self.data: list[str] = []
        self.rank: int = 0

    def validate(self, data: Any) -> bool:
        is_num = isinstance(data, (int, float))
        is_num_list = isinstance(data, list) and all(
		              isinstance(item, (int, float)) for item in data)
        return is_num or is_num_list

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError ("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.data.append(str(item))
                self.rank += 1
        else:
            self.data.append(str(data))
            self.rank += 1
