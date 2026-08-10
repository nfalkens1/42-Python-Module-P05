from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    """
    Parent class that defines abstractmethods validate and ingest
    Skips over abstractmethods
    Produces output by popping data at index 0 + the respected rank
    """
    data: list[str]
    rank: int
    name: str

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise IndexError("No data provided")
        rank = self.rank - len(self.data)
        data_out = self.data.pop(0)
        return (rank, data_out)


class NumericProcessor(DataProcessor):
    """
    Validate: checks if data is int/float or a list of int/float
    Ingest: raises TypeError if data is not numeric
    Formats items to str and adds then to the list while incrementing rank
    """

    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank: int = 0
        self.name: str = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        is_num = isinstance(data, (int, float))
        is_num_list = isinstance(data, list) and all(
                      isinstance(item, (int, float)) for item in data)
        return is_num or is_num_list

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.data.append(str(item))
                self.rank += 1
        else:
            self.data.append(str(data))
            self.rank += 1


class TextProcessor(DataProcessor):
    """
    Validate: Text input data (str or list of str)
    Ingest: appends Item to data and increments rank
    """

    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank: int = 0
        self.name: str = "Text Processor"

    def validate(self, data: Any) -> bool:
        is_str = isinstance(data, str)
        is_str_list = isinstance(data, list) and all(
                      isinstance(item, str) for item in data)
        return is_str or is_str_list

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self.data.append(item)
                self.rank += 1
        else:
            self.data.append(data)
            self.rank += 1


class LogProcessor(DataProcessor):
    """
    Validate: checks if every k/v pair in each dict is of type str
    Ingest: formats log messages
    """

    def __init__(self) -> None:
        self.data: list[str] = []
        self.rank: int = 0
        self.name: str = "Log Processor"

    def validate(self, data: Any) -> bool:
        is_str_dict = isinstance(data, dict) and all(
                      isinstance(key, str) and isinstance(value, str)
                      for key, value in data.items())
        is_list_str_dict = isinstance(data, list) and all(
                           isinstance(item, dict) and all(
                               isinstance(k, str) and isinstance(v, str) for
                               k, v in item.items()) for item in data)
        return is_str_dict or is_list_str_dict

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")

        if isinstance(data, list):
            for item in data:
                log_format = f"{item['log_level']}: {item['log_message']}"
                self.data.append(log_format)
                self.rank += 1
        else:
            log_format = f"{data['log_level']}: {data['log_message']}"
            self.data.append(log_format)
            self.rank += 1


class DataStream:
    """
    description goes here
    """

    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, processor: DataProcessor) -> None:
        self.processors.append(processor)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            for processor in self.processors:
                if processor.validate(item):
                    processor.ingest(item)
                    break
            else:
                print("DataStream error - "
                      f"Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        header_1 = "== DataStream statistics =="
        print(header_1)
        if not self.processors:
            print("No processor found, no data")
        for processor in self.processors:
            name = processor.name
            total = processor.rank
            remaining = len(processor.data)
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        ...


class ExportPlugin:
    """
    description goes here
    """
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...