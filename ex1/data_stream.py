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
    explanation goes here
    """

    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, processor: DataProcessor):
        self.processors.append(processor)

    def process_stream(self, stream: list[Any]):
        for item in stream:
            valid = False
            for processor in self.processors:
                if processor.validate(item):
                    processor.ingest(item)
                    valid = True
                    break
                if not valid:
                    print("DataStream error - "
                          f"Can't process element in stream: {item}")







"""
FOR REF:

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for processor in self.processors:
            name = type(processor).__name__.replace("Processor", " Processor")
            total = processor.rank
            remaining = len(processor.data)
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print("\nRegistering Numeric Processor\n")
    numeric = NumericProcessor()
    stream.register_processor(numeric)
    first_batch = ['Hello world', [3.14, -1, 2.71],
                   [{'log_level': 'WARNING',
                     'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO',
                    'log_message': 'User wil is connected'}],
                   42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {first_batch}")
    stream.process_stream(first_batch)
    stream.print_processors_stats()
    print("\nRegistering other data processors")
    textual = TextProcessor()
    log_processor = LogProcessor()
    stream.register_processor(textual)
    stream.register_processor(log_processor)
    print("Send the same batch again")
    stream.process_stream(first_batch)
    stream.print_processors_stats()
    num = 3
    tex = 2
    log = 1
    print(f"\nConsume some elements from the data processors: "
          f"Numeric {num}, Text {tex}, Log {log}")
    for i in range(num):
        numeric.output()
    for i in range(tex):
        textual.output()
    for i in range(log):
        log_processor.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
"""