from typing import Any, Protocol
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


class ExportPlugin(Protocol):
    """
    description goes here
    """
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


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
        for processor in self.processors:
            data: list[tuple[int, str]] = []
            for i in range(nb):
                try:
                    data.append(processor.output())
                except IndexError:
                    break
            plugin.process_output(data)


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        items = []
        for item in data:
            items.append(item[1])
        csv_out = ",".join(items)
        print(csv_out)


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        items = []
        for item in data:
            items.append(f'"item_{item[0]}": "{item[1]}"')
        json_format = ", ".join(items)
        start = "{"
        end = "}"
        json_out = start + json_format + end
        print(json_out)


if __name__ == "__main__":
    header = "=== Code Nexus - Data Pipeline ==="
    print(f"\n{header}\n")
    data_stream = DataStream()
    data_stream.print_processors_stats()
    print("\nRegistering Processors\n")
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    data_stream.register_processor(num_proc)
    data_stream.register_processor(text_proc)
    data_stream.register_processor(log_proc)
    log_entries = [
        {'log_level': 'WARNING',
         'log_message': 'Telnet access! Use ssh instead'},
        {'log_level': 'INFO',
         'log_message': 'User wil is connected'},
    ]
    first_batch: list[Any] = [
        'Hello world', [3.14, -1, 2.71], log_entries, 42, ['Hi', 'five'],
    ]
    print(f"Send first batch of data on stream: {first_batch}")
    print()
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVPlugin()
    data_stream.output_pipeline(3, csv_plugin)
    print()
    data_stream.print_processors_stats()
    second_log_entries = [
        {'log_level': 'ERROR',
         'log_message': '500 server crash'},
        {'log_level': 'NOTICE',
         'log_message': 'Certificate expires in 10 days'},
    ]
    second_batch: list[Any] = [
        21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        second_log_entries, [32, 42, 64, 84, 128, 168], 'World hello',
    ]
    print(f"\nSend another batch of data: {second_batch}")
    print()
    data_stream.process_stream(second_batch)
    data_stream.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONPlugin()
    data_stream.output_pipeline(5, json_plugin)
    print()
    data_stream.print_processors_stats()
