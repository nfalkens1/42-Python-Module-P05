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


if __name__ == "__main__":
    header = "=== Code Nexus - Data Processor ==="
    print(f"\n{header}\n")
    print("Testing Numeric Processor...")
    int_data = 42
    num_instance = NumericProcessor()
    validation_1 = num_instance.validate(int_data)
    str_data = "Hello"
    validation_2 = num_instance.validate(str_data)
    print(f" Trying to validate input '{int_data}': {validation_1}")
    print(f" Trying to validate input '{str_data}': {validation_2}")
    invalid_ingest = "foo"
    print(f" Test invalid ingestion of string '{invalid_ingest}'"
          " without prior validation:")
    try:
        num_instance.ingest(invalid_ingest)
    except TypeError as e:
        print(f" Got exception: {e}")
    valid_data: list[int | float] = [1, 2, 3, 4, 5]
    print(f" Processing data: {valid_data}")
    if num_instance.validate(valid_data):
        num_instance.ingest(valid_data)
        print(" Extracting 3 values")
        for i in range(3):
            num_out = num_instance.output()
            print(f" Numeric value {num_out[0]}: {num_out[1]}")
    print()
    print("Testing Text Processor...")
    text_instance = TextProcessor()
    validation_3 = text_instance.validate(int_data)
    print(f" Trying to validate input '{int_data}': {validation_3}")
    text_data = ["Hello", "Nexus", "World"]
    print(f" Processing data: {text_data}")
    if text_instance.validate(text_data):
        text_instance.ingest(text_data)
        print(" Extracting 1 value...")
        text_out = text_instance.output()
        print(f" Text value {text_out[0]}: {text_out[1]}")

    print()
    print("Testing Log Processor...")
    log_instance = LogProcessor()
    validation_4 = log_instance.validate(str_data)
    print(f" Trying to validate input '{str_data}': {validation_4}")
    log_list = [{"log_level": "NOTICE", "log_message": "Connection to server"},
                {"log_level": "ERROR", "log_message": "Unauthorized access!!"}]
    print(f" Processing data: {log_list}")
    if log_instance.validate(log_list):
        log_instance.ingest(log_list)
        print(" Extracting 2 values...")
        for i in range(2):
            log_out = log_instance.output()
            print(f" Log entry {log_out[0]}: {log_out[1]}")
