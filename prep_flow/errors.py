from typing import Any


# TODO: add __str__ function.
class SheetNotFoundError(Exception):
    def __init__(self, sheet: str) -> None:
        self.sheet = sheet


class ReferenceDataNotFoundError(Exception):
    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return f"The reference data, {self.name}, does not exist."


class ReferenceDataNotInitializationError(Exception):
    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return f"The reference data, {self.name}, is not initialized."


class DataColumnsError(Exception):
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns


# TODO: add __str__ function.
class NecessaryColumnsNotFoundError(DataColumnsError):
    pass


class UnnecessaryColumnsExistsError(DataColumnsError):
    def __str__(self) -> str:
        return f"Unnecessary columns, {self.columns}, exists."


class DataColumnError(Exception):
    def __init__(self, column: str) -> None:
        self.column = column


class ColumnCastError(DataColumnError):
    def __init__(self, column: str, from_: str, to_: str) -> None:
        super().__init__(column)
        self.from_ = from_
        self.to_ = to_

    def __str__(self) -> str:
        return f"Column, {self.column}, does not cast from {self.from_} to {self.to_}."


# TODO: add __str__ function.
class DataValueError(Exception):
    def __init__(self, column: str, row_number: int, value: Any) -> None:
        self.column = column
        self.row_number = row_number
        self.value = value


# TODO: add __str__ function.
class NullValueFoundError(DataValueError):
    pass


# TODO: add __str__ function.
class InvalidDateFoundError(DataValueError):
    pass


# TODO: add __str__ function.
class InvalidDateLiteralFoundError(DataValueError):
    pass


# TODO: add __str__ function.
class InvalidRegexpFoundError(DataValueError):
    def __init__(self, column: str, row_number: int, value: Any, regexp: str) -> None:
        super().__init__(column, row_number, value)
        self.regexp = regexp


# TODO: add __str__ function.
class InvalidCategoryFoundError(DataValueError):
    def __init__(self, column: str, row_number: int, value: Any, category: list[str]) -> None:
        super().__init__(column, row_number, value)
        self.category = category


# TODO: add __str__ function.
class ValueCastError(DataValueError):
    def __init__(self, column: str, row_number: int, value: Any, from_: str, to_: str) -> None:
        super().__init__(column, row_number, value)
        self.from_ = from_
        self.to_ = to_
