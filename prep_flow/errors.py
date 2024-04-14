from typing import Any, Optional


class SheetNotFoundError(Exception):
    def __init__(self, sheet: str) -> None:
        self.sheet = sheet

    def __str__(self) -> str:
        return f"There is no {self.sheet} sheet in excel file."


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


class NecessaryColumnsNotFoundError(DataColumnsError):
    pass

    def __str__(self) -> str:
        return f"Necessary columns, {self.columns}, does not exist."


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
        return f"Does not cast from {self.from_} to {self.to_}. (column: {self.column})"


class DataValueError(Exception):
    def __init__(self, column: str, row_number: int, value: Any) -> None:
        self.column = column
        self.row_number = row_number
        self.value = value


class NullValueFoundError(DataValueError):
    pass

    def __str__(self) -> str:
        return f"NULL is contained in columns where NULL is not allowed. (column: {self.column}, value: {self.value}, row: {self.row_number})"  # noqa


class InvalidDateFoundError(DataValueError):
    pass

    def __str__(self) -> str:
        return (
            f"A non-existent date is specified. (column: {self.column}, value: {self.value}, row: {self.row_number})"
        )


class InvalidDateLiteralFoundError(DataValueError):
    pass

    def __str__(self) -> str:
        return f"Contains a string that cannot be recognized as a date. (column: {self.column}, value: {self.value}, row: {self.row_number})"  # noqa


class InvalidRegexpFoundError(DataValueError):
    def __init__(self, column: str, row_number: int, value: Any, regexp: str) -> None:
        super().__init__(column, row_number, value)
        self.regexp = regexp

    def __str__(self) -> str:
        return f"Contains a string that does not match the regular expression. (column: {self.column}, value: {self.value}, row: {self.row_number}, regexp: {self.regexp})"  # noqa


class InvalidCategoryFoundError(DataValueError):
    def __init__(self, column: str, row_number: int, value: Any, category: list[str]) -> None:
        super().__init__(column, row_number, value)
        self.category = category

    def __str__(self) -> str:
        return f"Contains a string that is not included in the specified category.　(column: {self.column}, value: {self.value}, row: {self.row_number}, category: {self.category}"  # noqa


class ValueCastError(DataValueError):
    def __init__(self, column: str, row_number: int, value: Any, from_: str, to_: str) -> None:
        super().__init__(column, row_number, value)
        self.from_ = from_
        self.to_ = to_

    def __str__(self) -> str:
        return f"Does not cast from {self.from_} to {self.to_}. (column: {self.column}, value: {self.value}, row: {self.row_number})"  # noqa


class DecoratorError(Exception):
    def __init__(self, column: str, detail: Optional[str] = None) -> None:
        self.column = column
        self.detail = detail

    def __str__(self) -> str:
        return self.detail


class DecoratorReturnTypeError(Exception):
    def __init__(self, dtype: str, detail: Optional[str] = None) -> None:
        self.dtype = dtype
        self.detail = detail

    def __str__(self) -> str:
        return self.detail
