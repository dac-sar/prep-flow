from typing import Annotated, Any, Callable, Generic, Optional, Type, TypeVar, Union

import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp  # noqa
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainValidator,
    ValidationInfo,
    field_validator,
)

_DType = TypeVar("_DType")


class Dtype(Generic[_DType]):
    dtype: Union[str, int, float, bool, Timestamp]
    name: str

    @staticmethod
    def cast(value: Any) -> _DType:  # noqa
        NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        if not (hasattr(other, "dtype") and hasattr(other, "name")):
            return False

        if not (self.dtype == other.dtype and self.name == other.name):
            return False

        return True


class String(Dtype[str]):
    dtype = str
    name = "str"

    @staticmethod
    def cast(value) -> str:
        return str(value)


class Integer(Dtype[int]):
    dtype = int
    name = "int"

    @staticmethod
    def cast(value: Any) -> int:
        return int(value)


class Float(Dtype[float]):
    dtype = float
    name = "float"

    @staticmethod
    def cast(value: Any) -> float:
        return float(value)


class Boolean(Dtype[bool]):
    dtype = bool
    name = "bool"

    @staticmethod
    def cast(value: Any) -> bool:
        return bool(value)


class DateTime(Dtype[Timestamp]):
    dtype = Timestamp
    name = "datetime64[ns]"

    @staticmethod
    def cast(value: Any) -> Timestamp:
        return pd.to_datetime(value)


def validate_dtype(v: Any, _: ValidationInfo) -> Dtype:
    if not (hasattr(v, "dtype") and hasattr(v, "name")):
        raise TypeError(f"Expected String, Integer, Float, Boolean or DateTime, got {type(v)}")
    if not (
        v.dtype in [str, int, float, bool, Timestamp] and v.name in ["str", "int", "float", "bool", "datetime64[ns]"]
    ):  # noqa
        raise TypeError(f"Expected String, Integer, Float, Boolean or DateTime, got {type(v)}")

    return v


class Column(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    dtype: Annotated[Dtype, PlainValidator(validate_dtype)]
    name: Optional[str] = Field(default=None)
    nullable: bool = Field(default=True)
    regexp: Optional[str] = Field(default=None)
    category: Optional[list[Union[str, int]]] = Field(default=None)
    original_dtype: Optional[Annotated[Dtype, PlainValidator(validate_dtype)]] = Field(default=None)
    original_nullable: bool = Field(default=True)
    original_regexp: Optional[str] = Field(default=None)
    original_category: Optional[list[str]] = Field(default=None)
    modifier: Optional[Callable] = Field(default=None)
    order: int = Field(default=0)
    description: Optional[str] = Field(default=None)

    def __init__(
        self,
        dtype: Type[Dtype],
        name: Optional[str] = None,
        nullable: Optional[bool] = True,
        regexp: Optional[str] = None,
        category: Optional[list[Union[str, int]]] = None,
        original_dtype: Optional[Type[Dtype]] = None,
        original_nullable: Optional[bool] = True,
        original_regexp: Optional[str] = None,
        original_category: Optional[list[str]] = None,
        modifier: Optional[Callable] = None,
        order: int = 0,
        description: Optional[str] = None,
    ):

        super().__init__(
            **dict(
                (key, val)
                for key, val in {
                    "dtype": dtype,
                    "name": name,
                    "nullable": nullable,
                    "regexp": regexp,
                    "category": category,
                    "original_dtype": original_dtype,
                    "original_nullable": original_nullable,
                    "original_regexp": original_regexp,
                    "original_category": original_category,
                    "modifier": modifier,
                    "order": order,
                    "description": description,
                }.items()
                if val is not None
            )
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, Column):
            return False

        if not (
            self.dtype == other.dtype
            and self.name == other.name
            and self.nullable == other.nullable
            and self.regexp == other.regexp
            and self.category == other.category
            and self.original_dtype == other.original_dtype
            and self.original_nullable == other.original_nullable
            and self.original_regexp == other.original_regexp
            and self.original_category == other.original_category
            and self.modifier == other.modifier
            and self.order == other.order
            and self.description == other.description
        ):
            return False

        return True


class ReferenceColumn(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    column: Column
    on: Union[str | list[str]]
    how: str
    order: int = Field(default=0)
    dtype: Annotated[Dtype, PlainValidator(validate_dtype)] = Field(default=None)
    nullable: bool = Field(default=True)
    regexp: Optional[str] = Field(default=None)
    category: Optional[list[str]] = Field(default=None)
    modifier: Optional[Callable] = Field(default=None)
    description: Optional[str] = Field(default=None)

    def __init__(
        self,
        column: Column,
        on: Union[str | list[str]],
        how: str,
        order: int = 0,
        dtype: Optional[Type[Dtype]] = None,
        nullable: Optional[bool] = True,
        regexp: Optional[str] = None,
        category: Optional[list[str]] = None,
        modifier: Optional[Callable] = None,
        description: Optional[str] = None,
    ):

        super().__init__(
            **dict(
                (key, val)
                for key, val in {
                    "column": column,
                    "on": on,
                    "how": how,
                    "order": order,
                    "dtype": dtype,
                    "nullable": nullable,
                    "regexp": regexp,
                    "category": category,
                    "modifier": modifier,
                    "description": description,
                }.items()
                if val is not None
            )
        )

    @field_validator("how")
    def validate_how(cls, v: Any) -> str:  # noqa
        if v not in ["full", "left", "inner"]:
            raise ValueError(f"Expected full, left or inner, got {v}")

        return v

    def __eq__(self, other) -> bool:
        if not isinstance(other, ReferenceColumn):
            return False

        if not (
            self.column == other.column
            and self.on == other.on
            and self.how == other.how
            and self.order == other.order
            and self.dtype == other.dtype
            and self.nullable == other.nullable
            and self.regexp == other.regexp
            and self.category == other.category
            and self.modifier == other.modifier
            and self.description == other.description
        ):
            return False

        return True
