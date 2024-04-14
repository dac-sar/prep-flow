from __future__ import annotations

import abc
from typing import Callable, Optional, Union

import numpy as np
import pandas as pd

from prep_flow.decorators import CREATOR_KEY, DECORATOR_KEY, FILTER_KEY, MODIFIER_KEY
from prep_flow.errors import (
    ColumnCastError,
    DecoratorError,
    DecoratorReturnTypeError,
    ReferenceDataNotFoundError,
    ReferenceDataNotInitializationError,
    SheetNotFoundError,
    ValueCastError,
)
from prep_flow.expressions import Column, DateTime, Dtype, ReferenceColumn
from prep_flow.validator import CategoryCondition, RegexpCondition, Validator

DEFAULT_SHEET_NAME = "Sheet1"
PYPREP_PARENT_CLASS_NAME = "__pyprep_parent_class_name__"


class BaseFlow(abc.ABC):
    __sheetname__ = DEFAULT_SHEET_NAME
    __replace_none_to_nan__ = True

    def __init__(
        self,
        data: Union[pd.DataFrame, pd.ExcelFile],
        reference: Optional[list[BaseFlow]] = None,
    ) -> None:
        self.original = self.parse_data(data)
        self.pre_data = None
        self.data = self.original.copy()
        self.reference = [] if reference is None else reference
        self.validator = Validator()

        self.execute()

    @classmethod
    def parse_data(cls, data: Union[pd.DataFrame, pd.ExcelFile]) -> pd.DataFrame:
        """
        Receives and reads pd.DataFrame or pd.ExcelFile.

        Parameters
        ----------
        data: Union[pd.DataFrame, pd.ExcelFile]

        Returns
        -------
        pd.DataFrame
        """
        if isinstance(data, pd.ExcelFile):
            cls.validate_sheet_name(data)
            data = pd.read_excel(data, sheet_name=cls.__sheetname__)

        return data

    @classmethod
    def validate_sheet_name(cls, xlsx: pd.ExcelFile) -> None:
        """
        Check whether the specified Sheet exists in ExcelFile.

        Parameters
        ----------
        xlsx: pd.ExcelFile
        """
        if cls.__sheetname__ not in xlsx.sheet_names:
            raise SheetNotFoundError(sheet=cls.__sheetname__)

    @classmethod
    def definitions(cls) -> dict[str, Union[Column, ReferenceColumn]]:
        """
        Get the columns that should be present in the data and their definitions.

        Returns
        -------
        dict[str, Column]
        """
        return dict(
            [
                (key, val)
                for key, val in vars(cls).items()
                if isinstance(val, Column) or isinstance(val, ReferenceColumn)
            ]
        )

    def execute(self) -> None:
        # Argument Verification.
        self.confirm_reference_exists()

        # Add metadata.
        self.set_class_name_to_columns()

        # Convert raw data column names and verify that they are the expected type.
        self.rename()
        self.pre_validate()
        self.pre_cast()
        self.pre_data = self.data.copy()

        for order in self.orders():
            # Modify values with Column.modifier.
            self.apply_column_modifier(order=order)

            # Modify values with decorator referring to Column.
            self.apply_column_modifier_with_decorator(order=order)

            # Create user defined columns with decorator.
            self.apply_creator_with_decorator(order=order)

            # Filter data.
            self.apply_filter_with_decorator(order=order)

            # Merge Reference columns.
            self.merge(order=order)

            # Modify values with ReferenceColumn.modifier.
            self.apply_reference_column_modifier(order=order)

            # Modify values with decorator referring to ReferenceColumn.
            self.apply_reference_column_modifier_with_decorator(order=order)

        # Validate all columns.
        self.post_validate(only_base=False)
        self.post_cast(only_base=False)

        if self.__replace_none_to_nan__:
            self.replace_none_to_nan()

        self.sort_columns()

    def column_info(self, column: str) -> Column:
        return self.definitions()[column]

    def creator_columns(self) -> list[str]:
        return [column for _, (_, column, _) in self.get_decorators(CREATOR_KEY).items()]

    @classmethod
    def reference_columns(cls) -> list[str]:
        return [key for key, val in vars(cls).items() if isinstance(val, ReferenceColumn)]

    def additional_columns(self) -> list[str]:
        return self.creator_columns() + self.reference_columns()

    def columns(self, only_base: bool = False) -> list[str]:
        if only_base:
            return [column for column in self.definitions().keys() if column not in self.additional_columns()]
        else:
            return list(self.definitions().keys())

    def is_nullable_columns(self, only_base: bool = False) -> dict[str, bool]:
        if only_base:
            return dict(
                [
                    (key, val.nullable)
                    for key, val in self.definitions().items()
                    if (not val.nullable) and (key not in self.additional_columns())
                ]
            )
        else:
            return dict([(key, val.nullable) for key, val in self.definitions().items() if not val.nullable])

    def is_datetime_columns(self, only_base: bool = False) -> dict[str, bool]:
        if only_base:
            return dict(
                [
                    (key, True)
                    for key, val in self.definitions().items()
                    if (val.dtype == DateTime) and (key not in self.additional_columns())
                ]
            )
        else:
            return dict([(key, True) for key, val in self.definitions().items() if val.dtype == DateTime])

    def regexp_columns(self, only_base: bool = False) -> dict[str, RegexpCondition]:
        if only_base:
            return dict(
                [
                    (key, {"regexp": val.regexp, "nullable": val.nullable})
                    for key, val in self.definitions().items()
                    if (val.regexp is not None) and (key not in self.additional_columns())
                ]
            )
        else:
            return dict(
                [
                    (key, {"regexp": val.regexp, "nullable": val.nullable})
                    for key, val in self.definitions().items()
                    if val.regexp is not None
                ]
            )

    def category_columns(self, only_base: bool = False) -> dict[str, CategoryCondition]:
        if only_base:
            return dict(
                [
                    (key, {"category": val.category, "nullable": val.nullable})
                    for key, val in self.definitions().items()
                    if (val.category is not None) and (key not in self.additional_columns())
                ]
            )
        else:
            return dict(
                [
                    (key, {"category": val.category, "nullable": val.nullable})
                    for key, val in self.definitions().items()
                    if val.category is not None
                ]
            )

    def original_is_nullable_columns(self) -> dict[str, bool]:
        return dict(
            [
                (key, val.original_nullable)
                for key, val in self.definitions().items()
                if (isinstance(val, Column)) and (not val.original_nullable) and (key not in self.additional_columns())
            ]
        )

    def original_is_datetime_columns(self) -> dict[str, bool]:
        return dict(
            [
                (key, True)
                for key, val in self.definitions().items()
                if (isinstance(val, Column))
                and (val.original_dtype is not None)
                and (val.original_dtype == DateTime)
                and (key not in self.additional_columns())
            ]
        )

    def original_regexp_columns(self) -> dict[str, RegexpCondition]:
        return dict(
            [
                (
                    key,
                    {"regexp": val.original_regexp, "nullable": val.original_nullable},
                )
                for key, val in self.definitions().items()
                if (isinstance(val, Column))
                and (val.original_regexp is not None)
                and (key not in self.additional_columns())
            ]
        )

    def original_category_columns(self) -> dict[str, CategoryCondition]:
        return dict(
            [
                (key, {"category": val.original_category, "nullable": val.nullable})
                for key, val in self.definitions().items()
                if (isinstance(val, Column))
                and (val.original_category is not None)
                and (key not in self.additional_columns())
            ]
        )

    def modifier_columns(self, order: int) -> dict[str, Callable]:
        return dict(
            [
                (key, val.modifier)
                for key, val in self.definitions().items()
                if (not isinstance(val, ReferenceColumn)) and (val.order == order) and (val.modifier is not None)
            ]
        )

    def modifier_reference_columns(self, order: int) -> dict[str, Callable]:
        return dict(
            [
                (key, val.modifier)
                for key, val in self.definitions().items()
                if (isinstance(val, ReferenceColumn)) and (val.order == order) and (val.modifier is not None)
            ]
        )

    def rename_dict(self) -> dict:
        return dict(
            [
                (val.name, key)
                for key, val in self.definitions().items()
                if (isinstance(val, Column)) and (val.name is not None)
            ]
        )

    def dtype_dict(self) -> dict[str, Dtype]:
        return dict([(key, val.dtype) for key, val in self.definitions().items() if val.dtype is not None])

    def original_dtype_dict(self) -> dict[str, Dtype]:
        return dict(
            [
                (key, val.original_dtype)
                for key, val in self.definitions().items()
                if (isinstance(val, Column))
                and (val.original_dtype is not None)
                and (key not in self.additional_columns())
            ]
        )

    def rename(self) -> None:
        self.data.columns = [str(col) for col in self.data.columns]
        self.data = self.data.rename(columns=self.rename_dict())

    def pre_validate(self) -> None:
        self.validator.validate_necessary_columns(self.data, self.columns(only_base=True))
        self.validator.validate_unnecessary_columns(self.data, self.columns(only_base=True))
        self.validator.validate_nullable(self.data, self.original_is_nullable_columns())
        self.validator.validate_datetime(self.data, self.original_is_datetime_columns())
        self.validator.validate_regexp(self.data, self.original_regexp_columns())
        self.validator.validate_category(self.data, self.original_category_columns())

    def post_validate(self, only_base: bool = False) -> None:
        self.validator.validate_necessary_columns(self.data, self.columns(only_base))
        self.validator.validate_unnecessary_columns(self.data, self.columns(only_base))
        self.validator.validate_nullable(self.data, self.is_nullable_columns(only_base))
        self.validator.validate_datetime(self.data, self.is_datetime_columns(only_base))
        self.validator.validate_regexp(self.data, self.regexp_columns(only_base))
        self.validator.validate_category(self.data, self.category_columns(only_base))

    def cast_value(self, column: str, dtype: Dtype) -> None:
        # Cast Value level dtype
        series = self.data[column].copy()
        for i, val in enumerate(series):
            if pd.isna(val):
                continue
            try:
                self.data.loc[i, column] = dtype.cast(val)
            except Exception:
                raise ValueCastError(
                    column=column,
                    row_number=i + 1,
                    value=val,
                    from_=self.data[column].dtype.name,
                    to_=dtype.name,
                )

    def cast_series(self, column: str, dtype: Dtype) -> None:
        # Cast Series Level dtype
        try:
            if self.data.query(f"{column}.isna()").shape[0] > 0:
                return
            self.data[column] = self.data[column].astype(dtype.name)
        except Exception:
            raise ColumnCastError(column=column, from_=self.data[column].dtype.name, to_=dtype.name)

    def pre_cast(self) -> None:
        for column, dtype in self.original_dtype_dict().items():
            self.cast_series(column, dtype)
            self.cast_value(column, dtype)

    def post_cast(self, only_base: bool = False) -> None:
        for column, dtype in self.dtype_dict().items():
            if only_base and (column in self.additional_columns()):
                continue
            self.cast_series(column, dtype)
            self.cast_value(column, dtype)

    def replace_none_to_nan(self) -> None:
        self.data = self.data.infer_objects(copy=False).replace({None: np.nan})

    @classmethod
    def get_num_of_args(cls, func_name: str) -> int:
        return len(getattr(cls, func_name).__code__.co_varnames[: getattr(cls, func_name).__code__.co_argcount])

    @classmethod
    def get_decorators(cls, decorator_key: Optional[str] = None) -> dict:
        decorators = {}
        for attr, obj in vars(cls).items():
            if not isinstance(obj, classmethod):
                continue
            if hasattr(obj, DECORATOR_KEY):
                if decorator_key:
                    if getattr(obj, DECORATOR_KEY)[0] == decorator_key:
                        decorators[attr] = getattr(obj, DECORATOR_KEY)
                else:
                    decorators[attr] = getattr(obj, DECORATOR_KEY)

        return decorators

    def apply_column_modifier(self, order: int) -> None:
        modifier_columns = self.modifier_columns(order=order)
        for column, modifier in modifier_columns.items():
            self.data[column] = self.data[column].apply(modifier)

    def apply_reference_column_modifier(self, order: int) -> None:
        modifier_reference_columns = self.modifier_reference_columns(order=order)
        for column, modifier in modifier_reference_columns.items():
            self.data[column] = self.data[column].apply(modifier)

    def apply_creator_with_decorator(self, order: int) -> None:
        decorators = self.get_decorators(CREATOR_KEY)
        for attr, (_, _column, _order) in decorators.items():
            if order != _order:
                continue
            if _column in self.reference_columns():
                raise DecoratorError(
                    column=_column,
                    detail=f"Creator cannot specify reference-columns.(column: {_column})",
                )
            if self.get_num_of_args(attr) == 1:
                self.data[_column] = getattr(self, attr)()
            else:
                self.data[_column] = getattr(self, attr)(self.data.copy())

    def apply_column_modifier_with_decorator(self, order: int) -> None:
        decorators = self.get_decorators(MODIFIER_KEY)
        for attr, (_, _column, _order) in decorators.items():
            if _column in self.reference_columns():
                continue
            if order != _order:
                continue
            if _column not in self.columns(only_base=True):
                raise DecoratorError(
                    column=_column,
                    detail=f"You have specified a column name that does not exist.(column: {_column})",
                )
            if self.get_num_of_args(attr) == 1:
                self.data[_column] = getattr(self, attr)()
            else:
                self.data[_column] = getattr(self, attr)(self.data.copy())

    def apply_reference_column_modifier_with_decorator(self, order: int) -> None:
        decorators = self.get_decorators(MODIFIER_KEY)
        for attr, (_, _column, _order) in decorators.items():
            if _column not in self.reference_columns():
                continue
            if order != _order:
                continue
            if self.get_num_of_args(attr) == 1:
                self.data[_column] = getattr(self, attr)()
            else:
                self.data[_column] = getattr(self, attr)(self.data.copy())

    def apply_filter_with_decorator(self, order: int) -> None:
        decorators = self.get_decorators(FILTER_KEY)
        for attr, (_, _, _order) in decorators.items():
            if order != _order:
                continue
            result = getattr(self, attr)(self.data.copy())
            if not isinstance(result, pd.DataFrame):
                raise DecoratorReturnTypeError(
                    dtype=type(result),
                    detail=f"Expected return type is pd.DataFrame, But you return f{type(result)}",
                )
            self.data = result

    def sort_columns(self) -> None:
        self.data = self.data[self.columns()]

    @classmethod
    def set_class_name_to_columns(cls) -> None:
        for key, val in vars(cls).items():
            if not isinstance(val, Column):
                continue
            setattr(val, PYPREP_PARENT_CLASS_NAME, cls.__name__)

    @classmethod
    def args(cls, column: Union[Column, ReferenceColumn]) -> tuple[str, ...]:
        args = []
        for key, val in vars(cls).items():
            if val == column:
                args.append(key)
        return tuple(sorted(args))

    @classmethod
    def get_reference_info(cls) -> list[tuple]:
        names = []
        for key, val in vars(cls).items():
            if not isinstance(val, ReferenceColumn):
                continue
            if not hasattr(val.column, PYPREP_PARENT_CLASS_NAME):
                raise ReferenceDataNotInitializationError(key)
            names.append(
                (
                    getattr(val.column, PYPREP_PARENT_CLASS_NAME),  # class
                    cls.args(val),  # column_name
                    val.how,
                    tuple(sorted(val.on)) if isinstance(val.on, list) else (val.on,),
                    val.order,
                )
            )

        return list(set(names))

    def confirm_reference_exists(self) -> None:
        for _class_name, _, _, _, _ in self.get_reference_info():
            if _class_name in [data.__class__.__name__ for data in self.reference]:
                continue
            raise ReferenceDataNotFoundError(name=_class_name)

    def merge(self, order: int) -> None:
        for _class_name, _columns, _how, _on, _order in self.get_reference_info():
            if order != _order:
                continue
            reference_data = [data for data in self.reference if data.__class__.__name__ == _class_name][0]
            self.data = pd.merge(
                self.data,
                reference_data.data[list(_columns) + list(_on)],
                how=_how,
                on=_on,
            )

    def decorator_orders(self) -> list[int]:
        return list(set([val[2] for key, val in self.get_decorators().items()]))

    def column_orders(self) -> list[int]:
        return list(set([self.column_info(column).order for column in self.columns(only_base=True)]))

    def reference_orders(self) -> list[int]:
        return list(set([self.column_info(column).order for column in self.reference_columns()]))

    def orders(self) -> list[int]:
        return list(set(self.decorator_orders() + self.column_orders() + self.reference_orders()))
