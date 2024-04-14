from __future__ import annotations

import re
from typing import TypedDict, Union

import pandas as pd
from pandas._libs.tslibs.parsing import DateParseError  # noqa

from prep_flow.errors import (
    InvalidCategoryFoundError,
    InvalidDateFoundError,
    InvalidDateLiteralFoundError,
    InvalidRegexpFoundError,
    NecessaryColumnsNotFoundError,
    NullValueFoundError,
    UnnecessaryColumnsExistsError,
)


class RegexpCondition(TypedDict):
    regexp: str
    nullable: bool


class CategoryCondition(TypedDict):
    category: list[Union[str, int]]
    nullable: bool


class Validator:
    @staticmethod
    def validate_necessary_columns(data: pd.DataFrame, necessary_columns: list[str]) -> None:
        """
        Raise an error, if the data doesn't have necessary_columns.

        Parameters
        ----------
        data: pd.DataFrame
        necessary_columns: list[str]

        Raises
        ------
        NecessaryColumnsNotFoundError
        """
        results = []
        for column in necessary_columns:
            if column not in data.columns:
                results.append(column)
        if len(results) > 0:
            raise NecessaryColumnsNotFoundError(columns=results)

    @staticmethod
    def validate_unnecessary_columns(data: pd.DataFrame, necessary_columns: list[str]) -> None:
        """
        Raise an error, if the data have unnecessary columns that necessary_columns don't contain.

        Parameters
        ----------
        data: pd.DataFrame
        necessary_columns: list[str]

        Raises
        ------
        UnnecessaryColumnsExistsError
        """
        results = []
        for column in data.columns:
            if column not in necessary_columns:
                results.append(column)
        if len(results) > 0:
            raise UnnecessaryColumnsExistsError(columns=results)

    @staticmethod
    def validate_nullable(data: pd.DataFrame, conditions: dict[str, bool]) -> None:
        """
        Parameters
        ----------
        data: pd.DataFrame
        conditions: dict[str, bool]
            Key is a column_name. And value indicates nullable or not.
            Expected format is as follows.
            {
                "column_name_1": bool,
                "column_name_2": bool,
                ...,
            }

        Raises
        ------
        NullValueFoundError
        """
        for column, nullable in conditions.items():
            if nullable:
                continue
            for i, target in enumerate(data[column]):
                if pd.isna(target):
                    raise NullValueFoundError(column=column, row_number=i + 1, value=target)

    @staticmethod
    def validate_datetime(data: pd.DataFrame, conditions: dict[str, bool]) -> None:
        """
        Raise an error, if values are invalid datetime format.

        Parameters
        ----------
        data: pd.DataFrame
        conditions: dict[str, bool]
            Key is a column name. And value indicates datetime or not.
            Expected format is as follows.
            {
                "column_name_1": bool,
                "column_name_2": bool,
                ...,
            }

        Raises
        ------
        InvalidDateFoundError
            If a non-existent date is included.
        InvalidDateLiteralFoundError
            If a string that cannot be interpreted as a date is included.
        """
        for column, is_datetime in conditions.items():
            if not is_datetime:
                continue
            for i, target in enumerate(data[column]):
                try:
                    pd.to_datetime(target)
                except DateParseError as e:
                    if "day is out of range" in e.__str__():
                        raise InvalidDateFoundError(column=column, row_number=i + 1, value=target)
                    else:
                        raise InvalidDateLiteralFoundError(column=column, row_number=i + 1, value=target)

    @staticmethod
    def validate_regexp(data: pd.DataFrame, conditions: dict[str, RegexpCondition]) -> None:
        """
        Raise an error, if values don't match regular expressions.

        Parameters
        ----------
        data: pd.DataFrame
        conditions: dict[str, RegexpCondition]
             Key is a column name. Expected format is as follows.
            {
                "column_name_1": {"regexp": "some_regexp", "nullable": bool},
                "column_name_2": {"regexp": "some_regexp", "nullable": bool},
                ...,
            }

        Raises
        ------
        InvalidRegexpFoundError
            If values don't match regular expressions.
        """
        for column, condition in conditions.items():
            for i, target in enumerate(data[column]):
                if condition["nullable"] and pd.isna(target):
                    continue
                match_obj = re.match(pattern=condition["regexp"], string=str(target))
                if match_obj is None:
                    raise InvalidRegexpFoundError(
                        column=column,
                        row_number=i + 1,
                        value=target,
                        regexp=condition["regexp"],
                    )

    @staticmethod
    def validate_category(data: pd.DataFrame, conditions: dict[str, CategoryCondition]) -> None:
        """
        Raise an error, if the specified category doesn't contain values.

        Parameters
        ----------
        data: pd.DataFrame
        conditions: dict[str, CategoryCondition]
            Key is a column name. Expected format is as follows.
            {
                "column_name_1": {"category": ["category_1", "category_2", ...], "nullable": bool},
                "column_name_2": {"category": ["category_1", "category_2", ...], "nullable": bool},
                ...,
            }

        Raises
        ------
        InvalidCategoryFoundError
            If the specified category doesn't contain values.
        """
        for column, condition in conditions.items():
            for i, target in enumerate(data[column]):
                if condition["nullable"] and pd.isna(target):
                    continue
                if target not in condition["category"]:
                    raise InvalidCategoryFoundError(
                        column=column,
                        row_number=i + 1,
                        value=target,
                        category=condition["category"],
                    )
