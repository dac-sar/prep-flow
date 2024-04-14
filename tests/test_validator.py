import pandas as pd
import pytest

from prep_flow import (
    InvalidCategoryFoundError,
    InvalidDateFoundError,
    InvalidDateLiteralFoundError,
    InvalidRegexpFoundError,
    NecessaryColumnsNotFoundError,
    NullValueFoundError,
    UnnecessaryColumnsExistsError,
    Validator,
)


def test_validate_necessary_columns():
    data = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "age": [28, 26],
        }
    )
    necessary_columns_1 = ["name", "age", "gender"]
    with pytest.raises(NecessaryColumnsNotFoundError) as e:
        Validator.validate_necessary_columns(data, necessary_columns_1)
    assert e.value.columns == ["gender"]

    necessary_columns_2 = ["name", "age", "gender", "height"]
    with pytest.raises(NecessaryColumnsNotFoundError) as e:
        Validator.validate_necessary_columns(data, necessary_columns_2)
    assert e.value.columns == ["gender", "height"]


def test_validate_unnecessary_columns():
    data = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "age": [28, 26],
            "gender": ["man", "woman"],
            "height": [178, 162],
        }
    )
    necessary_columns_1 = ["name", "age"]
    with pytest.raises(UnnecessaryColumnsExistsError) as e:
        Validator.validate_unnecessary_columns(data, necessary_columns_1)
    assert e.value.columns == ["gender", "height"]

    necessary_columns_2 = ["name", "age", "gender"]
    with pytest.raises(UnnecessaryColumnsExistsError) as e:
        Validator.validate_unnecessary_columns(data, necessary_columns_2)
    assert e.value.columns == ["height"]


def test_validate_nullable():
    data = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "age": [28, None],
            "gender": ["man", "woman"],
        }
    )

    conditions_1 = {"name": False, "age": False, "gender": False}
    with pytest.raises(NullValueFoundError) as e:
        Validator.validate_nullable(data, conditions_1)
    assert e.value.column == "age"
    assert e.value.row_number == 2
    assert pd.isna(e.value.value)

    conditions_2 = {"name": False, "age": True, "gender": False}
    Validator.validate_nullable(data, conditions_2)
    assert True


def test_validate_datetime():
    data_1 = pd.DataFrame({"name": ["taro", "hanako"], "birthday": ["1995/10/19", "1998/3/25"]})
    conditions_1 = {"name": False, "birthday": True}
    Validator.validate_datetime(data_1, conditions_1)
    assert True

    data_2 = pd.DataFrame({"name": ["taro", "hanako"], "birthday": ["1995/10/19", "1998/3/40"]})
    conditions_2 = {"name": False, "birthday": True}
    with pytest.raises(InvalidDateFoundError) as e:
        Validator.validate_datetime(data_2, conditions_2)
    assert e.value.column == "birthday"
    assert e.value.row_number == 2
    assert e.value.value == "1998/3/40"

    data_3 = pd.DataFrame({"name": ["taro", "hanako"], "birthday": ["1995/10/19", "1998/3月/25"]})
    conditions_3 = {"name": False, "birthday": True}
    with pytest.raises(InvalidDateLiteralFoundError) as e:
        Validator.validate_datetime(data_3, conditions_3)
    assert e.value.column == "birthday"
    assert e.value.row_number == 2
    assert e.value.value == "1998/3月/25"

    data_4 = pd.DataFrame({"name": ["taro", "hanako"], "birthday": ["1995/10/19", None]})
    conditions_4 = {"name": False, "birthday": True}
    Validator.validate_datetime(data_4, conditions_4)
    assert True


def test_validate_regexp():
    data_1 = pd.DataFrame(
        {
            "id": ["p_0001", "p_0002"],
            "name": ["taro", "hanako"],
            "birthday": ["1995/10/19", "1998/3/25"],
        }
    )
    conditions_1 = {"id": {"regexp": r"p_[0-9]{4}", "nullable": False}}
    Validator.validate_regexp(data_1, conditions_1)
    assert True

    data_2 = pd.DataFrame(
        {
            "id": ["p_0001", "0002"],
            "name": ["taro", "hanako"],
            "birthday": ["1995/10/19", "1998/3/25"],
        }
    )
    conditions_2 = {"id": {"regexp": r"p_[0-9]{4}", "nullable": False}}
    with pytest.raises(InvalidRegexpFoundError) as e:
        Validator.validate_regexp(data_2, conditions_2)
    assert e.value.column == "id"
    assert e.value.row_number == 2
    assert e.value.value == "0002"
    assert e.value.regexp == r"p_[0-9]{4}"

    data_3 = pd.DataFrame(
        {
            "id": ["p_0001", None],
            "name": ["taro", "hanako"],
            "birthday": ["1995/10/19", "1998/3/25"],
        }
    )
    conditions_3_1 = {"id": {"regexp": r"p_[0-9]{4}", "nullable": False}}
    with pytest.raises(InvalidRegexpFoundError) as e:
        Validator.validate_regexp(data_3, conditions_3_1)
    assert e.value.column == "id"
    assert e.value.row_number == 2
    assert pd.isna(e.value.value)
    assert e.value.regexp == r"p_[0-9]{4}"

    conditions_3_2 = {"id": {"regexp": r"p_[0-9]{4}", "nullable": True}}
    Validator.validate_regexp(data_3, conditions_3_2)
    assert True


def test_validate_category():
    data_1 = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "gender": ["man", "woman"],
        }
    )
    conditions_1 = {"gender": {"category": ["man", "woman"], "nullable": False}}
    Validator.validate_category(data_1, conditions_1)
    assert True

    data_2 = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "gender": ["男", "woman"],
        }
    )
    conditions_2 = {"gender": {"category": ["man", "woman"], "nullable": False}}
    with pytest.raises(InvalidCategoryFoundError) as e:
        Validator.validate_category(data_2, conditions_2)
    assert e.value.column == "gender"
    assert e.value.row_number == 1
    assert e.value.value == "男"
    assert e.value.category == ["man", "woman"]

    data_3 = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "gender": [None, "woman"],
        }
    )
    conditions_3_1 = {"gender": {"category": ["man", "woman"], "nullable": False}}
    with pytest.raises(InvalidCategoryFoundError) as e:
        Validator.validate_category(data_3, conditions_3_1)
    assert e.value.column == "gender"
    assert e.value.row_number == 1
    assert pd.isna(e.value.value)
    assert e.value.category == ["man", "woman"]

    conditions_3_2 = {"gender": {"category": ["man", "woman"], "nullable": True}}
    Validator.validate_category(data_3, conditions_3_2)
    assert True

    data_4 = pd.DataFrame(
        {
            "name": ["taro", "hanako"],
            "gender": [0, 1],
        }
    )
    conditions_4 = {"gender": {"category": [0, 1], "nullable": False}}
    Validator.validate_category(data_4, conditions_4)
    assert True