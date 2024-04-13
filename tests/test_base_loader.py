import numpy as np
import pandas as pd
import pytest

from prep_flow import (
    BaseLoader,
    Boolean,
    Column,
    ColumnCastError,
    DateTime,
    Integer,
    InvalidCategoryFoundError,
    InvalidDateFoundError,
    InvalidDateLiteralFoundError,
    InvalidRegexpFoundError,
    NecessaryColumnsNotFoundError,
    NullValueFoundError,
    ReferenceColumn,
    ReferenceDataNotFoundError,
    ReferenceDataNotInitializationError,
    String,
    UnnecessaryColumnsExistsError,
    creator,
    data_filter,
    modifier,
)


class UserLoader(BaseLoader):
    id = Column(dtype=String, regexp=r"id_[0-9]{1,3}")
    age = Column(dtype=Integer, nullable=False)
    name = Column(dtype=String)
    gender = Column(dtype=String, category=["女", "男"])
    birthday = Column(dtype=DateTime)
    gender_flag = Column(dtype=Boolean)

    @creator("gender_flag")
    def create_flag(self, data: pd.DataFrame) -> int:
        return data["gender"] == "男"

    @modifier("name")
    def modify_gender(self, data: pd.DataFrame) -> pd.Series:
        return data["name"].str.upper()


def assert_dataframes(a: pd.DataFrame, b: pd.DataFrame) -> None:
    assert a.shape == b.shape

    for (_, a_row), (_, b_row) in zip(a.iterrows(), b.iterrows()):
        for a_val, b_val in zip(a_row, b_row):
            if pd.isna(a_val):
                assert pd.isna(b_val)
            else:
                assert a_val == b_val


def test_parse_date():
    class Loader(BaseLoader):
        name = Column(dtype=String)

    df = pd.DataFrame({"name": ["taro", "hanako"]})
    xlsx = pd.ExcelFile("tests/data/test_parse_data.xlsx")

    csv_loader = Loader(df)
    xlsx_loader = Loader(xlsx)

    assert_dataframes(csv_loader.data, df)
    assert_dataframes(xlsx_loader.data, df)


def test_validate_sheet_name():
    class Loader(BaseLoader):
        __sheetname__ = "test_sheet"

        name = Column(dtype=String)

    answer = pd.DataFrame({"name": ["taro", "hanako"]})
    xlsx = pd.ExcelFile("tests/data/test_validate_sheet_name.xlsx")
    xlsx_loader = Loader(xlsx)
    assert_dataframes(xlsx_loader.data, answer)


def test_definitions():
    class Loader(BaseLoader):
        name = Column(dtype=String)
        age = Column(dtype=Integer)

    assert Loader.definitions()["name"].dtype.name == "str"
    assert Loader.definitions()["age"].dtype.name == "int"


def test_columns():
    class Loader(BaseLoader):
        id = Column(
            dtype=String,
            regexp=r"id_[0-9]{5}",
            nullable=True,
            original_regexp=r"[0-9]{5}",
        )
        name = Column(dtype=String, name="氏名", nullable=True)
        birthday = Column(dtype=DateTime, nullable=True, original_dtype=DateTime)
        age = Column(dtype=Integer, nullable=False, original_nullable=False)
        gender = Column(
            dtype=String,
            category=["man", "woman"],
            nullable=False,
            original_category=["男", "女"],
        )
        is_f1 = Column(dtype=Boolean, nullable=False)
        id_2 = Column(dtype=String, regexp=r"id_[0-9]{5}", nullable=True)
        gender_2 = Column(dtype=String, category=["man", "woman"], nullable=False)
        birthday_2 = Column(dtype=DateTime, nullable=True)

        @modifier("id")
        def modify_id(self, data: pd.DataFrame) -> pd.Series:
            return "id_" + data["id"]

        @modifier("gender")
        def modify_gender(self, data: pd.DataFrame) -> pd.Series:
            return data["gender"].replace({"男": "man", "女": "woman"})

        @creator("is_f1")
        def create_is_f1(self, data: pd.DataFrame) -> pd.Series:
            return (data["gender"] == "woman") * (20 <= data["age"]) * (data["age"] <= 34)

        @creator("id_2")
        def create_id_2(self, data: pd.DataFrame) -> pd.Series:
            return data["id"]

        @creator("gender_2")
        def create_gender_2(self, data: pd.DataFrame) -> pd.Series:
            return data["gender"]

        @creator("birthday_2")
        def create_birthday_2(self, data: pd.DataFrame) -> pd.Series:
            return data["birthday"]

    df = pd.DataFrame(
        {
            "id": ["00001", "00002", "00003", "00004"],
            "氏名": ["taro", "hanako", "kanako", None],
            "birthday": ["1995-10-19", "1998-03-25", None, "1990-08-12"],
            "age": [28, 26, 40, 30],
            "gender": ["男", "女", "女", "男"],
        }
    )
    answer = pd.DataFrame(
        {
            "id": ["id_00001", "id_00002", "id_00003", "id_00004"],
            "name": ["taro", "hanako", "kanako", np.nan],
            "birthday": [
                pd.to_datetime("1995-10-19"),
                pd.to_datetime("1998-03-25"),
                None,
                pd.to_datetime("1990-08-12"),
            ],
            "age": [28, 26, 40, 30],
            "gender": ["man", "woman", "woman", "man"],
            "is_f1": [False, True, False, False],
            "id_2": ["id_00001", "id_00002", "id_00003", "id_00004"],
            "gender_2": ["man", "woman", "woman", "man"],
            "birthday_2": [
                pd.to_datetime("1995-10-19"),
                pd.to_datetime("1998-03-25"),
                None,
                pd.to_datetime("1990-08-12"),
            ],
        }
    )
    loader = Loader(df)

    assert_dataframes(loader.data, answer)
    assert loader.additional_columns() == ["is_f1", "id_2", "gender_2", "birthday_2"]
    assert loader.columns(only_base=False) == [
        "id",
        "name",
        "birthday",
        "age",
        "gender",
        "is_f1",
        "id_2",
        "gender_2",
        "birthday_2",
    ]
    assert loader.columns(only_base=True) == ["id", "name", "birthday", "age", "gender"]
    assert loader.is_nullable_columns(only_base=False) == {
        "age": False,
        "gender": False,
        "is_f1": False,
        "gender_2": False,
    }
    assert loader.is_nullable_columns(only_base=True) == {"age": False, "gender": False}
    assert loader.is_datetime_columns(only_base=False) == {
        "birthday": True,
        "birthday_2": True,
    }
    assert loader.is_datetime_columns(only_base=True) == {"birthday": True}
    assert loader.regexp_columns(only_base=False) == {
        "id": {"regexp": r"id_[0-9]{5}", "nullable": True},
        "id_2": {"regexp": r"id_[0-9]{5}", "nullable": True},
    }
    assert loader.regexp_columns(only_base=True) == {"id": {"regexp": r"id_[0-9]{5}", "nullable": True}}
    assert loader.category_columns(only_base=False) == {
        "gender": {"category": ["man", "woman"], "nullable": False},
        "gender_2": {"category": ["man", "woman"], "nullable": False},
    }
    assert loader.category_columns(only_base=True) == {"gender": {"category": ["man", "woman"], "nullable": False}}
    assert loader.original_is_nullable_columns() == {"age": False}
    assert loader.original_is_datetime_columns() == {"birthday": True}
    assert loader.original_regexp_columns() == {"id": {"regexp": r"[0-9]{5}", "nullable": True}}
    assert loader.original_category_columns() == {"gender": {"category": ["男", "女"], "nullable": False}}
    assert loader.rename_dict() == {"氏名": "name"}
    assert loader.dtype_dict() == {
        "id": String,
        "name": String,
        "birthday": DateTime,
        "age": Integer,
        "gender": String,
        "is_f1": Boolean,
        "id_2": String,
        "gender_2": String,
        "birthday_2": DateTime,
    }
    assert loader.original_dtype_dict() == {"birthday": DateTime}


def test_cast_value():
    class Loader(BaseLoader):
        age = Column(dtype=Integer, nullable=True)

    df = pd.DataFrame({"age": ["28", "26", None, np.nan]})
    loader = Loader(df)
    data = loader.data

    answer = pd.DataFrame({"age": [28, 26, np.nan, np.nan]})
    assert_dataframes(data, answer)


def test_base_loader():
    original = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "age": ["28", "26"],
            "name": ["tomohiko", "yui"],
            "gender": ["男", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
        }
    )
    answer = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "age": [28, 26],
            "name": ["TOMOHIKO", "YUI"],
            "gender": ["男", "女"],
            "birthday": pd.to_datetime(["1995-10-19", "1998-3-25"]),
            "gender_flag": [True, False],
        }
    )

    user_loader = UserLoader(original)
    assert_dataframes(user_loader.data, answer)


def test_necessary_column_not_found():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "gender": ["男", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
        }
    )

    with pytest.raises(NecessaryColumnsNotFoundError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.columns == ["age"]


def test_unnecessary_column_exists():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": [28, 26],
            "gender": ["男", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
            "postal_code": ["111-1111", "222-2222"],
        }
    )

    with pytest.raises(UnnecessaryColumnsExistsError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.columns == ["postal_code"]


def test_nullable():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": [28, None],
            "gender": ["男", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
        }
    )

    with pytest.raises(NullValueFoundError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.column == "age"
    assert pd.isna(e.value.value)
    assert e.value.row_number == 2


def test_is_datetime_1():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": [28, 26],
            "gender": ["男", "女"],
            "birthday": ["1995-10-40", "1998-3-25"],
        }
    )

    with pytest.raises(InvalidDateFoundError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.column == "birthday"
    assert e.value.value == "1995-10-40"
    assert e.value.row_number == 1


def test_is_datetime_2():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": [28, 26],
            "gender": ["男", "女"],
            "birthday": ["birthday", "1998-3-25"],
        }
    )

    with pytest.raises(InvalidDateLiteralFoundError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.column == "birthday"
    assert e.value.value == "birthday"
    assert e.value.row_number == 1


def test_regexp():
    data = pd.DataFrame(
        {
            "id": ["1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": [28, 26],
            "gender": ["男", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
        }
    )

    with pytest.raises(InvalidRegexpFoundError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.column == "id"
    assert e.value.value == "1"
    assert e.value.row_number == 1


def test_category():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": [28, 26],
            "gender": ["man", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
        }
    )

    with pytest.raises(InvalidCategoryFoundError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.column == "gender"
    assert e.value.value == "man"
    assert e.value.row_number == 1


def test_series_cast():
    data = pd.DataFrame(
        {
            "id": ["id_1", "id_2"],
            "name": ["tomohiko", "yui"],
            "age": ["二十八", 26],
            "gender": ["男", "女"],
            "birthday": ["1995-10-19", "1998-3-25"],
        }
    )

    with pytest.raises(ColumnCastError) as e:
        user_loader = UserLoader(data)  # noqa

    assert e.value.column == "age"
    assert e.value.from_ == "object"
    assert e.value.to_ == "int"


def test_filter():
    class Loader(BaseLoader):
        age = Column(dtype=Integer, nullable=True)

        @data_filter()
        def filter_age(self, data: pd.DataFrame) -> pd.DataFrame:
            return data.query("age >= 20").reset_index(drop=True)

    df = pd.DataFrame({"age": [28, 26, 18, 30, 10]})
    loader = Loader(df)

    answer = pd.DataFrame({"age": [28, 26, 30]})
    assert_dataframes(loader.data, answer)


def test_reference_column():
    class PrefectureLoader(BaseLoader):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberLoader(BaseLoader):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureLoader.prefecture_name, how="left", on="prefecture_code")

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )
    df_prefecture = pd.DataFrame(
        {
            "prefecture_code": ["001", "002"],
            "prefecture_name": ["tokyo", "osaka"],
        }
    )
    answer = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
            "prefecture_name": ["tokyo", "osaka", np.nan],
        }
    )

    prefecture_loader = PrefectureLoader(df_prefecture)
    member_loader = MemberLoader(df_member, reference_data=[prefecture_loader])

    assert_dataframes(member_loader.data, answer)


def test_reference_data_with_error_1():
    class PrefectureLoader(BaseLoader):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberLoader(BaseLoader):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureLoader.prefecture_name, how="left", on="prefecture_code")

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )

    with pytest.raises(ReferenceDataNotInitializationError) as e:
        _ = MemberLoader(df_member)

    assert e.value.name == "prefecture_name"


def test_reference_data_with_error_2():
    class PrefectureLoader(BaseLoader):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberLoader(BaseLoader):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureLoader.prefecture_name, how="left", on="prefecture_code")

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )
    df_prefecture = pd.DataFrame(
        {
            "prefecture_code": ["001", "002"],
            "prefecture_name": ["tokyo", "osaka"],
        }
    )

    with pytest.raises(ReferenceDataNotFoundError) as e:
        _ = PrefectureLoader(df_prefecture)
        _ = MemberLoader(df_member)

    assert e.value.name == "PrefectureLoader"


def test_order():
    class PrefectureLoader(BaseLoader):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberLoader(BaseLoader):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureLoader.prefecture_name, how="left", on="prefecture_code")

        @modifier("prefecture_name", order=1)
        def modify_prefecture_name(self, data: pd.DataFrame) -> pd.Series:
            return data["prefecture_name"].str.upper()

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )
    df_prefecture = pd.DataFrame(
        {
            "prefecture_code": ["001", "002"],
            "prefecture_name": ["tokyo", "osaka"],
        }
    )
    answer = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
            "prefecture_name": ["TOKYO", "OSAKA", np.nan],
        }
    )

    prefecture_loader = PrefectureLoader(df_prefecture)
    member_loader = MemberLoader(df_member, reference_data=[prefecture_loader])

    assert_dataframes(member_loader.data, answer)
