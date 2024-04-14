import numpy as np
import pandas as pd
import pytest

from prep_flow import (
    BaseFlow,
    Boolean,
    Column,
    ColumnCastError,
    DateTime,
    DecoratorError,
    DecoratorReturnTypeError,
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


class UserFlow(BaseFlow):
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
    class Flow(BaseFlow):
        name = Column(dtype=String)

    df = pd.DataFrame({"name": ["taro", "hanako"]})
    xlsx = pd.ExcelFile("tests/data/test_parse_data.xlsx")

    csv_flow = Flow(df)
    xlsx_flow = Flow(xlsx)

    assert_dataframes(csv_flow.data, df)
    assert_dataframes(xlsx_flow.data, df)


def test_validate_sheet_name():
    class Flow(BaseFlow):
        __sheetname__ = "test_sheet"

        name = Column(dtype=String)

    answer = pd.DataFrame({"name": ["taro", "hanako"]})
    xlsx = pd.ExcelFile("tests/data/test_validate_sheet_name.xlsx")
    xlsx_flow = Flow(xlsx)
    assert_dataframes(xlsx_flow.data, answer)


def test_definitions():
    class Flow(BaseFlow):
        name = Column(dtype=String)
        age = Column(dtype=Integer)

    assert Flow.definitions()["name"].dtype.name == "str"
    assert Flow.definitions()["age"].dtype.name == "int"


def test_columns():
    class Flow(BaseFlow):
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
    flow = Flow(df)

    assert_dataframes(flow.data, answer)
    assert flow.additional_columns() == ["is_f1", "id_2", "gender_2", "birthday_2"]
    assert flow.columns(only_base=False) == [
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
    assert flow.columns(only_base=True) == ["id", "name", "birthday", "age", "gender"]
    assert flow.is_nullable_columns(only_base=False) == {
        "age": False,
        "gender": False,
        "is_f1": False,
        "gender_2": False,
    }
    assert flow.is_nullable_columns(only_base=True) == {"age": False, "gender": False}
    assert flow.is_datetime_columns(only_base=False) == {
        "birthday": True,
        "birthday_2": True,
    }
    assert flow.is_datetime_columns(only_base=True) == {"birthday": True}
    assert flow.regexp_columns(only_base=False) == {
        "id": {"regexp": r"id_[0-9]{5}", "nullable": True},
        "id_2": {"regexp": r"id_[0-9]{5}", "nullable": True},
    }
    assert flow.regexp_columns(only_base=True) == {"id": {"regexp": r"id_[0-9]{5}", "nullable": True}}
    assert flow.category_columns(only_base=False) == {
        "gender": {"category": ["man", "woman"], "nullable": False},
        "gender_2": {"category": ["man", "woman"], "nullable": False},
    }
    assert flow.category_columns(only_base=True) == {"gender": {"category": ["man", "woman"], "nullable": False}}
    assert flow.original_is_nullable_columns() == {"age": False}
    assert flow.original_is_datetime_columns() == {"birthday": True}
    assert flow.original_regexp_columns() == {"id": {"regexp": r"[0-9]{5}", "nullable": True}}
    assert flow.original_category_columns() == {"gender": {"category": ["男", "女"], "nullable": False}}
    assert flow.rename_dict() == {"氏名": "name"}
    assert flow.dtype_dict() == {
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
    assert flow.original_dtype_dict() == {"birthday": DateTime}


def test_cast_value():
    class Flow(BaseFlow):
        name = Column(dtype=String, nullable=True)
        age = Column(dtype=Integer, nullable=True)

    df = pd.DataFrame(
        {
            "name": ["Taro", None, np.nan, "Hanako"],
            "age": ["28", "26", None, np.nan],
        }
    )
    flow = Flow(df)
    data = flow.data

    answer = pd.DataFrame({"name": ["Taro", np.nan, np.nan, "Hanako"], "age": [28, 26, np.nan, np.nan]})
    assert_dataframes(data, answer)
    assert data["name"].dtype == "object"
    assert data["age"].dtype == "float64"


def test_base_flow():
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

    user_flow = UserFlow(original)
    assert_dataframes(user_flow.data, answer)


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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

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
        user_flow = UserFlow(data)  # noqa

    assert e.value.column == "age"
    assert e.value.from_ == "object"
    assert e.value.to_ == "int"


def test_filter():
    class Flow(BaseFlow):
        age = Column(dtype=Integer, nullable=True)

        @data_filter()
        def filter_age(self, data: pd.DataFrame) -> pd.DataFrame:
            return data.query("age >= 20").reset_index(drop=True)

    df = pd.DataFrame({"age": [28, 26, 18, 30, 10]})
    flow = Flow(df)

    answer = pd.DataFrame({"age": [28, 26, 30]})
    assert_dataframes(flow.data, answer)


def test_reference_column_1():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code")

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

    prefecture_flow = PrefectureFlow(df_prefecture)
    member_flow = MemberFlow(df_member, reference=[prefecture_flow])

    assert_dataframes(member_flow.data, answer)


def test_reference_data_with_error_1():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code")

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )

    with pytest.raises(ReferenceDataNotInitializationError) as e:
        _ = MemberFlow(df_member)

    assert e.value.name == "prefecture_name"


def test_reference_data_with_error_2():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code")

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
        _ = PrefectureFlow(df_prefecture)
        _ = MemberFlow(df_member)

    assert e.value.name == "PrefectureFlow"


def test_reference_data_with_error_3():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(
            column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code", nullable=False
        )

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

    with pytest.raises(NullValueFoundError) as e:
        prefecture = PrefectureFlow(df_prefecture)
        _ = MemberFlow(df_member, reference=[prefecture])

    assert e.value.column == "prefecture_name"
    assert pd.isna(e.value.value)
    assert e.value.row_number == 3


def test_reference_data_with_error_4():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(
            column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code", regexp=r"^[A-Z]+$"
        )

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )
    df_prefecture = pd.DataFrame(
        {
            "prefecture_code": ["001", "002"],
            "prefecture_name": ["tokyo", "OSAKA"],
        }
    )

    with pytest.raises(InvalidRegexpFoundError) as e:
        prefecture = PrefectureFlow(df_prefecture)
        _ = MemberFlow(df_member, reference=[prefecture])

    assert e.value.column == "prefecture_name"
    assert e.value.regexp == r"^[A-Z]+$"
    assert e.value.value == "tokyo"
    assert e.value.row_number == 1


def test_reference_data_with_error_5():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(
            column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code", category=["tokyo", "osaka"]
        )

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "003"],
        }
    )
    df_prefecture = pd.DataFrame(
        {
            "prefecture_code": ["001", "002", "003"],
            "prefecture_name": ["tokyo", "osaka", "nagoya"],
        }
    )

    with pytest.raises(InvalidCategoryFoundError) as e:
        prefecture = PrefectureFlow(df_prefecture)
        _ = MemberFlow(df_member, reference=[prefecture])

    assert e.value.column == "prefecture_name"
    assert e.value.category == ["tokyo", "osaka"]
    assert e.value.value == "nagoya"
    assert e.value.row_number == 3


def test_reference_column_with_modifier():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code")

        @modifier("prefecture_name")
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

    prefecture_flow = PrefectureFlow(df_prefecture)
    member_flow = MemberFlow(df_member, reference=[prefecture_flow])

    assert_dataframes(member_flow.data, answer)


def test_order():
    class PrefectureFlow(BaseFlow):
        prefecture_code = Column(dtype=String)
        prefecture_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String)
        prefecture_code = Column(dtype=String)
        prefecture_name = ReferenceColumn(column=PrefectureFlow.prefecture_name, how="left", on="prefecture_code")
        prefecture_code_and_name = Column(dtype=String)

        @creator("prefecture_code_and_name", use_reference=True)
        def create_prefecture_code_and_name(self, data: pd.DataFrame) -> pd.Series:
            return data["prefecture_code"] + ": " + data["prefecture_name"]

        @data_filter(use_reference=True)
        def filter_prefecture_name(self, data: pd.DataFrame) -> pd.DataFrame:
            return data.query('prefecture_name == "TOKYO"').reset_index(drop=True)

    df_member = pd.DataFrame(
        {
            "name": ["taro", "hanako", "jiro"],
            "prefecture_code": ["001", "002", "002"],
        }
    )
    df_prefecture = pd.DataFrame(
        {
            "prefecture_code": ["001", "002"],
            "prefecture_name": ["TOKYO", "OSAKA"],
        }
    )
    answer = pd.DataFrame(
        {
            "name": ["taro"],
            "prefecture_code": ["001"],
            "prefecture_name": ["TOKYO"],
            "prefecture_code_and_name": ["001: TOKYO"],
        }
    )

    prefecture_flow = PrefectureFlow(df_prefecture)
    member_flow = MemberFlow(df_member, reference=[prefecture_flow])

    assert_dataframes(member_flow.data, answer)


def test_column_modifier():
    class CountryFlow(BaseFlow):
        country_code = Column(dtype=String)
        country_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String, modifier=lambda x: x.lower())
        country_code = Column(dtype=String)
        country_name = ReferenceColumn(
            CountryFlow.country_name, how="left", on="country_code", modifier=lambda x: x.lower()
        )

    df_country = pd.DataFrame({"country_code": ["JP", "US"], "country_name": ["JAPAN", "AMERICA"]})
    df_member = pd.DataFrame({"name": ["TARO", "JIRO", "HANAKO"], "country_code": ["JP", "JP", "US"]})
    df_answer = pd.DataFrame(
        {
            "name": ["taro", "jiro", "hanako"],
            "country_code": ["JP", "JP", "US"],
            "country_name": ["japan", "japan", "america"],
        }
    )

    country = CountryFlow(df_country)
    member = MemberFlow(df_member, reference=[country])

    assert_dataframes(member.data, df_answer)


def test_decorator_with_error_1():
    class MemberFlow(BaseFlow):
        name = Column(dtype=String)

        @modifier("age")
        def modify_age(self, data: pd.DataFrame) -> pd.Series:
            return data["age"] + 1

    df_member = pd.DataFrame({"name": ["Taro", "Hanako"]})

    with pytest.raises(DecoratorError) as e:
        _ = MemberFlow(df_member)

    assert e.value.column == "age"


def test_decorator_with_error_2():
    class CountryFlow(BaseFlow):
        country_code = Column(dtype=String)
        country_name = Column(dtype=String)

    class MemberFlow(BaseFlow):
        name = Column(dtype=String, modifier=lambda x: x.lower())
        country_code = Column(dtype=String)
        country_name = ReferenceColumn(CountryFlow.country_name, how="left", on="country_code")

        @creator("country_name", use_reference=True)
        def create_country_code_and_name(self, data: pd.DataFrame) -> pd.Series:
            return data["country_code"] + ": " + data["country_name"]

    df_country = pd.DataFrame({"country_code": ["JP", "US"], "country_name": ["JAPAN", "AMERICA"]})
    df_member = pd.DataFrame({"name": ["TARO", "JIRO", "HANAKO"], "country_code": ["JP", "JP", "US"]})

    with pytest.raises(DecoratorError) as e:
        country = CountryFlow(df_country)
        _ = MemberFlow(df_member, reference=[country])

        assert e.value.column == "country_name"


def test_decorator_with_error_3():
    class MemberFlow(BaseFlow):
        name = Column(dtype=String)

        @data_filter()
        def filter_name(self, data: pd.DataFrame):
            return data["name"] == "Taro"

    df_member = pd.DataFrame({"name": ["Taro", "Hanako"]})

    with pytest.raises(DecoratorReturnTypeError):
        _ = MemberFlow(df_member)

    assert True
