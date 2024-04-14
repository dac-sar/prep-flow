# prep-flow

Data preprocessing framework with type validation for data scientists.

## What is it?

`prep-flow` is a framework for declaratively describing data preprocessing, which is a common task in the field of data science. 
Data preprocessing using pandas often leads to procedural code and tends to be low readability and maintainability.
`prep-flow` solves this problem and provides declarative and highly readable code.


## Install

```shell
$> pip install prep-flow
```

## A Simple Example

```python
from datetime import datetime

import pandas as pd
from prep_flow import BaseFlow, Column, String, Integer, DateTime, modifier, creator

df_member = pd.DataFrame({
    "name": ["Taro Yamada", "John Smith", "Li Wei", "Hanako Tanaka"],
    "gender": ["man", "man", "man", "woman"],
    "birthday": ["1995/10/19", "1990/03/20", "2003/02/01", "1985/11/18"],
})

class MemberFlow(BaseFlow):
    name = Column(dtype=String, name="name", description='Add "Mr." or "Ms." depending on the gender.')
    gender = Column(dtype=String, category=["man", "woman"])
    birthday = Column(dtype=DateTime)
    age = Column(dtype=Integer)
    
    @modifier("name")
    def modify_name(self, data: pd.DataFrame) -> pd.Series:
        data["prefix"] = data["gender"].apply(lambda x: "Mr." if x == "man" else "Ms.")
        return data["prefix"] + data["name"]
    
    @creator("age")
    def create_age(self, data: pd.DataFrame) -> pd.Series:
        return data["birthday"].apply(lambda x: (datetime.now() - x).days // 365)

member = MemberFlow(df_member)
print(member.data)
```
| name             | gender | birthday   | age |
|------------------|--------|------------|-----|
| Mr.Taro Yamada   | man    | 1995/10/19 | 28  |
| Mr.John Smith    | man    | 1990/03/20 | 34  |
| Mr.Li Wei        | man    | 2003/02/01 | 21  |
| Ms.Hanako Tanaka | woman  | 1985/11/18 | 38  |
