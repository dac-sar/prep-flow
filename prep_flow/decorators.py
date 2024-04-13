from typing import Callable

DECORATOR_KEY = "__decorator__"
CREATOR_KEY = "__creator__"
MODIFIER_KEY = "__modifier__"
FILTER_KEY = "__filter__"


# TODO: ReferenceColumnに付与できないようにする
def creator(column: str, order: int = 0) -> Callable:
    if column is None:
        raise Exception("creator with no column specified.")

    def dec(f: Callable) -> classmethod:
        f_cls = classmethod(f)
        setattr(f_cls, DECORATOR_KEY, (CREATOR_KEY, column, order))
        return f_cls

    return dec


def modifier(column: str, order: int = 0) -> Callable:
    if column is None:
        raise Exception("modifier with no column specified.")

    def dec(f: Callable) -> classmethod:
        f_cls = f if isinstance(f, classmethod) else classmethod(f)
        setattr(f_cls, DECORATOR_KEY, (MODIFIER_KEY, column, order))
        return f_cls

    return dec


def data_filter(order: int = 0) -> Callable:
    def dec(f: Callable) -> classmethod:
        f_cls = f if isinstance(f, classmethod) else classmethod(f)
        setattr(f_cls, DECORATOR_KEY, (FILTER_KEY, None, order))
        return f_cls

    return dec
