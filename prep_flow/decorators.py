from typing import Callable

DECORATOR_KEY = "__decorator__"
CREATOR_KEY = "__creator__"
MODIFIER_KEY = "__modifier__"
FILTER_KEY = "__filter__"


def creator(column: str, use_reference: bool = False, order: int = 0) -> Callable:
    if column is None:
        raise Exception("creator with no column specified.")

    if use_reference:
        order = 1

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


def data_filter(use_reference: bool = False, order: int = 0) -> Callable:
    if use_reference:
        order = 1

    def dec(f: Callable) -> classmethod:
        f_cls = f if isinstance(f, classmethod) else classmethod(f)
        setattr(f_cls, DECORATOR_KEY, (FILTER_KEY, None, order))
        return f_cls

    return dec
