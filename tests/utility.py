from unittest.mock import MagicMock


def mockfn(func):
    mock = MagicMock(side_effect=func)
    mock.__name__ = func.__name__
    return mock
