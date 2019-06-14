from unittest.mock import MagicMock


def mockfn(fn):
    mock = MagicMock(side_effect=fn)
    mock.__name__ = fn.__name__
    return mock

