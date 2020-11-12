import importlib
import typing as t


def import_string(path: str) -> t.Any:
    module, *rest, member = path.rpartition('.')
    module = importlib.import_module(module)
    return getattr(module, member)
