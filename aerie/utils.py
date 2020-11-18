import importlib
import typing as t


def import_string(path: str) -> t.Any:
    module_name, *rest, member = path.rpartition(".")
    module = importlib.import_module(module_name)
    return getattr(module, member)
