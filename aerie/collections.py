from __future__ import annotations

import functools
import itertools
import statistics
import typing as t
from typing import Any, Callable, Dict, Generator, Generic, Iterable, List, Optional, Protocol, TypeVar, Union, overload

from aerie.utils import chunked

E = TypeVar("E", bound=object)


class CastsToString(Protocol):  # pragma: nocover
    def __str__(self) -> str:
        ...


StrLike = Union[str, CastsToString]


class EmptyError(ValueError):
    pass


def attribute_reader(obj: Any, attr: str, default: Any = None) -> Any:
    if hasattr(obj, "__getitem__"):
        return obj.get(attr, default)
    return getattr(obj, attr, default)


class Collection(Generic[E], Iterable[E]):
    def __init__(self, items: Iterable[E]) -> None:
        self.items = list(items)
        self._position = 0

    def first(self) -> Optional[E]:
        """Return the first item from the collection."""
        try:
            return next(self)
        except StopIteration:
            return None

    def last(self) -> Optional[E]:
        """Return the last item from the collection."""
        return self.reverse().first()

    def find(self, fn: Callable[[E], Optional[bool]]) -> Optional[E]:
        return self.filter(fn).first()

    def reverse(self) -> Collection[E]:
        """Reverse collection."""
        return Collection(list(reversed(self)))

    def chunk(self, batch: int) -> Generator[List[E], None, None]:
        """Split collection into chunks."""
        return chunked(self, batch)

    def pluck(self, key: StrLike) -> Collection[Any]:
        """Take a attribute/key named `key` from every item
        and return them in a new collection."""
        key = str(key)
        return Collection([attribute_reader(item, key, None) for item in self])

    def avg(self, field: StrLike = None) -> float:
        """Compute average value of all items.
        If `field` is given and items are not mappings/classes
         the exception will be raised."""
        if not len(self):
            raise EmptyError("Cannot find avg value: collection is empty.")
        items = self.pluck(field) if field else self.items
        return statistics.mean(items or [0])

    def min(self, field: StrLike = None) -> float:
        """Find the smallest item.

        When `field` is given it returns the smallest item by `field` value.
        If collection is empty it will raise ArithmeticError."""
        if not len(self):
            raise EmptyError("Cannot find min value: collection is empty.")
        items = self.pluck(field) if field else self.items
        return min(items)

    def max(self, field: StrLike = None) -> float:
        """Find the biggest item.

        When `field` is given it returns the biggest item by `field` value.
        If collection is empty it will raise ArithmeticError."""
        items = self.pluck(field) if field else self.items
        if not len(self):
            raise EmptyError("Cannot find max value: collection is empty.")
        return max(items)

    def sum(self, field: StrLike = None) -> float:
        """Find a sum of all items in a collection.

        If collection is empty it will raise ArithmeticError."""
        items = self.pluck(field) if field else self.items
        if not len(self):
            raise EmptyError("Cannot find sum value: collection is empty.")
        return sum(items)

    def map(self, fn: Callable[[E], Any]) -> Collection:
        """Apply a function on each collection item
        and return a new collection."""
        return Collection(list(map(fn, self)))

    def each(self, fn: Callable[[E, int], None]) -> Collection[E]:
        """Apply a function on each collection item
        and return same collection.

        Callback will receive current item as the first argument
        and current iteration index as the second argument."""
        for index, item in enumerate(self.items):
            fn(item, index)
        return self

    def every(self, fn: Callable[[E], bool]) -> bool:
        """Test if all items match the condition specified by `fn`."""
        return all(map(fn, self))

    def some(self, fn: Callable[[E], bool]) -> bool:
        """Test if at least one item matches the condition specified by `fn`."""
        return any(map(fn, self))

    def prepend(self, item: E) -> Collection[E]:
        """Add an item to the top of collection."""
        return Collection([item, *self])

    def append(self, item: E) -> Collection[E]:
        """Add an item to the bottom of collection."""
        return Collection([*self, item])

    def filter(self, fn: Callable[[E], Optional[bool]]) -> Collection[E]:
        """Filter collection items with `fn`."""
        return Collection(list(filter(fn, self.items)))

    def reduce(self, fn: Callable[[Any, Any], Any], start: Any = None) -> Any:
        return functools.reduce(fn, self.items, start)

    def sort(self, key: Union[Callable, str] = None, reverse: bool = False) -> Collection[E]:
        """Return a new sorted collection from the items in this collection."""
        if isinstance(key, str):
            key = functools.partial(attribute_reader, attr=key)
        # fixme:
        return Collection(list(sorted(self, key=key, reverse=reverse)))  # type: ignore

    def group_by(self, key: Union[Callable[[Any], str], str]) -> Dict[str, List[E]]:
        if isinstance(key, str):
            key = functools.partial(attribute_reader, attr=key)

        groups = itertools.groupby(sorted(self.items, key=key), key=key)
        return {k: list(v) for k, v in groups}

    def key_value(self, key: Union[Callable[[Any], str], str]) -> Dict[Any, E]:
        if isinstance(key, str):
            key = functools.partial(attribute_reader, attr=key)

        return {key(item): item for item in self}

    def as_list(self) -> List[E]:
        return list(self)

    def choices(self, label_col: str = 'name', value_col: str = 'id') -> list[t.Tuple[Any, Any]]:
        return [(attribute_reader(item, value_col), attribute_reader(item, label_col)) for item in self]

    def choices_dict(
        self, label_col: str = 'name', value_col: str = 'id', label_key: str = 'label', value_key: str = 'value'
    ) -> list[t.Dict[Any, Any]]:
        return [
            {value_key: attribute_reader(item, value_col), label_key: attribute_reader(item, label_col)}
            for item in self
        ]

    @overload
    def __getitem__(self, index: slice) -> list[E]:  # pragma: nocover
        ...

    @overload
    def __getitem__(self, index: int) -> E:  # pragma: nocover
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[E, list[E]]:
        if isinstance(index, slice):
            return [item for item in self.items[index.start : index.stop : index.step]]
        return self.items[index]

    def __setitem__(self, key: int, value: E) -> None:
        self.items.insert(key, value)

    def __delitem__(self, key: int) -> None:
        self.items.pop(key)

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Collection[E]:
        return self

    def __next__(self) -> E:
        if self._position < len(self):
            entity = self.items[self._position]
            self._position += 1
            return entity
        raise StopIteration()

    def __contains__(self, item: E) -> bool:
        return item in self.items

    def __eq__(self, other: Any) -> bool:
        return self.items == other.items

    def __reversed__(self) -> Collection[E]:
        return Collection(reversed(list(self)))

    def __str__(self) -> str:
        truncate = 10
        remainder = len(self) - truncate if len(self) > truncate else 0
        contents = ",".join(map(str, self[0:10]))
        suffix = f" and {remainder} items more" if remainder else ""
        return f"<Collection: [{contents}{suffix}]>"
