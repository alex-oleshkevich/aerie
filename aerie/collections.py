from __future__ import annotations

import functools
import itertools
import statistics
import typing as t

from aerie.protocols import StrLike

E = t.TypeVar("E")


def chunked(items: t.Iterator, size: int):
    result = []
    for value in items:
        result.append(value)
        if len(result) == size:
            yield result
            result = []

    if len(result):
        yield result


def attribute_reader(obj, attr, default=None) -> t.Any:
    if hasattr(obj, "__getitem__"):
        return obj.get(attr, default)
    return getattr(obj, attr, default)


class Collection(t.Generic[E]):
    def __init__(self, items: t.List[E]):
        self.items = list(items)
        self._position = 0

    def first(self) -> t.Optional[E]:
        """Return the first item from the collection."""
        try:
            return next(self)
        except StopIteration:
            return None

    def last(self) -> t.Optional[E]:
        """Return the last item from the collection."""
        return self.reverse().first()

    def find(self, fn: t.Callable[[E], bool]) -> t.Optional[E]:
        def _filter(entity: E) -> bool:
            return fn(entity)

        return self.filter(_filter).first()

    def reverse(self) -> Collection[E]:
        """Reverse collection."""
        return Collection(list(reversed(self.as_list())))

    def chunk(self, batch: int) -> t.Generator[E, None, None]:
        """Split collection into chunks."""
        return chunked(self, batch)

    def pluck(self, key: StrLike) -> Collection[t.Any]:
        """Take a attribute/key named `key` from every item
        and return them in a new collection."""
        key = str(key)
        return Collection([attribute_reader(item, key, None) for item in self])

    def avg(self, field: StrLike = None) -> float:
        """Compute average value of all items.
        If `field` is given and items are not mappings/classes
         the exception will be raised."""
        if not len(self):
            raise ArithmeticError("Cannot find avg value: collection is empty.")
        items = self.pluck(field).as_list() if field else self.items
        return statistics.mean(items or [0])

    def min(self, field: StrLike = None) -> float:
        """Find the smallest item.

        When `field` is given it returns the smallest item by `field` value.
        If collection is empty it will raise ArithmeticError."""
        if not len(self):
            raise ArithmeticError("Cannot find min value: collection is empty.")
        items = self.pluck(field).as_list() if field else self.items
        return min(items)

    def max(self, field: StrLike = None) -> float:
        """Find the biggest item.

        When `field` is given it returns the biggest item by `field` value.
        If collection is empty it will raise ArithmeticError."""
        items = self.pluck(field).as_list() if field else self.items
        if not len(self):
            raise ArithmeticError("Cannot find max value: collection is empty.")
        return max(items)

    def sum(self, field: StrLike = None) -> float:
        """Find a sum of all items in a collection.

        If collection is empty it will raise ArithmeticError."""
        items = self.pluck(field).as_list() if field else self.items
        if not len(self):
            raise ArithmeticError("Cannot find sum value: collection is empty.")
        return sum(items)

    def map(self, fn: t.Callable[[E], t.Any]) -> Collection:
        """Apply a function on each collection item
        and return a new collection."""
        return Collection(list(map(fn, self.items)))

    def each(self, fn: t.Callable[[E, int], None]) -> Collection[E]:
        """Apply a function on each collection item
        and return same collection.

        Callback will receive current item as the first argument
        and current iteration index as the second argument."""
        for index, item in enumerate(self.items):
            fn(item, index)
        return self

    def every(self, fn: t.Callable[[E], bool]) -> bool:
        """Test if all items match the condition specified by `fn`."""
        return all(map(fn, self))

    def some(self, fn: t.Callable[[E], bool]) -> bool:
        """Test if at least one item matches the condition specified by `fn`."""
        return any(map(fn, self))

    def prepend(self, item: E) -> Collection[E]:
        """Add an item to the top of collection."""
        return Collection([item, *self])

    def append(self, item: E) -> Collection[E]:
        """Add an item to the bottom of collection."""
        return Collection([*self, item])

    def filter(self, fn: t.Callable[[E], bool]) -> Collection[E]:
        """Filter collection items with `fn`."""
        return Collection(list(filter(fn, self.items)))

    def reduce(self, fn: t.Callable[[t.Any, E], bool], start: t.Any = None) -> t.Any:
        return functools.reduce(fn, self.items, start)

    def sort(
        self,
        key: t.Union[t.Callable[[t.Any], t.Any], str] = None,
        reverse: bool = False,
    ) -> Collection[E]:
        """Return a new sorted collection from the items in this collection."""
        sort_kwargs = {}
        if isinstance(key, str):
            key = functools.partial(attribute_reader, attr=key)

        if key is not None:
            sort_kwargs["key"] = key
        return Collection(list(sorted(self, reverse=reverse, **sort_kwargs)))

    def group_by(
        self, key: t.Union[t.Callable[[t.Any], str], str]
    ) -> t.Dict[str, t.List[E]]:
        if isinstance(key, str):
            key = functools.partial(attribute_reader, attr=key)

        groups = itertools.groupby(self, key=key)
        return {k: list(v) for k, v in groups}

    def key_value(
        self, key: t.Union[t.Callable[[t.Any], str], str]
    ) -> t.Dict[t.Any, E]:
        if isinstance(key, str):
            key = functools.partial(attribute_reader, attr=key)

        return {key(item): item for item in self}

    def as_list(self) -> t.List[E]:
        return list(self.items)

    @t.overload
    def __getitem__(self, index: slice) -> Collection[E]:
        ...

    @t.overload
    def __getitem__(self, index: int) -> E:
        ...

    def __getitem__(self, index: t.Union[int, slice]) -> t.Union[E, Collection[E]]:
        if isinstance(index, slice):
            return Collection(
                [item for item in self.items[index.start : index.stop : index.step]]
            )
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

    @t.overload
    def __eq__(self, other: Collection[E]) -> bool:
        ...

    @t.overload
    def __eq__(self, other: object) -> bool:
        ...

    def __eq__(self, other: t.Union[Collection[E], object]) -> bool:
        if not isinstance(other, Collection):
            raise ValueError("Can compare only collections.")
        return self.items == other.items

    def __str__(self) -> str:
        truncate = 10
        remainder = 0
        if len(self) > truncate:
            remainder = len(self) - truncate

        contents = ",".join(map(str, self[0:10]))
        suffix = ""
        if remainder:
            suffix = f" and {remainder} items more"

        return f"<Collection: [{contents}{suffix}]>"
