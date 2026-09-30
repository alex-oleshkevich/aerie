"""Pagination results.

Offset pages carry a total, which needs a second COUNT over the *unbounded* query --
`count()` deliberately respects the query's own LIMIT, so a paginator cannot reuse it.

Cursor pages carry no total. Computing one would cost the scan that keyset paging
exists to avoid.
"""

import dataclasses
import typing
from collections.abc import Iterator, Sequence


@dataclasses.dataclass(frozen=True, slots=True)
class Page[T]:
    """One page of an offset-paginated result."""

    items: Sequence[T]
    total: int
    number: int
    size: int

    def __iter__(self) -> Iterator[T]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    @property
    def pages(self) -> int:
        if self.size <= 0:
            return 0
        return -(-self.total // self.size)

    @property
    def has_next(self) -> bool:
        return self.number < self.pages

    @property
    def has_previous(self) -> bool:
        return self.number > 1


@dataclasses.dataclass(frozen=True, slots=True)
class CursorPage[T]:
    """One page of a keyset-paginated result.

    `next_cursor` is None once the final page is reached. Feed it back as `after`.
    """

    items: Sequence[T]
    next_cursor: tuple[typing.Any, ...] | None

    def __iter__(self) -> Iterator[T]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    @property
    def has_next(self) -> bool:
        return self.next_cursor is not None
