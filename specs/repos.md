# Base repository

```python
import typing as t
E = t.TypeVar('E', type)
PK = t.TypeVar('PK', int, list, tuple)

class BaseRepository(t.Generic[E, PK]):
    connection: str = 'default'
```

# CRUD repository

```python
import typing as t
E = t.TypeVar('E', type)
PK = t.TypeVar('PK', int, list, tuple)


class CRUDRepository(BaseRepository[E, PK]):
    async def save(self, entity: E) -> None: ...
    async def find(self, pk: PK) -> t.Optional[E]: ...
    async def find_all(self) -> t.Iterable[E]: ...
    async def count(self) -> int: ... 
    async def exists(self, pk: PK) -> bool: ... 
    async def delete(self, entity: E) -> None: ... 
```
