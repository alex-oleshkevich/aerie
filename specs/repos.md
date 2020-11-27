# Base repository

```python
import typing as t
E = t.TypeVar('E', type)

class BaseRepository(t.Generic[E]):
    connection: str = 'default'
```

# CRUD repository

```python
import typing as t
E = t.TypeVar('E', type)

class CRUDRepository(BaseRepository[E]):
    async def save(self, entity: E) -> None: ...
```
