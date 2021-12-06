import pytest
from typing import Any

from aerie.collections import Collection


def test_first() -> None:
    collection = Collection([1, 2, 3])
    assert collection.first() == 1


def test_first_on_empty() -> None:
    collection = Collection[int]([])
    assert collection.first() is None


def test_last() -> None:
    collection = Collection([1, 2, 3])
    assert collection.last() == 3


def test_find() -> None:
    collection = Collection([1, 2, 3])
    assert collection.find(lambda x: x == 2) == 2


def test_last_on_empty() -> None:
    collection = Collection[int]([])
    assert collection.last() is None


def test_reverse() -> None:
    collection = Collection([1, 2, 3])
    assert collection.reverse() == Collection([3, 2, 1])


def test_chunk() -> None:
    collection = Collection([1, 2, 3, 4, 5, 6])
    chunks = list(collection.chunk(2))
    assert len(chunks) == 3
    assert chunks[0] == [1, 2]
    assert chunks[1] == [3, 4]
    assert chunks[2] == [5, 6]


def test_pluck_on_dicts() -> None:
    collection = Collection([{"a": 1}, {"a": 2}])
    assert collection.pluck("a").as_list() == [1, 2]


def test_pluck_on_classes() -> None:
    class Class:
        def __init__(self, val: int) -> None:
            self.a = val

    collection = Collection([Class(1), Class(2)])
    assert collection.pluck("a").as_list() == [1, 2]


def test_avg_raises_if_empty() -> None:
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection[int]([])
        collection.avg()
    assert str(ex.value) == "Cannot find avg value: collection is empty."


def test_avg_over_scalar() -> None:
    collection = Collection([1, 2, 3])
    assert collection.avg() == 2


def test_avg_by_key_over_dicts() -> None:
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.avg("a") == 2


def test_avg_by_key_over_classes() -> None:
    class Class:
        def __init__(self, val: int) -> None:
            self.a = val

    collection = Collection([Class(1), Class(2), Class(3)])
    assert collection.avg("a") == 2


def test_min_raises_if_empty() -> None:
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection[int]([])
        collection.min()
    assert str(ex.value) == "Cannot find min value: collection is empty."


def test_min_over_scalar() -> None:
    collection = Collection([1, 2, 3])
    assert collection.min() == 1


def test_min_by_key_over_dicts() -> None:
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.min("a") == 1


def test_min_by_key_over_classes() -> None:
    class Class:
        def __init__(self, val: int) -> None:
            self.a = val

    obj1 = Class(1)
    obj2 = Class(2)
    obj3 = Class(3)

    collection = Collection([obj1, obj2, obj3])
    assert collection.min("a") == 1


def test_max_raises_if_empty() -> None:
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection[int]([])
        collection.max()
    assert str(ex.value) == "Cannot find max value: collection is empty."


def test_max_over_scalar() -> None:
    collection = Collection([1, 2, 3])
    assert collection.max() == 3


def test_max_by_key_over_dicts() -> None:
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.max("a") == 3


def test_max_by_key_over_classes() -> None:
    class Class:
        def __init__(self, val: int) -> None:
            self.a = val

    obj1 = Class(1)
    obj2 = Class(2)
    obj3 = Class(3)

    collection = Collection([obj1, obj2, obj3])
    assert collection.max("a") == 3


def test_sum_raises_if_empty() -> None:
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection[int]([])
        collection.sum()
    assert str(ex.value) == "Cannot find sum value: collection is empty."


def test_sum_over_scalar() -> None:
    collection = Collection([1, 2, 3])
    assert collection.sum() == 6


def test_sum_by_key_over_dicts() -> None:
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.sum("a") == 6


def test_sum_by_key_over_classes() -> None:
    class Class:
        def __init__(self, val: int) -> None:
            self.a = val

    obj1 = Class(1)
    obj2 = Class(2)
    obj3 = Class(3)

    collection = Collection([obj1, obj2, obj3])
    assert collection.sum("a") == 6


def test_map() -> None:
    def _callback(x: int) -> int:
        return x * 2

    collection = Collection([1, 2, 3])
    assert collection.map(_callback).as_list() == [2, 4, 6]


def test_each() -> None:
    call_count = 0

    def _each_fn(*args: Any) -> None:
        nonlocal call_count
        call_count += 1

    collection = Collection([1, 2, 3])
    assert collection.each(_each_fn).as_list() == [1, 2, 3]
    assert call_count == 3


def test_every() -> None:
    def _callback(x: int) -> bool:
        return True

    collection = Collection([1, 2, 3])
    assert collection.every(_callback)


def test_every_fails() -> None:
    def _callback(x: int) -> bool:
        return x % 2 == 0

    collection = Collection([1, 2, 3])
    assert not collection.every(_callback)


def test_some() -> None:
    def _callback(x: int) -> bool:
        return x % 2 == 0

    collection = Collection([1, 2, 3])
    assert collection.some(_callback)


def test_some_fails() -> None:
    def _callback(x: int) -> bool:
        return False

    collection = Collection([1, 2, 3])
    assert not collection.some(_callback)


def test_prepend() -> None:
    collection = Collection([1, 2, 3])
    assert collection.prepend(0).as_list() == [0, 1, 2, 3]


def test_append() -> None:
    collection = Collection([1, 2, 3])
    assert collection.append(0).as_list() == [1, 2, 3, 0]


def test_filter() -> None:
    def _callback(x: int) -> bool:
        return x == 2

    collection = Collection([1, 2, 3])
    assert collection.filter(_callback).as_list() == [2]


def test_reduce() -> None:
    def _callback(acc: int, val: int) -> int:
        return acc + val

    collection = Collection([1, 2, 3])
    assert collection.reduce(_callback, 0) == 6


def test_sort() -> None:
    collection = Collection([2, 1, 3])
    assert collection.sort().as_list() == [1, 2, 3]


def test_sort_reversed() -> None:
    collection = Collection([2, 1, 3])
    assert collection.sort(reverse=True).as_list() == [3, 2, 1]


def test_sort_by_key() -> None:
    collection = Collection([{"a": 2}, {"a": 3}, {"a": 1}])
    assert collection.sort(key=lambda x: x["a"]).as_list() == [
        {"a": 1},
        {"a": 2},
        {"a": 3},
    ]


def test_sort_by_string_key() -> None:
    collection = Collection([{"a": 2}, {"a": 3}, {"a": 1}])
    assert collection.sort(key="a").as_list() == [{"a": 1}, {"a": 2}, {"a": 3}]


def test_group_by() -> None:
    collection = Collection(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 3},
        ]
    )
    assert collection.group_by(lambda x: x["id"]) == {
        1: [{"id": 1}],
        2: [{"id": 2}],
        3: [{"id": 3}, {"id": 3}],
    }


def test_group_by_string_key() -> None:
    collection = Collection(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 3},
        ]
    )
    assert collection.group_by("id") == {
        1: [{"id": 1}],
        2: [{"id": 2}],
        3: [{"id": 3}, {"id": 3}],
    }


def test_key_value() -> None:
    collection = Collection(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 3},
        ]
    )
    assert collection.key_value(lambda x: x["id"]) == {
        1: {"id": 1},
        2: {"id": 2},
        3: {"id": 3},
    }


def test_key_value_by_string_key() -> None:
    collection = Collection(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 3},
        ]
    )
    assert collection.key_value("id") == {1: {"id": 1}, 2: {"id": 2}, 3: {"id": 3}}


def test_countable() -> None:
    collection = Collection([1, 2, 3])
    assert len(collection) == 3


def test_set_by_index() -> None:
    collection = Collection([1, 2, 3])
    collection[2] = 4
    assert collection.as_list() == [1, 2, 4, 3]


def test_get_by_index() -> None:
    collection = Collection([1, 2, 3])
    assert collection[1] == 2


def test_get_slice() -> None:
    collection = Collection([1, 2, 3])
    assert collection[1:] == [2, 3]


def test_delete_by_index() -> None:
    collection = Collection([1, 2, 3])
    del collection[1]
    assert collection.as_list() == [1, 3]


def test_contains() -> None:
    collection = Collection([1, 2, 3])
    assert 2 in collection


def test_stringable() -> None:
    collection = Collection([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    assert str(collection) == "<Collection: [1,2,3,4,5,6,7,8,9,10 and 1 items more]>"
