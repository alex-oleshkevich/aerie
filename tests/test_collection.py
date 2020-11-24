import pytest

from aerie.collections import Collection


def test_first():
    collection = Collection([1, 2, 3])
    assert collection.first() == 1


def test_first_on_empty():
    collection = Collection([])
    assert collection.first() is None


def test_last():
    collection = Collection([1, 2, 3])
    assert collection.last() == 3


def test_find():
    collection = Collection([1, 2, 3])
    assert collection.find(lambda x: x == 2) == 2


def test_last_on_empty():
    collection = Collection([])
    assert collection.last() is None


def test_reverse():
    collection = Collection([1, 2, 3])
    assert collection.reverse() == Collection([3, 2, 1])


def test_chunk():
    collection = Collection([1, 2, 3, 4, 5, 6])
    chunks = list(collection.chunk(2))
    assert len(chunks) == 3
    assert chunks[0] == [1, 2]
    assert chunks[1] == [3, 4]
    assert chunks[2] == [5, 6]


def test_pluck_on_dicts():
    collection = Collection([{"a": 1}, {"a": 2}])
    assert collection.pluck("a").as_list() == [1, 2]


def test_pluck_on_classes():
    class Class:
        def __init__(self, val):
            self.a = val

    collection = Collection([Class(1), Class(2)])
    assert collection.pluck("a").as_list() == [1, 2]


def test_avg_raises_if_empty():
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection([])
        collection.avg()
    assert str(ex.value) == "Cannot find avg value: collection is empty."


def test_avg_over_scalar():
    collection = Collection([1, 2, 3])
    assert collection.avg() == 2


def test_avg_by_key_over_dicts():
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.avg("a") == 2


def test_avg_by_key_over_classes():
    class Class:
        def __init__(self, val):
            self.a = val

    collection = Collection([Class(1), Class(2), Class(3)])
    assert collection.avg("a") == 2


def test_min_raises_if_empty():
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection([])
        collection.min()
    assert str(ex.value) == "Cannot find min value: collection is empty."


def test_min_over_scalar():
    collection = Collection([1, 2, 3])
    assert collection.min() == 1


def test_min_by_key_over_dicts():
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.min("a") == 1


def test_min_by_key_over_classes():
    class Class:
        def __init__(self, val):
            self.a = val

    obj1 = Class(1)
    obj2 = Class(2)
    obj3 = Class(3)

    collection = Collection([obj1, obj2, obj3])
    assert collection.min("a") == 1


def test_max_raises_if_empty():
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection([])
        collection.max()
    assert str(ex.value) == "Cannot find max value: collection is empty."


def test_max_over_scalar():
    collection = Collection([1, 2, 3])
    assert collection.max() == 3


def test_max_by_key_over_dicts():
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.max("a") == 3


def test_max_by_key_over_classes():
    class Class:
        def __init__(self, val):
            self.a = val

    obj1 = Class(1)
    obj2 = Class(2)
    obj3 = Class(3)

    collection = Collection([obj1, obj2, obj3])
    assert collection.max("a") == 3


def test_sum_raises_if_empty():
    with pytest.raises(ArithmeticError) as ex:
        collection = Collection([])
        collection.sum()
    assert str(ex.value) == "Cannot find sum value: collection is empty."


def test_sum_over_scalar():
    collection = Collection([1, 2, 3])
    assert collection.sum() == 6


def test_sum_by_key_over_dicts():
    collection = Collection([{"a": 1}, {"a": 2}, {"a": 3}])
    assert collection.sum("a") == 6


def test_sum_by_key_over_classes():
    class Class:
        def __init__(self, val):
            self.a = val

    obj1 = Class(1)
    obj2 = Class(2)
    obj3 = Class(3)

    collection = Collection([obj1, obj2, obj3])
    assert collection.sum("a") == 6


def test_map():
    collection = Collection([1, 2, 3])
    assert collection.map(lambda x: x * 2).as_list() == [2, 4, 6]


def test_each():
    call_count = 0

    def _each_fn(*args):
        nonlocal call_count
        call_count += 1

    collection = Collection([1, 2, 3])
    assert collection.each(_each_fn).as_list() == [1, 2, 3]
    assert call_count == 3


def test_every():
    collection = Collection([1, 2, 3])
    assert collection.every(lambda x: True)


def test_every_fails():
    collection = Collection([1, 2, 3])
    assert not collection.every(lambda x: x % 2 == 0)


def test_some():
    collection = Collection([1, 2, 3])
    assert collection.some(lambda x: x % 2 == 0)


def test_some_fails():
    collection = Collection([1, 2, 3])
    assert not collection.some(lambda x: False)


def test_prepend():
    collection = Collection([1, 2, 3])
    assert collection.prepend(0).as_list() == [0, 1, 2, 3]


def test_append():
    collection = Collection([1, 2, 3])
    assert collection.append(0).as_list() == [1, 2, 3, 0]


def test_filter():
    collection = Collection([1, 2, 3])
    assert collection.filter(lambda x: x == 2).as_list() == [2]


def test_reduce():
    collection = Collection([1, 2, 3])
    assert collection.reduce(lambda acc, val: acc + val, 0) == 6


def test_sort():
    collection = Collection([2, 1, 3])
    assert collection.sort().as_list() == [1, 2, 3]


def test_sort_reversed():
    collection = Collection([2, 1, 3])
    assert collection.sort(reverse=True).as_list() == [3, 2, 1]


def test_sort_by_key():
    collection = Collection([{"a": 2}, {"a": 3}, {"a": 1}])
    assert collection.sort(key=lambda x: x["a"]).as_list() == [
        {"a": 1},
        {"a": 2},
        {"a": 3},
    ]


def test_sort_by_string_key():
    collection = Collection([{"a": 2}, {"a": 3}, {"a": 1}])
    assert collection.sort(key="a").as_list() == [{"a": 1}, {"a": 2}, {"a": 3}]


def test_group_by():
    collection = Collection([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 3}, ])
    assert collection.group_by(lambda x: x["id"]) == {
        1: [{"id": 1}],
        2: [{"id": 2}],
        3: [{"id": 3}, {"id": 3}],
    }


def test_group_by_string_key():
    collection = Collection([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 3}, ])
    assert collection.group_by("id") == {
        1: [{"id": 1}],
        2: [{"id": 2}],
        3: [{"id": 3}, {"id": 3}],
    }


def test_key_value():
    collection = Collection([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 3}, ])
    assert collection.key_value(lambda x: x["id"]) == {
        1: {"id": 1},
        2: {"id": 2},
        3: {"id": 3},
    }


def test_key_value_by_string_key():
    collection = Collection([{"id": 1}, {"id": 2}, {"id": 3}, {"id": 3}, ])
    assert collection.key_value("id") == {1: {"id": 1}, 2: {"id": 2},
                                          3: {"id": 3}}


def test_countable():
    collection = Collection([1, 2, 3])
    assert len(collection) == 3


def test_set_by_index():
    collection = Collection([1, 2, 3])
    collection[2] = 4
    assert collection.as_list() == [1, 2, 4, 3]


def test_get_by_index():
    collection = Collection([1, 2, 3])
    assert collection[1] == 2


def test_get_slice():
    collection = Collection([1, 2, 3])
    assert collection[1:] == Collection([2, 3])


def test_delete_by_index():
    collection = Collection([1, 2, 3])
    del collection[1]
    assert collection.as_list() == [1, 3]


def test_contains():
    collection = Collection([1, 2, 3])
    assert 2 in collection


def test_stringable():
    collection = Collection([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    assert str(
        collection) == "<Collection: [1,2,3,4,5,6,7,8,9,10 and 1 items more]>"


def test_eq():
    col1 = Collection([1, 2])
    col2 = Collection([1, 2])
    col3 = Collection([2, 2])
    assert col1 == col2
    assert col1 != col3

    with pytest.raises(ValueError):
        col1 == True
