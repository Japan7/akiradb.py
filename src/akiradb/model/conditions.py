from typing import Any, cast

from akiradb.types.query import Label, Query, Params


class Condition:
    def __invert__(self) -> 'Not':
        return Not(self)

    def __eq__(self, o):
        return Equals(self, o)

    def __ne__(self, o):
        return NotEquals(self, o)

    def __lt__(self, o: 'Condition') -> 'LowerThan':
        return LowerThan(self, o)

    def __le__(self, o: 'Condition') -> 'LowerEquals':
        return LowerEquals(self, o)

    def __gt__(self, o: 'Condition') -> 'GreaterThan':
        return GreaterThan(self, o)

    def __ge__(self, o: 'Condition') -> 'GreaterEquals':
        return GreaterEquals(self, o)

    def __and__(self, o: 'Condition') -> 'And':
        return And(self, o)

    def __or__(self, o: 'Condition') -> 'Or':
        return Or(self, o)

    def __xor__(self, o: 'Condition') -> 'Xor':
        return Xor(self, o)

    def _query(self, _: int = 0) -> tuple[Query, Params]:
        return ('Unknown Condition', {})


class PropertyCondition(Condition):
    def __init__(self, property_name: str):
        self.property_name = property_name

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        value_name = cast(Query, f'value{value_id}')
        return ('%(' + value_name + ')s', {value_name: Label('n.' + self.property_name)})


class ValueCondition(Condition):
    def __init__(self, value: Any):
        self.value = value

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        value_name = cast(Query, f'value{value_id}')
        return ('%(' + value_name + ')s', {value_name: self.value})


class Not(Condition):
    def __init__(self, condition: Condition):
        self.condition = condition

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q, p = self.condition._query(value_id)
        return ('not (' + q + ')', p)


class BinaryCondition(Condition):
    def __init__(self, condition1: Condition, condition2: Condition | Any):
        self.condition1 = condition1
        if isinstance(condition2, Condition):
            self.condition2 = condition2
        else:
            self.condition2 = ValueCondition(condition2)


class Equals(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return (q1 + ' = ' + q2, dict(**p1, **p2))


class NotEquals(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return (q1 + ' <> ' + q2, dict(**p1, **p2))


class LowerThan(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return (q1 + ' < ' + q2, dict(**p1, **p2))


class LowerEquals(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return (q1 + ' <= ' + q2, dict(**p1, **p2))


class GreaterThan(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return (q1 + ' > ' + q2, dict(**p1, **p2))


class GreaterEquals(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return (q1 + ' >= ' + q2, dict(**p1, **p2))


class And(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return ('(' + q1 + ') and (' + q2 + ')', dict(**p1, **p2))


class Or(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return ('(' + q1 + ') or (' + q2 + ')', dict(**p1, **p2))


class Xor(BinaryCondition):
    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        q1, p1 = self.condition1._query(value_id)
        q2, p2 = self.condition2._query(value_id + len(p1))
        return ('(' + q1 + ') xor (' + q2 + ')', dict(**p1, **p2))
