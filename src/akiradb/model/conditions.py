from typing import Any

from akiradb.model.utils import _get_cypher_value


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


class PropertyCondition(Condition):
    def __init__(self, property_name: str):
        self.property_name = property_name

    def __str__(self):
        return f'n.{self.property_name}'


class ValueCondition(Condition):
    def __init__(self, value: Any):
        self.value = value

    def __str__(self):
        return _get_cypher_value(self.value)


class Not(Condition):
    def __init__(self, condition: Condition):
        self.condition = condition

    def __str__(self):
        return f'not ({self.condition})'


class BinaryCondition(Condition):
    def __init__(self, condition1: Condition, condition2: Condition | Any):
        self.condition1 = condition1
        if isinstance(condition2, Condition):
            self.condition2 = condition2
        else:
            self.condition2 = ValueCondition(condition2)


class Equals(BinaryCondition):
    def __str__(self) -> str:
        return f'{self.condition1!s} = {self.condition2!s}'


class NotEquals(BinaryCondition):
    def __str__(self) -> str:
        return f'{self.condition1!s} <> {self.condition2!s}'


class LowerThan(BinaryCondition):
    def __str__(self) -> str:
        return f'{self.condition1!s} < {self.condition2!s}'


class LowerEquals(BinaryCondition):
    def __str__(self) -> str:
        return f'{self.condition1!s} <= {self.condition2!s}'


class GreaterThan(BinaryCondition):
    def __str__(self) -> str:
        return f'{self.condition1!s} > {self.condition2!s}'


class GreaterEquals(BinaryCondition):
    def __str__(self) -> str:
        return f'{self.condition1!s} >= {self.condition2!s}'


class And(BinaryCondition):
    def __str__(self) -> str:
        return f'({self.condition1!s}) and ({self.condition2!s})'


class Or(BinaryCondition):
    def __str__(self) -> str:
        return f'({self.condition1!s}) or ({self.condition2!s})'


class Xor(BinaryCondition):
    def __str__(self) -> str:
        return f'({self.condition1!s}) xor ({self.condition2!s})'
