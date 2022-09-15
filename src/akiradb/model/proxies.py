from typing import Any, cast

from akiradb.types.query import Params, Query


class Change():
    def _query(self, _: int = 0):
        return ('Unknown Change', {})


class NewValue(Change):
    def __init__(self, property_name: str, new_value: Any):
        self.property_name: Query = cast(Query, property_name)
        self.new_value = new_value

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        value_name = cast(Query, f'value{value_id}')
        return 'n.' + self.property_name + ' = %(' + value_name + ')', {value_name: self.new_value}


class Addition(Change):
    def __init__(self, property_name: str, add_value: Any):
        self.property_name: Query = cast(Query, property_name)
        self.add_value = add_value

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        value_name = cast(Query, f'value{value_id}')
        return (
            'n.' + self.property_name + ' = n.' + self.property_name
            + ' + %(' + value_name + ')',
            {value_name: self.add_value}
        )


class Substraction(Change):
    def __init__(self, property_name: str, sub_value: Any):
        self.property_name: Query = cast(Query, property_name)
        self.sub_value = sub_value

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        value_name = cast(Query, f'value{value_id}')
        return (
            'n.' + self.property_name + ' = n.' + self.property_name
            + ' - %(' + value_name + ')',
            {value_name: self.sub_value}
        )


class Multiplication(Change):
    def __init__(self, property_name: str, mult_value: Any):
        self.property_name: Query = cast(Query, property_name)
        self.mult_value = mult_value

    def _query(self, value_id: int = 0) -> tuple[Query, Params]:
        value_name = cast(Query, f'value{value_id}')
        return (
            'n.' + self.property_name + ' = n.' + self.property_name
            + ' * %(' + value_name + ')',
            {value_name: self.mult_value}
        )


class PropertyChangesRecorder():
    def __init__(self, name: str, value: Any):
        self.name = name
        self.value = value
        self.changes: list[Change] = []

    def __eq__(self, other: Any):
        return self.value == other

    def __ne__(self, other: Any):
        return self.value != other

    def __gt__(self, other: Any):
        return self.value > other

    def __lt__(self, other: Any):
        return self.value < other

    def __le__(self, other: Any):
        return self.value <= other

    def __ge__(self, other: Any):
        return self.value >= other

    def __iadd__(self, other: Any):
        self.value += other
        if isinstance(self.value, str):
            self.add_change(NewValue(self.name, self.value))
        else:
            self.add_change(Addition(self.name, other))
        return self

    def __isub__(self, other: Any):
        self.value -= other
        self.add_change(Substraction(self.name, other))
        return self

    def __imul__(self, other: Any):
        self.value *= other
        self.add_change(Multiplication(self.name, other))
        return self

    def __repr__(self) -> str:
        return repr(self.value)

    def __str__(self) -> str:
        return str(self.value)

    def __hash__(self) -> int:
        return hash(self.value)

    def add_change(self, change: Change):
        self.changes.append(change)

    def clear_changes(self):
        self.changes = []


class PropertyChangesRecorderDescriptor():
    def __init__(self, name: str):
        self.name = name

    def __get__(self, instance, owner):
        if self.name in instance.property_recorders:
            return instance.property_recorders[self.name]
        else:
            return getattr(owner, self.name)

    def __set__(self, instance, new_value: Any):
        if self.name in instance.property_recorders:
            if not isinstance(new_value, PropertyChangesRecorder):
                instance.property_recorders[self.name].value = new_value
                instance.property_recorders[self.name].add_change(NewValue(self.name, new_value))
        else:
            instance.property_recorders[self.name] = PropertyChangesRecorder(self.name, new_value)
