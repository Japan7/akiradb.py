from typing import Any


class Change():
    pass


class PropertyChangesRecorder():
    def __init__(self, value: Any):
        self.value = value
        self.changes: list[Change] = []

    def __iadd__(self, other: Any):
        self.value += other
        return self

    def __isub__(self, other: Any):
        self.value -= other
        return self

    def __imul__(self, other: Any):
        self.value *= other
        return self

    def __repr__(self) -> str:
        return self.value.__repr__()

    def __str__(self) -> str:
        return self.value.__str__()

    def clear_changes(self):
        self.changes = []


class PropertyChangesRecorderDescriptor():
    def __init__(self, name: str):
        self.name = name

    def __get__(self, instance, owner):
        if self.name in instance._properties_recorders:
            return instance._properties_recorders[self.name]
        else:
            return getattr(owner, self.name)

    def __set__(self, instance, new_value: Any):
        if self.name in instance._properties_recorders:
            if not isinstance(new_value, PropertyChangesRecorder):
                instance._properties_recorders[self.name].value = new_value
        else:
            instance._properties_recorders[self.name] = PropertyChangesRecorder(new_value)
