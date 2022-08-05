from typing import Any


class PropertyChangesRecorder():
    def __init__(self, original_value: Any):
        self.original_value = original_value

    def __iadd__(self, other: Any):
        self.original_value += other
        return self

    def __isub__(self, other: Any):
        self.original_value -= other
        return self

    def __imul__(self, other: Any):
        self.original_value *= other
        return self

    def __repr__(self) -> str:
        return self.original_value.__repr__()

    def __str__(self) -> str:
        return self.original_value.__str__()


class PropertyChangesRecorderDescriptor():
    def __init__(self, property_name: str, original: Any):
        self.property_name = property_name
        self.original = original

    def __get__(self, instance, _):
        if self.property_name in instance.__dict__:
            return instance.__dict__[self.property_name]
        else:
            return self.original

    def __set__(self, instance, new_value: Any):
        if (self.property_name in instance.__dict__
                and not isinstance(new_value, PropertyChangesRecorder)
                and isinstance(instance.__dict__[self.property_name], PropertyChangesRecorder)):
            instance.__dict__[self.property_name].original_value = new_value
        else:
            instance.__dict__[self.property_name] = new_value
