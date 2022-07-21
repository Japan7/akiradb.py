from dataclasses import asdict, dataclass, field
from functools import partial
from typing import Generic, Type, TypeVar, Union, cast

from akiradb.model.base_model import BaseModel
from akiradb.model.utils import __dataclass_transform__

TModel = TypeVar('TModel', bound=BaseModel)


class Relation(Generic[TModel]):
    def __init__(self, name: str, invert: Union['Relation', None],
                 bidirectionnal: bool):
        self._name = name
        self._invert = invert
        self._bidirectionnal = bidirectionnal
        self._source: BaseModel | None = None

    async def _link(self, source: BaseModel, target: BaseModel,
                    properties: Union['Properties', None] = None):
        req = (f"{{cypher}} match (s), (t) "
               f"where id(s)='{source._rid}' and id(t)='{target._rid}' "
               f"create (s)-[:{self._name} {{"
               f"{properties._to_cypher() if properties else ''}"
               f"}}]->(t);")

        async with source._database_connection.execute(req):
            pass


TRelation = TypeVar('TRelation', bound=Relation)


class Many(Relation[TModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._elements: list[TModel] = []

    def add(self, element: TModel):
        self._elements.append(element)


class One(Relation[TModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._element: TModel | None = None

    def set(self, element: TModel):
        self._element = element


@__dataclass_transform__()
class MetaProperties(type):

    def __new__(cls, name, bases, dct):
        instance = cast(Type, super().__new__(cls, name, bases, dct))
        return dataclass(instance)


class Properties(metaclass=MetaProperties):
    def _to_cypher(self):
        return ",".join(f"{index}: {value!r}"
                        for index, value in asdict(self).items())


TProperties = TypeVar('TProperties', bound=Properties)


class ManyWithProperties(Many[TModel], Generic[TModel, TProperties]):
    def __init__(self, *args, **kwargs):
        Many.__init__(self, *args, **kwargs)
        self._properties: list[TProperties] = []

    def add(self, element: TModel, properties: TProperties, create_in_db=True):
        if self._source:
            self._source._operations_queue.append(self._link(self._source, element, properties))
            if self._bidirectionnal:
                self._source._operations_queue.append(
                    self._link(element, self._source, properties)
                )
                # TODO: manage bidirectionnal relations
                # element.add(self._source, properties, create_in_db=False)
        super().add(element)
        self._properties.append(properties)


class OneWithProperties(One[TModel], Generic[TModel, TProperties]):
    def __init__(self, *args, **kwargs):
        One.__init__(self, *args, **kwargs)
        self._properties: TProperties | None = None

    def set(self, element: TModel, properties: TProperties):
        super().set(element)
        self._properties = properties


def relation(name: str, cls: Type[TRelation], invert=None, bidirectionnal=False) -> TRelation:
    return field(default_factory=partial(cls, name=name, invert=invert,
                                         bidirectionnal=bidirectionnal),
                 init=False, metadata={'type': TRelation})
