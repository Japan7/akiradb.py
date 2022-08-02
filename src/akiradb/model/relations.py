from dataclasses import asdict, dataclass, field, fields
from functools import partial
from typing import ForwardRef, Generic, Type, TypeVar, Union, cast

from psycopg.rows import dict_row

from akiradb.model.base_model import BaseModel, MetaModel
from akiradb.model.utils import __dataclass_transform__

TModel = TypeVar('TModel', bound=BaseModel)


class Relation(Generic[TModel]):
    def __init__(self, name: str, invert: Union['Relation', None],
                 bidirectionnal: bool):
        self._name = name
        self._invert = invert
        self._bidirectionnal = bidirectionnal
        self._source: BaseModel | None = None
        self._attribute_name: str
        self._loaded = False

    async def _link(self, source: BaseModel, target: BaseModel,
                    properties: Union['Properties', None] = None):
        req = (f"{{cypher}} match (s), (t) "
               f"where id(s)='{source._rid}' and id(t)='{target._rid}' "
               f"create (s)-[:{self._name} {{"
               f"{properties._to_cypher() if properties else ''}"
               f"}}]->(t);")

        async with source._database_connection.execute(req):
            pass

    def _get_target_match_request(self, target_cls, properties_cls=None):
        assert self._source
        target_cls_properties = ['n2.' + property_name
                                 for property_name in target_cls._properties_names] + ['id(n2)']
        if properties_cls:
            target_cls_properties += ['r.' + property.name for property in fields(properties_cls)]

        return (f'{{cypher}} match (n1: {self._source.__class__.__qualname__}) '
                f'-[r:{self._name}]-> (n2: {target_cls.__qualname__}) '
                f'where id(n1) = "{self._source._rid}" '
                f'return {",".join(target_cls_properties)};')


TRelation = TypeVar('TRelation', bound=Relation)


class Many(Relation[TModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._elements: list[TModel] = []

    def add(self, element: TModel, invert_operation=False):
        if self._source and not invert_operation:
            self._source._operations_queue.append(self._link(self._source, element))
            if self._bidirectionnal:
                self._source._operations_queue.append(
                    self._link(element, self._source)
                )
                getattr(element, self._attribute_name).add(self._source, invert_operation=True)
        self._elements.append(element)

    async def get(self) -> list[TModel]:
        if not self._loaded:
            ref = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
            if isinstance(ref, ForwardRef):
                ref = MetaModel._models[ref.__forward_arg__]
            target_cls = ref

            assert self._source
            self._elements = []
            req = self._get_target_match_request(target_cls)
            async with self._source._database_connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(req)
                async for row in cursor:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.')}
                    instance = target_cls(**parameters)  # type: ignore
                    instance._rid = row['id(n2)']
                    self._elements.append(instance)

        return self._elements


class One(Relation[TModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._element: TModel | None = None

    def set(self, element: TModel, invert_operation=False):
        if self._source and not invert_operation:
            self._source._operations_queue.append(self._link(self._source, element))
            if self._bidirectionnal:
                self._source._operations_queue.append(
                    self._link(element, self._source)
                )
                getattr(element, self._attribute_name).set(self._source, invert_operation=True)
        self._element = element

    async def get(self) -> TModel | None:
        if not self._loaded:
            ref = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
            if isinstance(ref, ForwardRef):
                ref = MetaModel._models[ref.__forward_arg__]
            target_cls = ref

            assert self._source
            self._element = None
            req = self._get_target_match_request(target_cls)
            async with self._source._database_connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(req)
                row = await cursor.fetchone()
                if row:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.')}
                    instance = target_cls(**parameters)  # type: ignore
                    instance._rid = row['id(n2)']
                    self._element = instance

        return self._element


@__dataclass_transform__()
class MetaProperties(type):
    _properties: dict[str, 'MetaProperties'] = {}

    def __new__(cls, name, bases, dct):
        instance = cast(Type, super().__new__(cls, name, bases, dct))
        dataclass_instance = dataclass(instance)
        MetaProperties._properties[name] = dataclass_instance
        return dataclass_instance


class Properties(metaclass=MetaProperties):
    def _to_cypher(self):
        return ",".join(f"{index}: {value!r}"
                        for index, value in asdict(self).items())


TProperties = TypeVar('TProperties', bound=Properties)


class ManyWithProperties(Many[TModel], Generic[TModel, TProperties]):
    def __init__(self, *args, **kwargs):
        Many.__init__(self, *args, **kwargs)
        self._properties: list[TProperties] = []

    def add(self, element: TModel,  # type: ignore[override]
            properties: TProperties,
            invert_operation=False):
        if self._source and not invert_operation:
            self._source._operations_queue.append(self._link(self._source, element, properties))
            if self._bidirectionnal:
                self._source._operations_queue.append(
                    self._link(element, self._source, properties)
                )
                getattr(element, self._attribute_name).add(self._source, properties,
                                                           invert_operation=True)
        self._elements.append(element)
        self._properties.append(properties)

    async def get(self) -> list[tuple[TModel, TProperties]]:
        if not self._loaded:
            ref = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
            if isinstance(ref, ForwardRef):
                ref = MetaModel._models[ref.__forward_arg__]
            target_cls = ref

            ref = self.__orig_class__.__args__[1]  # type: ignore[attr-defined]
            if isinstance(ref, ForwardRef):
                ref = MetaProperties._properties[ref.__forward_arg__]
            properties_cls = ref

            assert self._source
            self._elements = []
            self._properties = []
            req = self._get_target_match_request(target_cls, properties_cls=properties_cls)
            async with self._source._database_connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(req)
                async for row in cursor:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.')}
                    instance = target_cls(**parameters)  # type: ignore
                    instance._rid = row['id(n2)']
                    properties_parameters = {name[2:]: value for (name, value) in row.items()
                                             if name.startswith('r.')}
                    properties_instance = properties_cls(**properties_parameters)  # type: ignore
                    self._elements.append(instance)
                    self._properties.append(properties_instance)

        return list(zip(self._elements, self._properties))


class OneWithProperties(One[TModel], Generic[TModel, TProperties]):
    def __init__(self, *args, **kwargs):
        One.__init__(self, *args, **kwargs)
        self._properties: TProperties | None = None

    def set(self, element: TModel, properties: TProperties,  # type: ignore[override]
            invert_operation=False):
        if self._source and not invert_operation:
            self._source._operations_queue.append(self._link(self._source, element, properties))
            if self._bidirectionnal:
                self._source._operations_queue.append(
                    self._link(element, self._source, properties)
                )
                getattr(element, self._attribute_name).set(self._source, properties,
                                                           invert_operation=True)
        self._element = element
        self._properties = properties

    async def get(self) -> tuple[TModel | None, TProperties | None]:
        if not self._loaded:
            ref = self.__orig_class__.__args__[0]  # type: ignore[attr-defined]
            if isinstance(ref, ForwardRef):
                ref = MetaModel._models[ref.__forward_arg__]
            target_cls = ref

            ref = self.__orig_class__.__args__[1]  # type: ignore[attr-defined]
            if isinstance(ref, ForwardRef):
                ref = MetaProperties._properties[ref.__forward_arg__]
            properties_cls = ref

            assert self._source
            self._element = None
            self._properties = None
            req = self._get_target_match_request(target_cls, properties_cls=properties_cls)
            async with self._source._database_connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(req)
                row = await cursor.fetchone()
                if row:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.')}
                    instance = target_cls(**parameters)  # type: ignore
                    instance._rid = row['id(n2)']
                    properties_parameters = {name[2:]: value for (name, value) in row.items()
                                             if name.startswith('r.')}
                    properties_instance = properties_cls(**properties_parameters)  # type: ignore
                    self._element = instance
                    self._properties = properties_instance

        return self._element, self._properties


def relation(name: str, cls: Type[TRelation], invert=None, bidirectionnal=False) -> TRelation:
    return field(default_factory=partial(cls, name=name, invert=invert,
                                         bidirectionnal=bidirectionnal),
                 init=False, metadata={'type': TRelation})  # type: ignore[misc]
