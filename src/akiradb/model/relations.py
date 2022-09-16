import asyncio
from contextlib import suppress
from dataclasses import asdict, dataclass, field, fields
from functools import partial
from typing import ClassVar, ForwardRef, Generic, Type, TypeVar, Union, cast

from psycopg.rows import dict_row
from akiradb.database_connection import AkiraAsyncClientCursor

from akiradb.model.base_model import BaseModel, MetaModel
from akiradb.model.utils import __dataclass_transform__
from akiradb.types.query import Label, Params, Query

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

    def _link(self, source: BaseModel, target: BaseModel,
              properties: Union['Properties', None] = None):
        async def coroutine(cursor: AkiraAsyncClientCursor):
            if properties:
                await cursor.execute_cypher(
                    'match (s), (t) where id(s)=%(s_rid)s and id(t)=%(t_rid)s '
                    'create (s)-[:%(rel_type_name)s %(properties)s]->(t)',
                    {
                        'rel_type_name': Label(self._name),
                        's_rid': source._rid,
                        't_rid': target._rid,
                        'properties': asdict(properties)
                    }
                )
            else:
                await cursor.execute_cypher(
                    'match (s), (t) where id(s)=%(s_rid)s and id(t)=%(t_rid)s '
                    'create (s)-[:%(rel_type_name)s]->(t)',
                    {
                        'rel_type_name': Label(self._name),
                        's_rid': source._rid,
                        't_rid': target._rid
                    }
                )
        return coroutine

    def _unlink(self, source: BaseModel, target: BaseModel):
        async def coroutine(cursor: AkiraAsyncClientCursor):
            await cursor.execute_cypher(
                'match (s)-[r:%(rel_type_name)s]->(t) where id(s)=%(s_rid)s and id(t)=%(t_rid)s '
                'delete r',
                {'rel_type_name': Label(self._name), 's_rid': source._rid, 't_rid': target._rid}
            )
        return coroutine

    def _get_target_match_request(self, target_cls, properties_cls=None) -> tuple[Query, Params]:
        assert self._source
        query = ('match (n1:%(n1_type_name)s) -[r:%(rel_type_name)s]-> (n2:%(n2_type_name)s) '
                 + 'where id(n1) = %(n1_rid)s return id(n2),labels(n2),')
        params = {
            'n1_type_name': Label(self._source.__class__.__qualname__),
            'rel_type_name': Label(self._name),
            'n2_type_name': Label(target_cls.__qualname__),
            'n1_rid': self._source._rid
        }

        i = 0
        properties_query = []
        for property_name in target_cls._properties_names:
            property_id = cast(Query, f'property{i}')
            properties_query.append('%(' + property_id + ')s')
            params[property_id] = Label('n2.' + property_name)
            i += 1

        if properties_cls:
            for property in fields(properties_cls):
                property_id = cast(Query, f'property{i}')
                properties_query.append('%(' + property_id + ')s')
                params[property_id] = Label('r.' + property.name)
                i += 1

        return query + ','.join(properties_query), params


TRelation = TypeVar('TRelation', bound=Relation)


class Many(Relation[TModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._elements: list[TModel] = []

    def add(self, element: TModel, invert_operation=False):
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(self._link(self._source, element)))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._link(element, self._source)
                ))
                getattr(element, self._attribute_name).add(self._source, invert_operation=True)
        self._elements.append(element)

    def remove(self, element: TModel, invert_operation=False):
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(self._unlink(self._source, element)))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._unlink(element, self._source)
                ))
                getattr(element, self._attribute_name).remove(self._source, invert_operation=True)
        with suppress(ValueError):
            self._elements.remove(element)

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
                await cursor.execute_cypher(*req)
                async for row in cursor:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.') and value is not None
                                  and value != '  cypher.null'}
                    inst_cls = MetaModel._models[row['labels(n2)']]
                    for property_name in inst_cls._properties_names:
                        if property_name not in parameters.keys():
                            parameters[property_name] = None
                    instance = inst_cls(**parameters)
                    instance._rid = row['id(n2)']
                    self._elements.append(instance)

        return self._elements


class One(Relation[TModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._element: TModel | None = None

    def set(self, element: TModel, invert_operation=False):
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(self._link(self._source, element)))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._link(element, self._source)
                ))
                getattr(element, self._attribute_name).set(self._source, invert_operation=True)
        self._element = element

    def unset(self, element: TModel, invert_operation=False):
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(self._unlink(self._source, element)))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._unlink(element, self._source)
                ))
                getattr(element, self._attribute_name).unset(self._source, invert_operation=True)
        self._element = None

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
                await cursor.execute_cypher(*req)
                row = await cursor.fetchone()
                if row:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.') and value is not None
                                  and value != '  cypher.null'}
                    inst_cls = MetaModel._models[row['labels(n2)']]
                    for property_name in inst_cls._properties_names:
                        if property_name not in parameters.keys():
                            parameters[property_name] = None
                    instance = inst_cls(**parameters)
                    instance._rid = row['id(n2)']
                    self._element = instance

        return self._element


@__dataclass_transform__()
class MetaProperties(type):
    _properties: dict[str, 'MetaProperties'] = {}

    def __new__(cls, name, bases, dct):
        instance = cast(Type, super().__new__(cls, name, bases, dct))
        dataclass_instance = dataclass(instance)

        dataclass_instance._properties_names = []
        for c_field in fields(dataclass_instance):
            dataclass_instance._properties_names.append(c_field.name)

        MetaProperties._properties[name] = dataclass_instance
        return dataclass_instance


class Properties(metaclass=MetaProperties):
    _properties_names: ClassVar[list[str]]


TProperties = TypeVar('TProperties', bound=Properties)


class ManyWithProperties(Many[TModel], Generic[TModel, TProperties]):
    def __init__(self, *args, **kwargs):
        Many.__init__(self, *args, **kwargs)
        self._properties: list[TProperties] = []

    def add(self, element: TModel,  # type: ignore[override]
            properties: TProperties,
            invert_operation=False):
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(
                self._link(self._source, element, properties)
            ))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._link(element, self._source, properties)
                ))
                getattr(element, self._attribute_name).add(self._source, properties,
                                                           invert_operation=True)
        self._elements.append(element)
        self._properties.append(properties)

    def remove(self, element: TModel,  # type: ignore[override]
               invert_operation=False):
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(self._unlink(self._source, element)))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._unlink(element, self._source)
                ))
                getattr(element, self._attribute_name).remove(self._source, invert_operation=True)

        with suppress(ValueError):
            index = self._elements.index(element)
            del self._elements[index]
            del self._properties[index]

    async def get(self) -> list[tuple[TModel, TProperties]]:  # type: ignore[override]
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
                await cursor.execute_cypher(*req)
                async for row in cursor:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.') and value is not None
                                  and value != '  cypher.null'}
                    inst_cls = MetaModel._models[row['labels(n2)']]
                    for property_name in inst_cls._properties_names:
                        if property_name not in parameters.keys():
                            parameters[property_name] = None
                    instance = inst_cls(**parameters)
                    instance._rid = row['id(n2)']
                    properties_parameters = {name[2:]: value for (name, value) in row.items()
                                             if name.startswith('r.') and value is not None
                                             and value != '  cypher.null'}
                    for property_name in properties_cls._properties_names:  # type: ignore
                        if property_name not in properties_parameters.keys():
                            properties_parameters[property_name] = None
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
            asyncio.create_task(self._source._add_operation(
                self._link(self._source, element, properties)
            ))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._link(element, self._source, properties)
                ))
                getattr(element, self._attribute_name).set(self._source, properties,
                                                           invert_operation=True)
        self._element = element
        self._properties = properties

    def unset(self, element: TModel, invert_operation=False):  # type: ignore[override]
        if self._source and not invert_operation:
            asyncio.create_task(self._source._add_operation(self._unlink(self._source, element)))
            if self._bidirectionnal:
                asyncio.create_task(self._source._add_operation(
                    self._unlink(element, self._source)
                ))
                getattr(element, self._attribute_name).unset(self._source, invert_operation=True)

        self._element = None
        self._properties = None

    async def get(self) -> tuple[TModel | None, TProperties | None]:  # type: ignore[override]
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
                await cursor.execute_cypher(*req)
                row = await cursor.fetchone()
                if row:
                    parameters = {name[3:]: value for (name, value) in row.items()
                                  if name.startswith('n2.') and value is not None
                                  and value != '  cypher.null'}
                    inst_cls = MetaModel._models[row['labels(n2)']]
                    for property_name in inst_cls._properties_names:
                        if property_name not in parameters.keys():
                            parameters[property_name] = None
                    instance = inst_cls(**parameters)
                    instance._rid = row['id(n2)']
                    properties_parameters = {name[2:]: value for (name, value) in row.items()
                                             if name.startswith('r.') and value is not None
                                             and value != '  cypher.null'}
                    for property_name in properties_cls._properties_names:  # type: ignore
                        if property_name not in properties_parameters.keys():
                            properties_parameters[property_name] = None
                    properties_instance = properties_cls(**properties_parameters)  # type: ignore
                    self._element = instance
                    self._properties = properties_instance

        return self._element, self._properties


def relation(name: str, cls: Type[TRelation], invert=None, bidirectionnal=False) -> TRelation:
    return field(default_factory=partial(cls, name=name, invert=invert,
                                         bidirectionnal=bidirectionnal),
                 init=False, metadata={'type': TRelation})  # type: ignore[misc]
