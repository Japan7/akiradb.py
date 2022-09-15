import asyncio
from dataclasses import MISSING, Field, dataclass, fields
from typing import TYPE_CHECKING, Any, ClassVar, ParamSpec, Type, TypeVar, cast

from psycopg.rows import dict_row

from akiradb.database_connection import AkiraAsyncClientCursor, DatabaseConnection
from akiradb.exceptions import (AkiraNodeNotFoundException,
                                AkiraNodeTypeAlreadyDefinedException, AkiraUnknownNodeException)
from akiradb.model.conditions import Condition, PropertyCondition
from akiradb.model.proxies import PropertyChangesRecorder, PropertyChangesRecorderDescriptor
from akiradb.model.utils import (__dataclass_transform__, _get_cypher_property_type,
                                 _parse_cypher_properties)
from akiradb.types.query import Params, Query

if TYPE_CHECKING:
    from akiradb.model.relations import Relation


@__dataclass_transform__()
class MetaModel(type):
    _models: dict[str, 'MetaModel'] = {}

    def __new__(cls, name, bases, dct,
                database_connection: DatabaseConnection | None = None):
        if '__annotations__' not in dct:
            dct['__annotations__'] = {}

        instance = cast(Type, super().__new__(cls, name, bases, dct))

        rec_dct = dct.copy()
        for base in bases:
            for field in fields(base):
                rec_dct[field.name] = field
                if 'type' in field.metadata:
                    setattr(instance, field.name, field)

        properties_names = []
        relations_names = []
        for key, value in rec_dct.items():
            if isinstance(value, Field) and 'type' in value.metadata:
                dct['__annotations__'][key] = value.metadata['type']
                relations_names.append(key)

        if database_connection is not None:
            instance._database_connection = database_connection

        dataclass_instance = cast(Type['BaseModel'], dataclass(instance))
        for field in fields(dataclass_instance):
            if field.name not in relations_names:
                properties_names.append(field.name)
                setattr(dataclass_instance, field.name,
                        PropertyChangesRecorderDescriptor(field.name))
        dataclass_instance._properties_names = properties_names
        dataclass_instance._relations_names = relations_names

        if name in MetaModel._models:
            raise AkiraNodeTypeAlreadyDefinedException(name)
        MetaModel._models[name] = dataclass_instance

        if hasattr(dataclass_instance, '_database_connection'):
            asyncio.run(dataclass_instance._create_type_and_properties())

        return dataclass_instance

    def __getattribute__(self, name: str) -> Any:
        try:
            properties_names = super().__getattribute__('_properties_names')
            if name in properties_names:
                return PropertyCondition(name)
        except AttributeError:
            pass
        return super().__getattribute__(name)


TModel = TypeVar('TModel', bound='BaseModel')
P = ParamSpec('P')


class BaseModel(metaclass=MetaModel):
    _properties_names: ClassVar[list[str]]
    _relations_names: ClassVar[list[str]]
    _database_connection: ClassVar[DatabaseConnection]

    def __new__(cls, **_):
        instance = super().__new__(cls)
        instance.property_recorders = {}
        return instance

    def __post_init__(self):
        self._rid: str
        self._operations_queue = []
        self._operations_queue_lock = asyncio.Lock()
        self._properties, self._relations = self._split_properties_and_relations()
        self.property_recorders: dict[str, PropertyChangesRecorder]
        for property_name, _ in self._properties.items():
            self.property_recorders[property_name].clear_changes()
        for relation_name, relation_value in self._relations.items():
            relation_value._source = self
            relation_value._attribute_name = relation_name

    @classmethod
    async def _create_type_and_properties(cls):
        await cls._database_connection.connect()
        supertypes = [type.__qualname__ for type in cls.__bases__ if type is not BaseModel]
        async with cls._database_connection.cursor() as cursor:
            if supertypes:
                await cursor.execute(
                    'create vertex type %(type_name)s if not exists extends %(supertypes)s;',
                    {'type_name': cls.__qualname__, 'supertypes': ','.join(supertypes)}
                )
            else:
                await cursor.execute(
                    'create vertex type %(type_name)s if not exists;',
                    {'type_name': cls.__qualname__}
                )
            for field in fields(cls):
                if field.name in cls._properties_names and field.name in cls.__annotations__:
                    await cursor.execute(
                        'create property %(property_name)s if not exists %(property_type)s;',
                        {
                            'property_name': f'{cls.__qualname__}.{field.name}',
                            'property_type': _get_cypher_property_type(field.type)
                        }
                    )
                    if field.default is not MISSING:
                        await cursor.execute(
                            'alter property %(property_name)s default %(default_value)s;',
                            {
                                'property_name': f'{cls.__qualname__}.{field.name}',
                                'default_value': field.default
                            }
                        )
        await cls._database_connection.close()

    @classmethod
    async def bulk_create(cls: Type[TModel], nodes: list[TModel]):
        async with cls._database_connection.cursor() as cursor:
            for node in nodes:
                await cursor.execute_cypher(*node._get_create_request())
                async for row in cursor:
                    assert row is not None
                    node._rid, = row

    @classmethod
    async def bulk_upsert(cls: Type[TModel], nodes: list[tuple[TModel, dict[str, Any]]]):
        async with cls._database_connection.cursor() as cursor:
            for node, identifying_properties in nodes:
                await cursor.execute_cypher(
                    *node._get_create_request(identifying_properties=identifying_properties)
                )
                async for row in cursor:
                    assert row is not None
                    node._rid, = row

    @classmethod
    async def bulk_delete(cls: Type[TModel], nodes: list[TModel]):
        async with cls._database_connection.cursor() as cursor:
            for node in nodes:
                await cursor.execute_cypher(*node._get_delete_request())

    def _get_create_request(self, identifying_properties: dict[str, Any] | None = None
                            ) -> tuple[Query, Params]:
        if identifying_properties:
            return (
                'merge (n:%(type_name)s %(cypher_identifying)s) set n = %(cypher_properties)s '
                + 'return id(n)',
                {
                    'type_name': self.__class__.__qualname__,
                    'cypher_identifying': identifying_properties,
                    'cypher_properties': self._properties
                }
            )
        else:
            return (
                'create (n:%(type_name)s %(cypher_identifying)s) return id(n)',
                {
                    'type_name': self.__class__.__qualname__,
                    'cypher_properties': self._properties
                }
            )

    def _get_delete_request(self) -> tuple[Query, Params]:
        return (
            'match (n:%(type_name)s) where id(n) = %(node_id)s detach delete n',
            {'type_name': self.__class__.__qualname__, 'node_id': self._rid}
        )

    @classmethod
    def _get_fetch_request(cls, rid: str | None = None,
                           condition: Condition | bool | None = None) -> tuple[Query, Params]:
        req = 'match (n:%(type_name)s) '
        params = {'type_name': cls.__qualname__}
        if condition is not None:
            if isinstance(condition, Condition):
                rc, pc = condition._query()
                req += 'where ' + rc + ' '
                params.update(pc)
        if rid is not None:
            req += 'where %(node_id)s '
            params['node_id'] = rid
        req += 'return n'
        return (req, params)

    async def create(self):
        async with self._database_connection.cursor() as cursor:
            await cursor.execute_cypher(*self._get_create_request())
            row = await cursor.fetchone()
            assert row is not None
            self._rid, = row

        return self

    async def upsert(self, **identifying_properties):
        async with self._database_connection.cursor() as cursor:
            await cursor.execute_cypher(
                *self._get_create_request(identifying_properties=identifying_properties)
            )
            row = await cursor.fetchone()
            assert row is not None
            self._rid, = row

        return self

    @classmethod
    async def fetch_one(cls: Type[TModel],
                        condition: Condition | bool | None, rid: str | None = None) -> TModel:
        if rid:
            req = cls._get_fetch_request(rid=rid)
        elif condition:
            req = cls._get_fetch_request(condition=condition)
        else:
            req = cls._get_fetch_request()

        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute_cypher(*req)
            row = await cursor.fetchone()
            if not row:
                raise AkiraNodeNotFoundException()

            instance = _parse_cypher_properties({name: value for (name, value) in row.items()
                                                 if not name.startswith('@')
                                                 and value is not None},
                                                MetaModel._models[row['@type']])
            instance._rid = row['@rid']

        return instance

    @classmethod
    async def fetch_many(cls: Type[TModel], condition: Condition | bool) -> list[TModel]:
        instances = []
        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute_cypher(*cls._get_fetch_request(condition=condition))
            async for row in cursor:
                instance = _parse_cypher_properties({name: value for (name, value) in row.items()
                                                     if not name.startswith('@')
                                                     and value is not None},
                                                    MetaModel._models[row['@type']])
                instance._rid = row['@rid']
                instances.append(instance)

        return instances

    @classmethod
    async def fetch_all(cls: Type[TModel]) -> list[TModel]:
        instances = []
        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute_cypher(*cls._get_fetch_request())
            async for row in cursor:
                instance = _parse_cypher_properties({name: value for (name, value) in row.items()
                                                     if not name.startswith('@')
                                                     and value is not None},
                                                    MetaModel._models[row['@type']])
                instance._rid = row['@rid']
                instances.append(instance)

        return instances

    async def _add_operation(self, operation):
        async with self._operations_queue_lock:
            self._operations_queue.append(operation)

    def _save_property_changes(self, property_recorder: PropertyChangesRecorder):
        async def coroutine(cursor: AkiraAsyncClientCursor):
            queries = []
            params = {}
            for i, change in enumerate(property_recorder.changes):
                q, p = change._query(i)
                queries.append(q)
                params.update(p)
            joined_query: Query = ','.join(queries)
            await cursor.execute_cypher(
                'match (n:%(type_name)s) where id(n) = %(node_id)s set ' + joined_query,
                dict(**params, type_name = self.__class__.__qualname__, node_id = self._rid)
            )
        return coroutine

    async def _save(self, cursor) -> None:
        async with self._operations_queue_lock:
            for property_recorder in self.property_recorders.values():
                if property_recorder.changes:
                    self._operations_queue.append(self._save_property_changes(property_recorder))
            if self._operations_queue:
                await asyncio.gather(*[coroutine(cursor) for coroutine in self._operations_queue])
                self._operations_queue = []

    async def save(self):
        async with self._database_connection.cursor() as cursor:
            await self._save(cursor)

    @staticmethod
    async def bulk_save(nodes: list[TModel]):
        if nodes:
            async with nodes[0]._database_connection.cursor() as cursor:
                await asyncio.gather(*[node._save(cursor) for node in nodes])

    async def delete(self) -> None:
        async with self._database_connection.cursor() as cursor:
            await cursor.execute_cypher(*self._get_delete_request())

    async def load(self) -> None:
        if not self._rid:
            raise AkiraUnknownNodeException()

        async with self._operations_queue_lock:
            if self._operations_queue:
                self._operations_queue = []

        async with self._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute_cypher(
                'match (n:%(type_name)s) where id(n) = %(node_id)s return n',
                {'type_name': self.__class__.__qualname__, 'node_id': self._rid}
            )
            async for row in cursor:
                if row is None:
                    raise AkiraUnknownNodeException()
                else:
                    for name, value in row.items():
                        if not name.startswith('@'):
                            setattr(self, name, value)

    def _split_properties_and_relations(self):
        properties: dict[str, Any] = {}
        relations: dict[str, 'Relation[BaseModel]'] = {}

        for field in fields(self):
            field_value = getattr(self, field.name)
            if field.name in self.__class__._relations_names:
                relations[field.name] = field_value
            else:
                properties[field.name] = field_value

        return properties, relations
