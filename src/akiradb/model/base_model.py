import asyncio
from dataclasses import MISSING, Field, fields, dataclass
from typing import (
    TYPE_CHECKING, Any, ClassVar, Coroutine, ParamSpec, Type, TypeVar, cast
)

from psycopg.rows import dict_row

from akiradb.database_connection import DatabaseConnection
from akiradb.exceptions import (
    AkiraNodeNotFoundException, AkiraNodeTypeAlreadyDefinedException, AkiraUnknownNodeException
)
from akiradb.model.conditions import Condition, PropertyCondition
from akiradb.model.proxies import PropertyChangesRecorder, PropertyChangesRecorderDescriptor
from akiradb.model.utils import __dataclass_transform__, _get_cypher_property_type

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
        instance._properties_recorders = {}
        return instance

    def __post_init__(self):
        self._rid: str
        self._operations_queue: list[Coroutine] = []
        self._properties, self._relations = self._split_properties_and_relations()
        self._properties_recorders: dict[str, PropertyChangesRecorder]
        for property_name, _ in self._properties.items():
            self._properties_recorders[property_name].clear_changes()
        for relation_name, relation_value in self._relations.items():
            relation_value._source = self
            relation_value._attribute_name = relation_name

    @classmethod
    async def _create_type_and_properties(cls):
        await cls._database_connection.connect()
        supertypes = [type.__qualname__ for type in cls.__bases__ if type is not BaseModel]
        async with cls._database_connection.cursor() as cursor:
            req = (f'create vertex type {cls.__qualname__} if not exists '
                   f'{("extends " + ",".join(supertypes)) if supertypes else ""};')
            await cursor.execute(req)
            for field in fields(cls):
                if field.name in cls._properties_names and field.name in cls.__annotations__:
                    req = (f'create property {cls.__qualname__}.{field.name} if not exists '
                           f'{_get_cypher_property_type(field.type)};')
                    await cursor.execute(req)
                    if field.default is not MISSING:
                        req = (f'alter property {cls.__qualname__}.{field.name} '
                               f'default {field.default!r};')
                        await cursor.execute(req)
        await cls._database_connection.close()

    @classmethod
    async def bulk_create(cls: Type[TModel], nodes: list[TModel]):
        async with cls._database_connection.cursor() as cursor:
            for node in nodes:
                await cursor.execute(node._get_create_request())
                async for row in cursor:
                    assert row is not None
                    node._rid, = row

            await cls._database_connection.commit()

    def _get_create_request(self) -> str:
        return (f'{{cypher}} create (n:{self.__class__.__qualname__} '
                f'{{{ self._transform_properties_to_cypher(self._properties) }}}) '
                f'return id(n);')

    @classmethod
    def _get_fetch_request(cls, rid: str | None = None,
                           condition: Condition | bool | None = None) -> str:
        return (f'{{cypher}} match (n:{cls.__qualname__}) '
                f'{("where " + str(condition)) if condition is not None else ""} '
                f'{("where id(n) = " + repr(rid)) if rid is not None else ""} '
                f'return n;')

    async def create(self):
        req = self._get_create_request()

        async with self._database_connection.execute(req) as cursor:
            await self._database_connection.commit()
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
            await cursor.execute(req)
            row = await cursor.fetchone()
            if not row:
                raise AkiraNodeNotFoundException()

            parameters = {name: value for (name, value) in row.items()
                          if not name.startswith('@') and value is not None}
            instance = MetaModel._models[row['@type']](**parameters)
            instance._rid = row['@rid']

        return instance

    @classmethod
    async def fetch_many(cls: Type[TModel], condition: Condition | bool) -> list[TModel]:
        req = cls._get_fetch_request(condition=condition)

        instances = []
        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(req)
            async for row in cursor:
                parameters = {name: value for (name, value) in row.items()
                              if not name.startswith('@') and value is not None}
                instance = MetaModel._models[row['@type']](**parameters)
                instance._rid = row['@rid']
                instances.append(instance)

        return instances

    @classmethod
    async def fetch_all(cls: Type[TModel]) -> list[TModel]:
        req = cls._get_fetch_request()

        instances = []
        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(req)
            async for row in cursor:
                parameters = {name: value for (name, value) in row.items()
                              if not name.startswith('@') and value is not None}
                instance = MetaModel._models[row['@type']](**parameters)
                instance._rid = row['@rid']
                instances.append(instance)

        return instances

    async def save(self) -> None:
        if self._operations_queue:
            await asyncio.gather(*self._operations_queue)
            await self._database_connection.commit()
            self._operations_queue = []

    async def load(self) -> None:
        if not self._rid:
            raise AkiraUnknownNodeException()

        if self._operations_queue:
            self._operations_queue = []

        req = (f'{{cypher}} match (n:{self.__class__.__qualname__}) '
               f'where id(n) = "{self._rid}" return n;')
        async with self._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(req)
            async for row in cursor:
                if row is None:
                    raise AkiraUnknownNodeException()
                else:
                    for name, value in row.items():
                        if not name.startswith('@'):
                            setattr(self, name, value)

    @staticmethod
    def _transform_properties_to_cypher(properties: dict[str, Any]):
        return ",".join(f"{index}: {value!r}"
                        for index, value in properties.items() if value is not None)

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
