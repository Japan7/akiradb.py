import asyncio
from dataclasses import Field, fields, dataclass
from typing import (
    TYPE_CHECKING, Any, Callable, ClassVar, Coroutine, ParamSpec, Type, TypeVar, cast
)

from psycopg.rows import dict_row

from akiradb.database_connection import DatabaseConnection
from akiradb.exceptions import AkiraNodeNotFoundException, AkiraUnknownNodeException
from akiradb.model.utils import __dataclass_transform__

if TYPE_CHECKING:
    from akiradb.model.relations import Relation


anything: Any = None


@__dataclass_transform__()
class MetaModel(type):
    _models: dict[str, 'MetaModel'] = {}

    def __new__(cls, name, bases, dct,
                database_connection: DatabaseConnection | None = None):
        if '__annotations__' not in dct:
            dct['__annotations__'] = {}

        properties_names = []
        relations_names = []
        for key, value in dct.items():
            if isinstance(value, Field) and 'type' in value.metadata:
                dct['__annotations__'][key] = value.metadata['type']
                relations_names.append(key)

        instance = cast(Type, super().__new__(cls, name, bases, dct))
        if database_connection is not None:
            instance._database_connection = database_connection

        dataclass_instance = dataclass(instance)
        for field in fields(dataclass_instance):
            if field.name not in relations_names:
                properties_names.append(field.name)
        dataclass_instance._properties_names = properties_names
        dataclass_instance._relations_names = relations_names
        # TODO: Simplify instance split from class split

        # TODO: Exception if name already exists
        MetaModel._models[name] = dataclass_instance
        return dataclass_instance


TModel = TypeVar('TModel', bound='BaseModel')
P = ParamSpec('P')


class BaseModel(metaclass=MetaModel):
    _database_connection: ClassVar[DatabaseConnection]

    def __post_init__(self):
        self._rid: str
        self._operations_queue: list[Coroutine] = []
        self._properties, self._relations = self._split_properties_and_relations()
        for relation_name, relation_value in self._relations.items():
            relation_value._source = self
            relation_value._attribute_name = relation_name

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
        return (f"{{cypher}} create (n:{self.__class__.__qualname__} "
                f"{{{ self._transform_properties_to_cypher(self._properties) }}}) "
                f"return id(n);")

    @classmethod
    def _get_fetch_request(cls, **kwargs) -> str:
        return (f"{{cypher}} match (n:{cls.__qualname__} "
                f"{{{ cls._transform_properties_to_cypher(kwargs) }}}) "
                f"return n;")

    async def create(self):
        req = self._get_create_request()

        async with self._database_connection.execute(req) as cursor:
            await self._database_connection.commit()
            row = await cursor.fetchone()
            assert row is not None
            self._rid, = row

        return self

    @classmethod
    async def fetch_one(cls: Callable[P, TModel],  # type: ignore[supertype]
                        rid: str | None = None, *_: P.args, **kwargs: P.kwargs) -> TModel:
        if rid:
            req = cls._get_fetch_request({'@rid': rid})
        else:
            req = cls._get_fetch_request(**kwargs)

        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(req)
            row = await cursor.fetchone()
            if not row:
                raise AkiraNodeNotFoundException()

            parameters = {name: value for (name, value) in row.items()
                          if not name.startswith('@')}
            instance = cls(**parameters)  # type: ignore
            instance._rid = row['@rid']

        return instance

    @classmethod
    async def fetch_many(cls: Callable[P, TModel],  # type: ignore[supertype]
                         *_: P.args, **kwargs: P.kwargs) -> list[TModel]:
        req = cls._get_fetch_request(**kwargs)

        instances = []
        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(req)
            async for row in cursor:
                parameters = {name: value for (name, value) in row.items()
                              if not name.startswith('@')}
                instance = cls(**parameters)  # type: ignore
                instance._rid = row['@rid']
                instances.append(instance)

        return instances

    @classmethod
    async def fetch_all(cls: Type[TModel]) -> list[TModel]:
        req = f"{{cypher}} match (n:{cls.__qualname__}) return n;"

        instances = []
        async with cls._database_connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(req)
            async for row in cursor:
                parameters = {name: value for (name, value) in row.items()
                              if not name.startswith('@')}
                instance = cls(**parameters)  # type: ignore
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
            mros = (f"{t.__module__}.{t.__name__}"
                    for t in type(field_value).__mro__)
            if 'akiradb.model.relations.Relation' in mros:
                relations[field.name] = field_value
            else:
                properties[field.name] = field_value

        return properties, relations
