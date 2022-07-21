import asyncio
from dataclasses import Field, fields, dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Coroutine, Type, cast

from akiradb.database_connection import DatabaseConnection
from akiradb.model.utils import __dataclass_transform__

if TYPE_CHECKING:
    from akiradb.model.relations import Relation


@__dataclass_transform__()
class MetaModel(type):

    def __new__(cls, name, bases, dct,
                database_connection: DatabaseConnection | None = None):
        if '__annotations__' not in dct:
            dct['__annotations__'] = {}

        for key, value in dct.items():
            if isinstance(value, Field) and 'type' in value.metadata:
                dct['__annotations__'][key] = value.metadata['type']

        instance = cast(Type, super().__new__(cls, name, bases, dct))
        if database_connection is not None:
            instance._database_connection = database_connection
        return dataclass(instance)


class BaseModel(metaclass=MetaModel):
    _database_connection: ClassVar[DatabaseConnection]

    def __post_init__(self):
        self._rid: str
        self._operations_queue: list[Coroutine] = []

    async def create(self):
        properties, relationships = self._split_properties_and_relationships()

        req = (f"{{cypher}} create (n:{self.__class__.__qualname__} "
               f"{{{ self._transform_properties_to_cypher(properties) }}}) "
               f"return id(n);")

        async with self._database_connection.execute(req) as cursor:
            await self._database_connection.commit()
            row = await cursor.fetchone()
            assert row is not None
            self._rid, = row

        for relationship in relationships.values():
            relationship._source = self

        return self

    async def save(self) -> None:
        await asyncio.gather(*self._operations_queue)
        await self._database_connection.commit()
        self._operations_queue = []

    def _transform_properties_to_cypher(self, properties: dict[str, Any]):
        return ",".join(f"{index}: {value!r}"
                        for index, value in properties.items())

    def _split_properties_and_relationships(self):
        properties: dict[str, Any] = {}
        relationships: dict[str, 'Relation[BaseModel]'] = {}

        for field in fields(self):
            field_value = getattr(self, field.name)
            mros = (f"{t.__module__}.{t.__name__}"
                    for t in type(field_value).__mro__)
            if 'akiradb.model.relations.Relation' in mros:
                relationships[field.name] = field_value
            else:
                properties[field.name] = field_value

        return properties, relationships
