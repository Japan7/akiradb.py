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
        self._properties, self._relations = self._split_properties_and_relations()
        for relation_name, relation_value in self._relations.items():
            relation_value._source = self
            relation_value._attribute_name = relation_name

    @classmethod
    async def bulk_create(cls, nodes: list['BaseModel']):
        async with cls._database_connection.pipeline() as pipeline:
            async with cls._database_connection.cursor() as cursor:
                for node in nodes:
                    await cursor.execute(node._get_create_request())

                await cls._database_connection.commit()

                await pipeline.sync()
                i = 0
                while True:
                    row = await cursor.fetchone()
                    assert row is not None
                    nodes[i]._rid = row
                    if not cursor.nextset():
                        break
                    i += 1
                print(i+1)

    def _get_create_request(self) -> str:
        return (f"{{cypher}} create (n:{self.__class__.__qualname__} "
                f"{{{ self._transform_properties_to_cypher(self._properties) }}}) "
                f"return id(n);")

    async def create(self):
        req = self._get_create_request()

        async with self._database_connection.execute(req) as cursor:
            await self._database_connection.commit()
            row = await cursor.fetchone()
            assert row is not None
            self._rid, = row

        return self

    async def save(self) -> None:
        if self._operations_queue:
            await asyncio.gather(*self._operations_queue)
            await self._database_connection.commit()
            self._operations_queue = []

    def _transform_properties_to_cypher(self, properties: dict[str, Any]):
        return ",".join(f"{index}: {value!r}"
                        for index, value in properties.items())

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
