from dataclasses import Field, dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Type, cast

from akiradb.database_connection import DatabaseConnection
from akiradb.model.utils import __dataclass_transform__

if TYPE_CHECKING:
    from akiradb.model.relations import Relation


@__dataclass_transform__()
class MetaModel(type):

    def __new__(cls, name, bases, dct, database_connection: DatabaseConnection | None = None):
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

    async def create(self):
        properties, _ = self._split_properties_and_relationships()

        req = (f"{{cypher}} create (:{self.__class__.__qualname__} "
               f"{{{ self._transform_properties_to_cypher(properties) }}});")

        await self._database_connection.execute(req)
        await self._database_connection.execute('commit;')
        return self

    async def save(self) -> None:
        pass

    def _transform_properties_to_cypher(self, properties: dict[str, Any]):
        return ",".join(f"{index}: {value!r}" for index, value in properties.items())

    def _split_properties_and_relationships(self):
        properties: dict[str, Any] = {}
        relationships: dict[str, 'Relation[BaseModel]'] = {}

        for field_name, field_value in self.__dict__.items():
            mros = (f"{t.__module__}.{t.__name__}" for t in type(field_value).__mro__)
            if 'akiradb.model.relations.Relation' in mros:
                relationships[field_name] = field_value
            else:
                properties[field_name] = field_value

        return properties, relationships
