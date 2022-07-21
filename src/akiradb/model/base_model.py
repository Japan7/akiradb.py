from dataclasses import Field

from typing import Any

from akiradb.database_connection import DatabaseConnection


class MetaModel(type):
    def __new__(cls, name, bases, dct):
        for (name, value) in dct.items():
            if isinstance(value, Field) and 'type' in value.metadata:
                if '__annotations__' in dct:
                    dct['__annotations__'][name] = value.metadata['type']
                else:
                    dct['__instance__'] = {name: value.metadata['type']}

        instance = super().__new__(cls, name, bases, dct)
        return instance


class BaseModel(metaclass=MetaModel):
    _database_connection: DatabaseConnection

    async def create(self):
        (properties, _) = self._split_properties_and_relationships()
        await self._database_connection.execute(f'{{cypher}} create (:{self.__class__.__qualname__} {{{ self._transform_properties_to_cypher(properties) }}});')
        await self._database_connection.execute('commit;')
        return self

    async def save(self) -> None:
        pass

    def _transform_properties_to_cypher(self, properties: dict[str, Any]):
        return ",".join(f"{index}: {value!r}" for index, value in properties.items())
        
    def _split_properties_and_relationships(self):
        properties = {}
        relationships = {}
        for (field_name, field_value) in self.__dict__.items():
            if 'akiradb.model.relations.Relation' in [t.__module__ + '.' + t.__name__ for t in type(field_value).__mro__]:
                relationships[field_name] = field_value
            else:
                properties[field_name] = field_value

        return (properties, relationships)
