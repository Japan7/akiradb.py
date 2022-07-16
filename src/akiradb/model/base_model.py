import dataclasses

from typing import Any

from akiradb.database_connection import DatabaseConnection


class BaseModel():
    _database_connection: DatabaseConnection

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    async def create(cls, *args, **kwargs):
        instance = cls(*args, **kwargs)
        (properties, _) = cls._split_properties_and_relationships(kwargs)
        await cls._database_connection.execute(f'{{cypher}} create (:{cls.__name__} {{{ cls._transform_properties_to_cypher(properties) }}});')
        await cls._database_connection.execute('commit;')
        return instance

    @staticmethod
    def _transform_properties_to_cypher(properties: dict[str, Any]):
        return ",".join(f"{index}: {value!r}" for index, value in properties.items())
        
    @classmethod
    def _split_properties_and_relationships(cls, arguments: dict[str, Any]):
        fields = dataclasses.fields(cls)
        properties = {}
        relationships = {}
        for f in fields:
            if hasattr(f.type, '__metadata__') and 'Relationship' in f.type.__metadata__:
                relationships[f.name] = arguments[f.name]
            else:
                properties[f.name] = arguments[f.name]

        return (properties, relationships)
