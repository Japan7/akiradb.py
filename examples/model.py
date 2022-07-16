import asyncio
from dataclasses import dataclass

from akiradb.database_connection import DatabaseConnection
from akiradb.model.base_model import BaseModel
from akiradb.typing.relationship import OneToManyRelationship, ManyToOneRelationship


class Model(BaseModel):
    _database_connection = DatabaseConnection(username='root', password='nihongonihongo', database='test')


@dataclass
class Person(Model):
    name: str
    spouses: OneToManyRelationship["Person"]
    spouses_invert: ManyToOneRelationship["Person"]


async def main():
    await Model._database_connection.connect()

    person1 = await Person.create(name="Nana-chan", spouses=[None], spouses_invert=None)
    await Person.create(name="Senpai-kun", spouses=[person1], spouses_invert=None)

    await Model._database_connection.close()


# class Person2(Model):
#     def __init__(self, name: str):
#         self.name = name
# 
#     spouses: OneToManyRelationship["Person2"]
#     spouses_invert: ManyToOneRelationship["Person2"]
# 
# 
# async def main2():
#     await Model._database_connection.connect()
# 
#     person1 = Person2(name="Nana-chan")
#     await person1.save()
# 
#     person2 = Person2(name="Senpai-kun")
#     person2.spouses.add(person1)
#     await person2.save()


if __name__ == '__main__':
    asyncio.run(main())

