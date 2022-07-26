import asyncio
from datetime import datetime

from akiradb.database_connection import DatabaseConnection
from akiradb.model.base_model import BaseModel
from akiradb.model.relations import ManyWithProperties, Properties, relation

database_connection = DatabaseConnection(username='root',
                                         password='nihongonihongo',
                                         database='test')


class Model(BaseModel, database_connection=database_connection):
    pass


class SinceProperties(Properties):
    since: str


class Person(Model):
    name: str

    spouses = relation('married_to',
                       ManyWithProperties["Person", SinceProperties],
                       bidirectionnal=True)


class Quantity(Properties):
    number: int


class Item(Model):
    name: str
    description: str


class Player(Model):
    items = relation('possesses', ManyWithProperties[Item, Quantity])


async def main():
    await Model._database_connection.connect()

    nana_chans = []
    senpai_kuns = []
    for _ in range(1000):
        nana_chans.append(Person(name="Nana-chan"))
        senpai_kuns.append(Person(name="Senpai-kun"))

    await Person.bulk_create(nana_chans)
    await Person.bulk_create(senpai_kuns)

    # nana_chan = await Person(name="Nana-chan").create()
    # senpai_kun = await Person(name="Senpai-kun").create()

    # nana_chan.spouses.add(senpai_kun, SinceProperties(since=str(datetime.now())))
    # await nana_chan.save()

    # player1 = await Player().create()
    # player2 = await Player().create()

    # gold = await Item(name="gold", description="Pieces of gold").create()

    # player1.items.add(gold, Quantity(number=100))
    # player2.items.add(gold, Quantity(number=200))

    # await player1.save()
    # await player2.save()

    await Model._database_connection.close()


if __name__ == '__main__':
    asyncio.run(main())
