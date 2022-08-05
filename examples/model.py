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
    married: bool = False

    spouses = relation('married_to',
                       ManyWithProperties["Person", SinceProperties],
                       bidirectionnal=True)


class BetterPerson(Person):
    better_name: str = "Hello World"


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
        nana_chans.append(BetterPerson(name="Nana-chan"))
        senpai_kuns.append(Person(name="Senpai-kun", married=False))

    await Person.bulk_create(nana_chans)
    await Person.bulk_create(senpai_kuns)

    nana_chan = await Person(name="Nana-chan", married=True).create()
    senpai_kun = await Person(name="Senpai-kun", married=True).create()

    nana_chan.spouses.add(senpai_kun, SinceProperties(since=str(datetime.now())))
    await nana_chan.save()

    player1 = await Player().create()
    player2 = await Player().create()

    gold = await Item(name="gold", description="Pieces of gold").create()

    player1.items.add(gold, Quantity(number=100))
    player2.items.add(gold, Quantity(number=200))

    await player1.save()
    await player2.save()

    await nana_chan.load()

    nana_chans_2 = await Person.fetch_many(Person.name == 'Nana-chan')
    print(len(nana_chans_2))
    print(len(await Person.fetch_all()))

    married_nana_chans = await Person.fetch_many((Person.name == 'Nana-chan')
                                                 & (Person.married == True))
    for married_nana_chan in married_nana_chans:
        married_nana_chan.name += '-senpai'
        print(await married_nana_chan.spouses.get())

    await Model._database_connection.close()


if __name__ == '__main__':
    asyncio.run(main())
