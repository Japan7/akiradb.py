import asyncio
from datetime import date, datetime, timedelta
from typing import Optional

from akiradb.database_connection import DatabaseConnection
from akiradb.model.base_model import BaseModel
from akiradb.model.relations import ManyWithProperties, Properties, relation

database_connection = DatabaseConnection(username='root',
                                         password='nihongonihongo',
                                         database='test')


class Model(BaseModel, database_connection=database_connection):
    pass


class SinceProperties(Properties):
    since: Optional[str]


class Person(Model):
    name: Optional[str]
    today: datetime = datetime(year=1970, month=1, day=1)
    married: bool = False

    spouses = relation('married_to',
                       ManyWithProperties["Person", SinceProperties],
                       bidirectionnal=True)


class BetterPerson(Person):
    birthday: date = date(year=1970, month=1, day=1)
    better_name: str = "Hello World"
    age: int = 0


class Quantity(Properties):
    number: int


class Item(Model):
    name: str
    description: str


class Player(Model):
    items = relation('possesses', ManyWithProperties[Item, Quantity])


class Media(Model):
    id: int
    name: str = "None"


async def main():
    await Model._database_connection.connect()

    nana_chans = []
    senpai_kuns = []
    for _ in range(1000):
        nana_chans.append(BetterPerson(name="Nana-chan", today=datetime.now()))
        senpai_kuns.append(Person(name="Senpai-kun", married=False, today=datetime.now()))

    await Person.bulk_create(nana_chans)
    await Person.bulk_create(senpai_kuns)

    nana_chan = await Person(name="Nana-chan", married=True, today=datetime.now()).create()
    senpai_kun = await Person(name="Senpai-kun", married=True, today=datetime.now()).create()

    nana_chan.spouses.add(senpai_kun, SinceProperties(since=None))
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
        married_nana_chan.married = False
        married_nana_chan.name += '-senpai'
        married_nana_chan.married = True
        await married_nana_chan.save()
        print(await married_nana_chan.spouses.get())

    vampires = [BetterPerson(name='Yupiel-sama', age=i, today=datetime.now(),
                             birthday=(date.today() - timedelta(days=i*365.2425)))
                for i in range(100)]
    await BetterPerson.bulk_create(vampires)

    for vampire in vampires:
        vampire.age += 1000
        await vampire.save()

    old_vampires = await BetterPerson.fetch_many(BetterPerson.age > 1069)
    await BetterPerson.bulk_delete([vampire for vampire in vampires if vampire.age <= 1069])
    print(len(old_vampires))

    married_nana_chans = await Person.fetch_many(Person.name == 'Nana-chan-senpai')
    for married_nana_chan in married_nana_chans:
        for spouse in [s for s, _ in await married_nana_chan.spouses.get()]:
            married_nana_chan.spouses.remove(spouse)
        married_nana_chan.name = 'Nana'
        married_nana_chan.married = False

    await BaseModel.bulk_save(married_nana_chans)

    await Media.bulk_delete(await Media.fetch_all())
    medias = [Media(id=1), Media(id=2)]
    await Media.bulk_create(medias)
    print([f'(rid: {m._rid}, {m})' for m in medias])

    medias[0].name = 'FLCL'
    medias[1].name = 'Serial Experiments Lain'
    medias.append(Media(id=3, name='Neon Genesis Evangelion'))

    await Media.bulk_upsert([(m, {'id': m.id}) for m in medias])
    print([f'(rid: {m._rid}, {m})' for m in medias])

    nonePerson = Person(name=None)
    await nonePerson.create()

    nonePerson2 = await Person.fetch_one(None, rid=nonePerson._rid)
    print(nonePerson2.name)

    await Model._database_connection.close()


if __name__ == '__main__':
    asyncio.run(main())
