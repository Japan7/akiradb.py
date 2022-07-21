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


async def main():
    await Model._database_connection.connect()

    nana_chan = await Person(name="Nana-chan").create()
    senpai_kun = await Person(name="Senpai-kun").create()

    nana_chan.spouses.add(senpai_kun, SinceProperties(since=str(datetime.now())))
    await nana_chan.save()

    await Model._database_connection.close()


if __name__ == '__main__':
    asyncio.run(main())
