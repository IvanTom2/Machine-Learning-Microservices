import json
import asyncpg
from asyncpg import Pool, Connection
from asyncio import Lock
from abc import ABC, abstractmethod


def load_config():
    with open("config.json") as file:
        return json.load(file)


config = load_config()

DB_POSTGRES: str = config["DB_POSTGRES"]
DB_POSTGRES_PASSWORD: str = config["DB_POSTGRES_PASSWORD"]

DB_USERNAME: str = config["DB_USERNAME"]
DB_PASSWORD: str = config["DB_PASSWORD"]
DB_HOST: str = config["DB_HOST"]
DB_PORT: int = config["DB_PORT"]


class DataBaseInitTemplate(ABC):
    def __init__(self, DB_NAME: str) -> None:
        self.lock = Lock()
        self.DB_NAME = DB_NAME

    async def __creator_conn(self) -> Connection:
        connection: Connection = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_POSTGRES,
            password=DB_POSTGRES_PASSWORD,
            database=DB_POSTGRES,
        )
        return connection

    async def __create_database(
        self,
        connection: Connection,
    ) -> None:
        async with self.lock:
            exists = await connection.fetch(
                f"""SELECT 1 FROM pg_catalog.pg_database
                    WHERE datname = '{self.DB_NAME}'"""
            )

            if not exists:
                await connection.execute(f"CREATE DATABASE {self.DB_NAME};")

        await self.__alter_db_owner(connection)

    async def __alter_db_owner(
        self,
        connection: Connection,
    ) -> None:
        async with self.lock:
            query = f"ALTER DATABASE {self.DB_NAME} OWNER TO {DB_USERNAME};"
            await connection.execute(query)

    async def _database_initiation(self) -> None:
        try:
            creator_conn = await self.__creator_conn()
            await self.__create_database(creator_conn)
            await creator_conn.close()

        finally:
            if creator_conn:
                await creator_conn.close()

    async def _user_conn(self) -> Connection:
        connection: Connection = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=self.DB_NAME,
        )

        return connection

    async def create_new_table(
        self,
        connection: Connection,
        creation_query: str,
    ) -> None:
        async with self.lock:
            await connection.execute(
                creation_query,
            )

    @abstractmethod
    async def init_tables(self):
        try:
            await self._database_initiation()
            connection = await self._user_conn()

            self.create_new_table(connection, "query1")
            self.create_new_table(connection, "query2")
            self.create_new_table(connection, "query3")

        finally:
            if connection:
                await connection.close()
