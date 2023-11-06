"""
There will be a pipeline for processing data entering the model.
"""
import asyncio
import pandas as pd

from database import DataBaseInterface
from notation import Patient


class DBAdapter(object):
    def __init__(self) -> None:
        pass

    async def _get_db_interface(self) -> DataBaseInterface:
        db = DataBaseInterface(6)
        await db.create_pool()
        return db

    async def _get_all_data(self, db: DataBaseInterface):
        return await db.get_data_to_train()

    async def main(self):
        db = await self._get_db_interface()
        data = await self._get_all_data(db)

        return data

    def get_data(self) -> pd.DataFrame:
        loop = asyncio.get_event_loop()

        data = loop.run_until_complete(self.main())
        data = list(map(dict, data))
        data = pd.DataFrame.from_dict(data)

        return data


if __name__ == "__main__":
    db_adapter = DBAdapter()

    data = db_adapter.get_data()
    print(data)
