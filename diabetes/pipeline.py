"""
There will be a pipeline for processing data entering the model.
"""
import re
import asyncio
import pickle
import pandas as pd
import numpy as np
from pathlib import Path
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from aiohttp.web_app import Application


from database import DataBaseInterface
from notation import (
    Patient,
    Feature,
    FinalReport,
    ObservationData,
    Observation,
    Feature,
)


RANDOM_STATE = 42
TEST_SAMPLE_SIZE = 0.3

MODELS_PATH = Path(__file__).parent / "models"
STD_PATH = "std_model.pickle"
MEDIANS_PATH = "medians.pickle"
MAIN_MODEL = "main_model.pickle"

PIPELINE_KEY = "pipeline"


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


def save_model(path, model) -> None:
    if ".pickle" not in str(path):
        raise ValueError("Path should contains {name}.pickle")

    with open(path, "wb") as file:
        pickle.dump(model, file)


def upload_model(path):
    with open(path, "rb") as file:
        model = pickle.load(file)

    model_name = re.split("[\/]", str(path))[-1]
    print(f"Uploaded model {model_name}")

    return model


class LearnPipeline(object):
    def __init__(self) -> None:
        self.medians = None
        self.std_model = None

        self.nulls_exception = set(
            [
                Feature.PREGNANCIES.name,
            ]
        )

    def _save_model(self, path, model) -> None:
        return save_model(path, model)

    def _count_age(self, row: pd.Series) -> pd.Series:
        row[Feature.AGE.name] = relativedelta(
            row[Observation.OBSERVATION_DATE.name],
            row[Patient.BIRTHDAY.name],
        ).years
        return row

    def isnull(self, value) -> bool:
        if not value or value == 0:
            return True
        return False

    def setup_nulls(
        self,
        data: pd.DataFrame,
        medians: pd.Series = None,
    ) -> pd.DataFrame:
        if self.medians is not None:
            medians = self.medians
        elif medians is not None:
            medians = medians
        else:
            medians = data.median()
            self._save_model(MODELS_PATH / MEDIANS_PATH, medians)

        for column in medians.index:
            if column not in self.nulls_exception:
                data.loc[
                    data[column].apply(self.isnull),
                    column,
                ] = medians[column]

        return data, medians

    def count_age(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.apply(self._count_age, axis=1)
        return data

    def extract_columns(self, data: pd.DataFrame) -> tuple[pd.DataFrame]:
        return data[
            [
                Feature.PREGNANCIES.name,
                Feature.GLUCOSE.name,
                Feature.BLOOD_PRESSURE.name,
                Feature.SKIN_THICKNESS.name,
                Feature.INSULIN.name,
                Feature.BMI.name,
                Feature.DIABETES_PEDIGREE_FUNCTION.name,
                Feature.AGE.name,
            ]
        ]

    def scale_features(
        self,
        data: pd.DataFrame,
        std_model: StandardScaler = None,
    ) -> pd.DataFrame:
        numeric = Feature.numeric_columns

        if self.std_model is not None:
            std_model = self.std_model
        elif std_model is not None:
            std_model = std_model
        else:
            std_model = StandardScaler()
            std_model = std_model.fit(data[numeric])
            self._save_model(MODELS_PATH / STD_PATH, std_model)

        data[numeric] = std_model.transform(data[numeric])
        return data, std_model

    def run(self):
        db_adapter = DBAdapter()
        data = db_adapter.get_data()
        data = self.count_age(data)

        X = self.extract_columns(data)
        y = data[[Feature.target]]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y.values.ravel(),
            test_size=TEST_SAMPLE_SIZE,
            random_state=RANDOM_STATE,
        )

        X_train, medians = self.setup_nulls(X_train)
        X_test, _ = self.setup_nulls(X_test, medians)

        X_train, std_model = self.scale_features(X_train)
        X_test, _ = self.scale_features(X_test, std_model)

        return X_train, X_test, y_train, y_test


class ProductPipeline(LearnPipeline):
    def __init__(self) -> None:
        super().__init__()

        self.medians = self._upload_model(MODELS_PATH / MEDIANS_PATH)
        self.std_model = self._upload_model(MODELS_PATH / STD_PATH)

    def _upload_model(self, path):
        return upload_model(path)

    def run(self, data: pd.DataFrame) -> tuple[pd.DataFrame]:
        data = self.count_age(data)
        data = self.extract_columns(data)

        data, _ = self.setup_nulls(data)
        data, _ = self.scale_features(data)

        return data


async def create_pipeline(app: Application) -> None:
    pipeline = ProductPipeline()
    app[PIPELINE_KEY] = pipeline


if __name__ == "__main__":
    pipeline = LearnPipeline()
    # pipeline = ProductPipeline()

    data = pipeline.run()
