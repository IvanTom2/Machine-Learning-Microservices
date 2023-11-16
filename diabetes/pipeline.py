"""
There will be a pipeline for processing data entering the model.
"""
import asyncio
import pickle
import pandas as pd
from dateutil.relativedelta import relativedelta
from sklearn.preprocessing import LabelEncoder, StandardScaler


from database import DataBaseInterface
from notation import Patient, Feature


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


def setup_new_features(row: pd.Series) -> pd.Series:
    row["age"] = relativedelta(row["observation_date"], row["birthday_date"]).years
    row["NF0"] = row["bmi"] * row["skin_thickness"]
    row["NF1"] = 1 if row["age"] <= 30 and row["glucose"] <= 120 else 0
    row["NF2"] = 1 if row["bmi"] <= 30 else 0
    row["NF3"] = 1 if row["age"] <= 30 and row["pregnancies"] <= 6 else 0
    row["NF4"] = 1 if row["glucose"] <= 105 and row["blood_pressure"] <= 80 else 0
    row["NF5"] = 1 if row["skin_thickness"] <= 20 else 0
    row["NF6"] = 1 if row["bmi"] < 30 and row["skin_thickness"] <= 20 else 0
    row["NF7"] = 1 if row["glucose"] <= 105 and row["bmi"] <= 30 else 0
    row["NF8"] = row["pregnancies"] / row["age"]
    row["NF9"] = 1 if row["insulin"] < 200 else 1
    row["NF10"] = 1 if row["blood_pressure"] < 80 else 0
    row["NF11"] = 1 if row["pregnancies"] < 4 and row["pregnancies"] != 0 else 0
    row["NF12"] = row["age"] / row["diabetes_predigree_function"]
    row["NF13"] = row["glucose"] / row["diabetes_predigree_function"]
    row["NF14"] = row["age"] / row["insulin"]
    row["NF15"] = 1 if row["NF0"] < 1034 else 0

    return row


def isnull(value) -> bool:
    if not value or value == 0:
        return True
    return False


def setup_nulls(data: pd.DataFrame) -> pd.DataFrame:
    data.loc[
        (data["diagnosis"] == 0) & (data["insulin"].apply(isnull)),
        "insulin",
    ] = 102.5
    data.loc[
        (data["diagnosis"] == 1) & (data["insulin"].apply(isnull)),
        "insulin",
    ] = 169.5

    data.loc[
        (data["diagnosis"] == 0) & (data["glucose"].apply(isnull)),
        "glucose",
    ] = 107
    data.loc[
        (data["diagnosis"] == 1) & (data["glucose"].apply(isnull)),
        "glucose",
    ] = 140

    data.loc[
        (data["diagnosis"] == 0) & (data["skin_thickness"].apply(isnull)),
        "skin_thickness",
    ] = 27
    data.loc[
        (data["diagnosis"] == 1) & (data["skin_thickness"].apply(isnull)),
        "skin_thickness",
    ] = 32

    data.loc[
        (data["diagnosis"] == 0) & (data["blood_pressure"].apply(isnull)),
        "blood_pressure",
    ] = 70
    data.loc[
        (data["diagnosis"] == 1) & (data["blood_pressure"].apply(isnull)),
        "blood_pressure",
    ] = 74.5

    data.loc[(data["diagnosis"] == 0) & (data["bmi"].apply(isnull)), "bmi"] = 30.1
    data.loc[(data["diagnosis"] == 1) & (data["bmi"].apply(isnull)), "bmi"] = 34.3

    return data


def extract_columns(data: pd.DataFrame):
    columns = data.columns
    not_need_cols = [
        "birthday_date",
        "observation_date",
        "patient_id",
        "observation_id",
    ]

    need_cols = [col for col in columns if col not in not_need_cols]
    return data[need_cols]


def scale_features_learner(data: pd.DataFrame) -> pd.DataFrame:
    target = Feature.TARGET.name

    numeric = [f.name for f in Feature.get_keys() if f.type == "NUMERIC"]
    binary = [f.name for f in Feature.get_keys() if f.type == "BINARY"]

    std = StandardScaler()
    std = std.fit(data[numeric])
    std_cols = numeric

    data[numeric] = std.transform(data[numeric])

    X = data[numeric + binary]
    y = data[target]
    return X, y, std, std_cols


def scale_features(row: pd.Series, std: StandardScaler) -> pd.Series:
    numeric = [f.name for f in Feature.get_keys() if f.type == "NUMERIC"]
    row[numeric] = std.transform(row[numeric])
    return row


def save_model(path, model) -> None:
    if ".pickle" not in str(path):
        raise ValueError("Path should contains {name}.pickle")

    with open(path, "wb") as file:
        pickle.dump(model, file)


def upload_model(path):
    with open(path, "rb") as file:
        model = pickle.load(file)
    return model


if __name__ == "__main__":
    db_adapter = DBAdapter()

    data = db_adapter.get_data()
    data = setup_nulls(data)
    data = data.apply(setup_new_features, axis=1)
    data = extract_columns(data)

    X, y = scale_features_learner(data)
