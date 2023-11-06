import sys
import random
import asyncio
import pandas as pd
import numpy as np
from pathlib import Path
from collections import namedtuple
from russian_names import RussianNames
from notation import (
    Patient,
    Observation,
    FinalReport,
    PreliminaryReport,
    DataSet,
    Attribute,
    Notation,
    ObservationData,
)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


PersonalInfo = namedtuple(
    "PerfonalInfo", ["first_name", "patronymic", "last_name", "phone"]
)


class EntityObject(dict):
    notation = Notation
    _allowed_keys = set([key.name for key in notation.get_keys()])

    def __getitem__(self, key):
        if key not in self._allowed_keys:
            raise ValueError(f"{self.__class__.__name__} has no attribute '{key}'")
        return super().get(key, None)

    def __setitem__(self, key, value):
        if key not in self._allowed_keys:
            raise ValueError(f"{self.__class__.__name__} has no attribute '{key}'")
        super().__setitem__(key, value)

    @classmethod
    def from_keys_and_values(self, keys: list[str], values: list) -> dict:
        if len(keys) != len(values):
            raise ValueError("Keys count should be equal to values count")
        return self({keys[i]: values[i] for i in range(len(keys))})

    @classmethod
    def from_dataframe(self, dataframe: pd.DataFrame) -> list[dict]:
        if len(self._allowed_keys) == 0:
            raise ValueError(f"There aren't any key in {self.__class__.__name__}")

        columns = set(list(dataframe.columns))
        keys = list(self._allowed_keys.intersection(columns))
        if len(keys) == 0:
            raise ValueError(
                f"In dataframe there aren't any key from {self.__class__.__name__}"
            )

        subset = dataframe[keys].to_records(index=False)
        items = list(
            map(
                lambda sub: self.from_keys_and_values(keys, sub),
                subset,
            )
        )
        return items


class PatientObject(EntityObject):
    notation = Patient
    _allowed_keys = set([key.name for key in notation.get_keys()])


class ObservationObject(EntityObject):
    notation = Observation
    _allowed_keys = set([key.name for key in notation.get_keys()])


class ObservationDataObject(EntityObject):
    notation = ObservationData
    _allowed_keys = set([key.name for key in notation.get_keys()])


class FinalReportObject(EntityObject):
    notation = FinalReport
    _allowed_keys = set([key.name for key in notation.get_keys()])


class PreliminaryReportObject(EntityObject):
    notation = PreliminaryReport
    _allowed_keys = set([key.name for key in notation.get_keys()])


class RussianPhoneNumber(object):
    def __init__(self):
        self._operators = self._get_operators()

    def _get_operators(self):
        return list(map(str, range(900, 1000)))

    def _get_operator(self):
        return random.sample(self._operators, 1)[0]

    def _get_number(self):
        return str(random.randint(1000000, 9999999))

    def generate_phone_number(self):
        country = "8"
        operator = self._get_operator()
        number = self._get_number()

        return country + operator + number


class PreviousDataGenerator(object):
    def __init__(self) -> None:
        self._phone_generator = RussianPhoneNumber()
        self._name_generator = RussianNames(gender=0)

    def _read_data(self):
        return pd.read_csv(Path(__file__).parent / "diabetes.csv")

    def _rename(self, data: pd.DataFrame) -> pd.DataFrame:
        mapper = {
            DataSet.PREGNANCIES.name: ObservationData.PREGNANCIES.name,
            DataSet.GLUCOSE.name: ObservationData.GLUCOSE.name,
            DataSet.BLOOD_PRESSURE.name: ObservationData.BLOOD_PRESSURE.name,
            DataSet.SKIN_THICKNESS.name: ObservationData.SKIN_THICKNESS.name,
            DataSet.INSULIN.name: ObservationData.INSULIN.name,
            DataSet.BMI.name: ObservationData.BMI.name,
            DataSet.DIABETES_PEDIGREE_FUNCTION.name: ObservationData.DIABETES_PEDIGREE_FUNCTION.name,
            DataSet.DIAGNOSIS.name: FinalReport.DIAGNOSIS.name,
        }

        return data.rename(mapper, axis=1)

    def _get_personal_info(self, row: pd.Series) -> pd.Series:
        (
            row[Patient.FIRST_NAME.name],
            row[Patient.PATRONYMIC.name],
            row[Patient.LAST_NAME.name],
        ) = self._name_generator.get_person().split()
        row[Patient.PHONE_NUMBER.name] = self._phone_generator.generate_phone_number()

        return row

    def _calculate_birthday(self, row: pd.Series) -> pd.Series:
        age = row[DataSet.AGE.name]
        obs_date = row[Observation.OBSERVATION_DATE.name]
        row[Patient.BIRTHDAY.name] = obs_date - timedelta(
            days=random.randint(age * 365 + 30, (age + 1) * 365 - 30)
        )
        return row

    def _calculate_report_date(self, row: pd.Series) -> pd.Series:
        obs_date = row[Observation.OBSERVATION_DATE.name]
        row[FinalReport.REPORT_DATE.name] = obs_date + timedelta(
            days=random.randint(1, 15)
        )
        return row

    def _determine_dates(self, data: pd.DataFrame) -> pd.DataFrame:
        cur_date = datetime.now()
        start_date = datetime(cur_date.year, cur_date.month, 1)
        end_date = cur_date

        data[Observation.OBSERVATION_DATE.name] = [
            start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
            for _ in range(len(data))
        ]
        data = data.apply(self._calculate_birthday, axis=1)
        data = data.apply(self._calculate_report_date, axis=1)

        return data

    def _check_age(self, row: pd.Series) -> pd.Series:
        spread = relativedelta(
            row[Observation.OBSERVATION_DATE.name], row[Patient.BIRTHDAY.name]
        )
        relative_age = spread.years
        row["age_spread"] = row[DataSet.AGE.name] - relative_age
        return row

    def check_age(self, data: pd.DataFrame):
        data = data.apply(self._check_age, axis=1)
        spread = data["age_spread"].sum()
        assert spread == 0

    def get_data(self):
        data = self._read_data()
        data = self._rename(data)

        data = data.apply(self._get_personal_info, axis=1)
        data = self._determine_dates(data)
        data[Patient.GENDER.name] = "f"
        data[FinalReport.DIAGNOSIS.name] = data[FinalReport.DIAGNOSIS.name].astype(bool)

        self.check_age(data)
        return data


if __name__ == "__main__":
    DGen = PreviousDataGenerator()
    data = DGen.get_data()

    print(data)

    # patient = PatientObject()
    # patient[Patient.FIRST_NAME.name] = "Ivan"

    # print(patient[Patient.FIRST_NAME.name])
    # print(patient[Patient.LAST_NAME.name])
