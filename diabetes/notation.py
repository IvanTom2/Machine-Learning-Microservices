from abc import ABCMeta
from dataclasses import dataclass
from typing import Optional


@dataclass
class Attribute(object):
    name: str
    type: str


class Notation(ABCMeta):
    TABLE_NAME = "table_name"

    @classmethod
    def get_keys(self) -> list[Attribute]:
        items = self.__dict__.items()
        items = [item[1] for item in items if isinstance(item[1], Attribute)]
        return items


class DataSet(Notation):
    PREGNANCIES = Attribute("Pregnancies", "")
    GLUCOSE = Attribute("Glucose", "")
    BLOOD_PRESSURE = Attribute("BloodPressure", "")
    SKIN_THICKNESS = Attribute("SkinThickness", "")
    INSULIN = Attribute("Insulin", "")
    BMI = Attribute("BMI", "")
    DIABETES_PEDIGREE_FUNCTION = Attribute("DiabetesPedigreeFunction", "")
    AGE = Attribute("Age", "")
    DIAGNOSIS = Attribute("Outcome", "INT")


class Patient(Notation):
    TABLE_NAME = "patient"
    PATIENT_ID = Attribute("patient_id", "SERIAL")
    FIRST_NAME = Attribute("first_name", "VARCHAR(100)")
    LAST_NAME = Attribute("last_name", "VARCHAR(100)")
    PATRONYMIC = Attribute("patronymic", "VARCHAR(100)")
    BIRTHDAY = Attribute("birthday_date", "DATE")
    PHONE_NUMBER = Attribute("phone_number", "VARCHAR(12)")
    GENDER = Attribute("gender", "CHAR(1)")


class Observation(Notation):
    TABLE_NAME = "observation"
    PATIENT_ID = Attribute("patient_id", "INT")
    OBSERVATION_DATE = Attribute("observation_date", "DATE")
    OBSERVATION_ID = Attribute("observation_id", "SERIAL")


class ObservationData(Notation):
    TABLE_NAME = "observation_data"
    OBSERVATION_ID = Attribute("observation_id", "INT")
    PREGNANCIES = Attribute("pregnancies", "INT")
    GLUCOSE = Attribute("glucose", "FLOAT")
    BLOOD_PRESSURE = Attribute("blood_pressure", "FLOAT")
    SKIN_THICKNESS = Attribute("skin_thickness", "FLOAT")
    INSULIN = Attribute("insulin", "FLOAT")
    BMI = Attribute("bmi", "FLOAT")
    DIABETES_PEDIGREE_FUNCTION = Attribute("diabetes_predigree_function", "FLOAT")


class FinalReport(Notation):
    TABLE_NAME = "final_report"
    OBSERVATION_ID = Attribute("observation_id", "INT")
    DIAGNOSIS = Attribute("diagnosis", "BOOL")
    REPORT_DATE = Attribute("final_report_date", "DATE")


class PreliminaryReport(Notation):
    TABLE_NAME = "preliminary_report"
    OBSERVATION_ID = Attribute("observation_id", "INT")
    PRELIMINARY_DIAGNOSIS = Attribute("preliminary_diagnosis", "FLOAT")
    REPORT_DATE = Attribute("preliminary_report_date", "DATE")


class Feature(Notation):
    TABLE_NAME = "features"
    TARGET = Attribute(FinalReport.DIAGNOSIS.name, "TARGET")

    PREGNANCIES = Attribute(ObservationData.PREGNANCIES.name, "NUMERIC")
    GLUCOSE = Attribute(ObservationData.GLUCOSE.name, "NUMERIC")
    BLOOD_PRESSURE = Attribute(ObservationData.BLOOD_PRESSURE.name, "NUMERIC")
    SKIN_THICKNESS = Attribute(ObservationData.SKIN_THICKNESS.name, "NUMERIC")
    INSULIN = Attribute(ObservationData.INSULIN.name, "NUMERIC")
    BMI = Attribute(ObservationData.BMI.name, "NUMERIC")
    DIABETES_PEDIGREE_FUNCTION = Attribute(
        ObservationData.DIABETES_PEDIGREE_FUNCTION.name, "NUMERIC"
    )
    AGE = Attribute("age", "NUMERIC")

    NF0 = Attribute("NF0", "NUMERIC")
    NF1 = Attribute("NF1", "BINARY")
    NF2 = Attribute("NF2", "BINARY")
    NF3 = Attribute("NF3", "BINARY")
    NF4 = Attribute("NF4", "BINARY")
    NF5 = Attribute("NF5", "BINARY")
    NF6 = Attribute("NF6", "BINARY")
    NF7 = Attribute("NF7", "BINARY")
    NF8 = Attribute("NF8", "NUMERIC")
    NF9 = Attribute("NF9", "BINARY")
    NF10 = Attribute("NF10", "BINARY")
    NF11 = Attribute("NF11", "BINARY")
    NF12 = Attribute("NF12", "NUMERIC")
    NF13 = Attribute("NF13", "NUMERIC")
    NF14 = Attribute("NF14", "NUMERIC")
    NF15 = Attribute("NF15", "BINARY")


if __name__ == "__main__":
    print(Feature.get_keys())
