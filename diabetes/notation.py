from abc import ABCMeta
from dataclasses import dataclass
from typing import Optional


@dataclass
class Attribute(object):
    name: str
    type: StopIteration


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


if __name__ == "__main__":
    print(Patient.get_keys())
