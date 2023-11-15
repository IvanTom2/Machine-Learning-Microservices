import sys
import asyncpg
import asyncio
import pandas as pd
from pathlib import Path
from asyncpg import Pool, Connection, Record
from asyncio import Lock
from typing import Any
from functools import wraps
from datetime import datetime, timedelta


sys.path.append(str(Path(__file__).parent.parent))
from common import DataBaseInitTemplate, config
from util import (
    PreviousDataGenerator,
    PatientObject,
    ObservationObject,
    ObservationData,
    ObservationDataObject,
    FinalReportObject,
    PreliminaryReportObject,
)
from notation import Patient, Observation, FinalReport, PreliminaryReport


async def get_observation_date():
    """STUB METHOD"""
    date = datetime.today().date() + timedelta(days=1)
    return date


class DataBaseInit(DataBaseInitTemplate):
    def __init__(self, *args) -> None:
        super().__init__(*args)

    async def create_patient_relation(
        self,
        connection: Connection,
    ) -> None:
        query = f"""
                CREATE TABLE {Patient.TABLE_NAME} (
                {Patient.LAST_NAME.name} {Patient.LAST_NAME.type} NOT NULL,
                {Patient.FIRST_NAME.name} {Patient.FIRST_NAME.type} NOT NULL,
                {Patient.PATRONYMIC.name} {Patient.PATRONYMIC.type},
                {Patient.BIRTHDAY.name} {Patient.BIRTHDAY.type} NOT NULL,
                {Patient.PHONE_NUMBER.name} {Patient.PHONE_NUMBER.type} NOT NULL,
                {Patient.GENDER.name} {Patient.GENDER.type} NOT NULL,
                {Patient.PATIENT_ID.name} {Patient.PATIENT_ID.type},
                PRIMARY KEY ({Patient.PATIENT_ID.name}),
                UNIQUE ({Patient.FIRST_NAME.name}, {Patient.LAST_NAME.name}, 
                        {Patient.PATRONYMIC.name}, 
                        {Patient.BIRTHDAY.name}, {Patient.PHONE_NUMBER.name})
                );
                """
        await self.create_new_table(connection, query)

    async def create_observation_relation(
        self,
        connection: Connection,
    ) -> None:
        query = f"""
                CREATE TABLE {Observation.TABLE_NAME} (
                {Observation.PATIENT_ID.name} {Observation.PATIENT_ID.type} REFERENCES 
                    {Patient.TABLE_NAME} ({Patient.PATIENT_ID.name}),
                {Observation.OBSERVATION_DATE.name} {Observation.OBSERVATION_DATE.type},
                {Observation.OBSERVATION_ID.name} {Observation.OBSERVATION_ID.type},
                PRIMARY KEY ({Observation.OBSERVATION_ID.name})
                );
                """
        await self.create_new_table(connection, query)

    async def create_observation_data_relation(
        self,
        connection: Connection,
    ) -> None:
        query = f"""
                CREATE TABLE {ObservationData.TABLE_NAME} (
                {ObservationData.OBSERVATION_ID.name} {ObservationData.OBSERVATION_ID.type} REFERENCES 
                    {Observation.TABLE_NAME} ({Observation.OBSERVATION_ID.name}),
                {ObservationData.PREGNANCIES.name} {ObservationData.PREGNANCIES.type},
                {ObservationData.GLUCOSE.name} {ObservationData.GLUCOSE.type},
                {ObservationData.BLOOD_PRESSURE.name} {ObservationData.BLOOD_PRESSURE.type},
                {ObservationData.SKIN_THICKNESS.name} {ObservationData.SKIN_THICKNESS.type},
                {ObservationData.INSULIN.name} {ObservationData.INSULIN.type},
                {ObservationData.BMI.name} {ObservationData.BMI.type},
                {ObservationData.DIABETES_PEDIGREE_FUNCTION.name} {ObservationData.DIABETES_PEDIGREE_FUNCTION.type}
                );
                """
        await self.create_new_table(connection, query)

    async def create_final_report_relation(
        self,
        connection: Connection,
    ) -> None:
        query = f"""
                CREATE TABLE {FinalReport.TABLE_NAME} (
                {FinalReport.OBSERVATION_ID.name} {FinalReport.OBSERVATION_ID.type} 
                    REFERENCES {Observation.TABLE_NAME} ({Observation.OBSERVATION_ID.name}),
                {FinalReport.DIAGNOSIS.name} {FinalReport.DIAGNOSIS.type},
                {FinalReport.REPORT_DATE.name} {FinalReport.REPORT_DATE.type}
                );
                """
        await self.create_new_table(connection, query)

    async def create_preliminary_report_relation(
        self,
        connection: Connection,
    ) -> None:
        query = f"""
                CREATE TABLE {PreliminaryReport.TABLE_NAME} (
                {PreliminaryReport.OBSERVATION_ID.name} {PreliminaryReport.OBSERVATION_ID.type} 
                    REFERENCES {Observation.TABLE_NAME} ({Observation.OBSERVATION_ID.name}),
                {PreliminaryReport.PRELIMINARY_DIAGNOSIS.name} {PreliminaryReport.PRELIMINARY_DIAGNOSIS.type},
                {PreliminaryReport.REPORT_DATE.name} {PreliminaryReport.REPORT_DATE.type}
                );
                """
        await self.create_new_table(connection, query)

    async def init_tables(self) -> None:
        try:
            await self._database_initiation()

            connection = await self._user_conn()
            await self.create_patient_relation(connection)
            await self.create_observation_relation(connection)
            await self.create_observation_data_relation(connection)
            await self.create_final_report_relation(connection)
            await self.create_preliminary_report_relation(connection)

        finally:
            if not connection.is_closed():
                await connection.close()


class DataBaseInterface(object):
    def __init__(
        self,
        db_connections_count: int,
    ) -> None:
        self.db_connections_count = db_connections_count

    async def create_pool(self):
        print("CREATE DB CONNECTIONS POOL")
        self._pool: Pool = await asyncpg.create_pool(
            host=config["DB_HOST"],
            port=config["DB_PORT"],
            user=config["DB_USERNAME"],
            password=config["DB_PASSWORD"],
            database=config["DB_DIABETES"],
            min_size=self.db_connections_count,
            max_size=self.db_connections_count,
        )

    async def insert_patient_data(
        self,
        patient: PatientObject,
    ) -> int:
        query = f"""
                    INSERT INTO {Patient.TABLE_NAME} (
                        {Patient.LAST_NAME.name}, 
                        {Patient.FIRST_NAME.name}, 
                        {Patient.PATRONYMIC.name}, 
                        {Patient.BIRTHDAY.name},
                        {Patient.PHONE_NUMBER.name}, 
                        {Patient.GENDER.name}
                    )
                    VALUES (
                        '{patient[Patient.LAST_NAME.name]}', 
                        '{patient[Patient.FIRST_NAME.name]}',
                        '{patient[Patient.PATRONYMIC.name]}', 
                        '{patient[Patient.BIRTHDAY.name]}',
                        '{patient[Patient.PHONE_NUMBER.name]}', 
                        '{patient[Patient.GENDER.name]}'
                    )
                    RETURNING {Patient.PATIENT_ID.name};
                """

        async with self._pool.acquire() as connection:
            connection: Connection

            patient_id = await connection.fetch(query)
            patient_id = patient_id[0][Patient.PATIENT_ID.name]

        return patient_id

    async def schedule_observation(
        self,
        patient: PatientObject,
        settled_date: datetime = None,
    ) -> int:
        if not settled_date:
            settled_date = await get_observation_date()

        query = f"""
                    INSERT INTO {Observation.TABLE_NAME} (
                        {Observation.PATIENT_ID.name}, 
                        {Observation.OBSERVATION_DATE.name}
                    ) 
                    VALUES (
                        '{patient[Patient.PATIENT_ID.name]}',
                        '{settled_date}'
                    )
                    RETURNING {Observation.OBSERVATION_ID.name};
                """

        async with self._pool.acquire() as connection:
            connection: Connection
            observation_id = await connection.fetch(query)
            observation_id = observation_id[0][Observation.OBSERVATION_ID.name]

        return observation_id

    async def insert_observation_data(
        self,
        observation: ObservationDataObject,
    ) -> None:
        query = f"""
                INSERT INTO {ObservationData.TABLE_NAME} (
                    {ObservationData.OBSERVATION_ID.name},
                    {ObservationData.PREGNANCIES.name},
                    {ObservationData.GLUCOSE.name},
                    {ObservationData.BLOOD_PRESSURE.name},
                    {ObservationData.SKIN_THICKNESS.name},
                    {ObservationData.INSULIN.name},
                    {ObservationData.BMI.name},
                    {ObservationData.DIABETES_PEDIGREE_FUNCTION.name}
                )
                VALUES (
                    {observation[ObservationData.OBSERVATION_ID.name]},
                    {observation[ObservationData.PREGNANCIES.name]},
                    {observation[ObservationData.GLUCOSE.name]},
                    {observation[ObservationData.BLOOD_PRESSURE.name]},
                    {observation[ObservationData.SKIN_THICKNESS.name]},
                    {observation[ObservationData.INSULIN.name]},
                    {observation[ObservationData.BMI.name]},
                    {observation[ObservationData.DIABETES_PEDIGREE_FUNCTION.name]}
                );
                """

        async with self._pool.acquire() as connection:
            connection: Connection
            await connection.execute(query)

    async def insert_final_report_data(
        self,
        report: FinalReportObject,
    ) -> None:
        query = f"""
                INSERT INTO {FinalReport.TABLE_NAME} (
                    {FinalReport.OBSERVATION_ID.name},
                    {FinalReport.DIAGNOSIS.name},
                    {FinalReport.REPORT_DATE.name}
                )
                VALUES (
                    '{report[FinalReport.OBSERVATION_ID.name]}',
                    '{report[FinalReport.DIAGNOSIS.name]}',
                    '{report[FinalReport.REPORT_DATE.name]}'
                );
                """

        async with self._pool.acquire() as connection:
            connection: Connection
            await connection.execute(query)

    async def insert_preliminary_report_data(
        self,
        report: PreliminaryReportObject,
    ) -> None:
        query = f"""
                INSERT INTO {PreliminaryReport.TABLE_NAME} (
                    {PreliminaryReport.OBSERVATION_ID.name},
                    {PreliminaryReport.PRELIMINARY_DIAGNOSIS.name},
                    {PreliminaryReport.REPORT_DATE.name}
                )
                VALUES (
                    '{report[PreliminaryReport.OBSERVATION_ID.name]}',
                    '{report[PreliminaryReport.PRELIMINARY_DIAGNOSIS.name]}',
                    '{report[PreliminaryReport.REPORT_DATE.name]}'
                );
                """

        async with self._pool.acquire() as connection:
            connection: Connection
            await connection.execute(query)

    async def get_data_to_train(self):
        query = f"""
                SELECT 
                {Patient.TABLE_NAME}.{Patient.PATIENT_ID.name},
                {Patient.TABLE_NAME}.{Patient.BIRTHDAY.name},
                {Observation.TABLE_NAME}.{Observation.OBSERVATION_ID.name},
                {Observation.TABLE_NAME}.{Observation.OBSERVATION_DATE.name},
                {ObservationData.TABLE_NAME}.{ObservationData.PREGNANCIES.name},
                {ObservationData.TABLE_NAME}.{ObservationData.GLUCOSE.name},
                {ObservationData.TABLE_NAME}.{ObservationData.BLOOD_PRESSURE.name},
                {ObservationData.TABLE_NAME}.{ObservationData.SKIN_THICKNESS.name},
                {ObservationData.TABLE_NAME}.{ObservationData.INSULIN.name},
                {ObservationData.TABLE_NAME}.{ObservationData.BMI.name},
                {ObservationData.TABLE_NAME}.{ObservationData.DIABETES_PEDIGREE_FUNCTION.name},
                {FinalReport.TABLE_NAME}.{FinalReport.DIAGNOSIS.name}
                FROM {Patient.TABLE_NAME} 
                    LEFT JOIN {Observation.TABLE_NAME} 
                        ON {Patient.TABLE_NAME}.{Patient.PATIENT_ID.name} 
                        = {Observation.TABLE_NAME}.{Observation.PATIENT_ID.name}
                    LEFT JOIN {ObservationData.TABLE_NAME} 
                        ON {Observation.TABLE_NAME}.{Observation.OBSERVATION_ID.name} 
                        = {ObservationData.TABLE_NAME}.{ObservationData.OBSERVATION_ID.name}
                    LEFT JOIN {FinalReport.TABLE_NAME}
                        ON {Observation.TABLE_NAME}.{Observation.OBSERVATION_ID.name} 
                        = {FinalReport.TABLE_NAME}.{FinalReport.OBSERVATION_ID.name}
                WHERE {FinalReport.TABLE_NAME}.{FinalReport.DIAGNOSIS.name} IS NOT NULL;
                """

        async with self._pool.acquire() as connection:
            connection: Connection
            data = await connection.fetch(query)

        return data


class DataBaseFiller(object):
    async def insert_patients(
        self,
        patients: list[PatientObject],
    ) -> list[int]:
        tasks = [
            asyncio.create_task(self.DB.insert_patient_data(patient))
            for patient in patients
        ]

        patients_ids = await asyncio.gather(*tasks)
        return patients_ids

    async def schedule_all(
        self,
        patients: list[PatientObject],
        observations: list[ObservationObject],
    ) -> list[int]:
        tasks = [
            asyncio.create_task(
                self.DB.schedule_observation(
                    patients[index],
                    observations[index][Observation.OBSERVATION_DATE.name],
                )
            )
            for index in range(len(patients))
        ]

        observations_ids = await asyncio.gather(*tasks)
        return observations_ids

    async def insert_observations_data(
        self,
        observations_data: list[ObservationDataObject],
    ) -> None:
        tasks = [
            asyncio.create_task(self.DB.insert_observation_data(observation_data))
            for observation_data in observations_data
        ]

        await asyncio.gather(*tasks)

    async def insert_reports(
        self,
        reports: list[FinalReportObject],
    ) -> None:
        tasks = [
            asyncio.create_task(self.DB.insert_final_report_data(report))
            for report in reports
        ]

        await asyncio.gather(*tasks)

    async def main(
        self,
        patients: list[PatientObject],
        observations: list[ObservationObject],
        observations_data: list[ObservationDataObject],
        reports: list[FinalReportObject],
    ):
        self.DB = DataBaseInterface(6)
        await self.DB.create_pool()

        patients_ids = await self.insert_patients(patients)
        for index in range(len(patients)):
            patients[index][Patient.PATIENT_ID.name] = patients_ids[index]

        observations_ids = await self.schedule_all(patients, observations)
        for index in range(len(observations_ids)):
            observations_data[index][
                ObservationData.OBSERVATION_ID.name
            ] = observations_ids[index]

            reports[index][FinalReport.OBSERVATION_ID.name] = observations_ids[index]

        await self.insert_observations_data(observations_data)
        await self.insert_reports(reports)

    def fill(self):
        DataGen = PreviousDataGenerator()
        data = DataGen.get_data()

        patients = PatientObject.from_dataframe(data)
        observations = ObservationObject.from_dataframe(data)
        observations_data = ObservationDataObject.from_dataframe(data)
        reports = FinalReportObject.from_dataframe(data)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self.main(
                patients,
                observations,
                observations_data,
                reports,
            )
        )


async def test():
    DB = DataBaseInterface(6)
    await DB.create_pool()

    DGen = PreviousDataGenerator()
    data = DGen.get_data()

    patients = PatientObject.from_dataframe(data)
    observations = ObservationObject.from_dataframe(data)
    observations_data = ObservationDataObject.from_dataframe(data)
    reports = FinalReportObject.from_dataframe(data)

    patient = patients[0]
    observation = observations[0]
    observations_data = observations_data[0]
    report = reports[0]

    # hand_data = {
    #     Patient.FIRST_NAME.name: "Ivan",
    #     Patient.LAST_NAME.name: "Tomilov",
    #     Patient.PATRONYMIC.name: "Vladimirovich",
    #     Patient.BIRTHDAY.name: datetime(1999, 6, 26),
    #     Patient.PHONE_NUMBER.name: "89012207967",
    #     Patient.GENDER.name: "m",
    #     Patient.PATIENT_ID.name: 1,
    # }
    # patients = [PatientObject(hand_data)]

    patient_id = await DB.insert_patient_data(patient)
    patient[Patient.PATIENT_ID.name] = patient_id

    obs_id = await DB.schedule_observation(
        patient,
        observation[Observation.OBSERVATION_DATE.name],
    )
    observation[Observation.OBSERVATION_ID.name] = obs_id

    observations_data[ObservationData.OBSERVATION_ID.name] = obs_id
    report[FinalReport.OBSERVATION_ID.name] = obs_id

    await DB.insert_observation_data(observations_data)
    await DB.insert_final_report_data(report)


if __name__ == "__main__":
    DBI = DataBaseInit(config["DB_DIABETES"])
    asyncio.run(DBI.init_tables())

    # asyncio.run(test())

    filler = DataBaseFiller()
    filler.fill()

    # asyncio.run(get_observation_date())
