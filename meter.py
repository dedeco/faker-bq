import datetime
from copy import deepcopy
from typing import List

import pandas
import pandas as pd
import numpy as np

from google.cloud import bigquery
from dataclasses import field, dataclass
import random

from faker import Faker

faker = Faker()
client = bigquery.Client()


@dataclass
class MeterLoad:
    meter_id: int = field(default_factory=lambda: random.randint(11111111, 99999999))
    manufacturer: str = field(default_factory=lambda: faker.bothify(text='?##').upper())
    service_name: str = 'E'
    source: str = field(default_factory=lambda: random.choice(["Web", "App"]))
    loaddate: datetime.date = field(
        default_factory=lambda:
        faker.date_time_between(start_date='-7d', end_date='now')
    )


@dataclass
class MeterReadingsLoad:
    readings_id: int = field(default_factory=lambda: random.randint(11111111, 99999999))
    meter_id: int = 1
    readings: float = field(default_factory=lambda: round(faker.pyfloat(), 1))
    readings_time: int = field(default_factory=lambda: faker.iso8601())
    source: str = field(default_factory=lambda: random.choice(["Web", "App"]))
    loaddate: datetime.date = field(
        default_factory=lambda:
        faker.date_time_between(start_date='-7d', end_date='now')
    )


def load_into_bq(table_id: str, data: pandas.DataFrame, schema: List):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )

    job.result()

    table = client.get_table(table_id)

    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


if __name__ == "__main__":

    samples = [MeterLoad().__dict__ for _ in range(0, 10000)]

    df_sample = pd.json_normalize(samples)
    df_sample['loaddate'] = df_sample['loaddate'].astype('datetime64[ns]')
    print(df_sample.head(2), df_sample.info())

    readings = []
    for s in samples:
        for _ in range(0, 5):
            sample_readings = MeterReadingsLoad()
            sample_readings.meter_id = s['meter_id']
            readings.append(sample_readings.__dict__)

    df_readings = pd.json_normalize(readings)
    df_readings['loaddate'] = df_readings['loaddate'].astype('datetime64[ns]')
    df_readings['readings_time'] = pd.to_datetime(df_readings['readings_time']).view(int) // 10 ** 9

    print(df_readings.head(2), df_readings.info())

    load_into_bq(
        table_id="andresousa-demo.staging.meter_load",
        data=df_sample,
        schema=[
            bigquery.SchemaField("meter_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("manufacturer", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("service_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("source", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("loaddate", bigquery.enums.SqlTypeNames.DATE),

        ]
    )

    load_into_bq(
        table_id="andresousa-demo.staging.meter_readings_load",
        data=df_readings,
        schema=[
            bigquery.SchemaField("readings_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("meter_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("readings", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("readings_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("source", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("loaddate", bigquery.enums.SqlTypeNames.DATE),
        ]
    )
