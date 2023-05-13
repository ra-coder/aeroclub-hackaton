import logging

import pandas as pd
from sqlalchemy import Engine

logging.getLogger().setLevel(logging.INFO)


class PreparedResult:
    def __init__(self, data: pd.DataFrame, target_column: list[str], features_columns: list[str]):
        self.data: pd.DataFrame = data
        self.target_column: list[str] = target_column
        self.features_columns: list[str] = features_columns

    @property
    def features_frame(self):
        return self.data[self.features_columns]

    @property
    def target_frame(self):
        return self.data[self.target_column]


class AbstractTrainFlow:
    model_name: str

    def __init__(self, db_engine: Engine):
        self.db_engine: Engine = db_engine
        self.model = None

    def prepare_features(self, limit: int | None = None) -> PreparedResult:
        """
        do operations to prepare and extract features for learn

        :param limit: limit data size to read from db for smock check learn run
        :return:
        """
        raise NotImplementedError

    def learn(self, prepared_data: PreparedResult):
        raise NotImplementedError

    def save_model(self):
        raise NotImplementedError

    def load_model(self):
        raise NotImplementedError

    def apply_model_in_db(self, res_table_name: str = 'test_scored'):
        raise NotImplementedError
