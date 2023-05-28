import logging
from collections import defaultdict

import pandas as pd
from sqlalchemy import Engine

logging.getLogger().setLevel(logging.INFO)


class PreparedResult:
    def __init__(
            self,
            data: pd.DataFrame,
            target_column: list[str],
            features_columns: list[str],
            text_features: list[str] | None = None,
    ):
        self.data: pd.DataFrame = data
        self.target_column: list[str] = target_column
        self.features_columns: list[str] = features_columns
        self.text_features: list[str] = text_features or []

    @property
    def features_frame(self):
        return self.data[self.features_columns]

    @property
    def target_frame(self):
        return self.data[self.target_column]

    @property
    def request_id_frame(self):
        return self.data[['requestid']]

    def make_pairs(self):
        result = []
        request_stat = {}
        for pos, rec in self.data.iterrows():
            request_stat.setdefault(
                rec['requestid'],
                {
                    'recs_from_sent_flights': {},
                    'trash_recs': [],
                }
            )
            if rec['sentoption_flight'] is False:
                request_stat[rec['requestid']]['trash_recs'].append(pos)
            else:
                request_stat[rec['requestid']]['recs_from_sent_flights'].setdefault(
                    rec['fligtoption'],
                    defaultdict(list),
                )
                variants = request_stat[rec['requestid']]['recs_from_sent_flights'][rec['fligtoption']]
                if rec['sentoption_fixed'] is True:
                    variants['sentoption'].append(pos)
                else:
                    variants['miss'].append(pos)
        import pprint
        for request_info in request_stat.values():
            for sentoption_flight_info in request_info['recs_from_sent_flights'].values():
                for win_pos in sentoption_flight_info.get('sentoption', []) :
                    for loos_pos in sentoption_flight_info.get('miss', []):
                        result.append([win_pos, loos_pos])
                    for trash_pos in request_info['trash_recs']:
                        result.append([win_pos, trash_pos])
        return result


class AbstractTrainFlow:
    model_name: str

    def __init__(self, db_engine: Engine, sampling_table_name: str | None = None):
        self.db_engine: Engine = db_engine
        self.sampling_table_name = sampling_table_name
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

    def apply_model(self, prepared_data: PreparedResult):
        raise NotImplementedError

    def apply_model_in_db(self):
        raise NotImplementedError
