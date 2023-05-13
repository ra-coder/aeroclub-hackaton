import logging
import re

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split
from sqlalchemy import Boolean, Column, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import insert

from ranking.catboost.src.lib import AbstractTrainFlow, PreparedResult


class NaiveCatboostTrainFlow(AbstractTrainFlow):
    model_name = 'model_001_naive_catboost'

    def prepare_features(self, limit: int | None = None, filter_for_test: bool = False) -> PreparedResult:
        if filter_for_test:
            assert isinstance(self.sampling_table_name, str)
            assert re.match("^[a-z0-9_]*$", self.sampling_table_name)
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f""" --sql
                    SELECT agent_requests.* 
                    FROM agent_requests
                    join agent_requests_sample_001 a on agent_requests.id = a.id and for_test=False
                    LIMIT {limit};
                """
            else:
                select_query = """
                    SELECT agent_requests.* 
                    FROM agent_requests 
                    join agent_requests_sample_001 a on agent_requests.id = a.id and for_test=False;
                """
        else:
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f"SELECT * FROM agent_requests LIMIT {limit};"
            else:
                select_query = "SELECT * FROM agent_requests;"

        train_data = pd.read_sql(select_query, self.db_engine)
        logging.info('Data select done')

        target = ['sentoption']
        exclude_but_keep = ['id']
        category_features = []  # ['travellergrade', 'class']
        num_features = ['segmentcount', 'amount']
        bool_features = [
            'isbaggage',
            'isrefundpermitted',
            'isexchangepermitted',
            'isdiscount',
            'intravelpolicy',
        ]
        used = target + category_features + num_features + bool_features + exclude_but_keep
        predictors = category_features + num_features + bool_features
        prepared_data = train_data.drop(columns=[col for col in train_data.columns if col not in used])
        logging.info('Data prepared')

        return PreparedResult(data=prepared_data, target_column=target, features_columns=predictors)

    def learn(self, prepared_data: PreparedResult):
        X_train, X_test, y_train, y_test = train_test_split(
            prepared_data.features_frame,
            prepared_data.target_frame,
            test_size=0.1,
            random_state=41,
        )
        # Prepare model
        model = CatBoostClassifier(iterations=50, eval_metric='Accuracy', verbose=True)
        # Fit model
        model.fit(X_train, y_train, eval_set=(X_test, y_test))
        self.model = model

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = CatBoostClassifier()
        from_file_model.load_model(self.model_name)
        self.model = from_file_model

    def apply_model_in_db(self):
        assert self.model is not None
        Session = sessionmaker(bind=self.db_engine)
        with Session() as session:

            session.execute(text(f"DROP TABLE if exists {self.model_name};"))
            session.execute(text(
                f"""
                    CREATE TABLE {self.model_name} (
                        id int primary key,
                        predict bool
                    );
                """
            ))
            logging.info('result table created')

            data = self.prepare_features()
            ids = data.data[['id']]
            predicts = self.model.predict(data.features_frame)
            logging.info('predicts calculated')

            Base = declarative_base()

            class PredictTable(Base):
                __tablename__ = self.model_name
                id = Column(Integer, primary_key=True)
                predict = Column(Boolean)

            id_with_predict = list(zip(ids['id'], predicts))
            chunk_size = 10000
            for chunk in range(0, len(id_with_predict) // chunk_size + 1):
                if chunk * chunk_size < len(id_with_predict):
                    session.execute(
                        insert(PredictTable),
                        [
                            {'id': id_value, 'predict': predict_value == 'True'}
                            for id_value, predict_value
                            in id_with_predict[chunk * chunk_size:(chunk + 1) * chunk_size]
                        ],
                    )
                logging.info('saved chunk %r', chunk)
            session.commit()
            logging.info('saved to db finished')
