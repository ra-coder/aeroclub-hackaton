import logging
import re

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sqlalchemy import Boolean, Column, Float, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import insert

from ranking.catboost.src.lib import AbstractTrainFlow, PreparedResult


class CatboostTrainFlow8(AbstractTrainFlow):
    model_name = 'model_008'

    def prepare_features(
            self,
            limit: int | None = None,
            filter_for_test: bool = False,
            table_prefix: str = 'agent',
    ) -> PreparedResult:
        if filter_for_test:
            assert isinstance(self.sampling_table_name, str)
            assert re.match("^[a-z0-9_]*$", self.sampling_table_name)
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f""" --sql
                    SELECT {table_prefix}_requests.*, {table_prefix}_requests_features.* 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.sampling_table_name} a on {table_prefix}_requests.id = a.id and for_test=False
                    LIMIT {limit};
                """
            else:
                select_query = f"""
                    SELECT {table_prefix}_requests.*, {table_prefix}_requests_features.*  
                    FROM {table_prefix}_requests 
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.sampling_table_name} a on {table_prefix}_requests.id = a.id and for_test=False;
                """
        else:
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f"""
                    SELECT * 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id 
                    LIMIT {limit};
                """
            else:
                select_query = f"""
                    SELECT * 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id 
                """

        train_data = pd.read_sql(select_query, self.db_engine)
        logging.info('Data select done')

        target = ['sentoption']
        exclude_but_keep = ['id']
        num_features = [
            'segmentcount',
            'amount',
            'min_price',
            'price_diff',
            'price_ratio',
            'min_return_time',
            'return_time',
            'return_time_ratio',
            'min_to_time',
            'to_time',
            'to_time_ratio',
            'min_departure_diff_seconds',
            'departure_diff_seconds',
            'client_travellergrade',
            'min_segments_count',
            'segments_diff',
            'departure_hour',
            'arrival_hour',
            'return_departure_hour',
            'return_arrival_hour',
            'total_flight_time',
            'min_total_flight_time',
            'total_flight_ratio',
        ]
        bool_features = [
            'isbaggage',
            'isrefundpermitted',
            'isexchangepermitted',
            'isdiscount',
            'intravelpolicy',
            'has_intravelpolicy_variant',
            'has_intravelpolicy_variant_1_segment',
            'has_not_economy_in_policy',
            'client_has_travellergrade',
            'class_is_economy',
            'class_is_business',
            'round_trip',
        ]
        used = target + num_features + bool_features + exclude_but_keep
        predictors = num_features + bool_features
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
        model = CatBoostClassifier(iterations=700, eval_metric='Logloss', verbose=True)
        # Fit model
        model.fit(X_train, y_train, eval_set=(X_test, y_test))
        self.model = model
        pred = model.predict(X_test)
        bool_result = list(map(lambda rec: rec == 'True', pred))
        print(classification_report(y_test, bool_result))
        print(model.get_feature_importance(prettified=True))

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = CatBoostClassifier()
        from_file_model.load_model(self.model_name)
        self.model = from_file_model

    def apply_model_in_db(self, to_client=False):
        assert self.model is not None
        Session = sessionmaker(bind=self.db_engine)
        with Session() as session:
            table_name = f"{self.model_name}{'_client' if to_client else ''}"
            session.execute(text(f"DROP TABLE if exists {table_name};"))
            session.execute(text(
                f"""
                    CREATE TABLE {table_name} (
                        id int primary key,
                        predict bool,
                        score float
                    );
                """
            ))
            logging.info('result table created')

            data = self.prepare_features(table_prefix='client' if to_client else 'agent')
            ids = data.data[['id']]
            predicts = self.model.predict(data.features_frame)
            predict_scores = self.model.predict_proba(data.features_frame)
            logging.info('predicts calculated')

            Base = declarative_base()

            class PredictTable(Base):
                __tablename__ = table_name
                id = Column(Integer, primary_key=True)
                predict = Column(Boolean)
                score = Column(Float)

            id_with_predict_and_score = list(zip(ids['id'], predicts, predict_scores))
            chunk_size = 10000
            for chunk in range(0, len(id_with_predict_and_score) // chunk_size + 1):
                if chunk * chunk_size < len(id_with_predict_and_score):
                    session.execute(
                        insert(PredictTable),
                        [
                            {'id': id_value, 'predict': predict_value == 'True', 'score': score_value[1]}
                            for id_value, predict_value, score_value
                            in id_with_predict_and_score[chunk * chunk_size:(chunk + 1) * chunk_size]
                        ],
                    )
                logging.info('saved chunk %r', chunk)
            session.commit()
            logging.info('saved to db finished')

"""
bestTest = 0.09128651833
bestIteration = 697

Shrink model to first 698 iterations.
              precision    recall  f1-score   support

       False       0.97      1.00      0.98     57294
        True       0.71      0.24      0.36      2270

    accuracy                           0.97     59564
   macro avg       0.84      0.62      0.67     59564
weighted avg       0.96      0.97      0.96     59564

                              Feature Id  Importances
0                                to_time    11.337816
1                            return_time     9.125723
2                          segments_diff     8.923952
3                          to_time_ratio     7.807428
4                      total_flight_time     7.794444
5                            price_ratio     6.596763
6             min_departure_diff_seconds     4.911682
7                              min_price     4.410211
8                      return_time_ratio     4.233397
9                             price_diff     3.832474
10                                amount     3.699718
11                           min_to_time     3.637219
12                departure_diff_seconds     3.286687
13                 min_total_flight_time     2.527812
14                       min_return_time     2.122713
15                          segmentcount     2.055380
16                            isdiscount     1.947820
17                          arrival_hour     1.684780
18                    total_flight_ratio     1.567418
19                 return_departure_hour     1.558527
20                        departure_hour     1.152631
21                    min_segments_count     1.084092
22                             isbaggage     1.022040
23                     class_is_business     0.790456
24                            round_trip     0.569517
25             has_not_economy_in_policy     0.467520
26                      class_is_economy     0.464265
27                   return_arrival_hour     0.416334
28                        intravelpolicy     0.284712
29                     isrefundpermitted     0.214258
30                 client_travellergrade     0.135840
31  has_intravelpolicy_variant_1_segment     0.132533
32            has_intravelpolicy_variant     0.099815
33                   isexchangepermitted     0.070135
34             client_has_travellergrade     0.033888

"""