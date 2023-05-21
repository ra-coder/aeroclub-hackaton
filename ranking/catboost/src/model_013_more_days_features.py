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


class CatboostTrainFlow13(AbstractTrainFlow):
    model_name = 'model_013'

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
                    SELECT 
                        {table_prefix}_requests.*, 
                        {table_prefix}_requests_features.* 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.sampling_table_name} a on {table_prefix}_requests.id = a.id and for_test=False
                    LIMIT {limit};
                """
            else:
                select_query = f"""
                    SELECT 
                        {table_prefix}_requests.*, 
                        {table_prefix}_requests_features.* 
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
            'return_time_abs',
            'return_time_abs_ratio',
            'min_to_time',
            'to_time',
            'to_time_abs',
            'to_time_abs_ratio',
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
            'timezone_diff',
            'to_city_timezone',
            'from_city_timezone',
            'departure_week_day',
            'return_week_day',
            'request_before_x_days',
            'stay_x_days',
            'price_rank',
            'duration_rank',
            'segments_rank',
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
            'one_segment_trip',
            'is_international',
        ]
        text_features = [
            'operator_code',  # enum
            'to_city_iatacode',
            'from_city_iatacode',
        ]
        used = target + num_features + bool_features + exclude_but_keep + text_features
        predictors = num_features + bool_features + text_features
        prepared_data = train_data.drop(columns=[col for col in train_data.columns if col not in used])
        logging.info('Data prepared')

        return PreparedResult(
            data=prepared_data,
            target_column=target,
            features_columns=predictors,
            text_features=text_features,
        )

    def learn(self, prepared_data: PreparedResult):
        X_train, X_test, y_train, y_test = train_test_split(
            prepared_data.features_frame,
            prepared_data.target_frame,
            test_size=0.1,
            random_state=41,
        )
        # Prepare model
        model = CatBoostClassifier(
            iterations=700,
            eval_metric='Logloss',
            verbose=True,
            cat_features=prepared_data.text_features,
        )
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
699:	learn: 0.0764280	test: 0.0861641	best: 0.0861570 (698)	total: 5m 25s	remaining: 0us

bestTest = 0.0861569775
bestIteration = 698

Shrink model to first 699 iterations.
              precision    recall  f1-score   support

       False       0.97      1.00      0.98     57255
        True       0.71      0.28      0.41      2309

    accuracy                           0.97     59564
   macro avg       0.84      0.64      0.70     59564
weighted avg       0.96      0.97      0.96     59564

                              Feature Id  Importances
0                          segments_rank    16.775845
1                             price_rank    10.934673
2                     total_flight_ratio     9.149779
3                            to_time_abs     7.928833
4                                to_time     3.531330
5                            return_time     3.195269
6                      total_flight_time     2.860620
7                       to_city_timezone     2.673244
8                          segments_diff     2.663037
9                                 amount     2.659688
10                           price_ratio     2.589100
11                         operator_code     2.556295
12                          segmentcount     2.245156
13                            price_diff     2.150353
14                         duration_rank     2.051903
15                            isdiscount     1.994937
16                           min_to_time     1.965882
17                departure_diff_seconds     1.887002
18            min_departure_diff_seconds     1.764259
19                 return_time_abs_ratio     1.613287
20                 min_total_flight_time     1.404441
21                     to_time_abs_ratio     1.337058
22                 request_before_x_days     1.291849
23                             min_price     1.242133
24                            round_trip     1.047811
25                    from_city_timezone     1.043525
26                        departure_hour     0.927090
27                      is_international     0.795932
28                 return_departure_hour     0.759134
29                       return_time_abs     0.689093
30                       min_return_time     0.688056
31                      to_city_iatacode     0.672779
32                          arrival_hour     0.648221
33                   return_arrival_hour     0.545613
34                      class_is_economy     0.535181
35                             isbaggage     0.492501
36                         timezone_diff     0.427114
37                           stay_x_days     0.341349
38                    from_city_iatacode     0.314106
39                    departure_week_day     0.291409
40                    min_segments_count     0.237485
41                      one_segment_trip     0.179376
42                     class_is_business     0.167903
43                       return_week_day     0.147250
44             has_not_economy_in_policy     0.145316
45                     isrefundpermitted     0.104812
46                 client_travellergrade     0.104677
47            has_intravelpolicy_variant     0.089993
48                   isexchangepermitted     0.072467
49                        intravelpolicy     0.028080
50             client_has_travellergrade     0.019564
51  has_intravelpolicy_variant_1_segment     0.018189
INFO:root:result table created

"""