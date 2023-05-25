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


class SupportModelCatboost3(AbstractTrainFlow):
    model_name = 'support_model_003_on_013'

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
            iterations=1000,
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

    def apply_model_in_db(self, to_client=False, to_final_test=False):
        assert self.model is not None
        Session = sessionmaker(bind=self.db_engine)
        with Session() as session:
            suffix = '_client' if to_client else ('_final_test' if to_final_test else '')
            table_name = f"{self.model_name}{suffix}"
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

            table_prefix = 'client' if to_client else ('final_test' if to_final_test else 'agent')
            data = self.prepare_features(table_prefix=table_prefix)
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
999:	learn: 0.0345829	test: 0.0416710	best: 0.0416710 (999)	total: 10m 4s	remaining: 0us

bestTest = 0.04167103234
bestIteration = 999

              precision    recall  f1-score   support

       False       0.99      1.00      0.99     96308
        True       0.85      0.64      0.73      2892

    accuracy                           0.99     99200
   macro avg       0.92      0.82      0.86     99200
weighted avg       0.99      0.99      0.99     99200

                              Feature Id  Importances
0                 departure_diff_seconds    10.957859
1                      total_flight_time     9.865505
2                             price_rank     9.129301
3                                to_time     7.236476
4                              isbaggage     4.854404
5                          duration_rank     4.806354
6                             isdiscount     4.475681
7                            min_to_time     3.926527
8                  return_departure_hour     3.820309
9                          operator_code     3.788854
10            min_departure_diff_seconds     3.593859
11                        departure_hour     3.299205
12                   return_arrival_hour     3.221935
13                                amount     3.206555
14                    total_flight_ratio     2.658398
15                           price_ratio     2.543409
16                          arrival_hour     2.198640
17                            price_diff     2.198525
18                     to_time_abs_ratio     1.499476
19                 request_before_x_days     1.330025
20                        intravelpolicy     1.300832
21                 min_total_flight_time     1.113630
22                           to_time_abs     1.001631
23                 return_time_abs_ratio     0.997813
24                             min_price     0.981632
25                           return_time     0.853271
26                       min_return_time     0.722188
27                          segmentcount     0.717194
28                    departure_week_day     0.696774
29                            round_trip     0.551466
30                         timezone_diff     0.525603
31                       return_time_abs     0.481930
32                       return_week_day     0.360825
33                           stay_x_days     0.348055
34                         segments_rank     0.318631
35                     isrefundpermitted     0.143982
36             has_not_economy_in_policy     0.062962
37            has_intravelpolicy_variant     0.062410
38                      to_city_iatacode     0.044403
39                   isexchangepermitted     0.038595
40  has_intravelpolicy_variant_1_segment     0.025157
41                     class_is_business     0.022165
42                      to_city_timezone     0.013722
43                      is_international     0.003450
44                    min_segments_count     0.000380
45                 client_travellergrade     0.000000
46                         segments_diff     0.000000
47                    from_city_timezone     0.000000
48             client_has_travellergrade     0.000000
49                      class_is_economy     0.000000
50                      one_segment_trip     0.000000
51                    from_city_iatacode     0.000000
INFO:root:result table created
"""
