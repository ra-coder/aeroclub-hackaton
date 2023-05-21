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


class CatboostTrainFlow12(AbstractTrainFlow):
    model_name = 'model_012'

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
699:	learn: 0.0800878	test: 0.0890552	best: 0.0890552 (699)	total: 5m 20s	remaining: 0us

bestTest = 0.08905515674
bestIteration = 699

              precision    recall  f1-score   support

       False       0.97      1.00      0.98     57255
        True       0.72      0.27      0.39      2309

    accuracy                           0.97     59564
   macro avg       0.85      0.63      0.69     59564
weighted avg       0.96      0.97      0.96     59564

                              Feature Id  Importances
0                          segments_diff    10.756315
1                     total_flight_ratio     9.331741
2                            to_time_abs     8.412757
3                            price_ratio     5.698203
4                                to_time     5.382197
5                        return_time_abs     4.539465
6                             price_diff     3.968128
7                    return_arrival_hour     3.436345
8                          operator_code     3.242727
9                      total_flight_time     2.791670
10            min_departure_diff_seconds     2.784740
11                           min_to_time     2.748390
12                departure_diff_seconds     2.650324
13                 min_total_flight_time     2.606085
14                                amount     2.329316
15                             min_price     2.322130
16                            isdiscount     2.255714
17                           return_time     2.219875
18                 return_time_abs_ratio     1.885478
19                      is_international     1.804035
20                     to_time_abs_ratio     1.767383
21                       min_return_time     1.713359
22                          segmentcount     1.654595
23                      to_city_timezone     1.598943
24                      one_segment_trip     1.489887
25                    from_city_timezone     1.418867
26                 return_departure_hour     1.379351
27                      to_city_iatacode     1.347066
28                    from_city_iatacode     0.957155
29                        departure_hour     0.942227
30                          arrival_hour     0.722345
31                             isbaggage     0.683740
32                    min_segments_count     0.606537
33                         timezone_diff     0.589727
34                     class_is_business     0.432905
35             has_not_economy_in_policy     0.349062
36  has_intravelpolicy_variant_1_segment     0.305343
37                      class_is_economy     0.244483
38            has_intravelpolicy_variant     0.218195
39                        intravelpolicy     0.143657
40                            round_trip     0.080705
41                     isrefundpermitted     0.075185
42                 client_travellergrade     0.066966
43                   isexchangepermitted     0.025981
44             client_has_travellergrade     0.020701
INFO:root:result table created

"""