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


class CatboostTrainFlow14(AbstractTrainFlow):
    model_name = 'model_014'
    support_model_scores_table = 'preprocess_scores_support_model_003_on_013'

    def prepare_features(
            self,
            limit: int | None = None,
            filter_for_test: bool = False,
            table_prefix: str = 'agent',
    ) -> PreparedResult:
        support_model_table_suffix = '_final_test' if table_prefix == 'final_test' else ''
        support_model_scores_table = self.support_model_scores_table + support_model_table_suffix
        logging.info(support_model_scores_table)
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
                    join {support_model_scores_table} 
                        on {support_model_scores_table}.id = {table_prefix}_requests.id 
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
                    join {support_model_scores_table} 
                        on {support_model_scores_table}.id = {table_prefix}_requests.id                         
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
                    join {support_model_scores_table}
                        on {support_model_scores_table}.id = {table_prefix}_requests.id                         
                    LIMIT {limit};                    
                """
            else:
                select_query = f"""
                    SELECT * 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {support_model_scores_table}
                        on {support_model_scores_table}.id = {table_prefix}_requests.id                         
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
            'score',  # from support
            'rank',  # from support
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
            'predict',  # from support
            'in_top5_rank',  # from support
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
            logging.info(table_prefix)
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
            for chunk in range(0, (len(id_with_predict_and_score) // chunk_size) + 1):
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

bestTest = 0.09027343359
bestIteration = 999

              precision    recall  f1-score   support

       False       0.97      1.00      0.98     61132
        True       0.73      0.30      0.43      2609

    accuracy                           0.97     63741
   macro avg       0.85      0.65      0.70     63741
weighted avg       0.96      0.97      0.96     63741

                              Feature Id  Importances
0                                   rank    12.512007
1                          segments_diff     9.999372
2                            to_time_abs     7.060817
3                     total_flight_ratio     4.543546
4                          segments_rank     4.332552
5                                to_time     3.913388
6                            return_time     3.606749
7                        return_time_abs     3.378822
8                      total_flight_time     3.206156
9                      to_time_abs_ratio     2.989674
10                           price_ratio     2.770638
11                                amount     2.464414
12                         operator_code     2.445149
13                            price_rank     2.323272
14                            price_diff     2.261286
15                      to_city_timezone     1.868417
16            min_departure_diff_seconds     1.860100
17                departure_diff_seconds     1.851020
18                                 score     1.725445
19                 min_total_flight_time     1.715538
20                   return_arrival_hour     1.565469
21                           min_to_time     1.561953
22                      is_international     1.468623
23                 request_before_x_days     1.347072
24                            isdiscount     1.312239
25                          in_top5_rank     1.252590
26                    from_city_timezone     1.202768
27                             min_price     1.138011
28                    min_segments_count     1.084805
29                         duration_rank     1.027922
30                      to_city_iatacode     1.015580
31                           stay_x_days     0.960923
32                        departure_hour     0.869060
33                          arrival_hour     0.790995
34                       min_return_time     0.673218
35                         timezone_diff     0.604658
36                     class_is_business     0.537190
37                          segmentcount     0.531706
38                      one_segment_trip     0.521292
39                    from_city_iatacode     0.509383
40                 return_time_abs_ratio     0.466736
41                 return_departure_hour     0.453982
42                             isbaggage     0.411762
43                            round_trip     0.372091
44                    departure_week_day     0.325615
45             has_not_economy_in_policy     0.234758
46                      class_is_economy     0.203518
47                       return_week_day     0.164188
48                        intravelpolicy     0.156291
49                     isrefundpermitted     0.099343
50                   isexchangepermitted     0.088788
51  has_intravelpolicy_variant_1_segment     0.071205
52                 client_travellergrade     0.060600
53            has_intravelpolicy_variant     0.035559
54                               predict     0.031604
55             client_has_travellergrade     0.020140
INFO:root:result table created

"""