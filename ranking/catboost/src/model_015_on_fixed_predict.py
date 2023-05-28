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


class CatboostTrainFlow15(AbstractTrainFlow):
    model_name = 'model_015'

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

        target = ['sentoption_fixed']
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
            'price_leg_rank',
            'duration_rank',
            'segments_rank',
            'flights_variability',
            'departuredate_variability',
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
            'same_options_best_price',
        ]
        text_features = [
            'operator_code',  # enum
            'to_city_iatacode',
            'from_city_iatacode',
            'client'
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
699:	learn: 0.0629045	test: 0.0729818	best: 0.0729818 (699)	total: 6m 8s	remaining: 0us

bestTest = 0.07298179504
bestIteration = 699

              precision    recall  f1-score   support

       False       0.98      0.99      0.98     57017
        True       0.75      0.47      0.58      2547

    accuracy                           0.97     59564
   macro avg       0.86      0.73      0.78     59564
weighted avg       0.97      0.97      0.97     59564

                              Feature Id  Importances
0                same_options_best_price    43.099741
1                      to_time_abs_ratio     5.320610
2                         price_leg_rank     4.622273
3                            to_time_abs     4.313195
4                     total_flight_ratio     3.030448
5                          segments_rank     2.870764
6                      total_flight_time     2.698091
7                          operator_code     2.144893
8                                to_time     2.137647
9                        min_return_time     1.964371
10                   flights_variability     1.865948
11                departure_diff_seconds     1.472479
12             departuredate_variability     1.459481
13                       return_time_abs     1.411518
14                           price_ratio     1.383075
15                           return_time     1.366589
16                         duration_rank     1.285179
17                   return_arrival_hour     1.181340
18                 request_before_x_days     1.171977
19            min_departure_diff_seconds     0.987436
20                                amount     0.935478
21                             min_price     0.892458
22                         segments_diff     0.837027
23                      to_city_timezone     0.827251
24                           min_to_time     0.812920
25                            price_diff     0.811726
26                 min_total_flight_time     0.785922
27                            price_rank     0.677569
28                        departure_hour     0.648682
29                    from_city_timezone     0.648506
30                 return_departure_hour     0.644476
31                          arrival_hour     0.641401
32                      to_city_iatacode     0.412823
33                      is_international     0.407970
34                      class_is_economy     0.407833
35                          segmentcount     0.401597
36                 return_time_abs_ratio     0.380602
37                   isexchangepermitted     0.375547
38                                client     0.363743
39                             isbaggage     0.336905
40                           stay_x_days     0.324263
41                    departure_week_day     0.278269
42                    from_city_iatacode     0.272810
43                     class_is_business     0.191461
44                            isdiscount     0.154041
45             has_not_economy_in_policy     0.115410
46                       return_week_day     0.107559
47                         timezone_diff     0.107550
48                     isrefundpermitted     0.088610
49  has_intravelpolicy_variant_1_segment     0.073275
50                            round_trip     0.071636
51                        intravelpolicy     0.068771
52            has_intravelpolicy_variant     0.041869
53                    min_segments_count     0.037129
54                 client_travellergrade     0.026319
55             client_has_travellergrade     0.003533
56                      one_segment_trip     0.000000

--model 13
test_rank_score,best_rank_score,f_test_rank_score,f_best_rank_score
11137,3930,13922,5159

--model 15
test_rank_score,best_rank_score,f_test_rank_score,f_best_rank_score
12684,3930,12335,5159

sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test,all_test_size,all_test_requests
534,1317,1851,3641,41764,774

rank_score,sentoption_miss,positive_success_count,sentoption_count,positive_count
159361,5915,19093,25008,47743


f_sentoption_miss_test,f_positive_success_count_in_test,f_sentoption_count_in_test,f_positive_count_in_test,all_test_size,all_test_requests
548,1529,2077,3636,41764,774


--on predict 
-- class,pricission,recall,f1_score,support
-- True,0.365,0.717,0.484,3636
-- False,0.986,0.942,0.964,38128
-- accuracy,,,,41764

class,pricission,recall,f1_score,support
True,0.362,0.712,0.480,3641
False,0.986,0.942,0.963,38123
accuracy,,,,41764



--on fixed predict 
--class,pricission,recall,f1_score,support
--True,0.421,0.736,0.535,3636
--False,0.986,0.947,0.966,38128
--accuracy,,,,41764

class,pricission,recall,f1_score,support
True,0.419,0.735,0.534,3641
False,0.986,0.947,0.966,38123
accuracy,,,,41764


"""

"""
TODO
1) log amount
2) price per flight minute
3) variability dep time

"""