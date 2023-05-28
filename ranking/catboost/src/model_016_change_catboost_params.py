import logging
import re

import pandas as pd
from catboost import CatBoostClassifier, Pool, CatBoostRanker
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sqlalchemy import Boolean, Column, Float, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import insert

from ranking.catboost.src.lib import AbstractTrainFlow, PreparedResult


class CatboostTrainFlow16(AbstractTrainFlow):
    model_name = 'model_016_pair_logit_2000'

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
                    order by requestid
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
                    join {self.sampling_table_name} a on {table_prefix}_requests.id = a.id and for_test=False
                    order by requestid
                """
        else:
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f"""
                    SELECT * 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    order by requestid 
                    LIMIT {limit};
                """
            else:
                select_query = f"""
                    SELECT * 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    order by requestid;
                """

        train_data = pd.read_sql(select_query, self.db_engine)
        logging.info('Data select done')

        target = ['sentoption_fixed']
        exclude_but_keep = ['id', 'requestid', 'sentoption_flight', 'fligtoption']
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
        # X_train, X_test, y_train, y_test = train_test_split(
        #     prepared_data.features_frame,
        #     prepared_data.target_frame,
        #     test_size=0.1,
        #     random_state=41,
        # )
        # Prepare model
        model = CatBoostRanker(
            iterations=2000,
            loss_function='PairLogit',
            # loss_function='YetiRank',  # 'MultiClass',  # MultiLogloss ???
            verbose=True,
            cat_features=prepared_data.text_features,
        )
        pairs = prepared_data.make_pairs()
        logging.info(len(pairs))
        pool = Pool(
            prepared_data.features_frame,
            label=prepared_data.data['id'],
            pairs=pairs,
            cat_features=prepared_data.text_features,
            group_id=prepared_data.request_id_frame,
        )
        model.fit(
            pool,
            verbose=True,
            # eval_set=pool,
        )
        self.model = model
        # pred = model.predict(X_test)
        # bool_result = list(map(lambda rec: rec == 'True', pred))
        # print(classification_report(y_test, bool_result))
        # print(model.get_feature_importance(prettified=True))

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = CatBoostRanker()
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
            # predict_scores = self.model.predict(data.features_frame)
            logging.info('predicts calculated')

            Base = declarative_base()

            class PredictTable(Base):
                __tablename__ = table_name
                id = Column(Integer, primary_key=True)
                predict = Column(Boolean)
                score = Column(Float)

            id_with_predict_and_score = list(zip(ids['id'], predicts))
            chunk_size = 10000
            for chunk in range(0, len(id_with_predict_and_score) // chunk_size + 1):
                if chunk * chunk_size < len(id_with_predict_and_score):
                    session.execute(
                        insert(PredictTable),
                        [
                            {'id': id_value, 'predict': score > 0.5, 'score': score}
                            for id_value, score
                            in id_with_predict_and_score[chunk * chunk_size:(chunk + 1) * chunk_size]
                        ],
                    )
                logging.info('saved chunk %r', chunk)
            session.commit()
            logging.info('saved to db finished')

"""

1999:	learn: 0.0553557	total: 58m 24s	remaining: 0us

---700
test_rank_score,best_rank_score,f_test_rank_score,f_best_rank_score
11422,3930,11754,5159

on predict 
class,pricission,recall,f1_score,support
True,0.366,0.717,0.484,3631
False,0.986,0.942,0.964,38133
accuracy,,,,41764


on fixed predict 

class,pricission,recall,f1_score,support
True,0.420,0.734,0.534,3631
False,0.986,0.947,0.966,38133
accuracy,,,,41764


---1100

1099:	learn: 0.0733926	test: 0.0733926	best: 0.0733926 (1099)	total: 34m 32s	remaining: 0us

bestTest = 0.07339263282
bestIteration = 1099


test_rank_score,best_rank_score,f_test_rank_score,f_best_rank_score
11428,3930,11848,5159

f_sentoption_miss_test,f_positive_success_count_in_test,f_sentoption_count_in_test,f_positive_count_in_test,all_test_size,all_test_requests
539,1538,2077,3641,41764,774

sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test,all_test_size
512,1339,1851,3641,41764

rank_score,sentoption_miss,positive_success_count,sentoption_count,positive_count
129050,6079,18929,25008,47676



on predict 
class,pricission,recall,f1_score,support
True,0.368,0.723,0.488,3641
False,0.987,0.942,0.964,38123
accuracy,,,,41764

on fixed predict 

class,pricission,recall,f1_score,support
True,0.422,0.740,0.538,3641
False,0.986,0.947,0.966,38123
accuracy,,,,41764

---2000
test_rank_score,best_rank_score,f_test_rank_score,f_best_rank_score
11746,3930,11767,5159

f_sentoption_miss_test,f_positive_success_count_in_test,f_sentoption_count_in_test,f_positive_count_in_test,all_test_size,all_test_requests
539,1538,2077,3643,41764,774

sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test
512,1339,1851,3643

rank_score,sentoption_miss,positive_success_count,sentoption_count,positive_count
120947,5289,19719,25008,47690

on predict 
class,pricission,recall,f1_score,support
True,0.368,0.723,0.487,3643
False,0.987,0.942,0.964,38121
accuracy,,,,41764


on fixed predict 
class,pricission,recall,f1_score,support
True,0.422,0.740,0.538,3643
False,0.986,0.947,0.966,38121
accuracy,,,,41764


"""