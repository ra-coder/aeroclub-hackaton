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


class CatBoostWithSupportScoresTrainFlow9(AbstractTrainFlow):
    model_name = 'model_009_support_scores'
    support_model_scores_table = 'preprocess_scores_support_model_002_on_008'

    def prepare_features(
            self,
            limit: int | None = None,
            filter_for_test: bool = False,
            table_prefix: str = 'agent',
    ) -> PreparedResult:
        if table_prefix == 'client':
            raise NotImplementedError

        if filter_for_test:
            assert isinstance(self.sampling_table_name, str)
            assert re.match("^[a-z0-9_]*$", self.sampling_table_name)
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f""" --sql
                    SELECT 
                        {table_prefix}_requests.*, 
                        {table_prefix}_requests_features.*, 
                        {self.support_model_scores_table}.* 
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.sampling_table_name} a on {table_prefix}_requests.id = a.id and for_test=False
                    join {self.support_model_scores_table} 
                        on {self.support_model_scores_table}.id = {table_prefix}_requests.id 
                    LIMIT {limit};
                """
            else:
                select_query = f"""
                    SELECT 
                        {table_prefix}_requests.*, 
                        {table_prefix}_requests_features.*, 
                        {self.support_model_scores_table}.* 
                    FROM {table_prefix}_requests 
                    join {table_prefix}_requests_features 
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.support_model_scores_table} 
                        on {self.support_model_scores_table}.id = {table_prefix}_requests.id                         
                    join {self.sampling_table_name} a on {table_prefix}_requests.id = a.id and for_test=False;
                """
        else:
            if limit is not None:
                assert isinstance(limit, int)
                select_query = f"""
                    SELECT 
                        {table_prefix}_requests.*,
                        {table_prefix}_requests_features.*,
                        {self.support_model_scores_table}.*
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.support_model_scores_table}
                        on {self.support_model_scores_table}.id = {table_prefix}_requests.id
                    LIMIT {limit};
                """
            else:
                select_query = f"""
                    SELECT
                        {table_prefix}_requests.*,
                        {table_prefix}_requests_features.*,
                        {self.support_model_scores_table}.*
                    FROM {table_prefix}_requests
                    join {table_prefix}_requests_features
                        on {table_prefix}_requests.id = {table_prefix}_requests_features.id
                    join {self.support_model_scores_table}
                        on {self.support_model_scores_table}.id = {table_prefix}_requests.id                         
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
            'predict',  # from support
            'in_top5_rank',  # from support
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
        model = CatBoostClassifier(iterations=600, eval_metric='Logloss', verbose=True)
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
Shrink model to first 599 iterations.
              precision    recall  f1-score   support

       False       0.97      1.00      0.98     57244
        True       0.70      0.25      0.37      2320

    accuracy                           0.97     59564
   macro avg       0.84      0.62      0.68     59564
weighted avg       0.96      0.97      0.96     59564

                              Feature Id  Importances
0                                   rank    15.521418
1                                to_time    12.644467
2                          segments_diff     7.401560
3                          to_time_ratio     7.156018
4                            return_time     5.320486
5                           segmentcount     4.650180
6                            price_ratio     4.126935
7                              min_price     3.952032
8                            min_to_time     3.463198
9                             price_diff     3.199852
10                     return_time_ratio     2.923096
11            min_departure_diff_seconds     2.730494
12                    total_flight_ratio     2.703598
13                departure_diff_seconds     2.684790
14                                amount     2.676091
15                     total_flight_time     2.642257
16                   return_arrival_hour     2.056079
17                          arrival_hour     1.977272
18                       min_return_time     1.947030
19                                 score     1.647770
20                 min_total_flight_time     1.403420
21                        departure_hour     1.373393
22                 return_departure_hour     0.926536
23                            isdiscount     0.921867
24                     class_is_business     0.706626
25                   isexchangepermitted     0.507221
26                             isbaggage     0.440781
27                        intravelpolicy     0.405833
28                      class_is_economy     0.387366
29                    min_segments_count     0.387079
30             has_not_economy_in_policy     0.364769
31                     isrefundpermitted     0.198678
32                          in_top5_rank     0.148658
33            has_intravelpolicy_variant     0.130323
34                 client_travellergrade     0.115743
35  has_intravelpolicy_variant_1_segment     0.048051
36                               predict     0.039540
37                            round_trip     0.035764
38             client_has_travellergrade     0.033728
INFO:root:result table created

on top 6 cat

    sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test
    600,1251,1851,3642
    
    sentoption_miss,positive_success_count,sentoption_count,positive_count
    7275,17733,25008,47774

on top 10 cat 


    sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test
    326,1525,1851,6093,
    
    sentoption_miss,positive_success_count,sentoption_count,positive_count
    4004,21004,25008,80292

    class,pricission,recall,f1_score,support
    True,0.250,0.824,0.384,6093
    False,0.991,0.886,0.935,35671
    accuracy,,,,41764

"""