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


class CatBoostWithSupportScoresTrainFlow7(AbstractTrainFlow):
    model_name = 'model_007_support_scores'
    support_model_scores_table = 'preprocess_scores_support_model_001_on_006'

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
bestTest = 0.09303472816
bestIteration = 594

Shrink model to first 595 iterations.
              precision    recall  f1-score   support

       False       0.97      1.00      0.98     57238
        True       0.71      0.25      0.37      2326

    accuracy                           0.97     59564
   macro avg       0.84      0.62      0.68     59564
weighted avg       0.96      0.97      0.96     59564

                              Feature Id  Importances
0                                to_time    14.144554
1                                   rank    12.403809
2                            return_time     8.901793
3                          segments_diff     7.972075
4                          to_time_ratio     7.343754
5                 departure_diff_seconds     5.612099
6             min_departure_diff_seconds     4.999939
7                           segmentcount     4.893876
8                      return_time_ratio     4.671282
9                        min_return_time     4.662402
10                           min_to_time     4.390732
11                           price_ratio     2.914004
12                             min_price     2.854030
13                                amount     2.635246
14                                 score     2.429750
15                            price_diff     2.366749
16                            isdiscount     1.187053
17                          in_top5_rank     1.177810
18                    min_segments_count     1.128215
19                     class_is_business     0.800549
20                            round_trip     0.474467
21                      class_is_economy     0.450403
22             has_not_economy_in_policy     0.397448
23                             isbaggage     0.323378
24            has_intravelpolicy_variant     0.286844
25                     isrefundpermitted     0.243705
26                        intravelpolicy     0.074137
27             client_has_travellergrade     0.064287
28                   isexchangepermitted     0.060538
29                 client_travellergrade     0.060391
30  has_intravelpolicy_variant_1_segment     0.048399
31                               predict     0.026286
INFO:root:result table created


fix by top < 6
        sentoption_miss,positive_success_count,sentoption_count,positive_count
        7302,17706,25008,48206,
        
        sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test
        597,1254,1851,3656
        
        class,pricission,recall,f1_score,support
        True,0.343,0.677,0.455,3656
        False,0.984,0.940,0.962,38108

fix by top < 10

    sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test
    323,1528,1851,6125,
    
    sentoption_miss,positive_success_count,sentoption_count,positive_count
    4081,20927,25008,80656
    
    class,pricission,recall,f1_score,support
    True,0.249,0.825,0.383,6125
    False,0.991,0.885,0.935,35639

"""