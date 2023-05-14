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
    support_model_scores_table = 'support_model_001_on_006'

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
            'predict', # from support
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
test learn on 10K

bestTest = 0.09989477445
bestIteration = 542

Shrink model to first 543 iterations.
              precision    recall  f1-score   support

       False       0.97      0.99      0.98       948
        True       0.68      0.52      0.59        52

    accuracy                           0.96      1000
   macro avg       0.82      0.75      0.78      1000
weighted avg       0.96      0.96      0.96      1000

                              Feature Id  Importances
0                                  score    11.876938
1                                to_time     9.121647
2                          to_time_ratio     8.889178
3                 departure_diff_seconds     7.789079
4                            price_ratio     6.804475
5                             price_diff     6.793800
6             min_departure_diff_seconds     6.286823
7                          segments_diff     5.647826
8                                 amount     4.815674
9                              min_price     4.467697
10                           return_time     4.097154
11                           min_to_time     3.931234
12                     return_time_ratio     3.599701
13                            isdiscount     2.468509
14                          segmentcount     2.266180
15                       min_return_time     1.906539
16                             isbaggage     1.689859
17                     isrefundpermitted     1.430471
18                               predict     1.414348
19                   isexchangepermitted     1.268051
20             has_not_economy_in_policy     1.017299
21                        intravelpolicy     0.756953
22                    min_segments_count     0.424792
23                            round_trip     0.366173
24  has_intravelpolicy_variant_1_segment     0.326264
25                     class_is_business     0.260724
26                      class_is_economy     0.246368
27            has_intravelpolicy_variant     0.030040
28                 client_travellergrade     0.004041
29             client_has_travellergrade     0.002164

"""