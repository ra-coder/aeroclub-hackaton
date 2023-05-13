import logging

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split

from ranking.catboost.src.lib import AbstractTrainFlow, PreparedResult


class NaiveCatboostTrainFlow(AbstractTrainFlow):
    model_name = 'model_001_naive_catboost'

    def prepare_features(self, limit: int | None = None) -> PreparedResult:
        if limit is not None:
            assert isinstance(limit, int)
            select_query = f"SELECT * FROM agent_requests LIMIT {limit};"
        else:
            select_query = f"SELECT * FROM agent_requests;"

        train_data = pd.read_sql(select_query, self.db_engine)
        logging.info('Data select done')

        target = ['sentoption']
        category_features = []  # ['travellergrade', 'class']
        num_features = ['segmentcount', 'amount']
        bool_features = [
            'isbaggage',
            'isrefundpermitted',
            'isexchangepermitted',
            'isdiscount',
            'intravelpolicy',
        ]
        used = target + category_features + num_features + bool_features
        predictors = category_features + num_features + bool_features
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
        model = CatBoostClassifier(iterations=50, eval_metric='Accuracy', verbose=True)
        # Fit model
        model.fit(X_train, y_train, eval_set=(X_test, y_test))
        self.model = model

    def save_model(self):
        assert self.model is not None
        self.model.save_model(self.model_name)

    def load_model(self):
        from_file_model = CatBoostClassifier()
        from_file_model.load_model(self.model_name)
        self.model = from_file_model

    def apply_model_in_db(self):
        raise NotImplementedError
